import { deserialize, ObjectId, serialize, type Document } from 'bson'
import { Buffer } from 'node:buffer'
import { ReactorSocket } from 'node:net'

export { ObjectId }
export type { Document }

export interface MongoClientOptions {
  connectTimeoutMS?: number
  serverSelectionTimeoutMS?: number
  socketTimeoutMS?: number
  maxPoolSize?: number
}

export class InsertOneResult {
  constructor(readonly acknowledged: boolean) {}
}

export class UpdateResult {
  constructor(
    readonly acknowledged: boolean,
    readonly matchedCount: number,
    readonly modifiedCount: number
  ) {}
}

export class DeleteResult {
  constructor(readonly acknowledged: boolean, readonly deletedCount: number) {}
}

/**
 * The MongoDB OP_MSG transport needed by the native application.
 *
 * This deliberately stays at the wire protocol boundary: BSON is compiled
 * from the vendored official package and the bytes travel over pooled native
 * sockets. Each command leases one reactor socket, leaves the Promise pending
 * while bytes are in flight, then returns that socket to the client-owned pool.
 */
class WireSocket {
  private readonly host_: string
  private readonly port_: number
  private readonly socket_: ReactorSocket
  private buffered_: Buffer
  private expectedLength_: number
  private responseResolve_: ((value: Buffer) => void) | null
  private responseReject_: ((reason: Error) => void) | null

  constructor(host: string, port: number, timeoutMs: number) {
    this.host_ = host
    this.port_ = port
    this.socket_ = new ReactorSocket()
    this.buffered_ = Buffer.alloc(0)
    this.expectedLength_ = 0
    this.responseResolve_ = null
    this.responseReject_ = null
    void timeoutMs
  }

  connect(): Promise<void> {
    return new Promise<void>((resolve, reject): void => {
      this.socket_.connect(this.port_, this.host_, {
        connect: (): void => resolve(),
        data: (chunk: Buffer): void => this.receive(chunk),
        error: (error: Error): void => {
          this.fail(error)
          reject(error)
        },
        close: (): void => this.fail(new Error('MongoDB socket closed'))
      })
    })
  }

  close(): void {
    this.socket_.close()
  }

  exchange(request: Buffer): Promise<Buffer> {
    if (this.responseResolve_ !== null) {
      return Promise.reject(new Error('MongoDB socket already has a request in flight'))
    }
    return new Promise<Buffer>((resolve, reject): void => {
      this.responseResolve_ = resolve
      this.responseReject_ = reject
      this.socket_.write(request)
    })
  }

  private receive(chunk: Buffer): void {
    this.buffered_ = this.buffered_.length === 0 ? chunk : Buffer.concat([this.buffered_, chunk])
    if (this.expectedLength_ === 0 && this.buffered_.length >= 4) {
      this.expectedLength_ = this.buffered_.readInt32LE(0)
      if (this.expectedLength_ < 21) {
        this.fail(new Error('MongoDB returned an invalid wire message length'))
        return
      }
    }
    if (this.expectedLength_ === 0 || this.buffered_.length < this.expectedLength_) return
    const response = this.buffered_.subarray(0, this.expectedLength_)
    this.buffered_ = this.buffered_.subarray(this.expectedLength_)
    this.expectedLength_ = 0
    const resolve = this.responseResolve_
    this.responseResolve_ = null
    this.responseReject_ = null
    if (resolve !== null) resolve(response)
  }

  private fail(error: Error): void {
    const reject = this.responseReject_
    this.responseResolve_ = null
    this.responseReject_ = null
    if (reject !== null) reject(error)
  }
}

class WireConnection {
  private readonly host_: string
  private readonly port_: number
  private readonly timeoutMs_: number
  private readonly maxPoolSize_: number
  private connected_: boolean
  private requestId_: number
  private readonly sockets_: WireSocket[]
  private readonly idle_: WireSocket[]
  private readonly waiters_: ((socket: WireSocket) => void)[]
  private opening_: number

  constructor(host: string, port: number, timeoutMs: number, maxPoolSize: number) {
    this.host_ = host
    this.port_ = port
    this.timeoutMs_ = timeoutMs
    this.maxPoolSize_ = maxPoolSize
    this.connected_ = false
    this.requestId_ = 1
    this.sockets_ = []
    this.idle_ = []
    this.waiters_ = []
    this.opening_ = 0
  }

  connect(): Promise<void> {
    if (this.connected_) return Promise.resolve()
    return this.openSocket().then((socket: WireSocket): void => {
      this.idle_.push(socket)
      this.connected_ = true
    })
  }

  close(): void {
    if (!this.connected_) return
    for (let index = 0; index < this.sockets_.length; index += 1) this.sockets_[index].close()
    this.sockets_.splice(0, this.sockets_.length)
    this.idle_.splice(0, this.idle_.length)
    this.waiters_.splice(0, this.waiters_.length)
    this.connected_ = false
  }

  command(database: string, command: Document): Promise<Document> {
    if (!this.connected_) throw new Error('MongoDB client is not connected')

    command['$db'] = database
    const bson = serialize(command)
    const message = Buffer.allocUnsafe(21 + bson.byteLength)
    message.writeInt32LE(message.length, 0)
    message.writeInt32LE(this.requestId_, 4)
    message.writeInt32LE(0, 8)
    message.writeInt32LE(2013, 12)
    message.writeInt32LE(0, 16)
    message.writeUInt8(0, 20)
    message.set(bson, 21)
    this.requestId_ += 1

    return this.lease().then((socket: WireSocket): Promise<Document> => {
      return socket
        .exchange(message)
        .catch((reason: unknown): Buffer => {
          this.release(socket)
          throw reason
        })
        .then((response: Buffer): Document => {
          this.release(socket)
          if (response.length < 21 || response.readInt32LE(12) !== 2013 || response.readUInt8(20) !== 0) {
            throw new Error('MongoDB returned an unsupported wire message')
          }
          return deserialize(response.subarray(21))
        })
    })
  }

  private openSocket(): Promise<WireSocket> {
    this.opening_ += 1
    const socket = new WireSocket(this.host_, this.port_, this.timeoutMs_)
    return socket
      .connect()
      .then((): WireSocket => {
        this.opening_ -= 1
        this.sockets_.push(socket)
        return socket
      })
      .catch((reason: unknown): WireSocket => {
        this.opening_ -= 1
        throw reason
      })
  }

  private lease(): Promise<WireSocket> {
    const available = this.idle_.shift()
    if (available !== undefined) return Promise.resolve(available)
    if (this.sockets_.length + this.opening_ < this.maxPoolSize_) return this.openSocket()
    return new Promise<WireSocket>((resolve): void => {
      this.waiters_.push(resolve)
    })
  }

  private release(socket: WireSocket): void {
    const waiter = this.waiters_.shift()
    if (waiter === undefined) this.idle_.push(socket)
    else waiter(socket)
  }
}

function checkedReply(reply: Document): Document {
  if ((reply.ok as number) !== 1) {
    const message = reply.errmsg
    throw new Error(typeof message === 'string' ? message : 'MongoDB command failed')
  }
  return reply
}

function replyBatch(reply: Document): Document[] {
  const cursor = reply.cursor as Document | undefined
  if (cursor === undefined) return []
  return (cursor.firstBatch as Document[] | undefined) ?? []
}

export class FindCursor<T extends { _id: ObjectId }> {
  private readonly collection_: Collection<T>
  private readonly filter_: Document
  private sort_: Document | null

  constructor(collection: Collection<T>, filter: Document) {
    this.collection_ = collection
    this.filter_ = filter
    this.sort_ = null
  }

  sort(specification: Document): this {
    this.sort_ = specification
    return this
  }

  toArray(): Promise<Document[]> {
    const command: Document = { find: this.collection_.name, filter: this.filter_, singleBatch: true }
    if (this.sort_ !== null) command.sort = this.sort_
    return this.collection_.database.command(command).then((result: Document): Document[] => {
      return replyBatch(checkedReply(result))
    })
  }
}

export class Collection<T extends { _id: ObjectId }> {
  readonly database: Db
  readonly name: string

  constructor(database: Db, name: string) {
    this.database = database
    this.name = name
  }

  find(filter: Document): FindCursor<T> {
    return new FindCursor<T>(this, filter)
  }

  findOne(filter: Document): Promise<Document | null> {
    const command: Document = { find: this.name, filter, limit: 1, singleBatch: true }
    return this.database.command(command).then((result: Document): Document | null => {
      const batch = replyBatch(checkedReply(result))
      return batch.length === 0 ? null : batch[0]
    })
  }

  insertOne(document: Document): Promise<InsertOneResult> {
    const command: Document = { insert: this.name, documents: [document], ordered: true }
    return this.database.command(command).then((result: Document): InsertOneResult => {
      checkedReply(result)
      return new InsertOneResult(true)
    })
  }

  updateOne(filter: Document, update: Document): Promise<UpdateResult> {
    const command: Document = { update: this.name, updates: [{ q: filter, u: update, multi: false, upsert: false }] }
    return this.database.command(command).then((result: Document): UpdateResult => {
      const reply = checkedReply(result)
      return new UpdateResult(true, (reply.n as number | undefined) ?? 0, (reply.nModified as number | undefined) ?? 0)
    })
  }

  deleteOne(filter: Document): Promise<DeleteResult> {
    const command: Document = { delete: this.name, deletes: [{ q: filter, limit: 1 }] }
    return this.database.command(command).then((result: Document): DeleteResult => {
      const reply = checkedReply(result)
      return new DeleteResult(true, (reply.n as number | undefined) ?? 0)
    })
  }
}

export class Db {
  private readonly client_: MongoClient
  readonly name: string

  constructor(client: MongoClient, name: string) {
    this.client_ = client
    this.name = name
  }

  collection<T extends { _id: ObjectId }>(name: string): Collection<T> {
    return new Collection<T>(this, name)
  }

  command(command: Document): Promise<Document> {
    return this.client_.command(this.name, command)
  }
}

export class MongoClient {
  private readonly wire_: WireConnection

  constructor(uri: string, options: MongoClientOptions = {}) {
    const withoutScheme = uri.startsWith('mongodb://') ? uri.slice('mongodb://'.length) : uri
    const authority = withoutScheme.split('/')[0] ?? withoutScheme
    const separator = authority.lastIndexOf(':')
    const host = separator < 0 ? authority : authority.slice(0, separator)
    const parsedPort = separator < 0 ? 27017 : Number(authority.slice(separator + 1))
    this.wire_ = new WireConnection(
      host || '127.0.0.1',
      parsedPort || 27017,
      options.socketTimeoutMS ?? 0,
      Math.max(1, Math.min(64, options.maxPoolSize ?? 4))
    )
  }

  connect(): Promise<this> {
    return this.wire_.connect().then((): this => this)
  }

  db(name: string): Db {
    return new Db(this, name)
  }

  close(): Promise<void> {
    this.wire_.close()
    return Promise.resolve()
  }

  command(database: string, command: Document): Promise<Document> {
    return this.wire_.command(database, command)
  }
}
