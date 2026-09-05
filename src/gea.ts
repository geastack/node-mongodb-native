import { deserialize, ObjectId, serialize, type Document } from 'bson'
import { Buffer } from 'node:buffer'

declare function __gea_node_mongodb_exchange(host: string, port: number, request: Buffer): Buffer

export { ObjectId }
export type { Document }

export interface MongoClientOptions {
  connectTimeoutMS?: number
  serverSelectionTimeoutMS?: number
  socketTimeoutMS?: number
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
 * from the vendored official package and the bytes travel over node:net's
 * native socket. Commands are serialized one at a time, which matches the
 * application's awaited CRUD flow and avoids pretending this small native
 * surface implements the official driver's pool, auth, TLS, or retry layers.
 */
class WireConnection {
  private readonly host_: string
  private readonly port_: number
  private readonly timeoutMs_: number
  private connected_: boolean
  private requestId_: number

  constructor(host: string, port: number, timeoutMs: number) {
    this.host_ = host
    this.port_ = port
    this.timeoutMs_ = timeoutMs
    this.connected_ = false
    this.requestId_ = 1
  }

  connect(): Promise<void> {
    this.connected_ = true
    void this.timeoutMs_
    return Promise.resolve()
  }

  close(): void {
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

    const response = __gea_node_mongodb_exchange(this.host_, this.port_, message)
    if (response.length < 21 || response.readInt32LE(12) !== 2013 || response.readUInt8(20) !== 0) {
      throw new Error('MongoDB returned an unsupported wire message')
    }
    return Promise.resolve(deserialize(response.subarray(21)))
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

  async toArray(): Promise<Document[]> {
    const command: Document = { find: this.collection_.name, filter: this.filter_, singleBatch: true }
    if (this.sort_ !== null) command.sort = this.sort_
    const reply = checkedReply(await this.collection_.database.command(command))
    const batch = replyBatch(reply)
    return batch
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

  async findOne(filter: Document): Promise<Document | null> {
    const command: Document = { find: this.name, filter, limit: 1, singleBatch: true }
    const reply = checkedReply(await this.database.command(command))
    const batch = replyBatch(reply)
    return batch.length === 0 ? null : batch[0]
  }

  async insertOne(document: Document): Promise<InsertOneResult> {
    const command: Document = { insert: this.name, documents: [document], ordered: true }
    checkedReply(await this.database.command(command))
    return new InsertOneResult(true)
  }

  async updateOne(filter: Document, update: Document): Promise<UpdateResult> {
    const command: Document = { update: this.name, updates: [{ q: filter, u: update, multi: false, upsert: false }] }
    const reply = checkedReply(await this.database.command(command))
    return new UpdateResult(true, (reply.n as number | undefined) ?? 0, (reply.nModified as number | undefined) ?? 0)
  }

  async deleteOne(filter: Document): Promise<DeleteResult> {
    const command: Document = { delete: this.name, deletes: [{ q: filter, limit: 1 }] }
    const reply = checkedReply(await this.database.command(command))
    return new DeleteResult(true, (reply.n as number | undefined) ?? 0)
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
    this.wire_ = new WireConnection(host || '127.0.0.1', parsedPort || 27017, options.socketTimeoutMS ?? 0)
  }

  async connect(): Promise<this> {
    await this.wire_.connect()
    return this
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
