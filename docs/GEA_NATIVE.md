# Gea native entry

`src/gea.ts` is the MongoDB package entry used when node-compat compiles a Gea
program from source. It provides a small typed API over real BSON and MongoDB
OP_MSG traffic. The standard package entry and the official Node.js driver
remain unchanged for normal Node execution.

## Selection

The package manifest does not redirect Node to this file. The node-compat
source builder explicitly maps the root `mongodb` specifier to `src/gea.ts`
when `mongodb` is included in `--from-source`. Application code keeps the
normal import:

```ts
import { MongoClient, ObjectId } from 'mongodb'
```

This design makes execution mode explicit in the build and prevents the
reduced native surface from masquerading as the complete official driver.

## Runtime contract

The entry expects the Gea host to implement these typed intrinsics:

```ts
declare function __gea_node_mongodb_pool_open(
  host: string,
  port: number,
  maxSize: number
): number

declare function __gea_node_mongodb_pool_exchange(
  poolId: number,
  request: Buffer
): Buffer

declare function __gea_node_mongodb_pool_close(poolId: number): void
```

The pool identifier is opaque to TypeScript. The host owns socket creation,
leasing, request/response exchange, and destruction. `pool_exchange` must send
one complete request, return exactly one complete response, and keep the
leased connection alive for later commands.

The node-compat implementation opens one connection eagerly, enables TCP keep
alive and `TCP_NODELAY`, leases idle sockets in last-returned-first order, and
clamps pool capacity to 1–64. The `MongoClient` default is four.

## Wire protocol

`WireConnection.command`:

1. Adds `$db` to the command document.
2. Serializes it with this package's BSON dependency.
3. Builds an OP_MSG header with opcode 2013 and a body-section kind of zero.
4. Sends the bytes through the pool exchange intrinsic.
5. Requires an OP_MSG response with the same supported section kind.
6. Deserializes and returns the response document.

Request IDs increase for the life of a `WireConnection`. Command failures are
reported from `errmsg` when MongoDB returns `ok` other than 1.

## Supported API

| Type | Members |
|---|---|
| `MongoClient` | constructor, `connect`, `db`, `close` |
| `Db` | `collection`, `command` |
| `Collection<T>` | `find`, `findOne`, `insertOne`, `updateOne`, `deleteOne` |
| `FindCursor<T>` | `sort`, `toArray` for `firstBatch` |
| Results | acknowledged insert, matched/modified update, deleted count |
| Re-exports | `ObjectId`, `Document` |

The result classes intentionally expose only the fields produced by this
surface. `find().toArray()` reads `firstBatch`; it does not issue `getMore`.

## URI and options

The current parser supports one direct hostname or IPv4 address with an
optional port. It defaults to `127.0.0.1:27017`. Database paths and query text
are ignored by the native entry.

`maxPoolSize` controls the native pool. `connectTimeoutMS`,
`serverSelectionTimeoutMS`, and `socketTimeoutMS` are accepted by the TypeScript
interface but are not currently enforced by the direct transport.

## Unsupported features

The native entry does not implement authentication, TLS, SRV records,
replica-set or sharded topology selection, SDAM/CMAP monitoring, retryable
operations, sessions, transactions, bulk writes, change streams, GridFS,
encryption, compression, or multi-batch cursors.

It also does not reconnect after a broken socket or provide an asynchronous
wait queue when every pool connection is leased. The current node-compat host
performs a synchronous request/response exchange, so the documented application
path returns a socket before another command can lease it.

## Correctness and performance evidence

The node-compat integration contains three separate proof layers:

- native ping and deterministic CRUD executables;
- a full-stack Hono todo lifecycle over HTTP;
- a seven-round benchmark against the official Node.js, Rust, and C++ drivers.

Those artifacts live in
`node-compat/apps/hono-mongodb-todo`. The benchmark measures warmed sequential
CRUD through one pooled connection. It is not a feature-completeness claim for
this entry.

## Versioning

Gea fork tags use the upstream package version plus a Gea revision suffix. The
first pooled transport revision is `gea-native-v7.1.1-2`.
