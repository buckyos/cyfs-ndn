# cyfs-ndn

[English](README.md) | [简体中文](README.zh-CN.md)

`cyfs-ndn` is the core implementation repository for the CYFS content network / Named Data Network. It implements the key concepts defined by `CYFS Protocol`, including named data, named objects, `Chunk`, `PathObject`, `FileObject`, and `ChunkList`, and provides object modeling, local storage, cross-zone retrieval, gateway access, filesystem mapping, and supporting tools.

In one sentence:

> This repository brings a content-addressed object network into a Rust workspace implementation.

## Repository Goals

Based on the protocol documents, this repository mainly addresses the following problems:

- How to represent immutable content as verifiable `Chunk` / `ObjectId`
- How to model structured content as `NamedObject`
- How to bind a semantic path to the current target object through `PathObject`
- How to represent large files and directories with `FileObject -> ChunkList -> Chunk`
- How to read, cache, upload, and distribute named data locally and across zones
- How to expose CYFS object capabilities as a local filesystem interface

## CYFS Protocol Positioning

As defined in [CYFS Protocol](doc/CYFS%20Protocol/CYFS%20Protocol.md), CYFS is a content-network protocol focused on the semantic layer rather than a monolithic protocol that tries to include every networking capability.

What it primarily defines:

- How content is named and verified
- How content objects are organized and referenced
- How semantic URLs are bound to the current object
- How large files are split into verifiable chunks
- How content propagates across zones with a pull-first model
- How protected content is accessed with purchase proofs / receipts

What it intentionally does not define or tightly bind:

- Identity system: it consumes W3C DID / BNS DID extensions instead of redefining identity in the protocol layer
- Payment protocol: it only requires a verifiable purchase proof, not a single settlement system
- Transport connection setup: delegated to the `cyfs-gateway` tunnel framework
- DHT: not used as a protocol primitive
- Piece-level P2P exchange: the protocol defines chunk semantics, not a finer-grained exchange protocol
- Traditional tracker: replaced by action chains, curators, and consumption proofs

This is also the implementation boundary of this repository: it focuses on content objects and the named-data network itself, rather than combining DID, payments, tunnels, and on-chain state into one codebase.

## Core Object Model

At the README level, the following concepts are the most important to understand first.

### 1. Chunk

A `Chunk` is the smallest immutable data unit. Its `ChunkId` is usually derived from the content hash, for example:

```text
sha256:<hash>
```

The protocol also supports a base32 representation that is more suitable for URLs / hostnames, but both forms refer to the same object identity.

### 2. ObjectId / NamedObject

CYFS extends content addressing from raw bytes to structured objects. Any object that is stably encoded and hashed can have an `ObjectId`, and therefore become a `NamedObject`.

That means:

- File content can be named
- JSON structures, directories, and path bindings can be named
- References between objects can form a content network

### 3. PathObject

`PathObject` solves the problem of "which object a semantic path currently points to". The protocol allows an HTTP response to return a signed path-binding object so that clients can verify, without fully relying on TLS:

- Which `ObjectId` a path currently points to
- Whether the binding was signed by a trusted publisher
- Whether the binding is still valid

This allows CYFS to support both direct `ObjectId` access and semantic URLs closer to the traditional web.

### 4. FileObject / ChunkList

Large files are not represented as a single chunk. Instead, they use:

```text
FileObject -> ChunkList -> Chunk
```

Where:

- `FileObject` describes the file itself and its metadata
- `ChunkList` describes which chunks make up the file
- Each `Chunk` can be verified, cached, and distributed independently

This is the basis for multi-source download, partial reads, transparent acceleration, and cross-node caching.

### 5. SameAs / inner_path

- `SameAs` expresses equivalence, references, or aliases between objects
- `inner_path` is used to continue locating fields or sub-objects inside a container object

Together, they extend object retrieval beyond fetching a single blob and enable trusted navigation into object internals.

## Distribution Model

The CYFS distribution model has several important implementation-oriented properties:

- Pull-first: cross-zone distribution ultimately becomes client-side pulling
- Source discovery and content verification are decoupled: source hints can be loose, content verification must be strict
- Multi-source: chunks can be fetched concurrently from multiple sources
- Transparent acceleration: caches, edge nodes, and HTTP extensions can all participate
- Proof-driven distribution: download proofs, consumption proofs, and action chains serve both incentives and trusted propagation

As a result, `cyfs-ndn` is better understood as infrastructure for a verifiable content network rather than a BitTorrent-style tracker + piece exchange implementation.

## Repository Layout

This repository is a Rust workspace defined in [src/Cargo.toml](src/Cargo.toml).

The main crates are:

- `src/ndn-lib`: the core object model and NDN base library, including `Chunk`, `Object`, `FileObject`, `DirObject`, and HTTP extensions
- `src/named_store`: local named-data storage, HTTP backends, gateways, GC, and store manager logic
- `src/cyfs-lib`: CYFS filesystem-related types and interface abstractions
- `src/cyfs`: higher-level CYFS capabilities, exporting interfaces such as `NamedFileMgr`
- `src/fs_meta`: filesystem metadata service
- `src/fs_buffer`: file buffer and mmap/local-cache related implementation
- `src/fs_daemon`: FUSE daemon that mounts CYFS capabilities into the local filesystem
- `src/ndn-toolkit`: tests, client utilities, and helper tools
- `src/package-lib`: package, publishing, and tooling-related extensions

## Quick Start

### Build the workspace

```bash
cd src
cargo build --workspace
```

### Run tests

```bash
cd src
cargo test --workspace
```

### Start the filesystem daemon

`fs_daemon` can expose CYFS capabilities as a local filesystem:

```bash
cd src
cargo run -p fs_daemon -- <mountpoint> [--store-config <path>] [--service-config <path>]
```

For example:

```bash
cd src
cargo run -p fs_daemon -- /mnt/cyfs \
  --store-config /opt/buckyos/etc/store_layout.json \
  --service-config /opt/buckyos/etc/fs_daemon.json
```

For more FUSE-related details, see [src/fs_daemon/readme.md](src/fs_daemon/readme.md).

## Suggested Reading Order

If this is your first time reading the repository, the recommended order is:

1. [README.md](README.md)
2. [doc/CYFS Protocol/CYFS Protocol.md](doc/CYFS%20Protocol/CYFS%20Protocol.md)
3. [src/ndn-lib/readme.md](src/ndn-lib/readme.md)
4. [src/fs_daemon/readme.md](src/fs_daemon/readme.md)
5. Source code under `src/named_store`, `src/cyfs`, and `src/fs_meta`

## Related Documents

- Protocol overview: [doc/CYFS Protocol/CYFS Protocol.md](doc/CYFS%20Protocol/CYFS%20Protocol.md)
- Content Network notes: [doc/CYFS Protocol/Content Network.md](doc/CYFS%20Protocol/Content%20Network.md)
- Standard objects reference: [doc/CYFS Protocol/CYFS 标准对象.md](doc/CYFS%20Protocol/CYFS%20%E6%A0%87%E5%87%86%E5%AF%B9%E8%B1%A1.md)
- NDM protocol overview: [doc/NDM Protocol/overview.md](doc/NDM%20Protocol/overview.md)
- Named FS v2: [doc/named_fs_v2.md](doc/named_fs_v2.md)
- Daemon and mount implementation: [src/fs_daemon/readme.md](src/fs_daemon/readme.md)

## Scope of This README

This README is only an entry point for the repository. It does not try to replace the protocol document itself. For lower-level protocol details such as:

- `PathObject JWT` field constraints
- `Canonical JSON` / `ObjectId` calculation rules
- URL rules for `inner_path`
- `get_object_by_url` / `open_reader_by_url` flows
- Purchase receipt verification

Please refer directly to the corresponding sections in the protocol documentation.
