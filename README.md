# @cocreate/server-mesh

A peer-to-peer WebSocket transport for server control, organization-scoped messaging, and inter-node event synchronization across the CoCreate ecosystem.

---

## Documentation

For complete API references, deployment guides, and architecture documentation, visit the CoCreate Server Mesh documentation.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture & Message Pipeline](#architecture--message-pipeline)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Routing Directives & Targeting Matrix](#routing-directives--targeting-matrix)
- [Tenant-Aware Routing Protocols](#tenant-aware-routing-protocols)
- [Connection Lifecycle](#connection-lifecycle)
- [Programmatic API Reference](#programmatic-api-reference)
- [Security & TLS Specification](#security--tls-specification)
- [Environment Variables](#environment-variables)
- [Announcements](#announcements)
- [Roadmap](#roadmap)
- [How to Contribute](#how-to-contribute)
- [About](#about)
- [License](#license)

---

## Overview

`@cocreate/server-mesh` is the inter-server transport plane for `@cocreate/cluster`. It establishes TLS WebSocket connections between server nodes for control messages and organization-scoped data propagation.

IPC handles local worker routing. Server mesh handles only connections and messages between servers.

---

## Key Features

- **Ephemeral TLS:** Automatically generates temporary TLS credentials for encrypted node-to-node communication.
- **Polymorphic Target Directives:** Unicast, multicast, broadcast, worker-specific, or tenant-aware routing using a unified payload format.
- **Message Identity:** Preserves an existing `uuid` and creates one for payloads sent through `ServerMesh.send()` when absent.
- **Tenant Isolation:** Tracks tenant-to-server membership so messages are only delivered to nodes actively serving an organization. Local worker membership is owned by `@cocreate/cluster` IPC.
- **Connection Recovery:** Unexpected network disconnects retry automatically; intentional organization shutdowns do not reconnect.
- **Cluster IPC Integration:** Transparently bridges Node.js Primary and Worker processes while extending communication across distributed servers.
- **Control and Organization Sockets:** Uses pathless sockets for server control and `/<organization_id>` sockets for organization traffic.

---

## Architecture & Message Pipeline

When a payload is dispatched through `@cocreate/server-mesh`, the routing engine evaluates execution context (Primary vs Worker), determines the target destination, and forwards the payload through IPC or encrypted peer connections.

$$
\text{Worker Thread}
\xrightarrow{\text{IPC}}
\text{Primary Process}
\xrightarrow{\text{Route Resolution}}
\begin{cases}
\text{Local Workers} \\
\text{Remote Peer Servers}
\end{cases}
$$

### Worker Context

Worker processes:

- Send outbound messages through native `process.send()`
- Receive inbound mesh traffic through IPC
- Never establish direct peer connections

### Primary Context

The Primary process:

- Hosts the secure mesh WebSocket server
- Loads peer servers from the database and creates pathless control connections
- Tracks active organizations and their organization-specific sockets
- Routes messages between workers and remote servers

---

## Installation

### NPM

```bash
npm install @cocreate/server-mesh
```

### Yarn

```bash
yarn add @cocreate/server-mesh
```

> **Note**
>
> `@cocreate/server-mesh` integrates directly with `@cocreate/cluster`, but it can also be initialized independently.

---

## Quick Start

### Initialize

```javascript
import CoCreateServer from "@cocreate/cluster";
import ServerMesh from "@cocreate/server-mesh";

await ServerMesh.init(CoCreateServer);
```

### Send a Message

Application code normally sends the unified envelope through `server.send()` so
IPC and mesh routing use the same rules:

Unicast to a single server:

```javascript
CoCreateServer.send({
	method: "cache.flush",
	server: "srv_64f8a1bc90d1",
	data: {
		cache: "users"
	}
});
```

Multicast to multiple servers:

```javascript
CoCreateServer.send({
	method: "worker.task",
	server: ["srv_64f8a1bc90d1", "srv_90e3b2ac81f4"],
	worker: 2,
	data: {
		taskId: 1042
	}
});
```

Broadcast to every node:

```javascript
CoCreateServer.send({
	broadcast: true,
	method: "reload.configuration"
});
```

### Listen for Messages

```javascript
ServerMesh.on("cache.flush", (message) => {
	console.log(message.data);
});
```

---

## Routing Directives & Targeting Matrix

| Target | Payload | Description |
| --- | --- | --- |
| Single Server | `{ server: "srv1" }` | Routes over the specified server's control or organization socket. |
| Multiple Servers | `{ server: ["srv1","srv2"] }` | Sends to every listed server. |
| Organization | `{ organization_id: "org1" }` | Uses only sockets in that organization's array. |
| Worker | `{ server: "srv1", worker: 2 }` | IPC targets a worker on the selected server. |
| Local Workers | `{ worker: [1,3] }` | IPC sends only to the selected local workers. |
| Broadcast | `{ broadcast: true }` | Delivers to every connected server and local worker. |

---

## Tenant-Aware Routing Protocols

IPC maintains each server's organization-to-worker map. Mesh creates a separate
peer socket for each active organization, using `/<organization_id>` as the path.

```text
Client Connect
        │
        ▼
registerOrg
        │
        ▼
IPC Maps Organization → Workers
        │
        ▼
Client Disconnect
        │
        ▼
60 Second Grace Timer
        │
        ▼
deregisterOrg
        │
        ▼
Close Organization Mesh Sockets
```

### Organization Events

| Event | Description |
| --- | --- |
| `registerOrg` | Registers an organization as active on a worker. |
| `deregisterOrg` | Removes an organization's worker mapping after all connections close. |

The first local worker creates an empty entry in `mesh.organizations` and sends
`orgRegistered` over every pathless control socket. A peer responds only when
the same organization exists in its own `mesh.organizations` map. The server
with the higher `serverId` then creates one organization socket to the lower-ID
server, preventing duplicate connections.

When the final local worker deregisters, mesh closes that organization's
connections and deletes its map entry. The remote server removes each socket
through its normal `close` event; no organization-deletion broadcast is needed.

Organization payloads retain their original envelope. IPC selects local workers;
the mesh selects remote servers from its organization membership map.

---

## Connection Lifecycle

Pathless `wss://ip` sockets form the server control and health channel and are
stored separately. The platform becomes an active organization through the same
`registerOrg` flow as every other organization and receives a distinct
`wss://ip/<platform_organization_id>` socket once both peers report active clients.
When a socket closes unexpectedly because of a network failure, it is removed
from the array and the outbound owner schedules reconnection. When mesh closes a
socket intentionally, it marks the socket before calling `close()`, cancels any
pending retry, and does not reconnect. The organization remains registered with
an empty array until IPC sends `deregisterOrg` after its final local worker
disconnects.

---

## Programmatic API Reference

### `ServerMesh.init(server)`

Initializes the mesh networking layer.

```javascript
await ServerMesh.init(server);
```

---

### `ServerMesh.send(payload)`

Routes an inter-server payload. Without `organization_id`, it uses pathless
control sockets. With `organization_id`, it uses only that organization's
socket array.

```javascript
ServerMesh.send({
	method: "custom.command",
	server: "srv123",
	data: {
		key: "value"
	}
});
```

---

### `ServerMesh.close()`

Gracefully closes all mesh connections and releases resources.

```javascript
await ServerMesh.close();
```

---

### Events

```javascript
ServerMesh.on("mesh.raw_message", handler);
```

---

## Security & TLS Specification

- **Encrypted Transport:** Peer communication uses TLS WebSocket connections.
- **Dedicated Mesh Port:** Mesh traffic is isolated from public HTTP and HTTPS traffic.
- **Ephemeral Certificates:** Temporary TLS certificates are generated during startup and automatically replaced on restart.
- **Peer Metadata:** Control connections include the server ID, IP, host, and configured cluster secret in their WebSocket protocol metadata.
- **UUID Propagation:** Application payloads retain the same UUID across IPC and mesh routing.

---

## Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `MESH_PORT` | `8090` | Port used for mesh communication. |
| `CLUSTER_SECRET` | — | Shared secret included in peer connection metadata. |
| `SERVER_ID` | Auto Generated | Persistent identifier for the current server node. |

---

## Announcements

Release notes, performance improvements, protocol updates, and networking enhancements are published with every release. Follow the repository to stay informed about new features and compatibility changes.

---

## Roadmap

Upcoming enhancements include:

- Native QUIC / HTTP/3 transport
- Adaptive routing based on latency measurements
- Automatic peer discovery across local networks
- Streaming binary frame optimizations
- Dynamic topology-aware routing

---

## How to Contribute

We welcome pull requests, bug reports, documentation improvements, performance optimizations, and security reviews.

Please review the project's `CONTRIBUTING.md` before submitting changes, and use the GitHub Issues page to report bugs or request new features.

Community feedback helps prioritize development and improve the platform.

---

## About

`@cocreate/server-mesh` is designed, built, and maintained by the CoCreate Developer Experience Team.

It serves as the distributed communication backbone of the CoCreate ecosystem, providing secure, low-latency messaging between clustered servers.

For questions, architecture discussions, or deployment guidance, visit the project's GitHub Discussions or join the CoCreate community.

---

## License

This software is dual-licensed under the GNU Affero General Public License version 3 (AGPLv3) and a commercial license.

### Open Source

For open-source projects and non-commercial use, this software is available under the AGPLv3. See the `LICENSE` file for the complete license text.

### Commercial

Organizations requiring proprietary use may obtain a commercial license from CoCreate.
