# @cocreate/server-mesh

A high-performance, self-healing, peer-to-peer (P2P) encrypted transport fabric built for distributed cluster orchestration, real-time tenant routing, and inter-node event synchronization across the CoCreate ecosystem.

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
- [Self-Healing & Reconnection Logic](#self-healing--reconnection-logic)
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

`@cocreate/server-mesh` serves as the real-time inter-server transport plane for `@cocreate/server`. It establishes zero-trust, mutual P2P encrypted WebSocket tunnels between cluster nodes, enabling seamless multi-tenant data propagation, worker thread relays, and distributed cluster command execution.

Designed as a pure transport fabric, `@cocreate/server-mesh` blindly routes and emits network payloads as standard `EventEmitter` events across the cluster without introducing business-logic overhead.

---

## Key Features

- **Zero-Trust Ephemeral TLS:** Automatically generates temporary TLS credentials for secure node-to-node communication over the mesh network.
- **Polymorphic Target Directives:** Unicast, multicast, broadcast, worker-specific, or tenant-aware routing using a unified payload format.
- **Cryptographic Traceability:** Automatically stamps every outgoing frame with a unique `crypto.randomUUID()` identifier for distributed tracing and replay protection.
- **Tenant Isolation:** Dynamically tracks tenant-to-worker and tenant-to-server mappings so messages are only delivered to nodes actively serving an organization.
- **Self-Healing Reconnection:** Uses exponential backoff with randomized jitter to prevent reconnection storms after network interruptions.
- **Cluster IPC Integration:** Transparently bridges Node.js Primary and Worker processes while extending communication across distributed servers.
- **EventEmitter API:** Receive mesh messages using familiar Node.js event listeners.

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
- Maintains encrypted peer connections
- Tracks tenant routing information
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
> `@cocreate/server-mesh` integrates directly with `@cocreate/server`, but it can also be initialized independently.

---

## Quick Start

### Initialize

```javascript
import CoCreateServer from "@cocreate/server";
import ServerMesh from "@cocreate/server-mesh";

await ServerMesh.init(CoCreateServer);
```

### Send a Message

Unicast to a single server:

```javascript
ServerMesh.send({
	method: "cache.flush",
	server: "srv_64f8a1bc90d1",
	data: {
		cache: "users"
	}
});
```

Multicast to multiple servers:

```javascript
ServerMesh.send({
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
ServerMesh.send({
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
| Single Server | `{ server: "srv1" }` | Routes only to the specified server. |
| Multiple Servers | `{ server: ["srv1","srv2"] }` | Sends to every listed server. |
| Worker | `{ server: "srv1", worker: 2 }` | Routes to a specific worker on the target server. |
| Local Workers | `{ worker: [1,3] }` | Sends only to selected local workers. |
| Broadcast | `{ broadcast: true }` | Delivers to every connected server and local worker. |

---

## Tenant-Aware Routing Protocols

The mesh minimizes unnecessary network traffic by maintaining live organization routing maps.

```text
Client Connect
        │
        ▼
mesh.registerOrg
        │
        ▼
Map Organization → Worker
        │
        ▼
Client Disconnect
        │
        ▼
60 Second Grace Timer
        │
        ▼
mesh.deregisterOrg
        │
        ▼
mesh.orgDeleted Broadcast
```

### Organization Events

| Event | Description |
| --- | --- |
| `mesh.registerOrg` | Registers a worker serving an organization. |
| `mesh.deregisterOrg` | Removes a worker's organization mapping after all connections close. |
| `mesh.orgAdded` | Broadcast to peer servers when an organization becomes active. |
| `mesh.orgDeleted` | Broadcast when an organization is no longer active on a server. |
| `mesh.orgData` | Routes organization-specific payloads only to subscribed servers and workers. |

---

## Self-Healing & Reconnection Logic

When peer servers disconnect, reconnection attempts use exponential backoff combined with randomized jitter to prevent synchronized reconnect storms.

$$
T_{delay}
=
\min\left(30000,\,
1000\times1.5^{attempt}
\right)
+
random(500,4500)
$$

Connections automatically retry until:

- the peer becomes available,
- the server shuts down,
- or the connection is intentionally closed.

---

## Programmatic API Reference

### `ServerMesh.init(server)`

Initializes the mesh networking layer.

```javascript
await ServerMesh.init(server);
```

---

### `ServerMesh.send(payload)`

Routes a payload across the cluster.

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
ServerMesh.on("mesh.orgAdded", handler);

ServerMesh.on("mesh.orgDeleted", handler);

ServerMesh.on("mesh.orgData", handler);

ServerMesh.on("mesh.raw_message", handler);
```

---

## Security & TLS Specification

- **Encrypted Transport:** All peer communication occurs over mutually authenticated encrypted WebSocket connections.
- **Dedicated Mesh Port:** Mesh traffic is isolated from public HTTP and HTTPS traffic.
- **Ephemeral Certificates:** Temporary TLS certificates are generated during startup and automatically replaced on restart.
- **Server Authentication:** Incoming peers must successfully complete server identity validation before joining the mesh.
- **UUID Tracing:** Every transmitted frame receives a unique identifier for distributed tracing and replay protection.

---

## Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `MESH_PORT` | `8090` | Port used for mesh communication. |
| `CLUSTER_SECRET` | — | Shared secret used during mesh authentication. |
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
