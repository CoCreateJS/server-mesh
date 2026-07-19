# CoCreate Server Mesh

A decentralized, low-latency inter-node communication plane designed to bridge server nodes into a high-speed, peer-to-peer (P2P) cluster federation. This module establishes secure, encrypted data tunnels that facilitate real-time multi-tenant state replication and cross-worker IPC relays with sub-millisecond overhead.

---

## Documentation

For full cluster network topology layouts, routing table configurations, and firewall optimization guides, refer to the [CoCreate Server Mesh Documentation](https://cocreatejs.com/docs/server-mesh).

## Table of Contents

* [The Masterless Control Plane Layer](#the-masterless-control-plane-layer)
* [Why CoCreate Server Mesh?](#why-cocreate-server-mesh)
* [Dynamic Mesh Network Topology](#dynamic-mesh-network-topology)
* [The Cluster Quorum Metric](#the-cluster-quorum-metric)
* [Zero-Copy Payload Bridging](#zero-copy-payload-bridging)
* [Configuration](#configuration)
* [Installation](#installation)
* [Announcements](#announcements)
* [Roadmap](#roadmap)
* [How to Contribute](#how-to-contribute)
* [About](#about)

---

## The Masterless Control Plane Layer

`@cocreate/server-mesh` provides the communication backbone that allows independent servers to operate as a singular, cohesive application cluster. By discarding traditional master-replica routing bottlenecks, the mesh implements an autonomous **Unicast/Broadcast P2P Control Layer**.

### Decentralized State Synchronization

Nodes dynamically discover one another using a shared registry database or secure internal network scans. Once a peer is discovered, a persistent, bi-directional WebSocket or TCP stream tunnel is negotiated directly between them, bypassing intermediate proxy bottlenecks.

### Split-Brain Mitigation

The mesh continuously calculates topological state weightings across all surviving peers. If network partitions divide the infrastructure, the disconnected segments use geometric vote counts to selectively cordon themselves, protecting database pools from race conditions or data corruption.

```text
 [Node A] ◄─────────────────── (Bi-Directional P2P Tunnel) ───────────────────► [Node B]
    │                                                                             │
    └────────► [Node C] ◄────────────────────────────────────────► [Node D] ──────┘
```

---

## Why CoCreate Server Mesh?

Traditional web clusters rely on a centralized pub/sub redis instance or reverse proxies to route inter-server events (like dispatching a message to a user connected to a different server instance). As connection volume scales, this central message broker inevitably becomes a processing bottleneck and a critical single point of failure (SPOF).

`@cocreate/server-mesh` solves this by connecting nodes directly in a **Fully Connected Mesh Topology**. When a multi-tenant update occurs on Server A that affects users on Server B, the packet travels over a single direct socket hop—drastically slashing message dispatch latencies and completely decoupling the cluster's scaling capabilities from an external broker.

---

## Dynamic Mesh Network Topology

The mesh plane handles routing optimization and node membership events across the cluster:

| Network Event | Mesh Plane Action | Operational Result |
| --- | --- | --- |
| **Node Join / Boot** | Handshake validation & link broadcast | Instantly introduces the new node to all active peers, expanding routing capacity. |
| **Unicast Event Route** | Target ID verification via internal routing table | Directly forwards tenant state updates over a single TCP hop to the target instance. |
| **Broadcast Relay** | Concurrent parallel socket writes | Flushes cluster-wide configuration modifications to all nodes simultaneously. |
| **Heartbeat Failure** | Auto-prune registry & connection teardown | Isolates dead nodes within 2500ms to preserve cluster connection health. |

---

## The Cluster Quorum Metric

To safely confirm cluster health and make authorization decisions during topology shifts, the mesh verifies consensus. The minimum required nodes for a healthy cluster partition ($Q_c$) is determined dynamically using the formula:

$$Q_c = \left\lfloor \frac{N_{\text{total}}}{2} \right\rfloor + 1$$

Where $N_{\text{total}}$ represents the total number of healthy instances registered within the active mesh control plane. If an isolated partition drops below the $Q_c$ threshold, it goes into defensive fallback mode—refusing local modifications and forcing incoming user traffic to migrate to the dominant partition.

---

## Zero-Copy Payload Bridging

Inter-node data bridging can quickly clog memory buses if payloads are constantly parsed and reserialized. `@cocreate/server-mesh` minimizes overhead by utilizing **Stream-Level Piping** and zero-copy binary buffers.

When a multi-tenant binary payload or large document packet is received from one node and destined for another, the mesh avoids translating the buffer into high-level JavaScript objects. Instead, the raw internal data streams are directly piped from the source network socket straight into the target peer's outbound stream, maximizing memory bus efficiency.

---

## Configuration

Drop a `mesh.config.json` into your server root directory to handle discovery protocols, link keys, and cluster boundaries.

```json
{
  "clusterName": "cocreate-production-cluster",
  "port": 8090,
  "discovery": {
    "type": "database",
    "heartbeatIntervalMs": 1000,
    "missedHeartbeatsLimit": 3
  },
  "security": {
    "encryptionEnabled": true,
    "algorithm": "aes-256-gcm",
    "secretKeyEnv": "COCREATE_MESH_SECRET"
  },
  "network": {
    "maxPeers": 64,
    "connectionTimeoutMs": 5000
  }
}
```

---

## Installation

### NPM Installation

```bash
npm i @cocreate/server-mesh
```

### Yarn Installation

```bash
yarn add @cocreate/server-mesh
```

---

## Announcements

Mesh topology optimizations, split-brain algorithm updates, and secure tunnel engine revisions are meticulously detailed in our release notes on the [CoCreate Server Mesh GitHub Releases](https://github.com/CoCreate-app/CoCreate-server-mesh/releases). Subscribe for live tracking.

---

## Roadmap

Upcoming foundational developments for our communication plane include:

* **Native QUIC / HTTP3 Transport:** Migrating inter-node tunnels onto UDP-based QUIC paths to gain superior connection multiplexing and zero head-of-line blocking.
* **Auto-Discovery Mesh Broadcast:** Seamless local network UDP multicast configurations for zero-config bare metal server clusters.

---

## How to Contribute

We encourage contribution to our libraries (you might even score some nifty swag), please see our [CONTRIBUTING.md](https://github.com/CoCreate-app/CoCreate-server-mesh/blob/master/CONTRIBUTING.md) guide for details. If you encounter any bugs or wish to make feature requests, please submit an issue on our [GitHub Issues](https://github.com/CoCreate-app/CoCreate-server-mesh/issues) tracker. We want this library to be community-driven, and CoCreate led. We need your help to realize this goal.

For broader system configurations and API guides, please visit our [CoCreate Server Mesh Documentation](https://cocreatejs.com/docs/server-mesh).

---

## About

`@cocreate/server-mesh` is designed, built, and supported by the CoCreate Developer Experience Team.

> [!NOTE]
> Please contact the Developer Experience Team via [GitHub Discussions](https://github.com/CoCreate-app/CoCreate-server-mesh/discussions) or join our Discord channel for any architecture or deployment queries.

`@cocreate/server-mesh` is maintained and funded by CoCreate. The names and logos for CoCreate are trademarks of CoCreate, LLC.