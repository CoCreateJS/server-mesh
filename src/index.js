/********************************************************************************
 * Copyright (C) 2023 CoCreate and Contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in commercials or open source under GNU AGPLv3.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 ********************************************************************************/

import { EventEmitter } from 'node:events';
import crypto from 'node:crypto';
import { ServerMeshInbound } from './inbound.js';
import { ServerMeshOutbound } from './outbound.js';

/**
 * ServerMesh orchestrates multi-tenant routing, unified IPC event forwarding,
 * shared active connection pools, and coordinates ServerMeshInbound and ServerMeshOutbound.
 */
export class ServerMesh extends EventEmitter {
    constructor() {
        super();
        this.server = null;

        // Sub-module transport managers
        this.inboundManager = new ServerMeshInbound(this);
        this.outboundManager = new ServerMeshOutbound(this);

        // Shared Connection & Multi-tenant Routing Tables
        this.activeConnections = new Map(); // serverId / IP -> Network Socket
        this.orgWorkers = new Map();        // organization_id -> Set(workerId)
        this.orgServers = new Map();        // organization_id -> Set(Network Socket)

        this.isClosed = false;
        this.handleServerEvent = this.handleServerEvent.bind(this);
    }

    /**
     * Initializes the ServerMesh orchestrator with the CoCreateServer instance context.
     * 
     * @param {Object} server - CoCreateServer instance
     * @returns {Promise<ServerMesh>}
     */
    async init(server) {
        if (!server) {
            throw new Error('[@cocreate/server-mesh] Failed to initialize: server instance is required.');
        }

        this.server = server;
        this.server.mesh = this; // Attach mesh instance directly to server context
        this.isClosed = false;

        // Register IPC listeners on server context
        this.setupServerListeners();

        const isPrimary = this.server.isPrimary ?? true;

        if (!isPrimary) {
            console.log(`[@cocreate/server-mesh] Initialized mesh layer in worker process context`);
            return this;
        }

        // Initialize inbound and outbound sub-modules for Primary node
        await this.inboundManager.init(this.server);
        await this.outboundManager.init(this.server);

        console.log(`[@cocreate/server-mesh] Initialized server mesh layer for node ${this.server.id || 'unassigned'}`);
        return this;
    }

    /**
     * Binds IPC message and broadcasting event handlers to this.server.
     */
    setupServerListeners() {
        if (!this.server || typeof this.server.on !== 'function') return;

        // Main unified IPC entry point
        this.server.on('mesh', (payload) => {
            this.handleIPCPayload(payload);
        });

        // Direct mesh dispatch listener
        this.server.on('mesh.send', (payload) => {
            this.send(payload);
        });

        // Register protocol routing logic
        this.setupProtocolListeners();
    }

    /**
     * Handles inbound IPC payloads from server.on('mesh').
     * 
     * @param {Object} payload - IPC message containing 'method' property
     */
    handleIPCPayload(payload) {
        if (!payload || typeof payload !== 'object' || !payload.method) return;

        const { method } = payload;

        if (method === 'mesh.send' || method === 'mesh.outbound_mesh_relay') {
            const dataToTransmit = payload.payload ? payload.payload : payload;
            this.send(dataToTransmit);
        } 
        else if (method === 'mesh.registerOrg' || method === 'mesh.orgAdded') {
            const { organization_id, workerId } = payload;
            let isNewOrg = false;

            if (!this.orgWorkers.has(organization_id)) {
                this.orgWorkers.set(organization_id, new Set());
                isNewOrg = true;
            }

            if (workerId) {
                this.orgWorkers.get(organization_id).add(workerId);
            }

            if (isNewOrg) {
                this.send({
                    method: 'mesh.orgAdded',
                    organization_id,
                    senderId: this.server.id
                });
            }
        } 
        else if (method === 'mesh.deregisterOrg' || method === 'mesh.orgDeleted') {
            const { organization_id, workerId } = payload;
            const workersSet = this.orgWorkers.get(organization_id);

            if (workersSet) {
                if (workerId) workersSet.delete(workerId);
                if (!workerId || workersSet.size === 0) {
                    this.orgWorkers.delete(organization_id);
                    this.send({
                        method: 'mesh.orgDeleted',
                        organization_id,
                        senderId: this.server.id
                    });
                }
            }
        } 
        else if (method === 'mesh.orgData') {
            const { organization_id, data, senderWorkerId } = payload;

            // Relay locally to sibling workers hosting tenant
            const activeWorkerIds = this.orgWorkers.get(organization_id);
            if (activeWorkerIds && activeWorkerIds.size > 0 && this.server) {
                const siblingWorkerIds = senderWorkerId 
                    ? Array.from(activeWorkerIds).filter(wId => wId !== senderWorkerId)
                    : Array.from(activeWorkerIds);

                if (siblingWorkerIds.length > 0) {
                    try {
                        this.server.send({
                            method: 'mesh.inbound',
                            workers: siblingWorkerIds,
                            master: false,
                            data: typeof data === 'string' ? data : JSON.stringify(data)
                        });
                    } catch (err) {
                        console.error("[@cocreate/server-mesh] Failed to relay local orgData:", err.message);
                    }
                }
            }

            // Relay to network peer sockets subscribed to tenant
            const targetSockets = this.orgServers.get(organization_id);
            if (targetSockets && targetSockets.size > 0) {
                this.socketSend(Array.from(targetSockets), {
                    method: 'mesh.orgData',
                    organization_id,
                    data
                });
            }
        } 
        else {
            const targetMethod = method.replace(/^mesh\./, '');
            const systemEvents = ['restart', 'reboot', 'shutdown', 'cordon', 'shed', 'drain'];

            if (systemEvents.includes(targetMethod)) {
                const actionPayload = { ...payload, method: targetMethod };
                if (typeof this.server.send === 'function') {
                    this.server.send(actionPayload);
                }
                this.emit(targetMethod, actionPayload);
            } else {
                this.emit(method, payload);
            }
        }
    }

    /**
     * Registers protocol handlers for multi-tenant subscriptions and consensus events.
     */
    setupProtocolListeners() {
        this.on('mesh.orgAdded', (message, ws) => {
            if (!ws) return;
            const organization_id = message.organization_id;
            const ip = ws.remoteAddress || ws._socket?.remoteAddress;
            const peerId = message.senderId || ip;

            if (!this.orgServers.has(organization_id)) {
                this.orgServers.set(organization_id, new Set());
            }
            this.orgServers.get(organization_id).add(ws);
            console.log(`[@cocreate/server-mesh] Peer ${peerId} registered subscription for org "${organization_id}"`);
        });

        this.on('mesh.orgDeleted', (message, ws) => {
            if (!ws) return;
            const organization_id = message.organization_id;
            const ip = ws.remoteAddress || ws._socket?.remoteAddress;
            const peerId = message.senderId || ip;

            const sockets = this.orgServers.get(organization_id);
            if (sockets) {
                sockets.delete(ws);
                if (sockets.size === 0) {
                    this.orgServers.delete(organization_id);
                }
                console.log(`[@cocreate/server-mesh] Peer ${peerId} de-registered subscription from org "${organization_id}"`);
            }
        });

        this.on('mesh.orgData', (message) => {
            const { organization_id, data } = message;

            const activeWorkerIds = this.orgWorkers.get(organization_id);
            if (activeWorkerIds && activeWorkerIds.size > 0 && this.server) {
                try {
                    this.server.send({
                        method: 'mesh.inbound',
                        workers: Array.from(activeWorkerIds),
                        master: false,
                        data: typeof data === 'string' ? data : JSON.stringify(data)
                    });
                } catch (err) {
                    console.error(`[@cocreate/server-mesh] Failed to relay incoming orgData for org "${organization_id}":`, err.message);
                }
            }
        });

        this.on('mesh.peer_pruned', (message) => {
            if (message.deadIp && this.outboundManager) {
                const activeTimerId = this.outboundManager.pruneTimers.get(message.deadIp);
                if (activeTimerId) {
                    clearTimeout(activeTimerId);
                    this.outboundManager.pruneTimers.delete(message.deadIp);
                    console.log(`[@cocreate/server-mesh] Cancelled prune schedule for ${message.deadIp} on peer consensus.`);
                }
            }
        });

        this.on('mesh.raw_message', (message) => {
            if (this.server) {
                try {
                    this.server.send({
                        method: 'mesh.inbound',
                        broadcast: true,
                        master: false,
                        data: JSON.stringify(message)
                    });
                } catch (err) {
                    console.error("[@cocreate/server-mesh] Failed to relay raw fallback message:", err.message);
                }
            }
        });
    }

    /**
     * Parses incoming network socket payloads and dispatches events.
     * 
     * @param {string|Buffer} rawData 
     * @param {Object} ws - Active network socket reference
     */
    handleIncomingMessage(rawData, ws) {
        let message;
        try {
            message = JSON.parse(rawData.toString());
        } catch (err) {
            return;
        }

        if (!message || typeof message !== 'object') return;

        if (message.server_id && message.server_id === this.server?.id) {
            return;
        }

        const method = message.method;
        if (!method) return;

        const targetMethod = method.replace(/^mesh\./, '');
        const systemEvents = ['restart', 'reboot', 'shutdown', 'cordon', 'shed', 'drain'];

        if (systemEvents.includes(targetMethod)) {
            const ipcPayload = { ...message, method: targetMethod };

            if (this.server && typeof this.server.send === 'function') {
                this.server.send(ipcPayload);
            }

            this.emit(targetMethod, ipcPayload, ws);
        } else {
            this.emit(method, message, ws);
        }
    }

    /**
     * Transmits payload across active network socket tunnels or targeted servers.
     * 
     * @param {Object} payload 
     */
    send(payload) {
        if (!payload || typeof payload !== 'object' || !payload.method) {
            console.error("[@cocreate/server-mesh] Cannot transmit mesh payload without a defined 'method' key.");
            return;
        }

        if (!payload.uuid) {
            payload.uuid = crypto.randomUUID();
        }

        const isPrimary = this.server?.isPrimary ?? true;

        if (!isPrimary) {
            if (typeof this.server?.send === 'function') {
                this.server.send({
                    method: 'mesh.outbound_mesh_relay',
                    payload: payload
                });
            }
        } else {
            const targets = [];

            if (payload.broadcast === true || !payload.servers) {
                targets.push(...this.activeConnections.values());
            } else {
                const serversList = Array.isArray(payload.servers) ? payload.servers : [payload.servers];
                for (const id of serversList) {
                    const socket = this.activeConnections.get(id);
                    if (socket) {
                        targets.push(socket);
                    }
                }
            }

            const uniqueSockets = Array.from(new Set(targets));
            this.socketSend(uniqueSockets, payload);
        }
    }

    /**
     * Safely serializes and sends payload data across network sockets without direct library coupling.
     * 
     * @param {Object|Object[]} sockets - Target socket reference(s)
     * @param {Object} data 
     */
    socketSend(sockets, data) {
        const targetSockets = Array.isArray(sockets) ? sockets : [sockets];
        const dataStr = typeof data === 'string' ? data : JSON.stringify(data);

        for (const socket of targetSockets) {
            try {
                // 1 corresponds to OPEN state in WebSocket standard protocol implementations
                if (socket && socket.readyState === 1) {
                    socket.send(dataStr);
                }
            } catch (err) {
                console.error(`[@cocreate/server-mesh] Error executing socketSend transaction:`, err.message);
            }
        }
    }

    /**
     * Safely removes a closed socket connection from tenant routing maps.
     * 
     * @param {Object} ws 
     */
    removeSocketFromAllOrgServers(ws) {
        for (const [orgId, sockets] of this.orgServers.entries()) {
            if (sockets.has(ws)) {
                sockets.delete(ws);
                if (sockets.size === 0) {
                    this.orgServers.delete(orgId);
                }
            }
        }
    }

    /**
     * Internal server event handler callback.
     */
    handleServerEvent(payload) {
        this.send(payload);
    }

    /**
     * Initiates coordinated mesh shutdown, closing server ports and outbound timers.
     * 
     * @returns {Promise<void>}
     */
    async close() {
        if (this.isClosed) return;
        this.isClosed = true;

        console.log(`[@cocreate/server-mesh] Coordinated secure mesh shutdown triggered.`);

        if (this.outboundManager) {
            this.outboundManager.close();
        }

        const uniqueSockets = new Set(this.activeConnections.values());
        for (const ws of uniqueSockets) {
            if (ws && ws.readyState === 1) {
                try { ws.close(); } catch {}
            }
        }
        this.activeConnections.clear();

        if (this.inboundManager) {
            await this.inboundManager.close();
        }
    }
}

// Export singleton instance as default export and named export
export const serverMesh = new ServerMesh();
export default serverMesh;