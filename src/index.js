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
 * @public
 * @environment server
 */
export class ServerMesh extends EventEmitter {
    constructor() {
        super();
        this.server = null;

        // Sub-module transport managers
        this.inboundManager = new ServerMeshInbound(this);
        this.outboundManager = new ServerMeshOutbound(this);

        // Shared Connection & Multi-tenant Routing Tables
        this.activeConnections = new Map(); // organization_id/serverId -> Network Socket
        this.controlSockets = [];
        this.organizations = new Map();     // organization_id -> Network Socket[]

        this.isClosed = false;
        this.handleServerEvent = this.handleServerEvent.bind(this);
    }

    addOrganizationSocket(organization_id, ws) {
        if (!organization_id || !ws) return;
        ws.organization_id = organization_id;
        if (!this.organizations.has(organization_id)) {
            this.organizations.set(organization_id, []);
        }
        const sockets = this.organizations.get(organization_id);
        if (!sockets.includes(ws)) sockets.push(ws);
    }

    addControlSocket(ws) {
        if (!ws || this.controlSockets.includes(ws)) return;
        ws.isControlSocket = true;
        this.controlSockets.push(ws);
    }

    openOrganization(organization_id) {
        if (!organization_id || !this.server?.isPrimary) return;
        if (!this.organizations.has(organization_id)) {
            this.organizations.set(organization_id, []);
        }
        this.announceOrganization(organization_id);
    }

    announceOrganization(organization_id, socket) {
        const controlSockets = socket ? [socket] : this.controlSockets;
        this.socketSend(controlSockets, {
            method: 'orgRegistered',
            organization_id,
            serverId: this.server.id
        });
    }

    syncOrganizations(socket) {
        for (const organization_id of this.organizations.keys()) {
            if (organization_id === this.server.organization_id) continue;
            this.announceOrganization(organization_id, socket);
        }
    }

    closeOrganization(organization_id) {
        if (!organization_id || !this.server?.isPrimary) return;
        this.outboundManager.closeOrganization(organization_id);
        const sockets = this.organizations.get(organization_id);
        if (!sockets) return;
        this.organizations.delete(organization_id);
        for (const ws of sockets) {
            if (ws.isControlSocket) continue;
            ws.intentionalClose = true;
            for (const [key, socket] of this.activeConnections) {
                if (socket === ws) this.activeConnections.delete(key);
            }
            try { ws.close(); } catch {}
        }
    }

    /**
     * Initializes the ServerMesh orchestrator with the CoCreateServer instance context.
     *
     * @param {Object} server - CoCreateServer instance
     * @returns {Promise<ServerMesh>}
 * @private
 * @environment server
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
            console.log(`[@cocreate/server-mesh] [WORKER ${this.server.workerId}] Initialized mesh layer in worker process context`);
            return this;
        }

        // Initialize inbound and outbound sub-modules for Primary node
        await this.inboundManager.init(this.server);
        await this.outboundManager.init(this.server);

        console.log(`[@cocreate/server-mesh] [MASTER] Initialized server mesh layer for node ${this.server.id || 'unassigned'}`);
        return this;
    }

    /**
     * Binds IPC message and broadcasting event handlers to this.server.
 * @private
 * @environment server
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
 * @private
 * @environment server
 */
    handleIPCPayload(payload) {
        if (!payload || typeof payload !== 'object' || !payload.method) return;

        const { method } = payload;

        if (method === 'mesh.send' || method === 'mesh.outbound_mesh_relay') {
            const dataToTransmit = payload.payload ? payload.payload : payload;
            this.send(dataToTransmit);
        } 
        else if (method === 'mesh.orgData') {
            const { organization_id, data } = payload;
            // Relay to network peer sockets subscribed to tenant
            const targetSockets = this.organizations.get(organization_id);
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
 * @private
 * @environment server
 */
    setupProtocolListeners() {
        this.on('mesh.orgAdded', (message, ws) => {
            if (!ws) return;
            const organization_id = message.organization_id;
            const ip = ws.remoteAddress || ws._socket?.remoteAddress;
            const peerId = message.serverId || message.senderId || ip;

            this.addOrganizationSocket(organization_id, ws);
            console.log(`[@cocreate/server-mesh] Peer ${peerId} registered subscription for org "${organization_id}"`);
        });

        this.on('mesh.orgDeleted', (message, ws) => {
            if (!ws) return;
            const organization_id = message.organization_id;
            const ip = ws.remoteAddress || ws._socket?.remoteAddress;
            const peerId = message.serverId || message.senderId || ip;

            const sockets = this.organizations.get(organization_id);
            if (sockets) {
                const index = sockets.indexOf(ws);
                if (index !== -1) sockets.splice(index, 1);
                if (sockets.length === 0) {
                    this.organizations.delete(organization_id);
                }
                console.log(`[@cocreate/server-mesh] Peer ${peerId} de-registered subscription from org "${organization_id}"`);
            }
        });

        this.on('mesh.orgData', (message) => {
            const { organization_id, data } = message;
            if (!this.server || !organization_id || !data) return;

            const payload = typeof data === 'string' ? JSON.parse(data) : data;
            this.server.send({
                ...payload,
                organization_id,
                master: false,
                status: 'received'
            });
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
 * @private
 * @environment server
 */
    handleIncomingMessage(rawData, ws) {
        let message;
        try {
            message = JSON.parse(rawData.toString());
        } catch (err) {
            return;
        }

        if (!message || typeof message !== 'object') return;

        if (message.cluster !== undefined) {
            const clusters = Array.isArray(message.cluster)
                ? message.cluster
                : [message.cluster];
            if (!clusters.includes(this.server?.cluster_id)) return;
        }

        if (message.serverId && message.serverId === this.server?.id) {
            return;
        }

        const method = message.method;
        if (!method) return;

        if (method === 'orgRegistered') {
            this.handleOrganizationRegistration(message, ws);
            return;
        }

        const targetMethod = method.replace(/^mesh\./, '');
        const systemEvents = ['restart', 'reboot', 'shutdown', 'cordon', 'shed', 'drain'];

        if (method === 'mesh.orgAdded' || method === 'mesh.orgDeleted') {
            this.emit(method, message, ws);
        } else if (systemEvents.includes(targetMethod)) {
            const ipcPayload = { ...message, method: targetMethod };

            if (this.server && typeof this.server.send === 'function') {
                this.server.send(ipcPayload);
            }

            this.emit(targetMethod, ipcPayload, ws);
        } else if (message.organization_id) {
            this.server.send({
                ...message,
                master: false,
                status: 'received'
            });
        } else {
            this.emit(method, message, ws);
        }
    }

    handleOrganizationRegistration(message, ws) {
        const organization_id = message.organization_id;
        if (!organization_id || !this.organizations.has(organization_id)) return;

        const remoteServerId = message.serverId || ws.serverId;
        if (!remoteServerId || remoteServerId === this.server.id) return;

        if (message.response !== true) {
            this.socketSend(ws, {
                method: 'orgRegistered',
                organization_id,
                serverId: this.server.id,
                response: true
            });
        }

        if (String(this.server.id) <= String(remoteServerId)) return;
        this.outboundManager.connectPeer({
            _id: remoteServerId,
            ip: ws.remoteAddress
        }, this.outboundManager.peers.length, organization_id);
    }

    /**
     * Transmits payload across active network socket tunnels or targeted servers.
     *
     * @param {Object} payload
 * @private
 * @environment server
 */
    send(payload) {
        if (!payload || typeof payload !== 'object' || !payload.method) {
            console.error("[@cocreate/server-mesh] Cannot transmit mesh payload without a defined 'method' key.");
            return;
        }

        if (!payload.uuid) {
            payload.uuid = crypto.randomUUID();
        }
        if (payload.serverId === undefined) {
            payload.serverId = this.server?.id;
            payload.status = 'sent';
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
            const serverTargets = payload.server ?? payload.servers;
            const availableSockets = payload.organization_id
                ? this.organizations.get(payload.organization_id) || []
                : this.controlSockets;
            const targets = serverTargets === undefined
                ? availableSockets
                : availableSockets.filter(socket => {
                    const servers = Array.isArray(serverTargets)
                        ? serverTargets
                        : [serverTargets];
                    return servers.includes(socket.serverId);
                });

            const uniqueSockets = Array.from(new Set(targets));
            this.socketSend(uniqueSockets, payload);
        }
    }

    /**
     * Safely serializes and sends payload data across network sockets without direct library coupling.
     *
     * @param {Object|Object[]} sockets - Target socket reference(s)
     * @param {Object} data
 * @private
 * @environment server
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
 * @private
 * @environment server
 */
    removeSocketFromAllOrgServers(ws) {
        for (const [key, socket] of this.activeConnections) {
            if (socket === ws) this.activeConnections.delete(key);
        }
        const controlIndex = this.controlSockets.indexOf(ws);
        if (controlIndex !== -1) this.controlSockets.splice(controlIndex, 1);
        for (const [orgId, sockets] of this.organizations.entries()) {
            const index = sockets.indexOf(ws);
            if (index !== -1) {
                sockets.splice(index, 1);
            }
        }
    }

    /**
     * Internal server event handler callback.
 * @private
 * @environment server
 */
    handleServerEvent(payload) {
        this.send(payload);
    }

    /**
     * Initiates coordinated mesh shutdown, closing server ports and outbound timers.
     *
     * @returns {Promise<void>}
 * @private
 * @environment server
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
                try {
                    ws.intentionalClose = true;
                    ws.close();
                } catch {}
            }
        }
        this.activeConnections.clear();

        if (this.inboundManager) {
            await this.inboundManager.close();
        }
    }
}

// Export singleton instance as default export and named export
/**
 *
 * @public
 * @environment server
 */
export const serverMesh = new ServerMesh();
export default serverMesh;

/**
 *
 * @name default
 * @kind member
 * @public
 * @environment server
 */

/**
 * Decentralized, event-driven P2P server mesh and transport fabric for CoCreate Server. Optimizes multi-tenant cluster concurrency with dynamic, tenant-isolated WebSocket data tunnels.
 *
 * @module server-mesh
 * @environment server
 */
