/********************************************************************************
 * Copyright (C) 2023 CoCreate and Contributors.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 *
 ********************************************************************************/

import { EventEmitter } from 'node:events';
import WebSocket from 'ws';

/**
 * ServerMesh manages multi-node peer-to-peer mesh synchronization across server
 * clusters. It routes system commands, node status updates, and broadcast messages
 * directly using server.send and server.on, without relying on Server or process-level checks.
 */
export class ServerMesh extends EventEmitter {
    constructor() {
        super();
        this.server = null;
        this.nodes = new Map(); // Connected peer node WebSocket instances
        this.peers = new Set(); // Peer server addresses/URLs
        this.reconnectTimers = new Map();
        this.heartbeatInterval = null;
        this.heartbeatMs = 15000;
        this.isClosed = false;

        this.handleServerEvent = this.handleServerEvent.bind(this);
    }

    /**
     * Initializes the ServerMesh module with the parent CoCreateServer instance.
     * Registers system event listeners using server.on and configures mesh command routing.
     * 
     * @param {Object} server - The CoCreateServer instance
     */
    async init(server) {
        if (!server) {
            throw new Error('[@cocreate/server-mesh] Failed to initialize: server instance is required.');
        }

        this.server = server;
        this.isClosed = false;

        // Register core event handlers using server.on
        this.setupServerListeners();

        // Start periodic mesh node heartbeat verification
        this.startHeartbeat();

        console.log(`[@cocreate/server-mesh] Initialized mesh network layer for server ${this.server.id || 'unassigned'}`);
        return this;
    }

    /**
     * Configures event listeners on the server using server.on.
     * Outbound mesh requests dispatch via mesh.send, while cluster management
     * commands received across the mesh are forwarded to server.send.
     */
    setupServerListeners() {
        if (!this.server || typeof this.server.on !== 'function') return;

        // Listen for internal server requests to broadcast over the mesh
        this.server.on('mesh.send', (payload) => {
            this.send(payload);
        });

        // Listen for cluster administration commands emitted locally or via IPC
        const systemEvents = ['restart', 'reboot', 'shutdown', 'cordon', 'shed', 'drain'];
        for (const eventName of systemEvents) {
            this.server.on(`mesh.${eventName}`, (payload) => {
                console.log(`[@cocreate/server-mesh] Routing remote mesh command '${eventName}' to server.send`);
                
                // Format payload for IPC dispatching
                const actionPayload = {
                    ...(payload && typeof payload === 'object' ? payload : {}),
                    method: eventName
                };

                // Forward command directly into server IPC dispatcher
                if (typeof this.server.send === 'function') {
                    this.server.send(actionPayload);
                }

                // Emit locally on mesh instance
                this.emit(eventName, actionPayload);
            });
        }

        // Handle mesh status updates dispatched from local server state transitions
        this.server.on('mesh.node_status_update', (payload) => {
            this.broadcast({
                method: 'mesh.node_status_update',
                server_id: this.server.id,
                status: payload?.status,
                timestamp: Date.now()
            });
        });
    }

    /**
     * Connects to a remote mesh peer node over WebSocket.
     * 
     * @param {string} peerUrl - WebSocket URL of the target remote mesh peer
     */
    connectPeer(peerUrl) {
        if (this.isClosed || !peerUrl || this.peers.has(peerUrl)) return;

        this.peers.add(peerUrl);

        try {
            const ws = new WebSocket(peerUrl, {
                headers: {
                    'x-server-id': this.server?.id || '',
                    'x-mesh-node': 'true'
                }
            });

            ws.on('open', () => {
                console.log(`[@cocreate/server-mesh] Connected to peer mesh node: ${peerUrl}`);
                this.nodes.set(peerUrl, ws);
                
                if (this.reconnectTimers.has(peerUrl)) {
                    clearTimeout(this.reconnectTimers.get(peerUrl));
                    this.reconnectTimers.delete(peerUrl);
                }

                // Handshake identify node
                ws.send(JSON.stringify({
                    method: 'mesh.handshake',
                    server_id: this.server?.id,
                    timestamp: Date.now()
                }));
            });

            ws.on('message', (data) => {
                this.handleIncomingMessage(data, peerUrl);
            });

            ws.on('error', (err) => {
                console.error(`[@cocreate/server-mesh] Error on peer connection ${peerUrl}:`, err.message);
            });

            ws.on('close', () => {
                console.warn(`[@cocreate/server-mesh] Disconnected from peer mesh node: ${peerUrl}`);
                this.nodes.delete(peerUrl);
                this.scheduleReconnect(peerUrl);
            });
        } catch (err) {
            console.error(`[@cocreate/server-mesh] Failed to initiate connection to peer ${peerUrl}:`, err.message);
            this.scheduleReconnect(peerUrl);
        }
    }

    /**
     * Parses and processes incoming mesh messages from remote peers.
     * 
     * @param {string|Buffer} rawData - Raw message data from socket
     * @param {string} peerUrl - Originating peer identification URL
     */
    handleIncomingMessage(rawData, peerUrl) {
        let payload;
        try {
            payload = JSON.parse(rawData.toString());
        } catch (err) {
            console.error(`[@cocreate/server-mesh] Failed to parse message from ${peerUrl}:`, err.message);
            return;
        }

        if (!payload || typeof payload !== 'object') return;

        // Ignore messages originated by this same server node
        if (payload.server_id && payload.server_id === this.server?.id) {
            return;
        }

        console.log(`[@cocreate/server-mesh] Received mesh event '${payload.method}' from node ${payload.server_id || peerUrl}`);

        // Handle mesh system commands by routing them directly into server.send
        const targetMethod = payload.method?.replace(/^mesh\./, '');
        const systemEvents = ['restart', 'reboot', 'shutdown', 'cordon', 'shed', 'drain'];

        if (systemEvents.includes(targetMethod)) {
            const ipcPayload = {
                ...payload,
                method: targetMethod
            };

            // Dispatch command locally via server.send
            if (this.server && typeof this.server.send === 'function') {
                this.server.send(ipcPayload);
            }

            // Emit command event locally on ServerMesh instance
            this.emit(targetMethod, ipcPayload);
        } else if (payload.method) {
            // Emit non-system custom mesh event locally
            this.emit(payload.method, payload);

            // Also emit via server.on bus if server instance is active
            if (this.server && typeof this.server.emit === 'function') {
                this.server.emit(payload.method, payload);
            }
        }
    }

    /**
     * Transmits a payload across all connected mesh nodes.
     * If local dispatch is required, forwards directly to server.send.
     * 
     * @param {Object} payload - Message payload to broadcast across mesh
     */
    send(payload = {}) {
        if (!payload || typeof payload !== 'object') return;

        const messagePayload = {
            ...payload,
            server_id: this.server?.id || null,
            timestamp: payload.timestamp || Date.now()
        };

        // Broadcast to all active peer WebSocket node connections
        this.broadcast(messagePayload);

        // If local worker/cluster broadcast requested, dispatch via server.send
        if (payload.local !== false && this.server && typeof this.server.send === 'function') {
            if (payload.method && !payload.method.startsWith('mesh.')) {
                this.server.send(messagePayload);
            }
        }
    }

    /**
     * Outbound broadcast helper sending JSON payloads to all socket connections.
     * 
     * @param {Object} payload - Serialized payload to send across peers
     */
    broadcast(payload) {
        if (this.nodes.size === 0) return;

        const serialized = JSON.stringify(payload);
        for (const [peerUrl, ws] of this.nodes.entries()) {
            if (ws && ws.readyState === WebSocket.OPEN) {
                try {
                    ws.send(serialized);
                } catch (err) {
                    console.error(`[@cocreate/server-mesh] Failed to send payload to peer ${peerUrl}:`, err.message);
                }
            }
        }
    }

    /**
     * Schedules auto-reconnection for a disconnected peer.
     * 
     * @param {string} peerUrl - Remote peer URL to reconnect
     */
    scheduleReconnect(peerUrl) {
        if (this.isClosed || this.reconnectTimers.has(peerUrl)) return;

        const timer = setTimeout(() => {
            this.reconnectTimers.delete(peerUrl);
            this.peers.delete(peerUrl);
            console.log(`[@cocreate/server-mesh] Attempting reconnection to peer: ${peerUrl}`);
            this.connectPeer(peerUrl);
        }, 5000);

        if (timer.unref) timer.unref();
        this.reconnectTimers.set(peerUrl, timer);
    }

    /**
     * Starts interval timer verifying health of active mesh peer connections.
     */
    startHeartbeat() {
        if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);

        this.heartbeatInterval = setInterval(() => {
            if (this.isClosed) return;

            const pingPayload = JSON.stringify({
                method: 'mesh.ping',
                server_id: this.server?.id,
                timestamp: Date.now()
            });

            for (const [peerUrl, ws] of this.nodes.entries()) {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(pingPayload);
                } else if (ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING) {
                    this.nodes.delete(peerUrl);
                    this.scheduleReconnect(peerUrl);
                }
            }
        }, this.heartbeatMs);

        if (this.heartbeatInterval.unref) {
            this.heartbeatInterval.unref();
        }
    }

    /**
     * Internal event handler reference for server listeners.
     */
    handleServerEvent(payload) {
        this.send(payload);
    }

    /**
     * Gracefully closes all active mesh sockets, clears reconnect timers,
     * and unbinds event listeners.
     */
    close() {
        this.isClosed = true;

        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
            this.heartbeatInterval = null;
        }

        // Clear reconnection timers
        for (const [peerUrl, timer] of this.reconnectTimers.entries()) {
            clearTimeout(timer);
        }
        this.reconnectTimers.clear();

        // Close peer socket connections
        for (const [peerUrl, ws] of this.nodes.entries()) {
            try {
                if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
                    ws.close(1000, 'Mesh shutting down');
                }
            } catch (err) {
                // Fail gracefully during shutdown
            }
        }

        this.nodes.clear();
        this.peers.clear();
        this.removeAllListeners();

        console.log(`[@cocreate/server-mesh] ServerMesh network layer closed.`);
    }
}

// Export singleton instance as default export
export const serverMesh = new ServerMesh();
export default serverMesh;