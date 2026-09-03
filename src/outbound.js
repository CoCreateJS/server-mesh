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

import { WebSocket } from 'ws';

/**
 * ServerMeshOutbound manages outbound peer discovery, connection establishment,
 * and server events requiring outbound dispatch.
 */
export class ServerMeshOutbound {
    constructor(mesh) {
        this.mesh = mesh;
        this.server = null;
        this.meshPort = process.env.MESH_PORT || 8090;
        this.retryTimers = new Map();
        this.pruneTimers = new Map();
        this.peers = [];
    }

    closeOrganization(organization_id) {
        for (const [key, timerId] of this.retryTimers) {
            if (!key.startsWith(`${organization_id}:`)) continue;
            clearTimeout(timerId);
            this.retryTimers.delete(key);
        }
    }

    /**
     * Initializes the outbound transport component with host server context and registers IPC handlers.
     * 
     * @param {Object} server - CoCreateServer instance context
     * @returns {Promise<void>}
     */
    async init(server) {
        this.server = server;

        if (this.server && typeof this.server.on === 'function') {
            this.server.on('mesh.outbound.reconnect', (payload) => {
                if (payload && payload.peerIp) {
                    this.connectPeer({ ip: payload.peerIp });
                }
            });

            // Fallback listener for legacy event name
            this.server.on('mesh.client.reconnect', (payload) => {
                if (payload && payload.peerIp) {
                    this.connectPeer({ ip: payload.peerIp });
                }
            });

            this.server.on('mesh.outbound_mesh_relay', (payload) => {
                if (payload) {
                    this.mesh.send(payload.payload ? payload.payload : payload);
                }
            });
        }

        await this.getServers();
    }

    /**
     * Queries database for existing peer server records (READ ONLY) and triggers outbound connection setup.
     */
    async getServers() {
        if (!this.server?.crud) return;

        const readPayload = {
            method: 'object.read',
            array: 'servers',
            filter: {
                query: { _id: { $lt: this.server.id } },
                sort: { _id: 1 },
                limit: 0
            },
            organization_id: this.server.organization_id,
            host: this.server.host
        };

        let readResult;
        try {
            readResult = await this.server.crud.send(readPayload);
        } catch (err) {
            console.error("[@cocreate/server-mesh/outbound] Failed to query peer server records:", err.message);
            return;
        }

        const peers = this.normalizePeersList(readResult);
        this.peers = peers;

        for (const targetPeer of peers) {
            this.connectPeer(targetPeer, peers.length);
        }
    }

    /**
     * Initiates secure outbound WebSocket transport link to a remote peer.
     * 
     * @param {Object|string} targetPeer - Peer metadata object or IP string
     * @param {number} totalPeers - Count of peers for calculating eviction delay
     */
    connectPeer(targetPeer, totalPeers = 1, organization_id) {
        const targetIp = typeof targetPeer === 'string' ? targetPeer : targetPeer?.ip;
        const targetId = typeof targetPeer === 'object' ? targetPeer._id : targetIp;
        const connectionKey = `${organization_id || 'control'}:${targetId || targetIp}`;

        if (!targetIp || targetIp === this.server.ip || this.mesh.activeConnections.has(connectionKey)) return;

        console.log(`[@cocreate/server-mesh/outbound] Initiating outbound link to peer: ${targetIp}`);

        let retryCount = 0;
        const maxRetries = 10;

        const connectToPeer = () => {
            if (this.mesh.isClosed) return;
            if (organization_id && !this.mesh.organizations.has(organization_id)) return;

            const path = organization_id
                ? `/${encodeURIComponent(organization_id)}`
                : '';
            const targetUrl = `wss://${targetIp}:${this.meshPort}${path}`;
            const clientSocket = new WebSocket(targetUrl, [
                encodeURIComponent(JSON.stringify({
                    type: 'peer',
                    ip: this.server.ip,
                    id: this.server.id,
                    token: process.env.CLUSTER_SECRET,
                    sniHost: this.server.host
                }))
            ], {
                servername: this.server.host,
                rejectUnauthorized: false
            });

            clientSocket.isAlive = true;
            clientSocket.remoteAddress = targetIp;
            clientSocket.serverId = targetId || targetIp;
            clientSocket.organization_id = organization_id;
            clientSocket.intentionalClose = false;
            this.mesh.activeConnections.set(connectionKey, clientSocket);
            if (!organization_id) {
                this.mesh.addControlSocket(clientSocket);
            } else {
                this.mesh.addOrganizationSocket(organization_id, clientSocket);
            }
            clientSocket.on('pong', () => { clientSocket.isAlive = true; });
            clientSocket.on('ping', () => { clientSocket.pong(); });

            clientSocket.on('open', () => {
                console.log(`[@cocreate/server-mesh/outbound] Outbound connection complete to peer: ${targetIp}`);
                retryCount = 0;

                if (this.retryTimers.has(connectionKey)) {
                    clearTimeout(this.retryTimers.get(connectionKey));
                }
                if (this.pruneTimers.has(targetIp)) clearTimeout(this.pruneTimers.get(targetIp));

                this.retryTimers.delete(connectionKey);
                this.pruneTimers.delete(targetIp);

                clientSocket.isPeer = true;
                clientSocket.isControlSocket = !organization_id;
                if (clientSocket.isControlSocket) {
                    this.mesh.syncOrganizations(clientSocket);
                }

            });

            clientSocket.on('close', () => {
                this.mesh.activeConnections.delete(connectionKey);
                this.retryTimers.delete(connectionKey);
                this.mesh.removeSocketFromAllOrgServers(clientSocket);

                if (this.mesh.isClosed || clientSocket.intentionalClose) return;
                if (organization_id && !this.mesh.organizations.has(organization_id)) return;

                if (retryCount < maxRetries) {
                    retryCount++;
                    const delay = 5000 + Math.floor(Math.random() * 2000);
                    const timerId = setTimeout(connectToPeer, delay);
                    this.retryTimers.set(connectionKey, timerId);
                }
            });

            clientSocket.on('message', (rawData) => {
                if (this.mesh && typeof this.mesh.handleIncomingMessage === 'function') {
                    this.mesh.handleIncomingMessage(rawData, clientSocket);
                }
            });

            clientSocket.on('error', (err) => {
                console.log(`[@cocreate/server-mesh/outbound] Socket transport failure (${targetIp}): ${err.message}`);
            });
        };

        connectToPeer();
    }

    /**
     * Extracts raw dataset arrays from standard database responses.
     */
    normalizePeersList(result) {
        if (!result) return [];
        if (Array.isArray(result)) return result;
        if (Array.isArray(result.object)) return result.object;
        if (Array.isArray(result.data)) return result.data;
        return [];
    }

    /**
     * Clears outbound retry and prune timers on teardown.
     */
    close() {
        for (const timerId of this.retryTimers.values()) clearTimeout(timerId);
        for (const timerId of this.pruneTimers.values()) clearTimeout(timerId);
        this.retryTimers.clear();
        this.pruneTimers.clear();
    }
}
