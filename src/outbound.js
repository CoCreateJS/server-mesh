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
 * reconnection retries, and reacting to server events requiring outbound dispatch.
 */
export class ServerMeshOutbound {
    constructor(mesh) {
        this.mesh = mesh;
        this.server = null;
        this.meshPort = process.env.MESH_PORT || 8090;
        this.retryTimers = new Map();
        this.pruneTimers = new Map();
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
    connectPeer(targetPeer, totalPeers = 1) {
        const targetIp = typeof targetPeer === 'string' ? targetPeer : targetPeer?.ip;
        const targetId = typeof targetPeer === 'object' ? targetPeer._id : targetIp;

        if (!targetIp || targetIp === this.server.ip || this.mesh.activeConnections.has(targetIp)) return;

        console.log(`[@cocreate/server-mesh/outbound] Initiating outbound link to peer: ${targetIp}`);

        let retryCount = 0;
        const maxRetries = 10;

        const connectToPeer = () => {
            if (this.mesh.isClosed) return;

            const targetUrl = `wss://${targetIp}:${this.meshPort}/${this.server.organization_id}`;
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
            clientSocket.on('pong', () => { clientSocket.isAlive = true; });
            clientSocket.on('ping', () => { clientSocket.pong(); });

            clientSocket.on('open', () => {
                console.log(`[@cocreate/server-mesh/outbound] Outbound connection complete to peer: ${targetIp}`);
                retryCount = 0;

                if (this.retryTimers.has(targetIp)) clearTimeout(this.retryTimers.get(targetIp));
                if (this.pruneTimers.has(targetIp)) clearTimeout(this.pruneTimers.get(targetIp));

                this.retryTimers.delete(targetIp);
                this.pruneTimers.delete(targetIp);

                clientSocket.isPeer = true;
                clientSocket.isPlatformTunnel = true;

                if (targetId) this.mesh.activeConnections.set(targetId, clientSocket);
                this.mesh.activeConnections.set(targetIp, clientSocket);
            });

            clientSocket.on('close', () => {
                if (targetId) this.mesh.activeConnections.delete(targetId);
                this.mesh.activeConnections.delete(targetIp);
                this.retryTimers.delete(targetIp);
                this.mesh.removeSocketFromAllOrgServers(clientSocket);

                if (this.mesh.isClosed) return;

                if (retryCount < maxRetries) {
                    retryCount++;
                    const delay = 5000 + Math.floor(Math.random() * 2000);
                    console.warn(`[@cocreate/server-mesh/outbound] Peer dropped (${targetIp}). Retrying in ${delay}ms...`);
                    const timerId = setTimeout(connectToPeer, delay);
                    this.retryTimers.set(targetIp, timerId);
                } else {
                    const staggerDelay = totalPeers * 1000;
                    console.warn(`[@cocreate/server-mesh/outbound] Peer dead (${targetIp}). Scheduling consensus notification in ${staggerDelay}ms...`);

                    const pruneTimerId = setTimeout(() => {
                        console.error(`[@cocreate/server-mesh/outbound] Peer node dead: ${targetIp}`);

                        this.mesh.send({
                            method: 'mesh.peer_pruned',
                            deadIp: targetIp,
                            isForwarded: true,
                            senderId: this.server.id
                        });

                        this.pruneTimers.delete(targetIp);
                    }, staggerDelay);

                    this.pruneTimers.set(targetIp, pruneTimerId);
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
     * Clears all outbound retry and prune timers on teardown.
     */
    close() {
        for (const timerId of this.retryTimers.values()) clearTimeout(timerId);
        for (const timerId of this.pruneTimers.values()) clearTimeout(timerId);
        this.retryTimers.clear();
        this.pruneTimers.clear();
    }
}