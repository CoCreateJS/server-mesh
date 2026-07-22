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

import { WebSocketServer, WebSocket } from 'ws';
import https from 'node:https';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import cluster from 'node:cluster';
import { execSync } from 'node:child_process';
import { EventEmitter } from 'node:events';
import crypto from 'node:crypto';

const activeConnections = new Map();     // server_id -> WebSocket P2P tunnels
const retryTimers = new Map();           // server_id -> Timeout ID
const orgWorkers = new Map();            // organization_id -> Set(workerId)
const orgServers = new Map();            // organization_id -> Set(WebSocket)

let isGracefulExitRunning = false;
let wsPingInterval = null;

let serverCtx = null;
const MESH_PORT = process.env.MESH_PORT || 8090;
let meshWss = null;
let secureMeshServer = null;

/**
 * Safely loops and sends JSON payloads to one or multiple sockets.
 * Wraps individual operations in independent try-catch layers to avoid process instability.
 * 
 * @param {WebSocket|WebSocket[]} sockets - Target WebSocket(s)
 * @param {Object} data - Payload content to dispatch
 */
function socketSend(sockets, data) {
    const targetSockets = Array.isArray(sockets) ? sockets : [sockets];
    const dataStr = typeof data === 'string' ? data : JSON.stringify(data);

    for (const socket of targetSockets) {
        try {
            if (socket && socket.readyState === WebSocket.OPEN) {
                socket.send(dataStr);
            }
        } catch (err) {
            console.error(`[@cocreate/server-mesh] Error executing socketSend transaction:`, err.message);
        }
    }
}

/**
 * ServerMesh acts as a pure transport fabric. It blindly dispatches
 * all inbound network payloads as standard EventEmitter events.
 */
class ServerMesh extends EventEmitter {
    constructor() {
        super();
    }

    /**
     * Sends a payload across the secure network mesh.
     * Automatically appends a unique ID (uuid) to guarantee end-to-end tracking.
     * 
     * @param {Object} payload - Object to transmit containing a required 'method'
     */
    send(payload) {
        if (!payload || !payload.method) {
            console.error("[@cocreate/server-mesh] [MESH ERROR] Cannot transmit mesh payload without a defined 'method' key.");
            return;
        }

        // Standardize: Every outgoing message must have a unique identifier (uuid)
        if (!payload.uuid) {
            payload.uuid = crypto.randomUUID();
        }

        const isPrimary = serverCtx?.isPrimary;

        if (!isPrimary) {
            // Worker Thread: Forward to Master process via native child IPC to relay
            if (typeof process.send === 'function') {
                process.send({
                    method: 'mesh.outbound_mesh_relay',
                    payload: payload
                });
            }
        } else {
            // Master Process: Determine target sockets dynamically based on payload directives
            const targets = [];
            let routeToWorkersLocally = false;
            let routeToNetworkPeers = false;

            // Target key: server (polymorphic: single string server ID or array of string server IDs)
            const targetServers = payload.server;
            const targetWorkers = payload.workers || payload.worker;

            // Determine routing scope
            if (payload.broadcast === true) {
                 routeToWorkersLocally = true;
                 routeToNetworkPeers = true;
            } else {
                 if (targetWorkers) {
                     routeToWorkersLocally = true;
                 }
                 if (targetServers) {
                     routeToNetworkPeers = true;
                 }
                 // If neither workers nor server is specified and not broadcast, default to peers
                 if (!targetWorkers && !targetServers) {
                      routeToNetworkPeers = true;
                 }
            }

            if (routeToWorkersLocally && serverCtx) {
                // Prevent infinite loops if the message originated from a worker and was just relayed up
                try {
                     const localIpcPayload = {
                         method: 'mesh.inbound', // Standardize local delivery method
                         data: typeof payload === 'string' ? payload : JSON.stringify(payload),
                         master: false
                     };

                     if (payload.broadcast === true) {
                         localIpcPayload.broadcast = true;
                     } else if (targetWorkers) {
                         localIpcPayload.worker = targetWorkers;
                     }

                     serverCtx.send(localIpcPayload);
                } catch (err) {
                     console.error(`[@cocreate/server-mesh] Failed to relay outgoing payload locally:`, err.message);
                }
            }

            if (routeToNetworkPeers) {
                 if (payload.broadcast === true || !targetServers) {
                     // Symmetrical Cluster Broadcast: Gather all active socket tunnels
                     targets.push(...activeConnections.values());
                 } else {
                     // Multicast/Unicast Routing: Handle polymorphic servers filter (single string or array of Server IDs)
                     const serversList = Array.isArray(targetServers) 
                         ? targetServers 
                         : [targetServers];

                     for (const id of serversList) {
                         const socket = activeConnections.get(id);
                         if (socket) {
                             targets.push(socket);
                         }
                     }
                 }

                 // Deduplicate target sockets
                 const uniqueSockets = Array.from(new Set(targets));
                 
                 // Dispatch safely using standard wrapper loop
                 socketSend(uniqueSockets, payload);
            }
        }
    }

    /**
     * Coordinates the graceful shutdown sequence. Called directly from the Master server class.
     * @returns {Promise<void>} Resolves when all connections have dropped and port binds are freed
     */
    async close() {
        return closeMesh();
    }
}

const serverMeshInstance = new ServerMesh();

// Listener A: Handle peer registration of an organization subscription
serverMeshInstance.on('mesh.orgAdded', (message, ws) => {
    if (!ws) return;
    const organization_id = message.organization_id;
    const peerId = message.senderId || ws.peerId;

    if (!orgServers.has(organization_id)) {
        orgServers.set(organization_id, new Set());
    }
    orgServers.get(organization_id).add(ws);
    console.log(`[@cocreate/server-mesh] Server ${peerId} registered subscription for org "${organization_id}"`);
});

// Listener B: Handle peer de-registration of an organization subscription
serverMeshInstance.on('mesh.orgDeleted', (message, ws) => {
    if (!ws) return;
    const organization_id = message.organization_id;
    const peerId = message.senderId || ws.peerId;

    const sockets = orgServers.get(organization_id);
    if (sockets) {
        sockets.delete(ws);
        if (sockets.size === 0) {
            orgServers.delete(organization_id);
        }
        console.log(`[@cocreate/server-mesh] Server ${peerId} de-registered subscription from org "${organization_id}"`);
    }
});

// Listener C: Handle incoming real-time network unicast transaction data
serverMeshInstance.on('mesh.orgData', (message) => {
    const { organization_id, data } = message;
    
    // Local Unicast: Relay directly to local child worker threads hosting this tenant
    const activeWorkerIds = orgWorkers.get(organization_id);
    if (activeWorkerIds && activeWorkerIds.size > 0 && serverCtx) {
        try {
            serverCtx.send({
                method: 'mesh.inbound',
                workers: Array.from(activeWorkerIds),
                master: false,
                data: typeof data === 'string' ? data : JSON.stringify(data)
            });
        } catch (err) {
            console.error(`[@cocreate/server-mesh] Failed to relay incoming orgData to workers of org "${organization_id}":`, err.message);
        }
    }
});

// Listener D: Handle fallback of unhandled incoming network packets
serverMeshInstance.on('mesh.raw_message', (message) => {
    if (serverCtx) {
        try {
            serverCtx.send({
                method: 'mesh.inbound',
                broadcast: true,
                master: false,
                data: JSON.stringify(message)
            });
        } catch (err) {
            console.error("[@cocreate/server-mesh] Failed to relay raw fallback message to workers:", err.message);
        }
    }
});

/**
 * Extracts raw dataset arrays from standard database responses.
 */
function normalizePeersList(result) {
    if (!result) return [];
    if (Array.isArray(result)) return result;
    if (Array.isArray(result.object)) return result.object;
    if (Array.isArray(result.data)) return result.data;
    return [];
}

/**
 * Generates secure, ephemeral TLS credentials on-the-fly for inter-node communication.
 */
function generateTLSCredentials() {
    const tmpDir = os.tmpdir();
    const keyPath = path.join(tmpDir, `mesh-key-${Date.now()}-${crypto.randomBytes(4).toString('hex')}.pem`);
    const certPath = path.join(tmpDir, `mesh-cert-${Date.now()}-${crypto.randomBytes(4).toString('hex')}.pem`);

    try {
        execSync(`openssl req -x509 -newkey rsa:2048 -keyout "${keyPath}" -out "${certPath}" -days 1 -nodes -subj "/CN=localhost"`, { stdio: 'ignore' });
        const key = fs.readFileSync(keyPath);
        const cert = fs.readFileSync(certPath);
        
        // Cleanup generated temporary certificate files
        try { fs.unlinkSync(keyPath); } catch {}
        try { fs.unlinkSync(certPath); } catch {}
        
        return { key, cert };
    } catch (err) {
        console.error('[@cocreate/server-mesh] Failed to generate ephemeral TLS credentials:', err.message);
        return null;
    }
}

/**
 * Safely removes a closed socket from all organization maps.
 */
function removeSocketFromAllOrgServers(ws) {
    for (const [orgId, sockets] of orgServers.entries()) {
        if (sockets.has(ws)) {
            sockets.delete(ws);
            if (sockets.size === 0) {
                orgServers.delete(orgId);
            }
        }
    }
}

/**
 * Boots the socket mesh and configures routing tunnels.
 * 
 * @param {Object} server - CoCreateServer instance context
 */
export async function init(server) {
    serverCtx = server;

    const isPrimary = serverCtx.isPrimary;

    if (!isPrimary) {
        // --- WORKER EVENT PIPELINE ---
        // Monitor process IPC from Primary
        process.on('message', (message) => {
            if (message && message.method) {
                serverMeshInstance.emit(message.method, message);
            }
        });
        return;
    }

    // --- MASTER TUNNEL & UNICAST ROUTING PIPELINE ---
    // Handle incoming IPC registrations from child workers
    cluster.on('message', (workerRef, message) => {
        if (message && message.method) {
            if (message.method === 'mesh.registerOrg') {
                const { organization_id, workerId } = message;
                let isNewOrg = false;

                if (!orgWorkers.has(organization_id)) {
                    orgWorkers.set(organization_id, new Set());
                    isNewOrg = true;
                }

                orgWorkers.get(organization_id).add(workerId);
                console.log(`[@cocreate/server-mesh] [LOCAL-MAP] Tenant "${organization_id}" registered on Worker ${workerId}`);

                if (isNewOrg) {
                    serverMeshInstance.send({
                        method: 'mesh.orgAdded',
                        organization_id,
                        senderId: serverCtx.id
                    });
                }
            } 
            else if (message.method === 'mesh.deregisterOrg') {
                const { organization_id, workerId } = message;
                const workersSet = orgWorkers.get(organization_id);

                if (workersSet) {
                    workersSet.delete(workerId);
                    if (workersSet.size === 0) {
                        orgWorkers.delete(organization_id);
                        console.log(`[@cocreate/server-mesh] [LOCAL-MAP] Tenant "${organization_id}" evicted. No remaining local workers.`);
                        serverMeshInstance.send({
                            method: 'mesh.orgDeleted',
                            organization_id,
                            senderId: serverCtx.id
                        });
                    }
                }
            } 
            else if (message.method === 'mesh.orgData') {
                const { organization_id, data, senderWorkerId } = message;

                // Local Unicast: Relay directly to child worker cores hosting the tenant (ignoring sender)
                const activeWorkerIds = orgWorkers.get(organization_id);
                if (activeWorkerIds && activeWorkerIds.size > 0) {
                    const siblingWorkerIds = Array.from(activeWorkerIds).filter(wId => wId !== senderWorkerId);
                    if (siblingWorkerIds.length > 0) {
                        try {
                            serverCtx.send({
                                method: 'mesh.inbound',
                                workers: siblingWorkerIds,
                                master: false,
                                data: typeof data === 'string' ? data : JSON.stringify(data)
                            });
                        } catch (err) {
                            console.error("[@cocreate/server-mesh] Failed to propagate orgData to local siblings:", err.message);
                        }
                    }
                }

                // Network Unicast: Relay over secure tunnels to peer servers hosting the tenant
                const targetSockets = orgServers.get(organization_id);
                if (targetSockets && targetSockets.size > 0) {
                    const envelope = {
                        method: 'mesh.orgData',
                        organization_id,
                        data
                    };
                    socketSend(Array.from(targetSockets), envelope);
                }
            } 
            else if (message.method === 'mesh.outbound_mesh_relay') {
                // Relay child worker message out to peer nodes
                if (message.payload) {
                    serverMeshInstance.send(message.payload);
                }
            }
            
            // Mirror all incoming process IPC commands to local listeners
            serverMeshInstance.emit(message.method, message, workerRef);
        }
    });

    // Boot HTTPS Secure Server
    const credentials = generateTLSCredentials();
    if (credentials) {
        secureMeshServer = https.createServer(credentials);
        secureMeshServer.on('error', (err) => {
            if (err.code === 'EADDRINUSE') {
                console.error(`\n[@cocreate/server-mesh] [FATAL] Port ${MESH_PORT} is already in use!`);
                process.exit(1);
            } else {
                console.error(`[@cocreate/server-mesh] Mesh server error:`, err);
            }
        });

        secureMeshServer.listen(MESH_PORT, '0.0.0.0', () => {
            console.log(`[@cocreate/server-mesh] [PRIMARY] Mesh securely listening on 0.0.0.0:${MESH_PORT}`);
        });
    } else {
        throw new Error("Unable to boot secure mesh: TLS credential generation failed.");
    }

    meshWss = new WebSocketServer({ server: secureMeshServer });

    meshWss.on('connection', (ws, req) => {
        const ip = req.socket.remoteAddress;
        let remoteServerId = null;

        const protocolHeader = req.headers['sec-websocket-protocol'];
        if (protocolHeader) {
            try {
                const parsed = JSON.parse(decodeURIComponent(protocolHeader));
                remoteServerId = parsed.id;
            } catch (err) {
                // Ignore decoding anomalies gracefully
            }
        }

        if (!remoteServerId) {
            console.warn(`[@cocreate/server-mesh] Rejecting inbound mesh connection from ${ip}: Missing Server ID in protocol header.`);
            ws.close();
            return;
        }

        console.log(`[@cocreate/server-mesh] Secure peer connection established: Server ID ${remoteServerId} (IP: ${ip})`);
        
        ws.peerId = remoteServerId;
        ws.isAlive = true;
        ws.on('pong', () => { ws.isAlive = true; });

        // Inlined Blind Event-Dispatch Loop: If no method, do nothing.
        ws.on('message', (rawData) => {
            try {
                const message = JSON.parse(rawData.toString());
                if (message && message.method) {
                    serverMeshInstance.emit(message.method, message, ws);
                }
            } catch (err) {
                // Safely catch parsing failures
            }
        });

        ws.on('close', () => {
            console.log(`[@cocreate/server-mesh] Peer connection ${ws.peerId} dropped.`);
            activeConnections.delete(ws.peerId);
            removeSocketFromAllOrgServers(ws);
        });
        
        ws.on('error', (err) => {
            console.error(`[@cocreate/server-mesh] Connection error (Server ${ws.peerId}):`, err.message);
        });
        
        activeConnections.set(remoteServerId, ws);
    });

    try {
        await getServers();
    } catch (err) {
        console.error("[@cocreate/server-mesh] Peer discovery reconciliation cycle failed on start:", err.message);
    }
    
    wsPingInterval = setInterval(pingActiveWebsockets, 30000);
}

/**
 * Registers this server instance once on startup to allow discovery.
 */
async function registerServer() {
    if (isGracefulExitRunning || !serverCtx?.crud) return;

    const registerPayload = {
        method: 'object.update',
        array: 'servers',
        object: {
            _id: serverCtx.id,
            ip: serverCtx.ip,
            host: serverCtx.host,
            status: 'active',
            infrastructure: serverCtx.infrastructure
        },
        upsert: true,
        organization_id: serverCtx.organization_id,
        host: serverCtx.host
    };

    try {
        const registerResult = await serverCtx.crud.send(registerPayload);
        if (registerResult && registerResult.error) {
            console.error(`[@cocreate/server-mesh] Startup registration rejected: ${JSON.stringify(registerResult.error)}`);
        } else {
            console.log(`[@cocreate/server-mesh] [PRIMARY] Successfully registered/updated server profile in db.`);
        }
    } catch (err) {
        console.error("[@cocreate/server-mesh] Startup registration network error:", err.message);
    }
}

/**
 * Monitors responsiveness of active WebSocket connections.
 */
function pingActiveWebsockets() {
    if (isGracefulExitRunning || !meshWss) return;

    meshWss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            console.warn(`[@cocreate/server-mesh] Client socket unresponsive. Terminating connection.`);
            return ws.terminate();
        }
        ws.isAlive = false;
        try {
            ws.ping();
        } catch (err) {
            console.error("[@cocreate/server-mesh] Error during websocket ping:", err.message);
        }
    });
}

/**
 * Cleanly handles node evictions and releases network bindings.
 * Rewritten to support fully coordinated async invocation from Primary Server controller without process.exit
 * @returns {Promise<void>} Resolves when the HTTPS mesh server port is successfully released.
 */
async function closeMesh() {
    if (isGracefulExitRunning) return;
    isGracefulExitRunning = true;
    
    console.log(`[@cocreate/server-mesh] Coordinated secure mesh shutdown triggered. Evicting state...`);

    clearInterval(wsPingInterval);

    if (serverCtx?.crud && serverCtx.id && serverCtx.organization_id) {
        try {
            await serverCtx.crud.send({
                method: 'object.update',
                array: 'servers',
                object: { 
                    _id: serverCtx.id,
                    status: 'inactive'
                },
                organization_id: serverCtx.organization_id,
                host: serverCtx.host
            });
            console.log(`[@cocreate/server-mesh] Server status set to inactive in database.`);
        } catch (err) {
            console.error("[@cocreate/server-mesh] Failed to update server status cleanly in DB:", err.message);
        }
    }

    for (const timerId of retryTimers.values()) clearTimeout(timerId);
    retryTimers.clear();

    const uniqueSockets = new Set(activeConnections.values());
    for (const ws of uniqueSockets) {
        if (ws && ws.readyState === WebSocket.OPEN) {
            try { ws.close(); } catch {}
        }
    }
    activeConnections.clear();

    if (meshWss) {
        try { meshWss.close(); } catch {}
    }

    return new Promise((resolve) => {
        if (secureMeshServer) {
            secureMeshServer.close(() => {
                console.log(`[@cocreate/server-mesh] Secure inter-node port ${MESH_PORT} successfully released.`);
                resolve();
            });
        } else {
            resolve();
        }
    });
}

/**
 * Calculates exponential backoff delay with a randomized jitter ("blip")
 * to prevent synchronized thundering-herd reconnection storms across peers.
 * 
 * @param {number} attempt - Current retry attempt count
 * @returns {number} Delay in milliseconds
 */
function getJitteredRetryDelay(attempt) {
    const baseDelay = Math.min(30000, 1000 * Math.pow(1.5, attempt));
    // Randomized blip between 500ms and 4500ms
    const blip = Math.floor(Math.random() * 4000) + 500;
    return baseDelay + blip;
}

/**
 * Scans for peer nodes and establishes persistent, self-healing connections with jittered retries.
 */
async function getServers() {
    if (isGracefulExitRunning || !serverCtx?.crud) return;

    // Register profile once on boot
    await registerServer();

    const readPayload = {
        method: 'object.read',
        array: 'servers',
        $filter: {
            query: { 
                _id: { $ne: serverCtx.id },
                status: { $ne: 'stopped' }
            },
            sort: { _id: 1 },
            limit: 0
        },
        organization_id: serverCtx.organization_id,
        host: serverCtx.host
    };

    let readResult;
    try {
        readResult = await serverCtx.crud.send(readPayload);
    } catch (err) {
        console.error("[@cocreate/server-mesh] Failed to fetch active peer server records:", err.message);
        return;
    }

    const peers = normalizePeersList(readResult);

    for (const targetPeer of peers) {
        const targetId = targetPeer._id;
        const targetIp = targetPeer.ip;

        if (!targetId || targetId === serverCtx.id) continue;
        if (activeConnections.has(targetId) || retryTimers.has(targetId)) continue;

        console.log(`[@cocreate/server-mesh] Initiating secure connection stream to target Server ID: ${targetId} (IP: ${targetIp})`);

        let retryCount = 0;

        const connectToPeer = () => {
            if (isGracefulExitRunning) return; 

            const targetUrl = `wss://${targetIp}:${MESH_PORT}/${serverCtx.organization_id}`;
            const clientSocket = new WebSocket(targetUrl, [
                encodeURIComponent(JSON.stringify({
                    type: 'peer',
                    ip: serverCtx.ip,
                    id: serverCtx.id,
                    token: process.env.CLUSTER_SECRET,
                    sniHost: serverCtx.host
                }))
            ], {
                servername: serverCtx.host,
                rejectUnauthorized: false
            });

            clientSocket.isAlive = true;
            clientSocket.on('pong', () => { clientSocket.isAlive = true; });
            clientSocket.on('ping', () => { clientSocket.pong(); });

            clientSocket.on('open', () => {
                console.log(`[@cocreate/server-mesh] Secure link complete to Server ID: ${targetId}`);
                retryCount = 0;
                
                if (retryTimers.has(targetId)) clearTimeout(retryTimers.get(targetId));
                retryTimers.delete(targetId);
                
                clientSocket.isPeer = true;
                clientSocket.isPlatformTunnel = true;
                clientSocket.peerId = targetId;
                
                activeConnections.set(targetId, clientSocket);
            });

            clientSocket.on('close', () => {
                activeConnections.delete(targetId);
                retryTimers.delete(targetId);
                removeSocketFromAllOrgServers(clientSocket);
                
                if (isGracefulExitRunning) return;

                retryCount++;
                const delay = getJitteredRetryDelay(retryCount);
                console.warn(`[@cocreate/server-mesh] Peer ${targetId} disconnected. Retrying in ${delay}ms (Attempt #${retryCount})...`);
                
                const timerId = setTimeout(connectToPeer, delay);
                retryTimers.set(targetId, timerId);
            });

            // Inlined Blind Event-Dispatch Loop
            clientSocket.on('message', (rawData) => {
                try {
                    const message = JSON.parse(rawData.toString());
                    if (message && message.method) {
                        serverMeshInstance.emit(message.method, message, clientSocket);
                    }
                } catch (err) {
                    // Safely catch parsing failures
                }
            });

            clientSocket.on('error', (err) => {
                console.log(`[@cocreate/server-mesh] Socket transport failure (Server ${targetId}): ${err.message}`);
            });
        };

        connectToPeer();
    }
}

// Attach init directly to the instance to allow standard default-import invocation
serverMeshInstance.init = init;

// Default export matches standard functional entry contract
export default serverMeshInstance;