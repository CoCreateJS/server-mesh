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

// Registry tracking active peer WebSocket connections (serverId/IP -> WebSocket)
const activeConnections = new Map();

// Registry of scheduled reconnect timers to allow cancellation (peerIp -> Timeout ID)
const retryTimers = new Map();

// Registry of scheduled DB prune operations to allow cancellation (peerIp -> Timeout ID)
const pruneTimers = new Map();

// Local-Tier Routing Map: Matches active organizations to local worker IDs
// Key: organization_id -> Value: Set(workerId)
const orgWorkers = new Map();

// Network-Tier Routing Map: Matches active organizations to connected peer sockets
// Key: organization_id -> Value: Set(WebSocket)
const orgServers = new Map();

// Execution flags to block overlapping async operations
let isReconciliationRunning = false;
let isGracefulExitRunning = false;

// Interval heartbeats and ping timers
let dbHeartbeatInterval = null;
let wsPingInterval = null;

let serverCtx = null;
const MESH_PORT = process.env.MESH_PORT || 8090;
let meshWss = null;
let secureMeshServer = null;

/**
 * Safely normalizes Mongo/BSON/JSON ObjectId definitions to standard 24-character strings.
 * Ensures strict JavaScript type parity ('===') during sorting and matches.
 */
function getObjectIdString(idVal) {
    if (!idVal) return '';
    if (typeof idVal === 'string') return idVal;
    if (idVal.$oid) return idVal.$oid;
    if (typeof idVal.toString === 'function') return idVal.toString();
    return String(idVal);
}

/**
 * Extracts the raw dataset array from standard CRUD server responses.
 * Accommodates variations across Point-Queries, Data envelopes, and arrays.
 */
function normalizePeersList(result) {
    if (!result) return [];
    if (Array.isArray(result)) return result;
    if (Array.isArray(result.object)) return result.object;
    if (Array.isArray(result.data)) return result.data;
    return [];
}

/**
 * Generates self-signed TLS credentials on-the-fly for secure inter-node cluster tunnels.
 */
function generateTLSCredentials() {
    const tmpDir = os.tmpdir();
    const keyPath = path.join(tmpDir, `mesh-key-${Date.now()}.pem`);
    const certPath = path.join(tmpDir, `mesh-cert-${Date.now()}.pem`);

    try {
        // Pipe-free generation using standard OpenSSL commands to avoid Node.js /dev/stdin crash
        execSync(`openssl req -x509 -newkey rsa:2048 -keyout ${keyPath} -out ${certPath} -days 1 -nodes -subj "/CN=localhost"`, { stdio: 'ignore' });
        
        const key = fs.readFileSync(keyPath);
        const cert = fs.readFileSync(certPath);
        
        // Clean up immediately (Zero-Persistence)
        fs.unlinkSync(keyPath);
        fs.unlinkSync(certPath);

        return { key, cert };
    } catch (err) {
        console.error('[@cocreate/socket-mesh] Failed to generate ephemeral TLS credentials:', err);
        return null;
    }
}

/**
 * Maps a peer's socket connection directly to an organization ID subscription.
 */
function registerPeerSubscription(peerId, organization_id) {
    const ws = activeConnections.get(peerId);
    if (!ws) return;

    if (!orgServers.has(organization_id)) {
        orgServers.set(organization_id, new Set());
    }
    orgServers.get(organization_id).add(ws);
    console.log(`[@cocreate/socket-mesh] Peer ${peerId} registered for org "${organization_id}"`);
}

/**
 * Removes a peer's socket connection from an organization ID subscription map.
 */
function deregisterPeerSubscription(peerId, organization_id) {
    const ws = activeConnections.get(peerId);
    if (!ws) return;

    const sockets = orgServers.get(organization_id);
    if (sockets) {
        sockets.delete(ws);
        if (sockets.size === 0) {
            orgServers.delete(organization_id);
        }
        console.log(`[@cocreate/socket-mesh] Peer ${peerId} de-registered from org "${organization_id}"`);
    }
}

/**
 * Sweeps and cleans all subscriptions for a dropped WebSocket node connection.
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
 * Broadcasts one-way control frames notifying active peer servers about our state changes.
 */
function notifyPeersOfOrganizationState(method, organization_id) {
    const payloadStr = JSON.stringify({
        method,
        organization_id,
        serverId: serverCtx.id
    });
    for (const ws of activeConnections.values()) {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(payloadStr);
        }
    }
}

/**
 * Handshake Sync: Transmits our currently active local organizations to a peer connection.
 * Instead of custom formats, we reuse the exact 'mesh.orgAdded' event structure.
 */
function syncActiveOrganizationsToSocket(ws) {
    for (const orgId of orgWorkers.keys()) {
        const payloadStr = JSON.stringify({
            method: 'mesh.orgAdded',
            organization_id: orgId,
            serverId: serverCtx.id
        });
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(payloadStr);
        }
    }
}

export async function init(server) {
    serverCtx = server;

    // Handle Graceful exit hooks to properly delete DB records and release ports
    process.on('SIGINT', handleGracefulShutdown);
    process.on('SIGTERM', handleGracefulShutdown);

    const isPrimary = serverCtx.isPrimary;

    if (!isPrimary) {
        // --- WORKER EVENT PIPELINE ---
        console.log(`[@cocreate/socket-mesh] Process ${process.pid} is Worker ${serverCtx.workerId}. Setting up event listeners.`);
        
        serverCtx.on('mesh.inbound', (payload) => {
            serverCtx.wsManager.broadcast(payload.data, { isForwarded: true });
        });
        return;
    }

    // --- MASTER TUNNEL & DUAL-TIER UNICAST ROUTING PIPELINE ---
    console.log(`[@cocreate/socket-mesh] Primary Orchestrator running on PID ${process.pid}. Initializing secure mesh...`);
    
    // Listen to local worker registrations
    serverCtx.on('mesh.registerOrg', (payload) => {
        const { organization_id, workerId } = payload;
        
        let isNewOrganizationOnServer = false;
        if (!orgWorkers.has(organization_id)) {
            orgWorkers.set(organization_id, new Set());
            isNewOrganizationOnServer = true;
        }
        
        orgWorkers.get(organization_id).add(workerId);
        console.log(`[@cocreate/socket-mesh] [LOCAL-MAP] Org "${organization_id}" mapped to Worker ${workerId}`);

        if (isNewOrganizationOnServer) {
            notifyPeersOfOrganizationState('mesh.orgAdded', organization_id);
        }
    });

    // Listen to local worker deregistrations
    serverCtx.on('mesh.deregisterOrg', (payload) => {
        const { organization_id, workerId } = payload;

        const workersSet = orgWorkers.get(organization_id);
        workersSet.delete(workerId);
        if (workersSet.size === 0) {
            orgWorkers.delete(organization_id);
            console.log(`[@cocreate/socket-mesh] [LOCAL-MAP] Org "${organization_id}" evicted. No remaining local workers.`);
            
            notifyPeersOfOrganizationState('mesh.orgDeleted', organization_id);
        }
    });

    // Dynamic dual-tier unicast mapping execution
    serverCtx.on('mesh.orgData', (payload) => {
        const { organization_id, data, senderWorkerId } = payload;

        // Step A: Distribute locally to other worker processes (Local Unicast - Excluding the original sender!)
        const activeWorkerIds = orgWorkers.get(organization_id);
        if (activeWorkerIds && activeWorkerIds.size > 0) {
            const siblingWorkerIds = Array.from(activeWorkerIds).filter(wId => wId !== senderWorkerId);
            if (siblingWorkerIds.length > 0) {
                serverCtx.send({
                    method: 'mesh.inbound',
                    workers: siblingWorkerIds,
                    master: false,
                    data: typeof data === 'string' ? data : JSON.stringify(data)
                });
            }
        }

        // Step B: Relay across P2P tunnels to connected peer servers hosting this tenant (Network Unicast)
        const targetSockets = orgServers.get(organization_id);
        if (targetSockets && targetSockets.size > 0) {
            const envelope = JSON.stringify({
                method: 'mesh.orgData',
                organization_id,
                data
            });
            for (const ws of targetSockets) {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(envelope);
                }
            }
        }
    });

    const credentials = generateTLSCredentials();
    
    if (credentials) {
        secureMeshServer = https.createServer(credentials);
        secureMeshServer.on('error', (err) => {
            if (err.code === 'EADDRINUSE') {
                console.error(`\n[@cocreate/socket-mesh] [FATAL] Port ${MESH_PORT} is already in use!`);
                process.exit(1);
            } else {
                console.error(`[@cocreate/socket-mesh] Mesh server error:`, err);
            }
        });

        secureMeshServer.listen(MESH_PORT, '0.0.0.0', () => {
            console.log(`[@cocreate/socket-mesh] Mesh securely listening on 0.0.0.0:${MESH_PORT}`);
        });
    } else {
        throw new Error("Unable to boot secure mesh: TLS credential generation failed.");
    }

    meshWss = new WebSocketServer({ server: secureMeshServer });

    meshWss.on('connection', (ws, req) => {
        const ip = req.socket.remoteAddress;
        let remoteServerId = null;

        // Decode subprotocol header to resolve serverId identification directly on handshake
        const protocolHeader = req.headers['sec-websocket-protocol'];
        if (protocolHeader) {
            const parsed = JSON.parse(decodeURIComponent(protocolHeader));
            remoteServerId = parsed.id;
        }

        const identifier = remoteServerId || ip;
        console.log(`[@cocreate/socket-mesh] Incoming peer connection established with identifier: ${identifier}`);
        
        ws.isAlive = true;
        ws.on('pong', () => {
            ws.isAlive = true;
        });

        ws.on('message', (message) => {
            handleIncomingMessage(message, ip, ws);
        });

        ws.on('close', () => {
            console.log(`[@cocreate/socket-mesh] Peer connection ${identifier} dropped.`);
            activeConnections.delete(identifier);
            if (remoteServerId) activeConnections.delete(ip);
            removeSocketFromAllOrgServers(ws);
        });
        
        ws.on('error', (err) => {
            console.error(`[@cocreate/socket-mesh] Peer connection error (${identifier}):`, err.message);
        });
        
        activeConnections.set(identifier, ws);
        if (remoteServerId) {
            activeConnections.set(ip, ws); // Map both for immediate index lookups
        }

        // Symmetric Handshake Exchange: On connection, push our active org list as orgAdded frames
        syncActiveOrganizationsToSocket(ws);
    });

    // Run Symmetrical Startup Connection Sync EXACTLY ONCE on boot.
    await reconcilePeerConnections();
    
    // Set up database-only heartbeat updates (write-only, zero database reads!)
    dbHeartbeatInterval = setInterval(publishDatabaseHeartbeat, 30000); 

    // Set up network-level WebSocket ping/pong checks (fully decentralized, zero DB overhead!)
    wsPingInterval = setInterval(pingActiveWebsockets, 30000);
}

/**
 * Executes a lightweight DB-only write heartbeat to register/keep our presence warm.
 * This runs with ZERO read queries, keeping database load near non-existent.
 */
async function publishDatabaseHeartbeat() {
    if (isGracefulExitRunning) return;

    const registerPayload = {
        method: 'object.update',
        array: 'servers',
        upsert: true,
        object: {
            _id: serverCtx.id,
            ip: serverCtx.ip,
            host: serverCtx.host,
            status: 'active',
            infrastructure: serverCtx.infrastructure,
            lastSeen: new Date().toISOString()
        },
        organization_id: serverCtx.organization_id,
        host: serverCtx.host
    };

    const registerResult = await serverCtx.crud.send(registerPayload);
    if (registerResult && registerResult.error) {
        throw new Error(`[@cocreate/socket-mesh] Heartbeat registration rejected: ${JSON.stringify(registerResult.error)}`);
    }
}

/**
 * Pings all connected server sockets to quickly catch half-open TCP failures.
 * This ensures silent network disconnects are cleaned up without hitting any database.
 */
function pingActiveWebsockets() {
    if (isGracefulExitRunning || !meshWss) return;

    meshWss.clients.forEach((ws) => {
        if (ws.isAlive === false) {
            console.warn(`[@cocreate/socket-mesh] Client socket unresponsive. Terminating connection.`);
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });
}

/**
 * Executes a full graceful shutdown: wipes our DB record, clears intervals, and frees the network ports
 */
async function handleGracefulShutdown() {
    if (isGracefulExitRunning) return;
    isGracefulExitRunning = true;
    
    console.log(`\n[@cocreate/socket-mesh] Graceful shutdown initiated. Cleaning up mesh state...`);

    // Clear all recurring utility intervals
    clearInterval(dbHeartbeatInterval);
    clearInterval(wsPingInterval);

    // 1. Unregister from the database to prevent ghost/zombie nodes
    if (serverCtx && serverCtx.crud && serverCtx.id && serverCtx.organization_id) {
        await serverCtx.crud.send({
            method: 'object.delete',
            array: 'servers',
            object: { _id: serverCtx.id },
            organization_id: serverCtx.organization_id,
            host: serverCtx.host
        });
        console.log(`[@cocreate/socket-mesh] Successfully wiped server ${serverCtx.id} from DB.`);
    }

    // 2. Clear all active retry and prune timers
    for (const timerId of retryTimers.values()) clearTimeout(timerId);
    for (const timerId of pruneTimers.values()) clearTimeout(timerId);
    retryTimers.clear();
    pruneTimers.clear();

    // 3. Close all active outbound P2P sockets
    for (const [ip, ws] of activeConnections.entries()) {
        if (ws.readyState === WebSocket.OPEN) ws.close();
    }
    activeConnections.clear();

    // 4. Release Port and exit
    meshWss.close();
    secureMeshServer.close(() => {
        console.log(`[@cocreate/socket-mesh] Port ${MESH_PORT} successfully released.`);
        process.exit(0);
    });

    // Failsafe: Force kill if the DB/Sockets hang for more than 3 seconds
    setTimeout(() => {
        console.warn('[@cocreate/socket-mesh] Graceful exit timed out. Forcing exit.');
        process.exit(1);
    }, 3000).unref();
}

/**
 * Resolves the global network cluster state and orchestrates direct socket links.
 * Employs a chronological sort logic based on server database IDs to prevent routing loops.
 * This is executed ONCE on startup.
 */
async function reconcilePeerConnections() {
    if (isReconciliationRunning || isGracefulExitRunning || !serverCtx.crud) return;
    isReconciliationRunning = true;

    console.log(`[@cocreate/socket-mesh] Starting startup peer reconciliation...`);

    // Check 1: Ensure IP address is active
    if (!serverCtx.ip) {
        throw new Error(`[@cocreate/socket-mesh] Assertion failed: Server core IP address is unresolved.`);
    }

    // Clean up legacy zombie servers matching our specific infrastructure instanceId
    const instanceId = serverCtx.infrastructure?.instanceId;
    if (instanceId) {
        await serverCtx.crud.send({
            method: 'object.delete',
            array: 'servers',
            $filter: {
                query: { 'infrastructure.instanceId': instanceId },
                limit: 0
            },
            organization_id: serverCtx.organization_id,
            host: serverCtx.host
        });
    }

    // Keep our registration fresh right before query
    await publishDatabaseHeartbeat();

    // Query to read registered servers inside the organization scope
    // Strictly targeting peers with an ID less than ours to prevent socket loops.
    const readPayload = {
        method: 'object.read',
        array: 'servers',
        $filter: {
            query: { _id: { $lt: serverCtx.id } },
            sort: { _id: 1 }, // Sort ascending
            limit: 0
        },
        organization_id: serverCtx.organization_id,
        host: serverCtx.host
    };

    const readResult = await serverCtx.crud.send(readPayload);
    const peers = normalizePeersList(readResult);

    // Negotiate outbound connections strictly to the retrieved lower-ID peers
    for (const targetPeer of peers) {
        const targetIp = targetPeer.ip;

        // Self-IP Guard: Never attempt to open a socket tunnel to our own IP address
        if (targetIp === serverCtx.ip) continue;
        if (activeConnections.has(targetIp)) continue; // Tunnel is already warm

        console.log(`[@cocreate/socket-mesh] Opening socket tunnel to priority peer: ${targetIp}`);

        let retryCount = 0;
        const maxRetries = 10;

        const connectToPeer = () => {
            if (isGracefulExitRunning) return; 

            // Symmetrical tunnel configuration pointing directly to secure WS endpoint
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
                servername: serverCtx.host, // Map TLS SNI explicitly
                rejectUnauthorized: false // Development safety fallback for direct IP-to-IP bridges
            });

            clientSocket.isAlive = true;
            clientSocket.on('pong', () => {
                clientSocket.isAlive = true;
            });
            clientSocket.on('ping', () => {
                clientSocket.pong();
            });

            clientSocket.on('open', () => {
                console.log(`[@cocreate/socket-mesh] Tunnel successfully established to peer: ${targetIp}`);
                retryCount = 0;
                
                // Clear pending timers
                if (retryTimers.has(targetIp)) clearTimeout(retryTimers.get(targetIp));
                if (pruneTimers.has(targetIp)) clearTimeout(pruneTimers.get(targetIp));
                
                retryTimers.delete(targetIp);
                pruneTimers.delete(targetIp);
                
                clientSocket.isPeer = true;
                clientSocket.isPlatformTunnel = true;
                
                // Store the established connection inside our connections lookup registry
                activeConnections.set(targetPeer._id, clientSocket);
                activeConnections.set(targetIp, clientSocket);

                // Symmetric Handshake Exchange: On connection open, push our active org list down the tunnel
                syncActiveOrganizationsToSocket(clientSocket);
            });

            clientSocket.on('close', () => {
                activeConnections.delete(targetPeer._id);
                activeConnections.delete(targetIp);
                retryTimers.delete(targetIp);
                removeSocketFromAllOrgServers(clientSocket);
                
                if (isGracefulExitRunning) return;

                if (retryCount < maxRetries) {
                    retryCount++;
                    // Reconnect Jitter: 0 to 2 seconds of random delay to stagger reconnects
                    const jitter = Math.floor(Math.random() * 2000);
                    const delay = 5000 + jitter;
                    
                    console.warn(`[@cocreate/socket-mesh] Tunnel closed to peer: ${targetIp}. Retrying (${retryCount}/${maxRetries}) in ${delay}ms...`);
                    const timerId = setTimeout(connectToPeer, delay);
                    retryTimers.set(targetIp, timerId);
                } else {
                    // Thundering Herd Prevention: Stagger deletions using relative index
                    const staggerDelay = peers.length * 1000;
                    console.warn(`[@cocreate/socket-mesh] Peer ${targetIp} dead. Scheduling DB prune in ${staggerDelay}ms...`);
                    
                    const pruneTimerId = setTimeout(() => {
                        console.error(`[@cocreate/socket-mesh] Executing DB prune for dead peer: ${targetIp}`);
                        
                        serverCtx.crud.send({
                            method: 'object.delete',
                            array: 'servers',
                            object: { _id: targetPeer._id },
                            organization_id: serverCtx.organization_id,
                            host: serverCtx.host
                        });
                        
                        // Broadcast fast mesh control message to tell others to cancel prune timers
                        const cancelPayload = {
                            type: 'mesh.peer_pruned',
                            deadIp: targetIp,
                            isForwarded: true,
                            originatorId: serverCtx.id
                        };
                        const payloadStr = JSON.stringify(cancelPayload);
                        
                        activeConnections.forEach(peerSocket => {
                            if (peerSocket.readyState === WebSocket.OPEN) peerSocket.send(payloadStr);
                        });
                        
                        pruneTimers.delete(targetIp);
                    }, staggerDelay);
                    
                    pruneTimers.set(targetIp, pruneTimerId);
                }
            });

            clientSocket.on('message', (rawData) => {
                try {
                    const msg = JSON.parse(rawData);
                    
                    // Intercept fast mesh control messages (e.g., prune cancellations)
                    if (msg.type === 'mesh.peer_pruned' && msg.deadIp) {
                        const activeTimerId = pruneTimers.get(msg.deadIp);
                        if (activeTimerId) {
                            clearTimeout(activeTimerId);
                            pruneTimers.delete(msg.deadIp);
                            console.log(`[@cocreate/socket-mesh] Cancelled DB prune for ${msg.deadIp} due to cluster consensus.`);
                        }
                        return; // Handled internally
                    }
                } catch(e) { }

                handleIncomingMessage(rawData, targetIp, clientSocket);
            });

            clientSocket.on('error', (err) => {
                console.log(`[@cocreate/socket-mesh] Error on connection to ${targetIp}: ${err.message}`);
            });
        };

        // Trigger the initial outbound connection attempt
        connectToPeer();
    }

    isReconciliationRunning = false;
}

/**
 * Directs inbound network payloads down to local worker threads.
 */
function handleIncomingMessage(rawData, senderIp, ws) {
    const message = JSON.parse(rawData.toString());

    if (message.method === 'mesh.orgData' && message.organization_id) {
        // Incoming real-time transaction from a sibling cluster node.
        // Dispatch locally to ALL local worker processes hosting active sockets for this organization.
        const activeWorkerIds = orgWorkers.get(message.organization_id);
        if (activeWorkerIds && activeWorkerIds.size > 0) {
            serverCtx.send({
                method: 'mesh.inbound',
                workers: Array.from(activeWorkerIds),
                master: false,
                data: typeof message.data === 'string' ? message.data : JSON.stringify(message.data)
            });
        }

        // Also forward to Master's wsManager in case Master is running in local-connection mode
        if (serverCtx.wsManager) {
            serverCtx.wsManager.broadcast(message.data, { isForwarded: true });
        }
    } 
    else if (message.method === 'mesh.orgAdded' && message.organization_id) {
        // Register subscription state for the remote peer server (Runtime and Handshake sync use this exact block!)
        registerPeerSubscription(message.serverId || senderIp, message.organization_id);
    } 
    else if (message.method === 'mesh.orgDeleted' && message.organization_id) {
        // Deregister subscription state for the remote peer server
        deregisterPeerSubscription(message.serverId || senderIp, message.organization_id);
    } 
    else {
        // General Fallback Broadcast
        serverCtx.send({
            method: 'mesh.inbound',
            broadcast: true,
            master: false,
            data: rawData.toString()
        });
        if (serverCtx.wsManager) {
            serverCtx.wsManager.broadcast(rawData, { isForwarded: true });
        }
    }
}

/**
 * Public network broadcast interface.
 */
export function send(payload) {
    const isPrimary = serverCtx.isPrimary;

    // Route upstream through Master process if called inside a Worker thread
    if (!isPrimary) {
        serverCtx.send({
            method: 'mesh.outbound',
            master: true,
            data: typeof payload === 'string' ? payload : JSON.stringify(payload)
        });
        return;
    }

    // Direct network broadcast to all peer nodes
    const dataStr = typeof payload === 'string' ? payload : JSON.stringify(payload);
    for (const ws of activeConnections.values()) {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(dataStr);
        }
    }
}

export default {
    init,
    send
};