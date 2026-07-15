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
 ********************************************************************************/

import { WebSocketServer, WebSocket } from 'ws';
import https from 'node:https';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { execSync } from 'node:child_process';

// Global memory registry of active peer WebSocket tunnel connections (peerIp -> WebSocket)
const activeConnections = new Map();

// Registry of active retry timers to allow cancellation (peerIp -> Timeout ID)
const retryTimers = new Map();

// Registry of scheduled DB prune operations to allow cancellation (peerIp -> Timeout ID)
const pruneTimers = new Map();

// Control locks to block overlapping reconciliation steps
let isReconciliationRunning = false;
let isGracefulExitRunning = false;

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

export async function init(server) {
    serverCtx = server;

    // Handle Graceful exit hooks to properly delete DB records and release ports
    process.on('SIGINT', handleGracefulShutdown);
    process.on('SIGTERM', handleGracefulShutdown);

    // If this is a worker process, we act as a lightweight proxy stub
    if (!serverCtx.isPrimary) {
        console.log(`[@cocreate/socket-mesh] Worker ${process.pid} acting as lightweight proxy stub.`);
        return;
    }

    console.log(`[@cocreate/socket-mesh] Primary Orchestrator initializing secure mesh on port ${MESH_PORT}...`);
    
    const credentials = generateTLSCredentials();
    
    if (credentials) {
        secureMeshServer = https.createServer(credentials);
        secureMeshServer.on('error', (err) => {
            if (err.code === 'EADDRINUSE') {
                console.error(`\n[@cocreate/socket-mesh] [FATAL] Port ${MESH_PORT} is already in use!`);
                console.error(`This usually means a previous instance crashed and didn't release the port.`);
                console.error(`Run 'sudo killall -9 node' or 'sudo fuser -k ${MESH_PORT}/tcp' to clear it, then restart.\n`);
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
        console.log(`[@cocreate/socket-mesh] Incoming peer connection from ${ip}`);
        
        ws.on('message', (message) => {
            handleIncomingMessage(message, ip);
        });

        ws.on('close', () => {
            console.log(`[@cocreate/socket-mesh] Peer ${ip} disconnected.`);
            activeConnections.delete(ip);
        });
        
        ws.on('error', (err) => {
            console.error(`[@cocreate/socket-mesh] Peer ${ip} connection error:`, err.message);
        });
        
        activeConnections.set(ip, ws);
    });

    await reconcilePeerConnections();
    
    // Set up heartbeat and zombie pruning interval (every 30 seconds)
    setInterval(reconcilePeerConnections, 30000); 
}

/**
 * Executes a full graceful shutdown: wipes the DB record and frees the network ports
 */
async function handleGracefulShutdown() {
    if (isGracefulExitRunning) return;
    isGracefulExitRunning = true;
    
    console.log(`\n[@cocreate/socket-mesh] Graceful shutdown initiated. Cleaning up mesh state...`);

    // 1. Unregister from the database to prevent ghost/zombie nodes
    if (serverCtx && serverCtx.crud && serverCtx.id && serverCtx.organization_id) {
        try {
            await serverCtx.crud.send({
                method: 'object.delete',
                array: 'servers',
                object: { _id: serverCtx.id },
                organization_id: serverCtx.organization_id,
                host: serverCtx.host
            });
            console.log(`[@cocreate/socket-mesh] Successfully wiped server ${serverCtx.id} from DB.`);
        } catch (err) {
            console.error(`[@cocreate/socket-mesh] DB cleanup failed on exit:`, err.message);
        }
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

    // 4. Release Port 8089 and exit
    if (meshWss) meshWss.close();
    if (secureMeshServer) {
        secureMeshServer.close(() => {
            console.log(`[@cocreate/socket-mesh] Port ${MESH_PORT} successfully released.`);
            process.exit(0);
        });
    } else {
        process.exit(0);
    }

    // Failsafe: Force kill if the DB/Sockets hang for more than 3 seconds
    setTimeout(() => {
        console.warn('[@cocreate/socket-mesh] Graceful exit timed out. Forcing exit.');
        process.exit(1);
    }, 3000).unref();
}

/**
 * Resolves the global network cluster state and orchestrates direct socket links.
 * Employs a deterministic, alphabetical sort logic based on server ObjectId strings.
 */
async function reconcilePeerConnections() {
    if (isReconciliationRunning || isGracefulExitRunning || !serverCtx.crud) return;
    isReconciliationRunning = true;

    try {
        console.log(`[@cocreate/socket-mesh] Starting Symmetrical peer reconciliation...`);

        // Check 1: Ensure IP address is active
        if (!serverCtx.ip) {
            console.warn(`[@cocreate/socket-mesh] Waiting for server core to resolve host IP...`);
            isReconciliationRunning = false;
            return;
        }

        // Clean up legacy zombie servers matching our specific infrastructure instanceId
        const instanceId = serverCtx.infrastructure?.instanceId;
        if (instanceId) {
            try {
                await serverCtx.crud.send({
                    method: 'object.delete',
                    array: 'servers',
                    $filter: {
                        query: { 'infrastructure.instanceId': instanceId },
                        limit: 0 // Target and wipe all duplicate matches in the collection
                    },
                    organization_id: serverCtx.organization_id,
                    host: serverCtx.host
                });
            } catch (pruneErr) {
                console.warn(`[@cocreate/socket-mesh] Non-blocking warning during legacy instance cleanup:`, pruneErr.message);
            }
        }

        // Heartbeat payload using "upsert: true" to safely register on boot and modify on ticks
        const registerPayload = {
            method: 'object.update',
            array: 'servers',
            upsert: true,
            object: {
                _id: serverCtx.id, // Symmetrical unique identification
                ip: serverCtx.ip,
                host: serverCtx.host,
                status: 'active',
                infrastructure: serverCtx.infrastructure
            },
            organization_id: serverCtx.organization_id,
            host: serverCtx.host
        };

        const registerResult = await serverCtx.crud.send(registerPayload);
        if (registerResult && registerResult.error) {
            console.error(`[@cocreate/socket-mesh] Heartbeat registration rejected:`, registerResult.error);
        }

        // Query to read registered servers inside the organization scope
        // Strictly targeting peers with an ID less than ours to prevent socket loops
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

                try {
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
                        activeConnections.set(targetIp, clientSocket);
                    });

                    clientSocket.on('close', () => {
                        activeConnections.delete(targetIp);
                        retryTimers.delete(targetIp);
                        
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
                                }).catch(err => console.error(`[@cocreate/socket-mesh] Failed to prune dead peer:`, err));
                                
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

                        handleIncomingMessage(rawData, targetIp);
                    });

                    clientSocket.on('error', (err) => {
                        console.log(`[@cocreate/socket-mesh] Error on connection to ${targetIp}: ${err.message}`);
                    });

                } catch (connectionError) {
                    console.error(`[@cocreate/socket-mesh] Connection payload failed for ${targetIp}:`, connectionError);
                }
            };

            // Trigger the initial outbound connection attempt
            connectToPeer();
        }

    } catch (err) {
        console.error('[@cocreate/socket-mesh] Error reconciling nodes:', err);
    } finally {
        isReconciliationRunning = false;
    }
}

function handleIncomingMessage(data, senderIp) {
    // 1. Forward directly to local WebSocket clients attached to this server instance
    if (serverCtx.wsManager && typeof serverCtx.wsManager.broadcast === 'function') {
        serverCtx.wsManager.broadcast(data, { isForwarded: true });
    }

    // 2. Forward down to internal Workers via Node.js IPC
    if (serverCtx.workers) {
        for (const [wId, wInstance] of serverCtx.workers.entries()) {
            try {
                wInstance.process.send({ type: 'mesh.inbound', data: data.toString() });
            } catch (err) {}
        }
    }
}

export function broadcastToPeers(data, excludeIp = null) {
    // If this is a worker, route the broadcast UP to the primary process via IPC
    if (!serverCtx.isPrimary) {
        process.send({ type: 'mesh.outbound', data: data.toString() });
        return;
    }

    // If this is the primary, blast the message out across the active P2P tunnels
    for (const [ip, ws] of activeConnections.entries()) {
        if (ip === excludeIp) continue;
        if (ws.readyState === WebSocket.OPEN) {
            try {
                ws.send(data);
            } catch (err) {
                console.error(`[@cocreate/socket-mesh] Failed to broadcast to ${ip}:`, err);
            }
        }
    }
}

export default {
    init,
    broadcastToPeers
};