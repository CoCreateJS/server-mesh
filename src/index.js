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

import ws from 'ws';

// Global memory registry of active peer WebSocket tunnel connections (peerIp -> WebSocket)
const activeConnections = new Map();

// Registry of active retry timers to allow cancellation (peerIp -> Timeout ID)
const retryTimers = new Map();

// Registry of scheduled DB prune operations to allow cancellation (peerIp -> Timeout ID)
const pruneTimers = new Map();

// Control locks to block overlapping reconciliation steps
let isReconciliationRunning = false;
let isGracefulExitRunning = false;


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
 * Resolves the global network cluster state and orchestrates direct socket links.
 * Employs a deterministic, alphabetical sort logic based on server ObjectId strings.
 * 
 * @param {Object} server - The core CoCreateServer instance context
 * @param {Object} socketServer - The active SocketServer instance context
 */
async function reconcilePeerConnections(server, socketServer) {
    if (isReconciliationRunning || isGracefulExitRunning) return;
    isReconciliationRunning = true;

    try {
        console.log(`[@cocreate/socket-mesh] Starting Symmetrical peer reconciliation...`);

        // Check 1: Ensure IP address is active
        if (!server.ip) {
            console.warn(`[@cocreate/socket-mesh] Waiting for server core to resolve host IP...`);
            isReconciliationRunning = false;
            return;
        }

        // Clean up legacy zombie servers matching our specific infrastructure instanceId
        const instanceId = server.infrastructure?.instanceId;
        if (instanceId) {
            console.log(`[@cocreate/socket-mesh] Pruning legacy zombie servers sharing instanceId: ${instanceId}`);
            try {
                // Execute a clean, direct deletion targeting the standardized camelCase key
                await server.crud.send({
                    method: 'object.delete',
                    array: 'servers',
                    $filter: {
                        query: {
                            'infrastructure.instanceId': instanceId
                        },
                        limit: 0 // Target and wipe all duplicate matches in the collection
                    },
                    organization_id: server.organization_id,
                    host: server.host
                });

                console.log(`[@cocreate/socket-mesh] Legacy registrations pruned successfully for instanceId: ${instanceId}`);
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
                _id: server.id, // Symmetrical unique identification (ObjectId string!)
                ip: server.ip,
                host: server.host,
                status: 'active',
                infrastructure: server.infrastructure
            },
            organization_id: server.organization_id,
            host: server.host
        };

        console.log(`[@cocreate/socket-mesh] Writing node heartbeat registration to "servers" collection...`);
        const registerResult = await server.crud.send(registerPayload);
        
        if (registerResult && registerResult.error) {
            console.error(`[@cocreate/socket-mesh] Heartbeat registration rejected:`, registerResult.error);
        } else {
            console.log(`[@cocreate/socket-mesh] Server registration database write completed.`);
        }

        // Query to read registered servers inside the organization scope
        // Strictly targeting peers with an ID less than ours to prevent socket loops
        const readPayload = {
            method: 'object.read',
            array: 'servers',
            $filter: {
                query: {
                    _id: { $lt: server.id }
                },
                sort: {
                    _id: 1 // Sort ascending
                },
                limit: 0
            },
            organization_id: server.organization_id,
            host: server.host
        };

        const readResult = await server.crud.send(readPayload);
        
        if (readResult && readResult.error) {
            console.error(`[@cocreate/socket-mesh] Collection read failed:`, readResult.error);
            isReconciliationRunning = false;
            return;
        }

        const peers = normalizePeersList(readResult);

        console.log(`[@cocreate/socket-mesh] Found ${peers.length} priority peers to connect to (Node ID: ${server.id})`);


        // Negotiate outbound connections strictly to the retrieved lower-ID peers
        for (const targetPeer of peers) {
            const targetIp = targetPeer.ip;

            // Self-IP Guard: Never attempt to open a socket tunnel to our own IP address
            if (targetIp === server.ip) {
                continue;
            }

            if (activeConnections.has(targetIp)) {
                continue; // Tunnel is already warm and running
            }

            console.log(`[@cocreate/socket-mesh] Opening socket tunnel to priority peer: ${targetIp}`);

            let retryCount = 0;
            const maxRetries = 10;

            const connectToPeer = () => {
                // Prevent retries if our own server is shutting down
                if (isGracefulExitRunning) return; 

                try {
                    // Symmetrical tunnel configuration pointing directly to secure WS endpoint
                    const targetUrl = `wss://${targetIp}/${server.organization_id}`;

                    const clientSocket = new ws(targetUrl, [
                        encodeURIComponent(JSON.stringify({
                            type: 'peer',
                            ip: server.ip,
                            id: server.id,
                            token: process.env.CLUSTER_SECRET,
                            sniHost: server.host
                        }))
                    ], {
                        servername: server.host, // Map TLS SNI explicitly to match standard secure certificate blocks
                        rejectUnauthorized: false // Development safety fallback for direct IP-to-IP bridges
                    });

                    clientSocket.on('open', () => {
                        console.log(`[@cocreate/socket-mesh] Tunnel successfully established to peer: ${targetIp}`);
                        retryCount = 0; // Reset retries on successful connection
                        retryTimers.delete(targetIp); // Clear any pending retry timers
                        pruneTimers.delete(targetIp); // Clear any pending prune timers
                        clientSocket.isPeer = true;
                        clientSocket.isPlatformTunnel = true;
                        activeConnections.set(targetIp, clientSocket);
                        socketServer.onWebSocket(clientSocket);
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
                            // Thundering Herd Prevention: Stagger deletions using our relative index
                            const staggerDelay = peers.length * 1000; // 1 second per item in index
                            
                            console.warn(`[@cocreate/socket-mesh] Peer ${targetIp} dead. Scheduling DB prune in ${staggerDelay}ms...`);
                            
                            const pruneTimerId = setTimeout(() => {
                                console.error(`[@cocreate/socket-mesh] Executing DB prune for dead peer: ${targetIp}`);
                                
                                // 1. Delete from DB
                                const prunePayload = {
                                    method: 'object.delete',
                                    array: 'servers',
                                    object: { _id: targetPeer._id },
                                    organization_id: server.organization_id,
                                    host: server.host
                                };
                                server.crud.send(prunePayload).catch(err => console.error(`[@cocreate/socket-mesh] Failed to prune dead peer:`, err));
                                
                                // 2. Broadcast fast mesh control message to tell others to cancel their prune timers
                                const cancelPayload = {
                                    type: 'mesh.peer_pruned',
                                    deadIp: targetIp,
                                    isForwarded: true,
                                    originatorId: server.id
                                };
                                const payloadStr = JSON.stringify(cancelPayload);
                                
                                // Broadcast to outbound tunnels
                                activeConnections.forEach(peerSocket => {
                                    if (peerSocket.readyState === ws.OPEN) peerSocket.send(payloadStr);
                                });
                                
                                // Broadcast to inbound tunnels (if supported by socketServer)
                                if (typeof socketServer.send === 'function') {
                                    socketServer.send(cancelPayload);
                                }
                                
                                pruneTimers.delete(targetIp);
                            }, staggerDelay);
                            
                            pruneTimers.set(targetIp, pruneTimerId);
                        }
                    });


                    // Listen for fast mesh control messages (e.g., prune cancellations)
                    clientSocket.on('message', (rawData) => {
                        try {
                            const msg = JSON.parse(rawData);
                            if (msg.type === 'mesh.peer_pruned' && msg.deadIp) {
                                const activeTimerId = prunePruneGet = pruneTimers.get(msg.deadIp);
                                if (activeTimerId) {
                                    console.log(`[@cocreate/socket-mesh] Received remote prune confirmation for ${msg.deadIp}. Canceling local DB write.`);
                                    clearTimeout(activeTimerId);
                                    pruneTimers.delete(msg.deadIp);
                                }
                            }
                        } catch (e) {
                            // Ignore parse errors for standard binary or non-JSON payloads
                        }
                    });

                    clientSocket.on('error', (err) => {
                        console.error(`[@cocreate/socket-mesh] Link error to peer ${targetIp}:`, err.message);
                    });

                } catch (connectionError) {
                    console.error(`[@cocreate/socket-mesh] Failed to initialize connection to ${targetIp}:`, connectionError.message);
                }
            };

            // Initiate first connection attempt
            connectToPeer();
        }

    } catch (err) {
        console.error('[@cocreate/socket-mesh] Error during peer reconciliation loop:', err);
    } finally {
        isReconciliationRunning = false;
    }
}


/**
 * Binds a custom outbound event observer on standard SocketServer transmissions.
 * Propagates local operations across cluster tunnels while preventing infinite broadcast echo storms.
 * 
 * @param {Object} server - The core Server instance context
 * @param {Object} socketServer - The active SocketServer context to register forwarding on
 */
function setupOutboundForwarding(server, socketServer) {
    socketServer.on('send', (data) => {
        // Echo Prevention Gatekeeper
        if (data.isForwarded) {
            return;
        }

        const forwardedPayload = {
            ...data,
            isForwarded: true,
            originatorId: server.id
        };

        const payloadStr = JSON.stringify(forwardedPayload);

        activeConnections.forEach((peerSocket, peerIp) => {
            try {
                if (peerSocket.readyState === ws.OPEN) {
                    peerSocket.send(payloadStr);
                }
            } catch (err) {
                console.error(`[@cocreate/socket-mesh] Forward failure to peer ${peerIp}:`, err.message);
            }
        });
    });

    console.log(`[@cocreate/socket-mesh] Registered outbound event forwarder on Socket Server.`);
}


/**
 * Gracefully prunes our registered server node from the primary DB collection during system shutdown.
 * Prevents stale zombie IP states from corrupting routing paths.
 * 
 * @param {Object} server - The core Server context to prune
 */
async function pruneAndExit(server) {
    if (isGracefulExitRunning) return;
    isGracefulExitRunning = true;

    console.log(`\n[@cocreate/socket-mesh] Graceful shutdown initiated. Pruning node registry...`);

    try {
        const prunePayload = {
            method: 'object.delete',
            array: 'servers',
            object: {
                _id: server.id // Delete directly via native ObjectId
            },
            organization_id: server.organization_id,
            host: server.host
        };

        const pruneResult = await server.crud.send(prunePayload);
        
        if (pruneResult && pruneResult.error) {
            console.error(`[@cocreate/socket-mesh] Node registration pruning failed:`, pruneResult.error);
        } else {
            console.log(`[@cocreate/socket-mesh] Cleaned database server registry successfully.`);
        }
    } catch (err) {
        console.error(`[@cocreate/socket-mesh] Error clearing node registrations during exit:`, err);
    } finally {
        // Shutdown tunnels
        activeConnections.forEach((socket) => {
            try { socket.close(); } catch (e) {}
        });
        activeConnections.clear();
        
        console.log(`[@cocreate/socket-mesh] Shutdown finalized. Terminating process.`);
        process.exit(0);
    }
}


/**
 * Symmetrical Platform Mesh Manager Initializer
 * 
 * @param {Object} server - The instantiated core Server orchestrator containing master references
 */
export async function init(server) {
    const socketServer = server.wsManager || server;

    setupOutboundForwarding(server, socketServer);

    // Initial boot coordination check
    await reconcilePeerConnections(server, socketServer);

    // Bind clean OS interrupt signals to secure shutdown hooks
    process.on('SIGINT', () => pruneAndExit(server));
    process.on('SIGTERM', () => pruneAndExit(server));
}

export default {
    reconcilePeerConnections,
    init
};