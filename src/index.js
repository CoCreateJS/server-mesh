import ws from 'ws';

// Memory directory tracking active peer WebSocket connections (peerIp -> WebSocket)
const activeConnections = new Map();

// Local cache mapping organization_id to its flat array of active peer IPs
const organizationPeers = new Map();

let isReconciliationRunning = false;

/**
 * Resolves the server's public IP once and registers it into the platform's central peer directory.
 * Establishes secure outbound connections to lower-indexed peers in the database.
 * 
 * @param {Object} server - The core CoCreateServer instance
 * @param {Object} socketServer - The active socket-server instance
 */
async function reconcilePlatformPeerConnections(server, socketServer) {
    if (isReconciliationRunning) return;
    isReconciliationRunning = true;

    try {
        console.log(`[@cocreate/socket-mesh] Starting Platform-Level Peer Reconciliation...`);

        // Safely extract pre-resolved IP from the core CoCreate server
        const myIp = server.ip;
        if (!myIp) {
            console.warn(`[@cocreate/socket-mesh] Warning: server.ip is not yet resolved by the core server. Retrying in 10s...`);
            setTimeout(() => reconcilePlatformPeerConnections(server, socketServer), 10000);
            isReconciliationRunning = false;
            return;
        }

        const peerPayload = {
            ip: myIp,
            status: 'active',
            infrastructure: server.infrastructure,
            sniHost: server.host
        };

        // Register raw IP/Infrastructure details into the database peers array
        const registerPayload = {
            method: 'object.update',
            array: 'organizations',
            object: {
                _id: server.organization_id,
                $addToSet: {
                    peers: peerPayload
                }
            },
            organization_id: server.organization_id,
            host: server.host
        };

        await server.crud.send(registerPayload);

        const readPayload = {
            method: 'object.read',
            array: 'organizations',
            object: { _id: server.organization_id },
            organization_id: server.organization_id,
            host: server.host
        };

        const platformDoc = await server.crud.send(readPayload);
        const peers = platformDoc?.peers || [];
        
        // Extract IP array and cache for active messaging routing checks
        const activeIps = peers.map(p => p.ip);
        organizationPeers.set(server.organization_id, activeIps);
        
        const myIndex = activeIps.indexOf(myIp);

        if (myIndex === -1) {
            console.warn(`[@cocreate/socket-mesh] Warning: Self IP (${myIp}) not registered in platform array yet. Retrying in 10s...`);
            setTimeout(() => reconcilePlatformPeerConnections(server, socketServer), 10000);
            isReconciliationRunning = false;
            return;
        }

        console.log(`[@cocreate/socket-mesh] Platform Index Assigned: ${myIndex} (IP: ${myIp})`);

        // Symmetrical routing rule: Connect ONLY to lower-index peers to prevent duplicate socket channels
        for (let i = 0; i < myIndex; i++) {
            const targetPeer = peers[i];

            if (activeConnections.has(targetPeer.ip)) {
                continue; 
            }

            console.log(`[@cocreate/socket-mesh] [Index ${myIndex} > Index ${i}] Opening Platform-Level Tunnel to lower-index peer: ${targetPeer.ip}`);

            try {
                const targetUrl = `wss://${targetPeer.ip}/${server.organization_id}`;

                const clientSocket = new ws(targetUrl, [
                    encodeURIComponent(JSON.stringify({
                        type: 'peer',
                        ip: myIp,
                        index: myIndex,
                        id: server.id,
                        token: process.env.CLUSTER_SECRET,
                        sniHost: server.host
                    }))
                ], {
                    servername: targetPeer.sniHost, // Negotiate SSL using standard host domain configuration
                    rejectUnauthorized: false // Fallback safety for raw IP-to-IP development tunnels
                });

                activeConnections.set(targetPeer.ip, clientSocket);

                clientSocket.on('open', () => {
                    console.log(`[@cocreate/socket-mesh] Platform Tunnel successfully established to peer: ${targetPeer.ip}`);
                    clientSocket.isPeer = true;
                    clientSocket.isPlatformTunnel = true;
                    socketServer.onWebSocket(clientSocket);
                });

                clientSocket.on('close', () => {
                    console.warn(`[@cocreate/socket-mesh] Platform Tunnel closed to peer: ${targetPeer.ip}. Cleared from active connections.`);
                    activeConnections.delete(targetPeer.ip);
                });

                clientSocket.on('error', (err) => {
                    console.error(`[@cocreate/socket-mesh] Connection error on Platform Tunnel to ${targetPeer.ip}:`, err.message);
                });

            } catch (connectionError) {
                console.error(`[@cocreate/socket-mesh] Failed to initialize connection to ${targetPeer.ip}:`, connectionError.message);
            }
        }

    } catch (err) {
        console.error('[@cocreate/socket-mesh] Error during platform peer reconciliation cycle:', err);
    } finally {
        isReconciliationRunning = false;
    }
}

/**
 * Sets up the event subscription on the SocketServer to handle P2P forwarding.
 * Real-time changes are dynamically forwarded across P2P tunnels ONLY to peers where the organization is active.
 * 
 * @param {Object} server - The core CoCreateServer instance
 * @param {Object} socketServer - The active SocketServer instance to subscribe to
 */
function subscribeToSocketServerBroadcasts(server, socketServer) {
    socketServer.on('send', (data) => {
        // Loop Prevention Gatekeeper: If this payload was already forwarded by another peer node, halt propagation
        if (data.isForwarded) {
            return;
        }

        const multiplexedPayload = {
            ...data,
            isForwarded: true, 
            originatorId: server.id
        };

        const payloadStr = JSON.stringify(multiplexedPayload);

        // Fetch the active peer IPs registered for this specific organization
        const allowedIps = organizationPeers.get(data.organization_id) || [];

        activeConnections.forEach((peerSocket, peerIp) => {
            // Forward only to tunnels where this specific organization context is active
            if (allowedIps.includes(peerIp)) {
                try {
                    if (peerSocket.readyState === ws.OPEN) {
                        peerSocket.send(payloadStr);
                    }
                } catch (err) {
                    console.error(`[@cocreate/socket-mesh] Multiplex failure to peer ${peerIp}:`, err.message);
                }
            }
        });
    });

    console.log(`[@cocreate/socket-mesh] Registered event listener for SocketServer broadcasts.`);
}

/**
 * Initializes the Platform-Level Socket-Mesh driver.
 * 
 * @param {Object} server - The instantiated Server orchestrator containing master variables
 */
export async function init(server) {
    console.log(`[@cocreate/socket-mesh] Initializing socket-mesh clustering driver...`);
    
    // Fall back to the main server instance if the WebSocket manager block is merged
    const socketServer = server.wsManager || server;

    // Set up the event-driven broadcast forwarding
    subscribeToSocketServerBroadcasts(server, socketServer);

    // Run the initial database registration and connection reconciliation loop
    await reconcilePlatformPeerConnections(server, socketServer);

    // Run active loop checks every 30 seconds to maintain stable tunnels and update IP list cache
    setInterval(() => reconcilePlatformPeerConnections(server, socketServer), 30000);
}

export {
    reconcilePlatformPeerConnections as reconcilePeerConnections,
    init
};

export default init;