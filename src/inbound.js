import { WebSocketServer } from 'ws';
import https from 'node:https';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { execSync } from 'node:child_process';
import crypto from 'node:crypto';

/**
 * ServerMeshInbound manages inbound HTTPS and WebSocket secure listening transports,
 * ephemeral TLS certificate generation, inbound socket heartbeats, and inbound message processing.
 */
export class ServerMeshInbound {
    constructor(mesh) {
        this.mesh = mesh;
        this.server = null;
        this.meshPort = process.env.MESH_PORT || 8090;
        this.secureMeshServer = null;
        this.meshWss = null;
        this.heartbeatInterval = null;
        this.heartbeatMs = 30000;
    }

    /**
     * Initializes the inbound server transport with host server context and IPC listeners.
     * 
     * @param {Object} server - CoCreateServer instance context
     * @returns {Promise<void>}
     */
    async init(server) {
        this.server = server;

        if (this.server && typeof this.server.on === 'function') {
            this.server.on('mesh.inbound.status', () => {
                console.log(`[@cocreate/server-mesh/inbound] Inbound transport status: ${this.secureMeshServer ? 'listening' : 'stopped'}`);
            });

            // Fallback listener for legacy event name
            this.server.on('mesh.server.status', () => {
                console.log(`[@cocreate/server-mesh/inbound] Inbound transport status: ${this.secureMeshServer ? 'listening' : 'stopped'}`);
            });

            this.server.on('mesh.inbound.relay', (payload) => {
                if (payload && payload.data) {
                    this.broadcastInboundLocal(payload);
                }
            });
        }

        await this.startServer();
        this.startHeartbeat();
    }

    /**
     * Generates short-lived RSA-2048 X.509 TLS credentials in temporary storage.
     * 
     * @returns {{key: Buffer, cert: Buffer}|null}
     */
    generateTLSCredentials() {
        const tmpDir = os.tmpdir();
        const keyPath = path.join(tmpDir, `mesh-key-${Date.now()}-${crypto.randomBytes(4).toString('hex')}.pem`);
        const certPath = path.join(tmpDir, `mesh-cert-${Date.now()}-${crypto.randomBytes(4).toString('hex')}.pem`);

        try {
            execSync(`openssl req -x509 -newkey rsa:2048 -keyout "${keyPath}" -out "${certPath}" -days 1 -nodes -subj "/CN=localhost"`, { stdio: 'ignore' });
            const key = fs.readFileSync(keyPath);
            const cert = fs.readFileSync(certPath);

            try { fs.unlinkSync(keyPath); } catch {}
            try { fs.unlinkSync(certPath); } catch {}

            return { key, cert };
        } catch (err) {
            console.error('[@cocreate/server-mesh/inbound] Failed to generate ephemeral TLS credentials:', err.message);
            return null;
        }
    }

    /**
     * Binds HTTPS and WebSocketServer listeners on MESH_PORT and configures inbound socket handlers.
     */
    async startServer() {
        const credentials = this.generateTLSCredentials();
        if (!credentials) {
            throw new Error("Unable to boot secure mesh server: TLS credential generation failed.");
        }

        this.secureMeshServer = https.createServer(credentials);

        this.secureMeshServer.on('error', (err) => {
            if (err.code === 'EADDRINUSE') {
                console.error(`\n[@cocreate/server-mesh/inbound] [FATAL] Port ${this.meshPort} is already in use!`);
                process.exit(1);
            } else {
                console.error(`[@cocreate/server-mesh/inbound] HTTPS transport error:`, err);
            }
        });

        await new Promise((resolve) => {
            this.secureMeshServer.listen(this.meshPort, '0.0.0.0', () => {
                console.log(`[@cocreate/server-mesh/inbound] [MASTER] Inbound mesh listening on 0.0.0.0:${this.meshPort}`);
                resolve();
            });
        });

        this.meshWss = new WebSocketServer({ server: this.secureMeshServer });

        this.meshWss.on('connection', (ws, req) => {
            const ip = req.socket.remoteAddress;
            const organization_id = decodeURIComponent(
                new URL(req.url, 'https://mesh.local').pathname.slice(1)
            );
            if (
                organization_id &&
                !this.mesh.organizations.has(organization_id)
            ) {
                ws.close();
                return;
            }
            let remoteServerId = null;

            const protocolHeader = req.headers['sec-websocket-protocol'];
            if (protocolHeader) {
                try {
                    const parsed = JSON.parse(decodeURIComponent(protocolHeader));
                    remoteServerId = parsed.id;
                } catch (err) {
                    // Ignore protocol decoding errors
                }
            }

            const identifier = remoteServerId || ip;
            console.log(`[@cocreate/server-mesh/inbound] Secure inbound peer connection established: ${identifier}`);

            ws.isAlive = true;
            ws.remoteAddress = ip;
            ws.serverId = remoteServerId || ip;
            if (!organization_id) {
                this.mesh.addControlSocket(ws);
            } else {
                this.mesh.addOrganizationSocket(organization_id, ws);
            }
            ws.on('pong', () => { ws.isAlive = true; });

            ws.on('message', (rawData) => {
                this.mesh.handleIncomingMessage(rawData, ws);
            });

            ws.on('close', () => {
                console.log(`[@cocreate/server-mesh/inbound] Inbound peer connection ${identifier} dropped.`);
                this.mesh.activeConnections.delete(identifier);
                if (remoteServerId) this.mesh.activeConnections.delete(ip);
                this.mesh.removeSocketFromAllOrgServers(ws);
            });

            ws.on('error', (err) => {
                console.error(`[@cocreate/server-mesh/inbound] Inbound socket error (${identifier}):`, err.message);
            });

            const connectionKey = `${organization_id || 'control'}:${identifier}`;
            this.mesh.activeConnections.set(connectionKey, ws);
            ws.isControlSocket = !organization_id;
            if (ws.isControlSocket) this.mesh.syncOrganizations(ws);
        });
    }

// ... existing code ...

    /**
     * Relays inbound payloads down to local worker processes.
     * 
     * @param {Object} payload 
     */
    broadcastInboundLocal(payload) {
        if (this.server && typeof this.server.send === 'function') {
            try {
                this.server.send({
                    method: 'mesh.inbound',
                    broadcast: payload.broadcast ?? true,
                    worker: payload.worker,
                    master: false,
                    data: typeof payload.data === 'string' ? payload.data : JSON.stringify(payload.data)
                });
            } catch (err) {
                console.error("[@cocreate/server-mesh/inbound] Failed to relay inbound message to workers:", err.message);
            }
        }
    }

    /**
     * Starts interval timer to ping connected clients and evict dead sockets.
     */
    startHeartbeat() {
        if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);

        this.heartbeatInterval = setInterval(() => {
            if (!this.meshWss) return;

            this.meshWss.clients.forEach((ws) => {
                if (ws.isAlive === false) {
                    console.warn(`[@cocreate/server-mesh/inbound] Unresponsive client socket detected. Terminating connection.`);
                    return ws.terminate();
                }
                ws.isAlive = false;
                try {
                    ws.ping();
                } catch (err) {
                    console.error("[@cocreate/server-mesh/inbound] Ping error:", err.message);
                }
            });
        }, this.heartbeatMs);

        if (this.heartbeatInterval.unref) {
            this.heartbeatInterval.unref();
        }
    }

    /**
     * Tears down the server instance, ending heartbeats and closing listening sockets.
     * 
     * @returns {Promise<void>}
     */
    async close() {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
            this.heartbeatInterval = null;
        }

        if (this.meshWss) {
            try { this.meshWss.close(); } catch {}
        }

        return new Promise((resolve) => {
            if (this.secureMeshServer) {
                this.secureMeshServer.close(() => {
                    console.log(`[@cocreate/server-mesh/inbound] Secure port ${this.meshPort} successfully released.`);
                    resolve();
                });
            } else {
                resolve();
            }
        });
    }
}
