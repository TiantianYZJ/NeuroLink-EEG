const dgram = require('dgram');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

// ── Configuration ────────────────────────────────────────────────────────────
const DEVICE_TYPE = (process.env.DEVICE_TYPE || 'ganglion').toLowerCase();
const isGanglion = DEVICE_TYPE === 'ganglion';
const UDP_PORT = parseInt(process.env.UDP_PORT || '12345', 10);        // OpenBCI GUI default UDP out port
const HTTP_PORT = parseInt(process.env.PORT || process.env.HTTP_PORT || '8080', 10);  // Web UI port
const PUBLIC_DIR = path.resolve(__dirname, '..', 'public');
const CHANNEL_COUNT = parseInt(process.env.CHANNELS || (isGanglion ? '4' : '8'), 10);  // Ganglion=4, Cyton=8/16
const CONFIG_SAMPLE_RATE = parseInt(process.env.SAMPLE_RATE || (isGanglion ? '200' : '250'), 10); // Ganglion=200Hz

// ── State ────────────────────────────────────────────────────────────────────
let packetCount = 0;
let lastStatsTime = Date.now();
let sampleRate = 0;    // estimated Hz

// ── UDP Server (receives OpenBCI data) ──────────────────────────────────────
const udpServer = dgram.createSocket('udp4');

/**
 * Parse Ganglion / Cyton binary UDP packet.
 *
 * Ganglion (4-ch) binary format over UDP:
 *   [0xA0] [sample_lo] [sample_mid] [sample_hi] [ch1_24bit] [ch2_24bit] [ch3_24bit] [ch4_24bit] [0xC0]
 *   total = 4 + 4*3 + 1 = 17 bytes minimum
 *
 * Cyton (8-ch) binary format over UDP:
 *   [0xA0] [sample_lo] [sample_mid] [sample_hi] [ch1_24bit] … [ch8_24bit] [accel_3×16bit] [0xC0]
 *   total = 4 + 8*3 + 6 + 1 = 35 bytes minimum
 */
function parseOpenBCIPacket(msg) {
  const len = msg.length;
  if (len < 5) return null;

  let offset = 0;
  // Skip leading bytes until we find the start marker 0xA0
  while (offset < len && msg[offset] !== 0xA0) offset++;
  if (offset >= len) return null;

  // Read sample number (24-bit, little-endian, 3 bytes)
  if (offset + 4 > len) return null;
  const sampleNumber = msg[offset + 1] | (msg[offset + 2] << 8) | (msg[offset + 3] << 16);
  offset += 4;

  // Read channels (each 24-bit signed, little-endian)
  const dataLen = CHANNEL_COUNT * 3;
  if (offset + dataLen + 1 > len) return null;

  const channels = [];
  for (let i = 0; i < CHANNEL_COUNT; i++) {
    let val = msg[offset] | (msg[offset + 1] << 8) | (msg[offset + 2] << 16);
    if (val & 0x800000) val |= ~0xFFFFFF;
    channels.push(val);
    offset += 3;
  }

  // Optional accelerometer (only for Cyton, 3 × 16-bit signed)
  let accel = null;
  if (!isGanglion && offset + 6 <= len && msg[offset] !== 0xC0) {
    const ax = msg[offset] | (msg[offset + 1] << 8);
    const ay = msg[offset + 2] | (msg[offset + 3] << 8);
    const az = msg[offset + 4] | (msg[offset + 5] << 8);
    accel = {
      x: ax & 0x8000 ? ax | 0xFFFF0000 : ax,
      y: ay & 0x8000 ? ay | 0xFFFF0000 : ay,
      z: az & 0x8000 ? az | 0xFFFF0000 : az,
    };
    offset += 6;
  }

  return { sampleNumber, channels, accel };
}

udpServer.on('message', (msg, rinfo) => {
  const parsed = parseOpenBCIPacket(msg);
  if (!parsed) return;

  // Rate estimation (average over 1-second window)
  packetCount++;
  const now = Date.now();
  if (now - lastStatsTime >= 1000) {
    sampleRate = Math.round(packetCount * 1000 / (now - lastStatsTime));
    packetCount = 0;
    lastStatsTime = now;
  }

  // Broadcast to all WebSocket clients
  const payload = JSON.stringify(parsed);
  wss.clients.forEach(client => {
    if (client.readyState === 1) {
      client.send(payload);
    }
  });
});

udpServer.on('error', err => {
  console.error('UDP error:', err.message);
  process.exit(1);
});

udpServer.on('listening', () => {
  const addr = udpServer.address();
  console.log(`[UDP] Listening on ${addr.address}:${addr.port}`);
});

// ── HTTP Server (serves frontend static files) ──────────────────────────────
const httpServer = http.createServer((req, res) => {
  let filePath = path.join(PUBLIC_DIR, req.url === '/' ? 'index.html' : req.url);
  const ext = path.extname(filePath).toLowerCase();

  const mimeMap = {
    '.html': 'text/html; charset=utf-8',
    '.js':   'application/javascript; charset=utf-8',
    '.css':  'text/css; charset=utf-8',
    '.png':  'image/png',
    '.svg':  'image/svg+xml',
  };

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not Found');
      return;
    }
    res.writeHead(200, { 'Content-Type': mimeMap[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

// ── WebSocket Server (pushes parsed data to browsers) ───────────────────────
const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws, req) => {
  const clientIp = req.socket.remoteAddress;
  console.log(`[WS] Client connected: ${clientIp}`);

  // Send welcome / status
  ws.send(JSON.stringify({
    type: 'status',
    message: 'Connected to OpenBCI UDP bridge',
    deviceType: DEVICE_TYPE,
    udpPort: UDP_PORT,
    channels: CHANNEL_COUNT,
    sampleRate: sampleRate || CONFIG_SAMPLE_RATE,
  }));

  ws.on('close', () => {
    console.log(`[WS] Client disconnected: ${clientIp}`);
  });
});

// ── Start ────────────────────────────────────────────────────────────────────
httpServer.listen(HTTP_PORT, () => {
  console.log(`[HTTP] Server running at http://localhost:${HTTP_PORT}`);
  console.log(`[HTTP] Serving static files from ${PUBLIC_DIR}`);
});

udpServer.bind(UDP_PORT);

process.on('SIGINT', () => {
  console.log('\nShutting down...');
  udpServer.close();
  wss.close();
  httpServer.close();
  process.exit(0);
});

console.log(`── OpenBCI UDP→WebSocket Bridge ──`);
console.log(`  Device          : ${DEVICE_TYPE === 'ganglion' ? 'Ganglion (4-ch, 200Hz)' : 'Cyton (8/16-ch, 250Hz)'}`);
console.log(`  UDP Listen Port : ${UDP_PORT} (set UDP_OUT in OpenBCI GUI to this port)`);
console.log(`  HTTP/WS Port    : ${HTTP_PORT}`);
console.log(`  Expected Channels: ${CHANNEL_COUNT}`);
console.log(`  Open browser to http://localhost:${HTTP_PORT}`);
