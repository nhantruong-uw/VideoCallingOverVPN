# Spectre

Private one-to-one video calling for a small group, reachable only over a WireGuard VPN. A Rust server tracks who's online and relays call setup messages, and the audio and video go directly between the two browsers over WebRTC.

I built it so my family could video call without sending calls through a third-party service. It's also my first Rust project.

## How it works

```
 Browser A                      Rust server (:3000)                    Browser B
 ─────────                      ───────────────────                    ─────────
 page + JS  ◀── HTTPS ───────── serves web/
 WebSocket  ◀── wss:// /ws ───▶ roster + message relay ◀── wss:// /ws ──▶ WebSocket
     │                                                                      │
     └──────────────── WebRTC audio/video, peer to peer ───────────────────┘
                       (over the WireGuard network)
```

1. Each browser opens a WebSocket to the server. The server gives it an ID and sends everyone the updated roster (name, online or offline).
2. **Bring up WireGuard**, then **start the server** from the repository root (it serves `web/` and reads `certs/` relative to the current folder). Set `SPECTRE_ADDR` to the server's WireGuard address:

   ```bash
   SPECTRE_ADDR=10.0.0.1:3000 cargo run --release
   ```

   The server listens only on that address, so devices outside the tunnel can't connect. If `SPECTRE_ADDR` isn't set, it listens on `127.0.0.1:3000` (the server machine only). If WireGuard isn't up yet, the address doesn't exist and the server exits with an error.
3. Once the peers have exchanged an offer, an answer and their ICE candidates, audio and video flow directly between them.

No STUN or TURN servers are configured. Every device is on the same WireGuard network, so the peers can reach each other's VPN addresses directly.

## Features

- Live roster of who's online, with names
- Ringing, accept and reject
- 10-second no-answer timeout, with a "missed call" notice for the person called
- Busy signal when the person is already in a call
- Mute and camera-off, with the other person's mute state shown on screen
- Camera and microphone selection
- Call quality indicator (good, fair, poor), from round-trip time and packet loss read every 3 seconds with `getStats()`

## Stack

| Part | Choice |
|---|---|
| Server | Rust: axum (HTTP and WebSockets), tokio, axum-server with rustls (TLS), serde |
| Client | Plain JavaScript (ES modules), HTML, CSS; WebRTC and `getUserMedia` |
| Network | WireGuard VPN |

## Running it

Requirements: Rust 1.85 or newer (the crate uses the 2024 edition), and a WireGuard network that includes the server and every device that will make calls.

1. **Create a TLS certificate.** Browsers only allow camera and microphone access on HTTPS pages, so the server needs a certificate even inside the VPN. The server reads `certs/cert.pem` and `certs/key.pem`, which are git-ignored. For a self-signed certificate for the server's VPN address (replace `10.0.0.1` with yours):

   ```bash
   mkdir -p certs
   openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
     -keyout certs/key.pem -out certs/cert.pem \
     -subj "/CN=spectre" -addext "subjectAltName=IP:10.0.0.1"
   ```

   Browsers will warn about a self-signed certificate. To avoid the warning, use a tool such as `mkcert` and install its root certificate on each device.

2. **Start the server** from the repository root (it serves `web/` and reads `certs/` relative to the current folder):

   ```bash
   cargo run --release
   ```

   It listens on port 3000.

3. **Open the app** on each device: `https://<server VPN address>:3000`. Enter a name, allow camera and microphone access, and call someone from the roster.

## Security notes

- **Access control comes from the network.** The server has no login of its own. Instead, it listens only on the WireGuard address, so only devices with a key for the tunnel can reach it. I tested this from the internet and from my home LAN with WireGuard off: neither can connect. If no address is given, it defaults to `127.0.0.1` rather than all interfaces, so a missing setting fails closed.
- **Signaling is encrypted with TLS** (rustls), and the server sets the sender ID on every relayed message itself, so a client can't pretend to be someone else.
- **Media is encrypted by WebRTC** (DTLS-SRTP) and travels inside the WireGuard tunnel.

## Limitations

- One-to-one calls only; no group calls.
- Disconnected users stay in the roster as offline until the server restarts.
- No TURN fallback, so it only works between devices that can reach each other directly (which the VPN provides).

## Project layout

```
src/main.rs      signaling server: TLS, static files, WebSocket roster and relay (bind address from SPECTRE_ADDR)
web/index.html   page and UI
web/webrtc.js    signaling client, call states, peer connection, quality monitor
web/capture.js   camera and microphone capture, mute, device lists
web/style.css    styles
```

## License

MIT
