import { getCurrentStream } from "./capture.js";

let ws = null;
let pc = null;
let remoteId = null;
let onRemoteStream = null;
let onCallEnded = null;
let onRoster = null;
let onWelcome = null;
let onSignalingClosed = null;
let onRemoteTrackMuted = null;
let onPeerMediaState = null;
let onQualityChange = null;
let qualityInterval = null;
let onIncomingCall = null;

function send(msg) {
    ws.send(JSON.stringify(msg));
}

export function connectSignaling({
    onRemoteStream: cb,
    onCallEnded: endedCb,
    onRoster: rosterCb,
    onWelcome: welcomeCb,
    onSignalingClosed: closedCb,
    onRemoteTrackMuted: trackMutedCb,
    onPeerMediaState: mediaStateCb,
    onQualityChange: qualityCb,
    onIncomingCall: incomingCb,
} = {}) {
    onRemoteStream = cb;
    onCallEnded = endedCb;
    onRoster = rosterCb;
    onWelcome = welcomeCb;
    onSignalingClosed = closedCb;
    onRemoteTrackMuted = trackMutedCb;
    onPeerMediaState = mediaStateCb;
    onQualityChange = qualityCb;
    onIncomingCall = incomingCb;

    return new Promise((resolve, reject) => {
        ws = new WebSocket(`wss://${location.host}/ws`);
        ws.onopen = () => resolve();
        ws.onerror = (err) => reject(err);
        ws.onclose = () => onSignalingClosed?.();
        ws.onmessage = (event) => handleSignal(JSON.parse(event.data));
    });
}

async function computeQuality(conn) {
    const stats = await conn.getStats();
    let rtt = null;
    let packetsLost = 0;
    let packetsReceived = 0;

    stats.forEach((report) => {
        if (report.type === "candidate-pair" && report.state === "succeeded" && typeof report.currentRoundTripTime === "number") {
            rtt = report.currentRoundTripTime;
        }
        if (report.type === "inbound-rtp") {
            packetsLost += report.packetsLost || 0;
            packetsReceived += report.packetsReceived || 0;
        }
    });

    if (rtt === null) return "unknown";

    const lossRatio = packetsReceived > 0 ? packetsLost / (packetsLost + packetsReceived) : 0;
    if (rtt < 0.15 && lossRatio < 0.02) return "good";
    if (rtt < 0.4 && lossRatio < 0.05) return "fair";
    return "poor";
}

function startQualityMonitor(conn) {
    qualityInterval = setInterval(async () => {
        const quality = await computeQuality(conn);
        onQualityChange?.(quality, remoteId);
    }, 3000);
}

function stopQualityMonitor() {
    clearInterval(qualityInterval);
    qualityInterval = null;
    onQualityChange?.(null, null);
}

function createPeerConnection() {
    const conn = new RTCPeerConnection();

    conn.onicecandidate = (event) => {
        if (event.candidate) {
            send({ type: "ice-candidate", sender: 0, target: remoteId, payload: event.candidate.toJSON() });
        }
    };

    conn.ontrack = (event) => {
        onRemoteStream?.(event.streams[0]);

        event.track.onmute = () => onRemoteTrackMuted?.(event.track.kind, true);
        event.track.onunmute = () => onRemoteTrackMuted?.(event.track.kind, false);
    };

    conn.onconnectionstatechange = () => {
        if (conn.connectionState === "connected") {
            startQualityMonitor(conn);
        } else if (conn.connectionState === "disconnected" || conn.connectionState === "failed") {
            stopQualityMonitor();
            onCallEnded?.();
        } else if (conn.connectionState === "closed") {
            stopQualityMonitor();
        }
    };

    const stream = getCurrentStream();
    stream?.getTracks().forEach((track) => conn.addTrack(track, stream));

    return conn;
}

export function identify(name) {
    send({ type: "identify", sender: 0, target: 0, payload: { name } });
}

export function hangup() {
    stopQualityMonitor();
    pc?.close();
    pc = null;
    remoteId = null;
}

export function sendMediaState(kind, enabled) {
    if (remoteId === null) return;
    send({ type: "media-state", sender: 0, target: remoteId, payload: { kind, enabled } });
}

export async function call(targetId) {
    remoteId = targetId;
    pc = createPeerConnection();

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);

    send({ type: "offer", sender: 0, target: remoteId, payload: pc.localDescription });
}

async function handleSignal(msg) {
    if (msg.type === "offer") {
        remoteId = msg.sender;
        onIncomingCall?.(msg.sender);
        pc = createPeerConnection();

        await pc.setRemoteDescription(new RTCSessionDescription(msg.payload));
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);

        send({ type: "answer", sender: 0, target: remoteId, payload: pc.localDescription });
    } else if (msg.type === "answer") {
        await pc.setRemoteDescription(new RTCSessionDescription(msg.payload));
    } else if (msg.type === "ice-candidate") {
        await pc?.addIceCandidate(new RTCIceCandidate(msg.payload));
    } else if (msg.type === "roster") {
        onRoster?.(msg.payload);
    } else if (msg.type === "welcome") {
        onWelcome?.(msg.payload.id);
    } else if (msg.type === "media-state") {
        onPeerMediaState?.(msg.payload.kind, msg.payload.enabled);
    }
}
