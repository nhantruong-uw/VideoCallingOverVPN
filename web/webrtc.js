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
let onCallRejected = null;
let onCallRinging = null;
let onCallBusy = null;
let onNoAnswer = null;
let onMissedCall = null;
let pendingOffer = null;
let pendingCandidates = [];
let callTimeoutId = null;

const CALL_TIMEOUT_MS = 10000;

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
    onCallRejected: rejectedCb,
    onCallRinging: ringingCb,
    onCallBusy: busyCb,
    onNoAnswer: noAnswerCb,
    onMissedCall: missedCb,
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
    onCallRejected = rejectedCb;
    onCallRinging = ringingCb;
    onCallBusy = busyCb;
    onNoAnswer = noAnswerCb;
    onMissedCall = missedCb;

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
            conn.close();
            if (pc === conn) {
                pc = null;
                remoteId = null;
            }
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
    clearTimeout(callTimeoutId);
    if (remoteId !== null) {
        send({ type: "hangup", sender: 0, target: remoteId, payload: {} });
    }
    pc?.close();
    pc = null;
    remoteId = null;
    pendingOffer = null;
    pendingCandidates = [];
}

export async function acceptCall() {
    if (!pendingOffer) return;
    const offer = pendingOffer;
    pendingOffer = null;

    pc = createPeerConnection();
    await pc.setRemoteDescription(new RTCSessionDescription(offer.payload));

    for (const candidate of pendingCandidates) {
        await pc.addIceCandidate(new RTCIceCandidate(candidate));
    }
    pendingCandidates = [];

    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);

    send({ type: "answer", sender: 0, target: remoteId, payload: pc.localDescription });
}

export function rejectCall() {
    if (!pendingOffer) return;
    send({ type: "reject", sender: 0, target: remoteId, payload: {} });
    pendingOffer = null;
    pendingCandidates = [];
    remoteId = null;
}

export function sendMediaState(kind, enabled) {
    if (remoteId === null) return;
    send({ type: "media-state", sender: 0, target: remoteId, payload: { kind, enabled } });
}

export async function call(targetId) {
    remoteId = targetId;
    onCallRinging?.(targetId);
    pc = createPeerConnection();

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);

    send({ type: "offer", sender: 0, target: remoteId, payload: pc.localDescription });

    callTimeoutId = setTimeout(() => {
        send({ type: "timeout", sender: 0, target: remoteId, payload: {} });
        pc?.close();
        pc = null;
        remoteId = null;
        onNoAnswer?.();
    }, CALL_TIMEOUT_MS);
}

async function handleSignal(msg) {
    if (msg.type === "offer") {
        if (pc !== null || pendingOffer !== null) {
            send({ type: "busy", sender: 0, target: msg.sender, payload: {} });
            return;
        }

        remoteId = msg.sender;
        pendingOffer = msg;
        pendingCandidates = [];
        onIncomingCall?.(msg.sender);
    } else if (msg.type === "answer") {
        clearTimeout(callTimeoutId);
        await pc.setRemoteDescription(new RTCSessionDescription(msg.payload));
        for (const candidate of pendingCandidates) {
            await pc.addIceCandidate(new RTCIceCandidate(candidate));
        }
        pendingCandidates = [];
    } else if (msg.type === "ice-candidate") {
        if (pc && pc.remoteDescription) {
            await pc.addIceCandidate(new RTCIceCandidate(msg.payload));
        } else {
            pendingCandidates.push(msg.payload);
        }
    } else if (msg.type === "reject") {
        clearTimeout(callTimeoutId);
        pc?.close();
        pc = null;
        remoteId = null;
        onCallRejected?.();
    } else if (msg.type === "hangup") {
        stopQualityMonitor();
        pc?.close();
        pc = null;
        remoteId = null;
        pendingOffer = null;
        pendingCandidates = [];
        onCallEnded?.();
    } else if (msg.type === "roster") {
        onRoster?.(msg.payload);
    } else if (msg.type === "welcome") {
        onWelcome?.(msg.payload.id);
    } else if (msg.type === "media-state") {
        onPeerMediaState?.(msg.payload.kind, msg.payload.enabled);
    } else if (msg.type === "busy") {
        clearTimeout(callTimeoutId);
        pc?.close();
        pc = null;
        remoteId = null;
        onCallBusy?.();
    } else if (msg.type === "timeout") {
        stopQualityMonitor();
        pc?.close();
        pc = null;
        remoteId = null;
        pendingOffer = null;
        pendingCandidates = [];
        onMissedCall?.(msg.sender);
    }
}
