import { getCurrentStream } from "./capture.js";

let ws = null;
let pc = null;
let remoteId = null;
let onRemoteStream = null;
let onCallEnded = null;
let onRoster = null;
let onWelcome = null;
let onSignalingClosed = null;

function send(msg) {
    ws.send(JSON.stringify(msg));
}

export function connectSignaling({
    onRemoteStream: cb,
    onCallEnded: endedCb,
    onRoster: rosterCb,
    onWelcome: welcomeCb,
    onSignalingClosed: closedCb,
} = {}) {
    onRemoteStream = cb;
    onCallEnded = endedCb;
    onRoster = rosterCb;
    onWelcome = welcomeCb;
    onSignalingClosed = closedCb;

    return new Promise((resolve, reject) => {
        ws = new WebSocket(`wss://${location.host}/ws`);
        ws.onopen = () => resolve();
        ws.onerror = (err) => reject(err);
        ws.onclose = () => onSignalingClosed?.();
        ws.onmessage = (event) => handleSignal(JSON.parse(event.data));
    });
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
    };

    conn.onconnectionstatechange = () => {
        if (conn.connectionState === "disconnected" || conn.connectionState === "failed") {
            onCallEnded?.();
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
    pc?.close();
    pc = null;
    remoteId = null;
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
    }
}
