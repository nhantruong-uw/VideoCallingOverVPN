let currentStream = null;

export async function startCapture(constraints, { onDeviceEnded } = {}) {
    const stream = await navigator.mediaDevices.getUserMedia(constraints);

    stopCapture();
    currentStream = stream;

    if (onDeviceEnded) {
        stream.getTracks().forEach((track) => {
            track.onended = () => onDeviceEnded(track);
        });
    }

    return stream;
}

export function stopCapture() {
    if (!currentStream) return;

    currentStream.getTracks().forEach((track) => track.stop());
    currentStream = null;
}

export function getCurrentStream() {
    return currentStream;
}

export function toggleAudio() {
    const track = currentStream?.getAudioTracks()[0];
    if (!track) return null;

    track.enabled = !track.enabled;
    return track.enabled;
}

export function toggleVideo() {
    const track = currentStream?.getVideoTracks()[0];
    if (!track) return null;

    track.enabled = !track.enabled;
    return track.enabled;
}

export async function listDevices() {
    const devices = await navigator.mediaDevices.enumerateDevices();
    return {
        cameras: devices.filter((d) => d.kind === "videoinput"),
        mics: devices.filter((d) => d.kind === "audioinput"),
    };
}

window.addEventListener("pagehide", stopCapture);
