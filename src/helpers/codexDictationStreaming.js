const WebSocket = require("ws");
const { randomUUID } = require("node:crypto");

const MODEL = "codex-transcribe";
const MAX_PENDING_BYTES = 2 * 1024 * 1024;

function streamingUrl(baseUrl) {
  const url = new URL(baseUrl);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error("Use a proxy HTTP URL without credentials or query parameters");
  }
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname =
    url.pathname.replace(/\/$/, "").replace(/\/audio\/transcriptions(?:\/stream)?$/, "") +
    "/audio/transcriptions/stream";
  return url.toString();
}

// Shares AudioManager's mono 16 kHz PCM capture and retained batch recording.
// A final is usable only after session.close is acknowledged for every utterance.
class CodexDictationStreaming {
  constructor({
    createSocket = (url, options) => new WebSocket(url, options),
    timeoutMs = 10000,
    finalTimeoutMs = 8000,
  } = {}) {
    this.createSocket = createSocket;
    this.timeoutMs = timeoutMs;
    this.finalTimeoutMs = finalTimeoutMs;
    this.isConnected = false;
    this.audioBytesSent = 0;
    this.currentModel = MODEL;
    this.failure = null;
    this.finalPromise = null;
    this.utterances = new Map();
    this.pending = [];
    this.pendingBytes = 0;
    this.disposed = false;
    this.closed = false;
  }

  beginConnecting() {}

  connect({ baseUrl, apiKey, language } = {}) {
    if (this.disposed) return Promise.reject(new Error("Dictation canceled"));
    const url = streamingUrl(baseUrl);
    this.connectPromise = new Promise((resolve, reject) => {
      this.resolveConnect = resolve;
      this.rejectConnect = reject;
    });
    this.connectPromise.catch(() => {});
    const fail = () => this.fail("Dictation connection failed");
    this.timer = setTimeout(() => this.fail("Dictation connection timed out"), this.timeoutMs);
    try {
      this.ws = this.createSocket(url, {
        headers: apiKey ? { Authorization: "Bearer " + apiKey } : {},
        maxPayload: 4 * 1024 * 1024,
      });
      const ws = this.ws;
      ws.on("open", () => {
        if (this.disposed) return;
        this.send({
          type: "session.start",
          dictation_session_id: randomUUID(),
          attempt_id: randomUUID(),
          config: {
            input_audio_format: "pcm16",
            sample_rate_hz: 16000,
            num_channels: 1,
            ...(language && language !== "auto" ? { language } : {}),
          },
        });
      });
      ws.on("message", (data) => {
        if (this.disposed || this.ws !== ws) return;
        try {
          this.receive(JSON.parse(data.toString()));
        } catch {
          this.fail("Invalid dictation response");
        }
      });
      ws.on("error", fail);
      ws.on("unexpected-response", (_request, response) => {
        response.resume();
        fail();
      });
      ws.on("close", () => {
        this.isConnected = false;
        if (!this.disposed && !this.closed)
          this.fail("Dictation connection closed before finalization");
      });
    } catch {
      fail();
    }
    return this.connectPromise;
  }

  send(message) {
    if (this.ws?.readyState !== WebSocket.OPEN)
      throw new Error("Dictation connection is unavailable");
    if (this.ws.bufferedAmount > MAX_PENDING_BYTES) {
      this.fail("Dictation connection cannot keep up with audio");
      return;
    }
    this.ws.send(JSON.stringify(message), (error) => {
      if (error && !this.disposed) this.fail("Unable to send dictation audio");
    });
  }

  sendAudio(audio) {
    if (this.disposed || this.failure || this.finalPromise) return;
    if (!audio.length || audio.length % 2) return;
    if (!this.isConnected) {
      this.pendingBytes += audio.length;
      if (this.pendingBytes > MAX_PENDING_BYTES) {
        this.fail("Dictation startup audio buffer exceeded");
        return;
      }
      this.pending.push(Buffer.from(audio));
      return;
    }
    try {
      this.send({ type: "audio.append", audio: audio.toString("base64") });
      this.audioBytesSent += audio.length;
    } catch {
      this.fail("Unable to send dictation audio");
    }
  }

  text(includePartial = false) {
    return [...this.utterances.values()]
      .map((entry) => entry.final?.text ?? (includePartial ? entry.partial?.text : "") ?? "")
      .filter(Boolean)
      .join(" ")
      .trim();
  }

  receive(event) {
    if (event.type === "session.started") {
      if (this.isConnected || this.closed) return;
      clearTimeout(this.timer);
      this.isConnected = true;
      const pending = this.pending;
      this.pending = [];
      this.pendingBytes = 0;
      for (const audio of pending) this.sendAudio(audio);
      this.resolveConnect?.();
      return;
    }
    if (event.type === "transcript.failed" || event.type === "session.error") {
      this.fail("ChatGPT dictation failed");
      return;
    }
    if (
      event.type === "speech.started" ||
      event.type === "transcript.segment" ||
      event.type === "transcript.final"
    ) {
      if (typeof event.utterance_id !== "string") throw new Error("Missing utterance");
      let entry = this.utterances.get(event.utterance_id);
      if (!entry) {
        entry = { final: null, partial: null };
        this.utterances.set(event.utterance_id, entry);
      }
      if (event.type !== "speech.started") {
        if (!Number.isInteger(event.revision) || typeof event.text !== "string")
          throw new Error("Invalid transcript");
        if (event.type === "transcript.final") {
          if (event.revision >= (entry.final?.revision ?? -1)) {
            entry.final = { revision: event.revision, text: event.text };
            entry.partial = null;
          }
        } else if (!entry.final && event.revision >= (entry.partial?.revision ?? -1)) {
          entry.partial = { revision: event.revision, text: event.text };
        }
        this.onPartialTranscript?.(this.text(true));
      }
      return;
    }
    if (event.type === "session.updated" && event.session?.status === "closed") {
      if (!this.finalPromise || [...this.utterances.values()].some((entry) => !entry.final)) {
        this.fail("Dictation closed before all speech was finalized");
        return;
      }
      clearTimeout(this.finalTimer);
      this.closed = true;
      this.isConnected = false;
      const text = this.text();
      this.onFinalTranscript?.(text);
      this.resolveFinal?.({ text, model: MODEL, audioBytesSent: this.audioBytesSent });
      this.ws?.close();
    }
  }

  finalize() {
    if (this.finalPromise) return this.finalPromise;
    if (this.failure || this.disposed || !this.isConnected)
      return Promise.reject(new Error(this.failure || "Dictation connection is unavailable"));
    this.finalPromise = new Promise((resolve, reject) => {
      this.resolveFinal = resolve;
      this.rejectFinal = reject;
    });
    this.finalPromise.catch(() => {});
    this.finalTimer = setTimeout(
      () => this.fail("Dictation finalization timed out"),
      this.finalTimeoutMs
    );
    try {
      this.send({ type: "session.close" });
    } catch {
      this.fail("Unable to finalize dictation");
    }
    return this.finalPromise;
  }

  fail(message) {
    if (this.failure || this.disposed || this.closed) return;
    this.failure = message;
    this.isConnected = false;
    clearTimeout(this.timer);
    clearTimeout(this.finalTimer);
    this.pending = [];
    this.pendingBytes = 0;
    this.rejectConnect?.(new Error(message));
    this.rejectFinal?.(new Error(message));
    this.onError?.(new Error(message));
    this.ws?.terminate();
  }

  async disconnect() {
    this.disposed = true;
    this.isConnected = false;
    clearTimeout(this.timer);
    clearTimeout(this.finalTimer);
    this.rejectConnect?.(new Error("Dictation canceled"));
    if (!this.closed) this.rejectFinal?.(new Error("Dictation canceled"));
    this.pending = [];
    this.pendingBytes = 0;
    this.ws?.terminate();
    this.onClose?.();
    return {
      text: this.closed ? this.text() : "",
      model: MODEL,
      audioBytesSent: this.audioBytesSent,
    };
  }
}

module.exports = { CodexDictationStreaming, streamingUrl };
