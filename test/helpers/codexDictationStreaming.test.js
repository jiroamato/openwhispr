const test = require("node:test");
const assert = require("node:assert/strict");
const { once } = require("node:events");
const WebSocket = require("ws");
const {
  CodexDictationStreaming,
  streamingUrl,
} = require("../../src/helpers/codexDictationStreaming");

async function fixture(t, handle) {
  const server = new WebSocket.Server({ port: 0, host: "127.0.0.1" });
  await once(server, "listening");
  t.after(async () => {
    for (const socket of server.clients) socket.terminate();
    await new Promise((resolve) => server.close(resolve));
  });
  server.on("connection", (socket, request) => {
    assert.equal(request.headers.authorization, "Bearer synthetic-key");
    socket.on("message", (data) => handle(socket, JSON.parse(data)));
  });
  const client = new CodexDictationStreaming({ timeoutMs: 1000, finalTimeoutMs: 1000 });
  t.after(() => client.disconnect());
  return {
    client,
    options: {
      baseUrl: "http://127.0.0.1:" + server.address().port + "/v1",
      apiKey: "synthetic-key",
      language: "en",
    },
  };
}
const send = (socket, value) => socket.send(JSON.stringify(value));
const transcript = (socket, type, revision, text, utterance_id = "u1") =>
  send(socket, { type, revision, text, utterance_id });

test("partials precede stop; revisions merge and repeated stop commits once", async (t) => {
  let closes = 0;
  const { client, options } = await fixture(t, (socket, frame) => {
    if (frame.type === "session.start") {
      assert.equal(frame.config.sample_rate_hz, 16000);
      assert.equal(frame.config.language, "en");
      assert.equal(frame.model, undefined);
      assert.equal(frame.config.prompt, undefined);
      send(socket, { type: "session.started" });
    } else if (frame.type === "audio.append") {
      assert.equal(frame.audio, "AQACAA==");
      transcript(socket, "transcript.segment", 2, "Hello there");
      transcript(socket, "transcript.segment", 1, "stale");
    } else if (frame.type === "session.close") {
      closes++;
      transcript(socket, "transcript.final", 3, "Hello world.");
      transcript(socket, "transcript.final", 2, "stale final");
      transcript(socket, "transcript.final", 1, "Another sentence.", "u2");
      send(socket, { type: "session.updated", session: { status: "closed" } });
    }
  });
  const partial = new Promise((resolve) => {
    client.onPartialTranscript = resolve;
  });
  let commits = 0;
  client.onFinalTranscript = () => commits++;
  client.sendAudio(Buffer.from([1, 0, 2, 0]));
  await client.connect(options);
  assert.equal(await partial, "Hello there");
  assert.equal(commits, 0);
  const result = client.finalize();
  assert.strictEqual(client.finalize(), result);
  assert.equal((await result).text, "Hello world. Another sentence.");
  assert.equal(closes, 1);
  assert.equal(commits, 1);
});

for (const mode of ["cancel", "disconnect", "missing-final"]) {
  test(mode + " never commits incomplete text", async (t) => {
    let closes = 0;
    const { client, options } = await fixture(t, (socket, frame) => {
      if (frame.type === "session.start") send(socket, { type: "session.started" });
      if (frame.type === "audio.append") {
        transcript(socket, "transcript.segment", 1, "incomplete");
        if (mode === "disconnect") socket.close();
      }
      if (frame.type === "session.close") {
        closes++;
        send(socket, { type: "session.updated", session: { status: "closed" } });
      }
    });
    await client.connect(options);
    const partial = new Promise((resolve) => {
      client.onPartialTranscript = resolve;
    });
    const failed = new Promise((resolve) => {
      client.onError = resolve;
    });
    client.sendAudio(Buffer.from([0, 0]));
    await partial;
    if (mode === "cancel") await client.disconnect();
    if (mode === "disconnect") await failed;
    await assert.rejects(client.finalize());
    assert.equal((await client.disconnect()).text, "");
    assert.equal(closes, mode === "missing-final" ? 1 : 0);
  });
}

test("cancel during startup settles connection", async (t) => {
  const { client, options } = await fixture(t, () => {});
  const connected = client.connect(options);
  await client.disconnect();
  await assert.rejects(connected, /canceled/);
});

test("proxy URLs preserve batch base and reject embedded credentials", () => {
  assert.equal(
    streamingUrl("http://r4.taile55c7a.ts.net:8317/v1"),
    "ws://r4.taile55c7a.ts.net:8317/v1/audio/transcriptions/stream"
  );
  assert.throws(() => streamingUrl("https://user:secret@proxy.test/v1"));
});

test("revision zero and empty final transcripts remain valid", async (t) => {
  const { client, options } = await fixture(t, (socket, frame) => {
    if (frame.type === "session.start") send(socket, { type: "session.started" });
    if (frame.type === "session.close") {
      transcript(socket, "transcript.final", 0, "");
      send(socket, { type: "session.updated", session: { status: "closed" } });
    }
  });
  await client.connect(options);
  assert.equal((await client.finalize()).text, "");
});

test("a finalization timeout rejects and exposes only a safe stack", async (t) => {
  const { client, options } = await fixture(t, (socket, frame) => {
    if (frame.type === "session.start") send(socket, { type: "session.started" });
  });
  await client.connect(options);
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const closing = client.finalize();
  t.mock.timers.tick(1001);
  await assert.rejects(closing, /timed out/);
});
