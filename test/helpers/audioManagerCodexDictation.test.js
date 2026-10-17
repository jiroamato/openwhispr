const test = require("node:test");
const assert = require("node:assert/strict");
const { loadAudioManager } = require("./harness/audioManager");

const settings = {
  useLocalWhisper: false,
  cloudTranscriptionMode: "byok",
  cloudTranscriptionProvider: "custom",
  cloudTranscriptionModel: "codex-transcribe",
  cloudTranscriptionBaseUrl: "http://127.0.0.1:8317/v1",
  customTranscriptionApiKey: "synthetic-key",
  useCleanupModel: false,
  preferredLanguage: "en",
};
async function load(t) {
  const result = await loadAudioManager(t, {
    cachePrefix: "codex-dictation-",
    settingsKey: "__codexDictationSettings",
    settings,
  });
  result.window.dispatchEvent = () => {};
  return result;
}

test("custom codex-transcribe selects streaming and retains authenticated batch fallback", async (t) => {
  const { createManager, vite } = await load(t);
  const manager = createManager();
  assert.equal(manager.shouldUseStreaming(), true);
  assert.equal(manager.getStreamingProviderName(), "codex-dictation");
  assert.equal(
    manager.classifyStreamingStartResult({ success: false }, { useLocalWhisper: false })
      .needsFallback,
    true
  );
  const { resolveTranscriptionRoute } = await vite.ssrLoadModule("/helpers/transcriptionRoute.ts");
  const route = resolveTranscriptionRoute({
    settings: {
      ...settings,
      transcriptionMode: "self-hosted",
      remoteTranscriptionUrl: settings.cloudTranscriptionBaseUrl,
      remoteTranscriptionModel: "codex-transcribe",
    },
  });
  assert.deepEqual(route.auth, { scheme: "bearer", keyRef: "custom" });
  assert.equal(route.model, "codex-transcribe");
});

for (const acknowledged of [true, false]) {
  test(
    (acknowledged ? "acknowledged close" : "lost close") +
      " delivers once using the complete recording",
    async (t) => {
      const { createManager } = await load(t);
      const recording = new Blob(["whole recording"], { type: "audio/webm" });
      let batches = 0;
      const completions = [];
      const manager = createManager({
        isRecording: true,
        isStreaming: true,
        isProcessing: false,
        streamingStartInProgress: false,
        _streamingStartSettlementWaiters: [],
        _streamingCancellationGeneration: 0,
        _processingCancellationGeneration: 0,
        _activeStreamingSessionId: 1,
        _streamingSessionGeneration: 1,
        _activeStreamingProviderName: "codex-dictation",
        _streamingStopPromise: null,
        _streamingStopMode: null,
        recordingStartTime: Date.now() - 500,
        streamingFinalText: "earlier utterance",
        streamingPartialText: "incomplete tail",
        streamingCleanupFns: [],
        _streamingFallbackSegments: [],
        micRecovery: { stop() {} },
        finishStreamingFallbackSegment: async () => {},
        mergeRecordedSegments: async () => recording,
        getStreamingProvider: () => ({
          finalizeAcknowledged: true,
          finalize: async () =>
            acknowledged
              ? { success: true, text: "complete streamed recording" }
              : { success: false },
          stop: async () => ({ success: true, model: "codex-transcribe" }),
        }),
        processWithOpenAIAPI: async (blob) => {
          assert.strictEqual(blob, recording);
          batches++;
          return {
            text: "complete batch recording",
            rawText: "complete batch recording",
            source: "custom",
          };
        },
        finalizeChineseScript: async (text) => text,
        onStateChange() {},
        onTranscriptionComplete: (result) => {
          completions.push(result);
        },
      });
      await Promise.all([manager.stopStreamingRecording(), manager.stopStreamingRecording()]);
      assert.equal(batches, acknowledged ? 0 : 1);
      assert.equal(completions.length, 1);
      assert.equal(
        completions[0].text,
        acknowledged ? "complete streamed recording" : "complete batch recording"
      );
      assert.equal(manager.isProcessing, false);
    }
  );
}
