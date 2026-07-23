import { test } from "bun:test";
import assert from "node:assert/strict";
import { CaptionPacketSchema, StatusPacketSchema } from "@transhooter/contracts";
import {
  acceptsCaption,
  acceptsStatus,
  audioGains,
  CAPTION_REVISION_RECENT_LIMIT,
  CAPTION_REVISION_TOMBSTONE_LIMIT,
  CaptionRevisionPolicy,
  releaseLocalTrack,
} from "./browser/room-policy.ts";
import { durableConsultationDestination } from "./shared/consultation-routing.ts";

const consultationId = "10000000-0000-4000-8000-000000000001";
const destinationParticipantId = "10000000-0000-4000-8000-000000000002";
const sourceParticipantId = "10000000-0000-4000-8000-000000000003";
const utteranceId = "10000000-0000-4000-8000-000000000004";
const expectedCaption = {
  consultationId,
  destinationParticipantId,
  sourceParticipantId,
};
const finalCaption = CaptionPacketSchema.parse({
  schemaVersion: 1,
  consultationId,
  destinationParticipantId,
  sourceParticipantId,
  utteranceId,
  revision: 2,
  finality: "final",
  sourceLanguage: "en-US",
  targetLanguage: "de-DE",
  sourceText: "Hello",
  translatedText: "Hallo",
  sourceSampleStart: 0,
  sourceSampleEnd: 16000,
  occurredAtMs: 10,
});

void test("caption route binding and revisions fail closed", () => {
  assert.equal(acceptsCaption(finalCaption, undefined, expectedCaption, true), true);
  assert.equal(
    acceptsCaption(
      {
        ...finalCaption,
        sourceParticipantId: "10000000-0000-4000-8000-000000000099",
      },
      undefined,
      expectedCaption,
      true,
    ),
    false,
  );
  assert.equal(
    acceptsCaption({ ...finalCaption, revision: 1 }, finalCaption, expectedCaption, true),
    false,
  );
  assert.equal(
    acceptsCaption(
      { ...finalCaption, revision: 3, finality: "provisional" },
      finalCaption,
      expectedCaption,
      true,
    ),
    false,
  );
});

void test("caption revision tracking stays bounded and rejects late regressions", () => {
  const policy = new CaptionRevisionPolicy();
  const totalTracked = CAPTION_REVISION_RECENT_LIMIT + CAPTION_REVISION_TOMBSTONE_LIMIT;
  const packet = (index: number, revision = 1, finality: "provisional" | "final" = "final") =>
    CaptionPacketSchema.parse({
      ...finalCaption,
      utteranceId: `10000000-0000-4000-8001-${index.toString(16).padStart(12, "0")}`,
      revision,
      finality,
      sourceSampleStart: index * 16000,
      sourceSampleEnd: (index + 1) * 16000,
      occurredAtMs: index + 100,
    });

  for (let index = 0; index < totalTracked + 20; index += 1) {
    assert.equal(policy.accepts(packet(index)), true);
  }

  assert.equal(policy.trackedUtteranceCount, totalTracked);
  assert.equal(policy.accepts(packet(0, 2)), false);
  assert.equal(policy.accepts(packet(0, 1)), false);
  assert.equal(policy.accepts(packet(0, 3, "provisional")), false);
  assert.equal(policy.accepts(packet(20, 1)), false);
  assert.equal(policy.accepts(packet(20, 2, "provisional")), false);
});

void test("caption tombstones preserve finality and revision ordering after recent eviction", () => {
  const policy = new CaptionRevisionPolicy();
  const original = { ...finalCaption, sourceSampleStart: 0, sourceSampleEnd: 16000 };
  assert.equal(policy.accepts(original), true);

  for (let index = 1; index <= CAPTION_REVISION_RECENT_LIMIT; index += 1) {
    assert.equal(
      policy.accepts({
        ...finalCaption,
        utteranceId: `10000000-0000-4000-8002-${index.toString(16).padStart(12, "0")}`,
        sourceSampleStart: index * 16000,
        sourceSampleEnd: (index + 1) * 16000,
        occurredAtMs: index + 100,
      }),
      true,
    );
  }

  assert.equal(policy.accepts({ ...original, revision: 1 }), false);
  assert.equal(policy.accepts({ ...original, revision: 3, finality: "provisional" }), false);
  assert.equal(policy.accepts({ ...original, revision: 3, finality: "final" }), true);
});

void test("caption trust binding requires a LiveKit agent sender", () => {
  assert.equal(acceptsCaption(finalCaption, undefined, expectedCaption, false), false);
});

void test("status accepts only absent server sender in the current generation", () => {
  const status = StatusPacketSchema.parse({
    schemaVersion: 1,
    consultationId,
    generation: 7,
    occurredAtMs: 10,
    state: "ready",
    reasonCode: "CAPTURE_READY",
    subjectParticipantId: destinationParticipantId,
    participantEgressId: "egress-1",
    shutdownAtMs: null,
  });
  assert.equal(acceptsStatus(status, false, { consultationId, generation: 7 }), true);
  assert.equal(acceptsStatus(status, true, { consultationId, generation: 7 }), false);
  assert.equal(acceptsStatus(status, false, { consultationId, generation: 8 }), false);
});

void test("audio modes fall back atomically to original audio", () => {
  assert.deepEqual(audioGains("interpreted", true, false), {
    original: 0,
    interpretation: 1,
  });
  assert.deepEqual(audioGains("overlay", true, false), {
    original: 0.18,
    interpretation: 1,
  });
  assert.deepEqual(audioGains("original", true, false), {
    original: 1,
    interpretation: 0,
  });
  assert.deepEqual(audioGains("overlay", false, false), {
    original: 1,
    interpretation: 0,
  });
  assert.deepEqual(audioGains("interpreted", false, true), {
    original: 1,
    interpretation: 0,
  });
});

void test("ready consultations enter the room before capture and worker barriers settle", () => {
  const pending = {
    id: consultationId,
    state: "ready" as const,
    roomName: "room-id",
    roomSid: null,
    dispatchId: null,
    compositeEgressId: null,
  };
  assert.equal(durableConsultationDestination(pending), null);
  assert.equal(
    durableConsultationDestination({ ...pending, roomSid: "room-sid" }),
    `/consultations/${consultationId}/room`,
  );
  assert.equal(
    durableConsultationDestination({
      ...pending,
      state: "ended",
      roomName: null,
      roomSid: null,
      compositeEgressId: null,
    }),
    `/archives/${consultationId}`,
  );
});

void test("local tracks stop even when LiveKit unpublish rejects", async () => {
  let detached = 0;
  let stopped = 0;
  const localTrack = {
    detach: () => {
      detached += 1;
    },
    stop: () => {
      stopped += 1;
    },
  };

  await assert.rejects(
    releaseLocalTrack(localTrack, () => Promise.reject(new Error("disconnected"))),
  );
  assert.equal(detached, 1);
  assert.equal(stopped, 1);
});
