import { test } from "bun:test";
import assert from "node:assert/strict";
import { CaptionPacketSchema, StatusPacketSchema } from "@transhooter/contracts";
import { durableConsultationDestination } from "./consultation-routing.ts";
import { acceptsCaption, acceptsStatus, audioGains, releaseLocalTrack } from "./room-policy.ts";

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

void test("caption trust binding and revisions fail closed", () => {
  assert.equal(acceptsCaption(finalCaption, undefined, expectedCaption), true);
  assert.equal(
    acceptsCaption(
      {
        ...finalCaption,
        sourceParticipantId: "10000000-0000-4000-8000-000000000099",
      },
      undefined,
      expectedCaption,
    ),
    false,
  );
  assert.equal(
    acceptsCaption({ ...finalCaption, revision: 1 }, finalCaption, expectedCaption),
    false,
  );
  assert.equal(
    acceptsCaption(
      { ...finalCaption, revision: 3, finality: "provisional" },
      finalCaption,
      expectedCaption,
    ),
    false,
  );
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

void test("ready consultations remain in the lobby until every remote media barrier exists", () => {
  const pending = {
    id: consultationId,
    state: "ready" as const,
    roomName: "room-id",
    roomSid: "room-sid",
    dispatchId: null,
    compositeEgressId: "egress-id",
  };
  assert.equal(durableConsultationDestination(pending), null);
  assert.equal(
    durableConsultationDestination({ ...pending, dispatchId: "dispatch-id" }),
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
