import { test } from "bun:test";
import assert from "node:assert/strict";
import {
  EgressStatus,
  ParticipantEgressRequest,
  RoomCompositeEgressRequest,
  WebEgressRequest,
} from "@livekit/protocol";
import {
  normalizedEgressWebhookKind,
  normalizeEgressRequestSource,
} from "./shared/livekit-webhook.ts";

void test("normalizes supported nested Egress request sources", () => {
  assert.deepEqual(
    normalizeEgressRequestSource({
      request: {
        case: "roomComposite",
        value: new RoomCompositeEgressRequest({ roomName: "room-a" }),
      },
    }),
    { kind: "room_composite", roomName: "room-a" },
  );
  assert.deepEqual(
    normalizeEgressRequestSource({
      request: {
        case: "participant",
        value: new ParticipantEgressRequest({
          roomName: "room-b",
          identity: "00000000-0000-4000-8000-000000000002",
        }),
      },
    }),
    {
      kind: "participant",
      roomName: "room-b",
      identity: "00000000-0000-4000-8000-000000000002",
    },
  );
});

void test("rejects incomplete and unrecognized Egress request sources", () => {
  assert.equal(
    normalizeEgressRequestSource({
      request: {
        case: "participant",
        value: new ParticipantEgressRequest({ roomName: "room-b" }),
      },
    }),
    null,
  );
  assert.equal(
    normalizeEgressRequestSource({
      request: { case: "web", value: new WebEgressRequest({ url: "https://invalid.example" }) },
    }),
    null,
  );
  assert.equal(normalizeEgressRequestSource(undefined), null);
});

void test("accepts only ACTIVE as the publication barrier", () => {
  assert.equal(
    normalizedEgressWebhookKind("egress_started", EgressStatus.EGRESS_STARTING),
    "ignored",
  );
  assert.equal(
    normalizedEgressWebhookKind("egress_updated", EgressStatus.EGRESS_ACTIVE),
    "egress_active",
  );
  assert.equal(
    normalizedEgressWebhookKind("egress_updated", EgressStatus.EGRESS_ENDING),
    "ignored",
  );
});

void test("distinguishes successful and failed terminal Egress states", () => {
  assert.equal(
    normalizedEgressWebhookKind("egress_ended", EgressStatus.EGRESS_COMPLETE),
    "egress_complete",
  );
  assert.equal(
    normalizedEgressWebhookKind("egress_ended", EgressStatus.EGRESS_FAILED),
    "egress_failed",
  );
  assert.equal(
    normalizedEgressWebhookKind("egress_ended", EgressStatus.EGRESS_ABORTED),
    "egress_failed",
  );
  assert.equal(
    normalizedEgressWebhookKind("egress_ended", EgressStatus.EGRESS_LIMIT_REACHED),
    "egress_failed",
  );
});

void test("never derives an Egress transition from an unrelated event", () => {
  assert.equal(normalizedEgressWebhookKind("participant_joined", EgressStatus.EGRESS_ACTIVE), null);
  assert.equal(normalizedEgressWebhookKind(undefined, undefined), null);
});
