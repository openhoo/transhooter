import { test } from "bun:test";
import assert from "node:assert/strict";
import { EgressStatus } from "@livekit/protocol";
import { normalizedEgressWebhookKind } from "./livekit-webhook.ts";

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
