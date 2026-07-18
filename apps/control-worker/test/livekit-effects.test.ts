import { test } from "bun:test";
import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { EgressStatus } from "livekit-server-sdk";
import { egressStatusName, LiveKitEffects } from "../src/adapters/livekit-effects";
import type { Effect } from "../src/orchestration/model";

const consultationId = "50000000-0000-4000-8000-000000000001";
const token = "control-worker-test-secret";
const signingKey = "egress-layout-signing-secret";
const effect: Effect = {
  id: "50000000-0000-4000-8000-000000000002",
  consultationId,
  generation: 6,
  kind: "ROOM_COMPOSITE_EGRESS",
  subjectId: consultationId,
  occurrenceKey: "ROOM_COMPOSITE_EGRESS:test",
  state: "planned",
  plan: {},
  requestBytes: null,
  requestSha256: null,
  remoteId: null,
  attempt: 0,
  leaseOwner: null,
  leaseExpiresAt: null,
};

function createAdapter() {
  return new LiveKitEffects(
    {
      url: "http://livekit:7880",
      apiKey: "key",
      apiSecret: "secret",
      internalToken: token,
      egressLayoutSigningKey: signingKey,
      egressLayoutUrl: "http://web:3000/egress-layout",
      s3: {
        accessKey: "access",
        secretKey: "secret",
        endpoint: "http://minio:9000",
        bucket: "archive",
        region: "eu-central-1",
        forcePathStyle: true,
      },
    },
    {} as never,
  );
}

test("composite Egress request persists the generation-bound signed layout URL", () => {
  const adapter = createAdapter();
  const expires = 123_456;
  const request = {
    roomName: "50000000-0000-4000-8000-000000000003",
    outputPrefix: `v1/meetings/${consultationId}/media/composite/6`,
    layoutExpiresAtMs: expires,
  };
  const encoded = JSON.parse(
    Buffer.from(adapter.canonicalRequest(effect, request)).toString("utf8"),
  ) as {
    customBaseUrl: string;
    roomName: string;
    segmentOutputs: unknown[];
    kind?: string;
  };
  const url = new URL(encoded.customBaseUrl);
  const expectedSignature = createHmac("sha256", signingKey)
    .update(`${consultationId}\n6\n${expires}`)
    .digest("hex");

  assert.equal(encoded.kind, undefined);
  assert.equal(encoded.roomName, request.roomName);
  assert.equal(encoded.segmentOutputs.length, 1);
  assert.equal(url.pathname, "/egress-layout");
  assert.equal(url.searchParams.get("generation"), "6");
  assert.equal(url.searchParams.get("expires"), String(expires));
  assert.equal(url.searchParams.get("signature"), expectedSignature);
});

test("delete drain callback carries the fenced archive write epoch", async () => {
  let submitted: unknown;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
    submitted = JSON.parse(String(init?.body));
    return new Response("true", {
      status: 200,
      headers: {
        "content-type": "application/json",
      },
    });
  }) as typeof fetch;

  try {
    const adapter = createAdapter();
    const drained = await adapter.notifyDeleteDrain(consultationId, 9);

    assert.equal(drained, true);
    assert.deepEqual(submitted, {
      consultationId,
      writeEpoch: 9,
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("dispatch deletion is terminal once its room is absent", async () => {
  const adapter = createAdapter();
  let dispatchListed = false;
  Object.assign(adapter, {
    rooms: {
      listRooms: async () => [],
    },
    dispatch: {
      listDispatch: async () => {
        dispatchListed = true;
        throw new Error("dispatch service must not be queried after room deletion");
      },
    },
  });

  const adoption = await adapter.adopt(
    { ...effect, kind: "DISPATCH_DELETE" },
    {
      roomName: "50000000-0000-4000-8000-000000000003",
      dispatchId: "AD_test",
    },
  );

  assert.deepEqual(adoption, {
    remoteId: "AD_test",
    matchesRequest: true,
    terminal: true,
  });
  assert.equal(dispatchListed, false);
});

test("LiveKit numeric Egress statuses persist as canonical literals", () => {
  assert.deepEqual(
    [
      EgressStatus.EGRESS_STARTING,
      EgressStatus.EGRESS_ACTIVE,
      EgressStatus.EGRESS_ENDING,
      EgressStatus.EGRESS_COMPLETE,
      EgressStatus.EGRESS_FAILED,
      EgressStatus.EGRESS_ABORTED,
      EgressStatus.EGRESS_LIMIT_REACHED,
    ].map(egressStatusName),
    [
      "EGRESS_STARTING",
      "EGRESS_ACTIVE",
      "EGRESS_ENDING",
      "EGRESS_COMPLETE",
      "EGRESS_FAILED",
      "EGRESS_ABORTED",
      "EGRESS_LIMIT_REACHED",
    ],
  );
});
