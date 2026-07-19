import { test } from "bun:test";
import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { TrackSource } from "@livekit/protocol";
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

function createAdapter(internalToken: () => Promise<string> = async () => token) {
  return new LiveKitEffects(
    {
      url: "http://livekit:7880",
      apiKey: "key",
      apiSecret: "secret",
      internalToken,
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

test("internal callback reloads a rotated projected bearer for every request", async () => {
  let projectedToken = "first";
  const authorizations: string[] = [];
  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
    authorizations.push(String(new Headers(init?.headers).get("authorization")));
    return new Response("true", {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }) as typeof fetch;

  try {
    const adapter = createAdapter(async () => projectedToken);
    await adapter.notifyDeleteDrain(consultationId, 9);
    projectedToken = "second";
    await adapter.notifyDeleteDrain(consultationId, 9);
    assert.deepEqual(authorizations, ["Bearer first", "Bearer second"]);
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

test("status delivery is terminal once its room is absent", async () => {
  const adapter = createAdapter();
  Object.assign(adapter, {
    rooms: {
      listRooms: async () => [],
    },
  });
  const status = {
    ...effect,
    kind: "STATUS_PACKET" as const,
    plan: {
      roomName: "50000000-0000-4000-8000-000000000003",
    },
  };

  assert.deepEqual(await adapter.adopt(status, status.plan), {
    remoteId: status.id,
    matchesRequest: true,
    terminal: true,
    result: { sent: false, skipped: "room_absent" },
  });
});

test("room drain adoption waits for every Egress terminal", async () => {
  const adapter = createAdapter();
  let status = EgressStatus.EGRESS_ACTIVE;
  Object.assign(adapter, {
    rooms: {
      listRooms: async () => [],
    },
    egress: {
      listEgress: async () => [{ status }],
    },
  });
  const drain = {
    ...effect,
    kind: "ROOM_DRAIN" as const,
    plan: {
      roomName: "50000000-0000-4000-8000-000000000003",
    },
  };

  assert.equal(await adapter.adopt(drain, drain.plan), null);
  status = EgressStatus.EGRESS_COMPLETE;
  assert.equal((await adapter.adopt(drain, drain.plan))?.matchesRequest, true);
});

test("participant grant adoption retries until the exact least-privilege grant exists", async () => {
  const adapter = createAdapter();
  let permission = {
    canSubscribe: true,
    canPublish: true,
    canPublishData: false,
    canPublishSources: [TrackSource.MICROPHONE, TrackSource.CAMERA, TrackSource.SCREEN_SHARE],
  };
  Object.assign(adapter, {
    rooms: {
      getParticipant: async () => ({
        sid: "PA_test",
        permission,
      }),
    },
  });
  const grant = {
    ...effect,
    kind: "PARTICIPANT_GRANT" as const,
    plan: {
      roomName: "50000000-0000-4000-8000-000000000003",
      participantIdentity: "50000000-0000-4000-8000-000000000004",
    },
  };

  assert.equal(await adapter.adopt(grant, grant.plan), null);
  permission = {
    canSubscribe: true,
    canPublish: true,
    canPublishData: false,
    canPublishSources: [TrackSource.MICROPHONE, TrackSource.CAMERA],
  };
  assert.equal((await adapter.adopt(grant, grant.plan))?.matchesRequest, true);
});

test("compensation discovers an ambiguous deterministic room create", async () => {
  const adapter = createAdapter();
  const deleted: string[] = [];
  Object.assign(adapter, {
    rooms: {
      listRooms: async () => [
        {
          sid: "RM_test",
          metadata: JSON.stringify({ adoptionId: effect.id }),
        },
      ],
      deleteRoom: async (roomName: string) => {
        deleted.push(roomName);
      },
    },
  });
  const roomCreate = {
    ...effect,
    kind: "ROOM_CREATE" as const,
    plan: {
      roomName: "50000000-0000-4000-8000-000000000003",
      emptyTimeout: 300,
    },
  };

  await adapter.compensate(roomCreate);

  assert.deepEqual(deleted, [roomCreate.plan.roomName]);
});

test("compensation propagates dispatch, Egress, and room cleanup failures", async () => {
  const failure = new Error("cleanup unavailable");
  const dispatch = createAdapter();
  Object.assign(dispatch, {
    dispatch: {
      listDispatch: async () => [{ id: "AD_test" }],
      deleteDispatch: async () => {
        throw failure;
      },
    },
  });
  await assert.rejects(
    () =>
      dispatch.compensate({
        ...effect,
        kind: "WORKER_DISPATCH",
        remoteId: "AD_test",
        plan: { roomName: "room" },
      }),
    failure,
  );

  const egress = createAdapter();
  Object.assign(egress, {
    egress: {
      listEgress: async () => [{ egressId: "EG_test", status: EgressStatus.EGRESS_ACTIVE }],
      stopEgress: async () => {
        throw failure;
      },
    },
  });
  await assert.rejects(() => egress.compensate({ ...effect, remoteId: "EG_test" }), failure);

  const room = createAdapter();
  Object.assign(room, {
    rooms: {
      listRooms: async () => [{ sid: "RM_test" }],
      deleteRoom: async () => {
        throw failure;
      },
    },
  });
  await assert.rejects(
    () =>
      room.compensate({
        ...effect,
        kind: "ROOM_CREATE",
        remoteId: "RM_test",
        plan: { roomName: "room" },
      }),
    failure,
  );
});

test("compensation accepts only confirmed absent or terminal resources without cleanup calls", async () => {
  const cleanupCalls: string[] = [];
  const dispatch = createAdapter();
  Object.assign(dispatch, {
    dispatch: {
      listDispatch: async () => [],
      deleteDispatch: async () => {
        cleanupCalls.push("dispatch");
      },
    },
  });
  await dispatch.compensate({
    ...effect,
    kind: "WORKER_DISPATCH",
    remoteId: "AD_absent",
    plan: { roomName: "room" },
  });

  const egress = createAdapter();
  Object.assign(egress, {
    egress: {
      listEgress: async () => [{ egressId: "EG_terminal", status: EgressStatus.EGRESS_COMPLETE }],
      stopEgress: async () => {
        cleanupCalls.push("egress");
      },
    },
  });
  await egress.compensate({ ...effect, remoteId: "EG_terminal" });

  const room = createAdapter();
  Object.assign(room, {
    rooms: {
      listRooms: async () => [],
      deleteRoom: async () => {
        cleanupCalls.push("room");
      },
    },
  });
  await room.compensate({
    ...effect,
    kind: "ROOM_CREATE",
    remoteId: "RM_absent",
    plan: { roomName: "room" },
  });

  assert.deepEqual(cleanupCalls, []);
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
