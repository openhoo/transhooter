import { test } from "bun:test";
import assert from "node:assert/strict";
import { TrackSource } from "@livekit/protocol";
import { EgressStatus } from "livekit-server-sdk";
import { LiveKitEffects } from "../src/adapters/livekit-effects";
import { egressStatusName, isViableEgressAdoption } from "../src/adapters/livekit-effects/egress";
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

test("canonical Egress intent contains only durable identities and semantic output", () => {
  const adapter = createAdapter();
  const request = {
    roomName: "50000000-0000-4000-8000-000000000003",
    outputPrefix: `v1/meetings/${consultationId}/media/composite/6`,
    layoutExpiresAtMs: 123_456,
  };
  const bytes = Buffer.from(adapter.canonicalRequest(effect, request));
  const encoded = JSON.parse(bytes.toString("utf8"));

  assert.deepEqual(encoded, {
    consultationId,
    egressIdentity: effect.id,
    generation: 6,
    kind: "ROOM_COMPOSITE_EGRESS",
    output: {
      format: "segmented_hls",
      segmentDurationSeconds: 2,
    },
    render: {
      audioOnly: false,
      layout: "speaker",
      videoOnly: false,
    },
    roomName: request.roomName,
  });
  for (const forbidden of [
    request.outputPrefix,
    String(request.layoutExpiresAtMs),
    "customBaseUrl",
    "access",
    "secret",
    "http://minio:9000",
    "archive",
  ]) {
    assert.equal(bytes.includes(forbidden), false, `persisted ${forbidden}`);
  }
});

test("shutdown status is broadcast even when an old plan targets only humans", () => {
  const adapter = createAdapter();
  const request = {
    roomName: "50000000-0000-4000-8000-000000000003",
    topic: "consultation.status.v1",
    reasonCode: "SHUTDOWN",
    state: "finalizing",
    shutdownAtMs: 123_456,
    occurredAtMs: 123_000,
    destinationIdentities: [
      "50000000-0000-4000-8000-000000000004",
      "50000000-0000-4000-8000-000000000005",
    ],
  };
  const bytes = Buffer.from(
    adapter.canonicalRequest({ ...effect, kind: "STATUS_PACKET" }, request),
  );
  const encoded = JSON.parse(bytes.toString("utf8"));

  assert.deepEqual(encoded.destinationIdentities ?? [], []);
});

test("Egress creation returns the durable accepted status without claiming ACTIVE", async () => {
  const adapter = createAdapter();
  Object.assign(adapter, {
    egress: {
      startRoomCompositeEgress: async () => ({
        egressId: "EG_starting",
        status: EgressStatus.EGRESS_STARTING,
      }),
    },
  });

  const result = await adapter.execute(effect, {
    roomName: "50000000-0000-4000-8000-000000000003",
    outputPrefix: `v1/meetings/${consultationId}/media/composite/6`,
    layoutExpiresAtMs: Date.now() + 60_000,
  });

  assert.deepEqual(result, {
    remoteId: "EG_starting",
    result: { egressId: "EG_starting", status: "EGRESS_STARTING" },
  });
});

test("Egress adoption preserves STARTING until verified ACTIVE evidence arrives", async () => {
  const adapter = createAdapter();
  const roomName = "50000000-0000-4000-8000-000000000003";
  Object.assign(adapter, {
    egress: {
      listEgress: async () => [
        {
          egressId: "EG_adopted",
          roomName,
          request: { effectId: effect.id },
          status: EgressStatus.EGRESS_STARTING,
        },
      ],
    },
  });

  const adoption = await adapter.adopt(effect, {
    roomName,
    outputPrefix: `v1/meetings/${consultationId}/media/composite/6`,
    layoutExpiresAtMs: Date.now() + 60_000,
  });

  assert.deepEqual(adoption, {
    remoteId: "EG_adopted",
    matchesRequest: true,
    terminal: false,
    result: { egressId: "EG_adopted", status: "EGRESS_STARTING" },
  });
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
    const drained = await adapter.notifyDeleteDrain(consultationId, 9, "retention_delete");

    assert.equal(drained, true);
    assert.deepEqual(submitted, {
      consultationId,
      writeEpoch: 9,
      reason: "retention_delete",
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("delete drain rejects a blank durable reason", async () => {
  const adapter = createAdapter();
  await assert.rejects(
    () => adapter.notifyDeleteDrain(consultationId, 9, "  "),
    /reason must be nonblank/,
  );
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
    await adapter.notifyDeleteDrain(consultationId, 9, "retention_delete");
    projectedToken = "second";
    await adapter.notifyDeleteDrain(consultationId, 9, "retention_delete");
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

test("participant Egress adoption uses the persisted resource room and Egress identity", async () => {
  const adapter = createAdapter();
  const resourceRoomName = "50000000-0000-4000-8000-000000000003";
  const participantIdentity = "50000000-0000-4000-8000-000000000004";
  const queriedRooms: string[] = [];
  Object.assign(adapter, {
    egress: {
      listEgress: async ({ roomName }: { roomName: string }) => {
        queriedRooms.push(roomName);
        return [
          {
            egressId: "EG_stale",
            roomName: resourceRoomName,
            status: EgressStatus.EGRESS_ACTIVE,
            request: { segmentOutputs: [{ filenamePrefix: "archive/old-effect-id" }] },
          },
          {
            egressId: "EG_current",
            roomName: resourceRoomName,
            status: EgressStatus.EGRESS_ACTIVE,
            request: {
              identity: participantIdentity,
              segmentOutputs: [{ filenamePrefix: `archive/${effect.id}` }],
            },
          },
        ];
      },
    },
  });
  const participantEgress = {
    ...effect,
    kind: "PARTICIPANT_EGRESS" as const,
  };
  const request = {
    roomName: "cleanup-generation-room",
    resourceRoomName,
    participantIdentity,
    outputPrefix: `v1/meetings/${consultationId}/media/participants/6`,
  };

  assert.deepEqual(await adapter.adopt(participantEgress, request), {
    remoteId: "EG_current",
    matchesRequest: true,
    terminal: false,
    result: { egressId: "EG_current", status: "EGRESS_ACTIVE" },
  });
  assert.deepEqual(queriedRooms, [resourceRoomName]);
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

test("participant grant compensation revokes the exact persisted identity", async () => {
  const adapter = createAdapter();
  const updates: unknown[] = [];
  const roomName = "50000000-0000-4000-8000-000000000003";
  const participantIdentity = "50000000-0000-4000-8000-000000000004";

  Object.assign(adapter, {
    rooms: {
      getParticipant: async (room: string, identity: string) => {
        assert.equal(room, roomName);
        assert.equal(identity, participantIdentity);
        return { sid: "PA_untrusted_for_cleanup" };
      },
      updateParticipant: async (room: string, identity: string, update: unknown) => {
        updates.push({ room, identity, update });
      },
    },
  });

  await adapter.compensate({
    ...effect,
    kind: "PARTICIPANT_GRANT",
    remoteId: "PA_untrusted_for_cleanup",
    plan: { roomName, participantIdentity },
  });

  assert.deepEqual(updates, [
    {
      room: roomName,
      identity: participantIdentity,
      update: {
        permission: {
          canSubscribe: false,
          canPublish: false,
          canPublishData: false,
          canPublishSources: [],
        },
      },
    },
  ]);
});

test("participant removal targets the persisted resource room", async () => {
  const adapter = createAdapter();
  const resourceRoomName = "50000000-0000-4000-8000-000000000003";
  const participantIdentity = "50000000-0000-4000-8000-000000000004";
  const rooms: string[] = [];
  Object.assign(adapter, {
    rooms: {
      listParticipants: async (roomName: string) => {
        rooms.push(roomName);
        return [];
      },
      removeParticipant: async (roomName: string, identity: string) => {
        rooms.push(roomName);
        assert.equal(identity, participantIdentity);
      },
    },
  });
  const removal = {
    ...effect,
    kind: "PARTICIPANT_REMOVE" as const,
  };
  const request = {
    roomName: "cleanup-generation-room",
    resourceRoomName,
    participantIdentity,
  };
  const canonical = JSON.parse(
    Buffer.from(adapter.canonicalRequest(removal, request)).toString("utf8"),
  ) as { room: string };

  assert.equal(canonical.room, resourceRoomName);
  assert.equal((await adapter.adopt(removal, request))?.terminal, true);
  await adapter.execute(removal, request);
  assert.deepEqual(rooms, [resourceRoomName, resourceRoomName]);
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

test("Egress adoption reuses only live lifecycle states", () => {
  assert.deepEqual(
    [
      EgressStatus.EGRESS_STARTING,
      EgressStatus.EGRESS_ACTIVE,
      EgressStatus.EGRESS_ENDING,
      EgressStatus.EGRESS_COMPLETE,
      EgressStatus.EGRESS_FAILED,
      EgressStatus.EGRESS_ABORTED,
      EgressStatus.EGRESS_LIMIT_REACHED,
    ].map(isViableEgressAdoption),
    [true, true, false, false, false, false, false],
  );
});
