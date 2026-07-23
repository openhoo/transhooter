import { describe, expect, it, mock } from "bun:test";
import type { Consultation } from "../src/consultations/domain";
import { ConsultationService } from "../src/consultations/service";
import type { AuthRepository, ConsultationRepository, Transaction } from "../src/ports/index";

const TRANSACTION = {
  opaque: Symbol("consultation-test"),
} satisfies Transaction;
const NOW = new Date("2026-01-01T00:00:00Z");
const EMPLOYEE_ID = "00000000-0000-4000-8000-000000000001";
const CUSTOMER_ID = "00000000-0000-4000-8000-000000000002";
const PROFILE_ID = "00000000-0000-4000-8000-000000000003";
const PROFILE_NAME = "google-eu";
const CREATION_KEY = "00000000-0000-4000-8000-000000000004";
const OTHER_CUSTOMER_ID = "00000000-0000-4000-8000-000000000005";
const OTHER_PROFILE_ID = "00000000-0000-4000-8000-000000000006";
const OTHER_EMPLOYEE_ID = "00000000-0000-4000-8000-000000000007";
const ENABLED_PROFILE_REVISION = {
  profileId: PROFILE_ID,
  revision: 9,
};

function createConsultationFixture() {
  let aggregate: Consultation | null = null;
  let sequence = 10;
  let profilesAvailable = true;
  const creations = new Map<string, Consultation>();
  const auditAppend = mock(async () => undefined);
  const enqueue = mock(async () => undefined);
  const tokenIssue = mock(async () => "token");
  const repository = {
    transaction: async <T>(work: (value: Transaction) => Promise<T>) => work(TRANSACTION),
    create: async (value: Consultation, employeeUserId: string, creationIdempotencyKey: string) => {
      const scope = `${employeeUserId}:${creationIdempotencyKey}`;
      if (creations.has(scope)) {
        return false;
      }
      creations.set(scope, value);
      aggregate = value;
      return true;
    },
    findByCreationIdempotencyKey: async (employeeUserId: string, creationIdempotencyKey: string) =>
      creations.get(`${employeeUserId}:${creationIdempotencyKey}`) ?? null,
    lock: async () => aggregate,
    get: async () => aggregate,
    listForUser: async () => (aggregate ? [aggregate] : []),
    save: async (value: Consultation) => {
      aggregate = value;
      return true;
    },
  } as unknown as ConsultationRepository;
  const revokeConsultationLinks = mock(async () => undefined);
  const authRepository = {
    revokeConsultationLinks,
  } as unknown as AuthRepository;
  const currentEnabledRevision = mock(async (profileReference: string) => {
    if (!profilesAvailable) {
      throw new Error("provider profiles unavailable");
    }
    return profileReference === OTHER_PROFILE_ID
      ? { profileId: OTHER_PROFILE_ID, revision: 3 }
      : ENABLED_PROFILE_REVISION;
  });
  const service = new ConsultationService(
    repository,
    {
      plan: async () => {
        throw new Error("unused");
      },
      enqueue,
    } as never,
    {
      currentEnabledRevision,
      assertFreshAndHealthy: async () => undefined,
    } as never,
    { issue: tokenIssue },
    { append: auditAppend },
    { now: () => NOW },
    {
      uuid: () => `00000000-0000-4000-8000-${String(sequence++).padStart(12, "0")}`,
    },
    {
      sha256Canonical: () => "hash",
      sha256Text: () => "copy",
    },
    authRepository,
  );

  return {
    service,
    revokeConsultationLinks,
    enqueue,
    tokenIssue,
    auditAppend,
    setAggregate: (value: Consultation) => {
      aggregate = value;
    },
    removeProfiles: () => {
      profilesAvailable = false;
    },
    currentEnabledRevision,
  };
}

describe("ConsultationService", () => {
  it("freezes the enabled profile revision, uses canonical participant identities, and revokes the invite on cancellation", async () => {
    const { service, revokeConsultationLinks, enqueue } = createConsultationFixture();

    const created = await service.create({
      employeeUserId: EMPLOYEE_ID,
      customerUserId: CUSTOMER_ID,
      providerProfileId: PROFILE_ID,
      creationIdempotencyKey: CREATION_KEY,
    });

    expect(created.providerProfileId).toBe(PROFILE_ID);
    expect(created.providerProfileRevision).toBe(9);
    expect(
      created.participants.every((participant) => participant.id === participant.livekitIdentity),
    ).toBe(true);

    const cancelled = await service.cancel(created.id, EMPLOYEE_ID);

    expect(revokeConsultationLinks).toHaveBeenCalledWith(created.id, NOW, TRANSACTION);
    expect(cancelled.state).toBe("cancelled");
    expect(cancelled.admissionFencedAt).toEqual(NOW);
    expect(enqueue).toHaveBeenLastCalledWith(
      expect.objectContaining({
        topic: "consultation.cancelled",
        aggregateId: created.id,
        generation: 1,
        payload: {
          consultationId: created.id,
          generation: 1,
          resourceGeneration: 0,
        },
      }),
      TRANSACTION,
    );
  });

  it("hides consultation existence from authenticated non-members", async () => {
    const { service } = createConsultationFixture();
    const created = await service.create({
      employeeUserId: EMPLOYEE_ID,
      customerUserId: CUSTOMER_ID,
      providerProfileId: PROFILE_ID,
      creationIdempotencyKey: CREATION_KEY,
    });

    await expect(service.join(created.id, OTHER_CUSTOMER_ID)).rejects.toMatchObject({
      code: "NOT_FOUND",
    });
  });
  it("scopes keys by employee, converges concurrent retries, and rejects changed payloads", async () => {
    const { service, auditAppend } = createConsultationFixture();
    const input = {
      employeeUserId: EMPLOYEE_ID,
      customerUserId: CUSTOMER_ID,
      providerProfileId: PROFILE_ID,
      creationIdempotencyKey: CREATION_KEY,
    };

    const [first, retry] = await Promise.all([service.create(input), service.create(input)]);

    expect(retry.id).toBe(first.id);
    expect(auditAppend).toHaveBeenCalledTimes(1);
    await expect(
      service.create({
        ...input,
        customerUserId: OTHER_CUSTOMER_ID,
      }),
    ).rejects.toMatchObject({ code: "CONSULTATION_CREATION_CONFLICT" });
    await expect(
      service.create({
        ...input,
        providerProfileId: OTHER_PROFILE_ID,
      }),
    ).rejects.toMatchObject({ code: "CONSULTATION_CREATION_CONFLICT" });
    const otherEmployee = await service.create({
      ...input,
      employeeUserId: OTHER_EMPLOYEE_ID,
    });
    expect(otherEmployee.id).not.toBe(first.id);
  });
  it("returns the original consultation when a profile-name replay resolves to its stored UUID", async () => {
    const { service, auditAppend } = createConsultationFixture();
    const input = {
      employeeUserId: EMPLOYEE_ID,
      customerUserId: CUSTOMER_ID,
      providerProfileId: PROFILE_NAME,
      creationIdempotencyKey: CREATION_KEY,
    };

    const first = await service.create(input);
    const nameReplay = await service.create(input);
    const uuidReplay = await service.create({
      ...input,
      providerProfileId: PROFILE_ID,
    });

    expect(nameReplay).toBe(first);
    expect(uuidReplay).toBe(first);
    expect(first.providerProfileId).toBe(PROFILE_ID);
    expect(auditAppend).toHaveBeenCalledTimes(1);
  });

  it("replays a committed create after its profile is removed and still rejects mismatches", async () => {
    const { service, removeProfiles, currentEnabledRevision, auditAppend } =
      createConsultationFixture();
    const input = {
      employeeUserId: EMPLOYEE_ID,
      customerUserId: CUSTOMER_ID,
      providerProfileId: PROFILE_ID,
      creationIdempotencyKey: CREATION_KEY,
    };
    const committed = await service.create(input);
    removeProfiles();

    const replay = await service.create(input);

    expect(replay).toBe(committed);
    expect(currentEnabledRevision).toHaveBeenCalledTimes(1);
    expect(auditAppend).toHaveBeenCalledTimes(1);
    await expect(
      service.create({
        ...input,
        providerProfileId: OTHER_PROFILE_ID,
      }),
    ).rejects.toMatchObject({ code: "CONSULTATION_CREATION_CONFLICT" });
    await expect(
      service.create({
        ...input,
        customerUserId: OTHER_CUSTOMER_ID,
      }),
    ).rejects.toMatchObject({ code: "CONSULTATION_CREATION_CONFLICT" });
    expect(currentEnabledRevision).toHaveBeenCalledTimes(1);
  });

  it("returns CONSENT_REQUIRED while only one participant has consented", async () => {
    const fixture = createConsultationFixture();
    const created = await fixture.service.create({
      employeeUserId: EMPLOYEE_ID,
      customerUserId: CUSTOMER_ID,
      providerProfileId: PROFILE_ID,
      creationIdempotencyKey: CREATION_KEY,
    });
    fixture.setAggregate({
      ...created,
      providerSelection: {} as Consultation["providerSelection"],
      snapshotHash: "hash",
      participants: [
        {
          ...created.participants[0],
          language: "en-US",
          consent: {
            version: 1,
            copyHash: "copy",
            snapshotHash: "hash",
            consentedAt: NOW,
          },
        },
        {
          ...created.participants[1],
          language: "de-DE",
        },
      ],
    });

    await expect(fixture.service.join(created.id, EMPLOYEE_ID)).rejects.toThrowError(
      /CONSENT_REQUIRED/,
    );
    expect(fixture.enqueue).not.toHaveBeenCalledWith(
      expect.objectContaining({ topic: "consultation.provisioning_requested" }),
      TRANSACTION,
    );
  });

  it("issues a non-publishing room token before Egress and worker barriers settle", async () => {
    const fixture = createConsultationFixture();
    const created = await fixture.service.create({
      employeeUserId: EMPLOYEE_ID,
      customerUserId: CUSTOMER_ID,
      providerProfileId: PROFILE_ID,
      creationIdempotencyKey: CREATION_KEY,
    });
    fixture.setAggregate({
      ...created,
      state: "ready",
      roomName: `consultation-${created.id}`,
      roomSid: "RM_1",
      providerSelection: {} as Consultation["providerSelection"],
      snapshotHash: "hash",
      participants: [
        {
          ...created.participants[0],
          consent: {
            version: 1,
            copyHash: "copy",
            snapshotHash: "hash",
            consentedAt: NOW,
          },
        },
        created.participants[1],
      ],
    });

    await expect(fixture.service.issueLiveKitToken(created.id, EMPLOYEE_ID)).resolves.toBe("token");
    expect(fixture.tokenIssue).toHaveBeenCalledWith(
      expect.objectContaining({
        identity: created.participants[0].livekitIdentity,
        roomName: `consultation-${created.id}`,
        grants: {
          roomJoin: true,
          canPublish: false,
          canPublishData: false,
          canSubscribe: true,
        },
      }),
    );
  });
});
