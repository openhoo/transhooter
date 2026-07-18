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
const ENABLED_PROFILE_REVISION = {
  profileId: PROFILE_ID,
  revision: 9,
};

function createConsultationFixture() {
  let aggregate: Consultation | null = null;
  let sequence = 10;
  const enqueue = mock(async () => undefined);
  const repository = {
    transaction: async <T>(work: (value: Transaction) => Promise<T>) => work(TRANSACTION),
    create: async (value: Consultation) => {
      aggregate = value;
    },
    lock: async () => aggregate,
    get: async () => aggregate,
    listForUser: async () => (aggregate ? [aggregate] : []),
    save: async (value: Consultation) => {
      aggregate = value;
      return true;
    },
    isCurrentEgress: async () => false,
  } as unknown as ConsultationRepository;
  const revokeConsultationLinks = mock(async () => undefined);
  const authRepository = {
    revokeConsultationLinks,
  } as unknown as AuthRepository;
  const service = new ConsultationService(
    repository,
    {
      plan: async () => {
        throw new Error("unused");
      },
      enqueue,
    } as never,
    {
      currentEnabledRevision: async () => ENABLED_PROFILE_REVISION,
    } as never,
    { issue: async () => "token" },
    { append: async () => undefined },
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
  };
}

describe("ConsultationService", () => {
  it("freezes the enabled profile revision, uses canonical participant identities, and revokes the invite on cancellation", async () => {
    const { service, revokeConsultationLinks, enqueue } = createConsultationFixture();

    const created = await service.create({
      employeeUserId: EMPLOYEE_ID,
      customerUserId: CUSTOMER_ID,
      providerProfileId: "google-eu",
    });

    expect(created.providerProfileId).toBe(PROFILE_ID);
    expect(created.providerProfileRevision).toBe(9);
    expect(
      created.participants.every((participant) => participant.id === participant.livekitIdentity),
    ).toBe(true);

    const cancelled = await service.cancel(created.id, EMPLOYEE_ID);

    expect(revokeConsultationLinks).toHaveBeenCalledWith(created.id, NOW, TRANSACTION);
    expect(cancelled.state).toBe("cancelled");
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
});
