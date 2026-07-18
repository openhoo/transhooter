import { describe, expect, it } from "bun:test";
import type { RoomProviderSelection } from "@transhooter/contracts";
import {
  beginFinalization,
  beginProvisioning,
  type Consultation,
  grantCapture,
  joinEligibility,
  type ParticipantSlot,
  withConsent,
  withPreferences,
  withProviderSelection,
} from "../src/consultations/domain";

const EMPLOYEE = "00000000-0000-4000-8000-000000000001";
const CUSTOMER = "00000000-0000-4000-8000-000000000002";
const CONSULTATION = "00000000-0000-4000-8000-000000000003";
const PROFILE = "00000000-0000-4000-8000-000000000004";
const NOW = new Date("2026-01-01T00:00:00.000Z");
const SNAPSHOT_HASH = "snapshot-1";
const PROVIDER_SELECTION = {} as RoomProviderSelection;

function participant(id: string, role: "employee" | "customer"): ParticipantSlot {
  return {
    id,
    role,
    userId: id,
    livekitIdentity: id,
    displayName: null,
    language: null,
    consent: null,
    present: false,
    eventWatermark: null,
    eventOccurredAt: null,
    publicationGranted: false,
    participantEgressId: null,
  };
}

function consultation(): Consultation {
  return {
    id: CONSULTATION,
    state: "invited",
    archiveState: "pending",
    providerProfileId: PROFILE,
    providerProfileRevision: 1,
    participants: [participant(EMPLOYEE, "employee"), participant(CUSTOMER, "customer")],
    providerSelection: null,
    snapshotHash: null,
    generation: 0,
    roomName: null,
    roomSid: null,
    dispatchId: null,
    compositeEgressId: null,
    workerIdentity: null,
    readyDeadlineAt: null,
    finalizeDeadlineAt: null,
    bothAbsentSince: null,
    admissionFencedAt: null,
    createdAt: NOW,
    updatedAt: NOW,
  };
}

function consentedConsultation(): Consultation {
  let value = withPreferences(consultation(), EMPLOYEE, "Employee", "en-US", NOW);
  value = withPreferences(value, CUSTOMER, "Customer", "de-DE", NOW);
  value = withProviderSelection(value, PROVIDER_SELECTION, SNAPSHOT_HASH, NOW);
  value = withConsent(value, EMPLOYEE, SNAPSHOT_HASH, "copy", NOW);
  return withConsent(value, CUSTOMER, SNAPSHOT_HASH, "copy", NOW);
}

describe("consultation domain", () => {
  it("requires both preferences and consents bound to the current snapshot", () => {
    let value = withPreferences(consultation(), EMPLOYEE, "Employee", "en-US", NOW);

    expect(joinEligibility(value)).toBe("WAITING_FOR_PREFERENCES");

    value = withPreferences(value, CUSTOMER, "Customer", "de-DE", NOW);
    value = withProviderSelection(value, PROVIDER_SELECTION, SNAPSHOT_HASH, NOW);

    expect(joinEligibility(value)).toBe("CONSENT_REQUIRED");

    value = withConsent(value, EMPLOYEE, SNAPSHOT_HASH, "copy", NOW);
    value = withConsent(value, CUSTOMER, SNAPSHOT_HASH, "copy", NOW);

    expect(joinEligibility(value)).toBe("eligible");
    expect(() => withConsent(value, CUSTOMER, "old", "copy", NOW)).toThrowError(/SNAPSHOT_CHANGED/);
  });

  it("sets literal deadlines and independently advances archive state", () => {
    let value = consentedConsultation();

    value = beginProvisioning(value, `consultation-${value.id}`, NOW);

    expect(value.readyDeadlineAt?.toISOString()).toBe("2026-01-01T00:05:00.000Z");

    value = grantCapture(value, EMPLOYEE, "egress-1", NOW);

    expect(value.state).toBe("active");

    value = beginFinalization(value, NOW);

    expect(value.state).toBe("finalizing");
    expect(value.archiveState).toBe("reconciling");
    expect(value.generation).toBe(1);
    expect(value.finalizeDeadlineAt?.toISOString()).toBe("2026-01-01T00:15:00.000Z");
  });

  it("invalidates both consents when an invited preference changes", () => {
    let value = consentedConsultation();

    value = withPreferences(value, CUSTOMER, "Customer", "fr-FR", NOW);

    expect(value.participants.every((slot) => slot.consent === null)).toBe(true);
    expect(value.snapshotHash).toBeNull();
  });
});
