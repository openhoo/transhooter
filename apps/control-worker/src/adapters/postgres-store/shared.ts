import { ExternalEffectStateSchema } from "@transhooter/contracts";
import type { Row as PostgresRow } from "postgres";
import type {
  ConsultationState,
  Deadline,
  Effect,
  OutboxItem,
  WorkerReservation,
} from "../../orchestration/model";

export interface ExternalEffectRow extends PostgresRow {
  readonly id: string;
  readonly consultation_id: string;
  readonly generation: number;
  readonly effect_kind: string;
  readonly subject_id: string;
  readonly occurrence_key: string;
  readonly state: unknown;
  readonly request_bytes: Uint8Array | null;
  readonly request_hash: string | null;
  readonly lease_owner: string | null;
  readonly lease_expires_at: Date | string | null;
  readonly result: unknown;
  readonly attempts: number;
}

export interface OutboxRow extends PostgresRow {
  readonly id: string;
  readonly aggregate_id: string;
  readonly generation: number;
  readonly topic: string;
  readonly payload: unknown;
  readonly attempts: number;
}

export interface DeadlineRow extends PostgresRow {
  readonly consultation_id: string;
  readonly generation: number;
  readonly kind: Deadline["kind"];
  readonly due_at: Date | string;
}

export interface ReservationRow extends PostgresRow {
  readonly consultation_id: string;
  readonly generation: number;
  readonly worker_id: string;
  readonly epoch: number;
  readonly heartbeat_at: Date | string;
  readonly lease_expires_at: Date | string;
  readonly accepting_load: boolean;
}

export interface IdRow extends PostgresRow {
  readonly id: string;
}

export interface AppliedTransitionRow extends PostgresRow {
  readonly transitioned: boolean;
}

export interface RoomResourceRow extends PostgresRow {
  readonly resource_room_name: string | null;
}

export interface ReserveConsultationRow extends PostgresRow {
  readonly worker_identity: string | null;
  readonly snapshot_hash: string | null;
}

export interface CancellationConsultationRow extends PostgresRow {
  readonly generation: number;
  readonly state: ConsultationState;
}

export interface WorkerEpochTerminalRow extends PostgresRow {
  readonly terminal_at: Date | string | null;
}

export interface ArchiveStateRow extends PostgresRow {
  readonly id: string;
  readonly state: unknown;
}

export interface ReconciliationArchiveRow extends ArchiveStateRow {
  readonly reconciliation_deadline_at: Date | string | null;
}

export interface ConsultationIdRow extends PostgresRow {
  readonly consultation_id: string;
}

export interface WorkerDispatchRow extends PostgresRow {
  readonly room_name: string;
  readonly worker_identity: string;
  readonly snapshot_hash: string;
  readonly provider_selection: unknown;
  readonly epoch: number;
  readonly write_epoch: number;
}

export interface WorkerDirectionRow extends PostgresRow {
  readonly source_participant_id: string;
  readonly destination_participant_id: string;
}

export interface ParticipantIdentityRow extends PostgresRow {
  readonly id: string;
  readonly livekit_identity: string;
}

export interface ExpectationRow extends PostgresRow {
  readonly id: string;
  readonly object_class: string;
  readonly causal_key: string;
  readonly sample_start: number | null;
  readonly sample_end: number | null;
  readonly fulfilled_object_id: string | null;
}

export interface ArchiveObjectRow extends PostgresRow {
  readonly id: string;
  readonly object_class: string;
  readonly key: string;
  readonly version_id: string;
  readonly size: number;
  readonly sha256: string;
  readonly s3_checksum: string;
  readonly content_type: string;
}

export interface ProviderGapRow extends PostgresRow {
  readonly attempt_id: string;
  readonly stage: string;
  readonly provider: string;
  readonly direction_id: string;
  readonly operation_id: string;
  readonly attempt_number: number;
  readonly outcome: string;
  readonly error_kind: string | null;
  readonly accepted_input_watermark: number | null;
  readonly received_output_watermark: number | null;
  readonly emitted_output_watermark: number | null;
  readonly retry_decision: unknown;
}

export interface CheckpointRow extends PostgresRow {
  readonly checkpoint: unknown;
}

export interface EgressResultRow extends PostgresRow {
  readonly id: string;
  readonly egress_id: string | null;
  readonly kind: string;
  readonly state: string;
  readonly output_prefix: string;
  readonly terminal_result: unknown;
}

export interface DrainResultRow extends PostgresRow {
  readonly result: unknown;
}
export interface EgressIdRow extends PostgresRow {
  readonly egress_id: string;
}

export interface LiveKitIdentityRow extends PostgresRow {
  readonly livekit_identity: string;
}

export interface DispatchIdRow extends PostgresRow {
  readonly dispatch_id: string;
}

export type CapacityDimension = {
  readonly key: string;
  readonly capacity: number;
  readonly units: number;
};

export type ReconciliationEgressResult = {
  readonly egressId: unknown;
  readonly state: unknown;
  readonly terminal: boolean;
  readonly result: unknown;
  readonly kind: unknown;
  readonly outputPrefix: unknown;
};

export function mapOutboxItem(row: OutboxRow): OutboxItem {
  return {
    id: String(row.id),
    aggregateId: String(row.aggregate_id),
    generation: Number(row.generation),
    type: String(row.topic),
    payload: row.payload,
    attempts: Number(row.attempts),
  };
}

export function mapDeadline(row: DeadlineRow): Deadline {
  return {
    consultationId: String(row.consultation_id),
    generation: Number(row.generation),
    kind: row.kind as Deadline["kind"],
    dueAt: new Date(String(row.due_at)),
  };
}

export function perRoomQuotaUnits(stage: "stt" | "translation" | "tts", dimension: string): number {
  const normalized = dimension.toLowerCase().replaceAll("_", "-");
  const isAudioDuration =
    normalized.includes("audio") || normalized.includes("second") || normalized.includes("minute");

  if (stage === "stt") {
    if (normalized.includes("message")) {
      return 250;
    }
    if (isAudioDuration) {
      return 60;
    }
    return 1;
  }

  if (stage === "translation") {
    if (normalized.includes("character") || normalized.includes("char")) {
      return 50_000;
    }
    if (normalized.includes("request")) {
      return 50;
    }
    return 1;
  }

  if (normalized.includes("character") || normalized.includes("char")) {
    return 1_200;
  }
  if (normalized.includes("start")) {
    return 20;
  }
  if (normalized.includes("flush")) {
    return 10;
  }
  if (isAudioDuration) {
    return 60;
  }
  return 1;
}

export function nullableString(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value !== "string") {
    throw new Error("database value must be a string");
  }
  return value;
}

export function nullableDate(value: unknown): Date | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (value instanceof Date) {
    return value;
  }
  if (typeof value !== "string" && typeof value !== "number") {
    throw new Error("database timestamp has invalid type");
  }
  return new Date(value);
}

export function mapEffect(row: ExternalEffectRow): Effect {
  const result =
    row.result !== null && typeof row.result === "object" && !Array.isArray(row.result)
      ? (row.result as Readonly<Record<string, unknown>>)
      : {};
  const plan =
    result.plan !== null && typeof result.plan === "object" && !Array.isArray(result.plan)
      ? (result.plan as Readonly<Record<string, unknown>>)
      : {};
  return {
    id: String(row.id),
    consultationId: String(row.consultation_id),
    generation: Number(row.generation),
    kind: String(row.effect_kind) as Effect["kind"],
    subjectId: String(row.subject_id),
    occurrenceKey: String(row.occurrence_key),
    plan,
    state: ExternalEffectStateSchema.parse(row.state),
    requestBytes: row.request_bytes instanceof Uint8Array ? row.request_bytes : null,
    requestSha256: nullableString(row.request_hash),
    remoteId: typeof result.remoteId === "string" ? result.remoteId : null,
    appliedResult: "value" in result ? result.value : null,
    attempt: Number(row.attempts),
    leaseOwner: nullableString(row.lease_owner),
    leaseExpiresAt: nullableDate(row.lease_expires_at),
  };
}

export function mapReservation(row: ReservationRow): WorkerReservation {
  return {
    consultationId: String(row.consultation_id),
    generation: Number(row.generation),
    workerId: String(row.worker_id),
    epoch: Number(row.epoch),
    heartbeatAt: new Date(String(row.heartbeat_at)),
    leaseExpiresAt: new Date(String(row.lease_expires_at)),
    acceptingLoad: Boolean(row.accepting_load),
  };
}
