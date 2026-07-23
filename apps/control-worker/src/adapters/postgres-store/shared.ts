import { ExternalEffectStateSchema } from "@transhooter/contracts";
import type { Prisma, PrismaClient } from "@transhooter/server-core/persistence";
import type {
  ConsultationState,
  Deadline,
  Effect,
  OutboxItem,
  WorkerReservation,
} from "../../orchestration/model";
export type PrismaConnection = PrismaClient | Prisma.TransactionClient;

export function withTransaction<T>(
  client: PrismaConnection,
  operation: (transaction: Prisma.TransactionClient) => Promise<T>,
  options?: { readonly maxWait?: number; readonly timeout?: number },
): Promise<T> {
  if ("$transaction" in client) {
    return options === undefined
      ? client.$transaction(operation)
      : client.$transaction(operation, options);
  }
  return operation(client);
}

export function safeDatabaseInteger(value: bigint | number, column: string): number {
  const converted = typeof value === "bigint" ? Number(value) : value;
  if (!Number.isSafeInteger(converted)) {
    throw new Error(`${column} is outside the JavaScript safe integer range`);
  }
  return converted;
}

export function nullableSafeDatabaseInteger(
  value: bigint | number | null,
  column: string,
): number | null {
  return value === null ? null : safeDatabaseInteger(value, column);
}

export interface ExternalEffectRow {
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

export interface OutboxRow {
  readonly id: string;
  readonly aggregate_id: string;
  readonly generation: number;
  readonly topic: string;
  readonly payload: unknown;
  readonly attempts: number;
}

export interface DeadlineRow {
  readonly consultation_id: string;
  readonly generation: number;
  readonly kind: Deadline["kind"];
  readonly due_at: Date | string;
}

export interface ReservationRow {
  readonly consultation_id: string;
  readonly generation: number;
  readonly worker_id: string;
  readonly epoch: bigint;
  readonly heartbeat_at: Date | string;
  readonly lease_expires_at: Date | string;
  readonly accepting_load: boolean;
}

export interface IdRow {
  readonly id: string;
}

export interface AppliedTransitionRow {
  readonly transitioned: boolean;
}

export interface RoomResourceRow {
  readonly resource_room_name: string | null;
}

export interface ReserveConsultationRow {
  readonly worker_identity: string | null;
  readonly snapshot_hash: string | null;
}

export interface CancellationConsultationRow {
  readonly generation: number;
  readonly state: ConsultationState;
}

export interface WorkerEpochTerminalRow {
  readonly terminal_at: Date | string | null;
}

export interface ArchiveStateRow {
  readonly id: string;
  readonly state: unknown;
}

export interface ReconciliationArchiveRow extends ArchiveStateRow {
  readonly reconciliation_deadline_at: Date | string | null;
}

export interface ConsultationIdRow {
  readonly consultation_id: string;
}

export interface WorkerDispatchRow {
  readonly room_name: string;
  readonly worker_identity: string;
  readonly snapshot_hash: string;
  readonly provider_selection: unknown;
  readonly epoch: bigint;
  readonly write_epoch: number;
}

export interface WorkerDirectionRow {
  readonly source_participant_id: string;
  readonly destination_participant_id: string;
}

export interface ParticipantIdentityRow {
  readonly id: string;
  readonly livekit_identity: string;
}

export interface ExpectationRow {
  readonly id: string;
  readonly object_class: string;
  readonly causal_key: string;
  readonly sample_start: bigint | null;
  readonly sample_end: bigint | null;
  readonly fulfilled_object_id: string | null;
}

export interface ArchiveObjectRow {
  readonly id: string;
  readonly object_class: string;
  readonly key: string;
  readonly version_id: string;
  readonly size: bigint;
  readonly sha256: string;
  readonly s3_checksum: string;
  readonly content_type: string;
}

export interface ReconciliationProviderAttemptRow {
  readonly attempt_id: string;
  readonly stage: string;
}

export interface ProviderGapRow {
  readonly attempt_id: string;
  readonly stage: string;
  readonly provider: string;
  readonly direction_id: string;
  readonly operation_id: string;
  readonly attempt_number: number;
  readonly outcome: string;
  readonly error_kind: string | null;
  readonly accepted_input_watermark: bigint | null;
  readonly received_output_watermark: bigint | null;
  readonly emitted_output_watermark: bigint | null;
  readonly retry_decision: unknown;
}

export interface ReconciliationDirectionRow {
  readonly mode: string;
  readonly destination_participant_id: string;
  readonly emitted_output: bigint;
}

export interface CheckpointRow {
  readonly checkpoint: unknown;
}

export interface EgressResultRow {
  readonly id: string;
  readonly egress_id: string | null;
  readonly kind: string;
  readonly state: string;
  readonly output_prefix: string;
  readonly terminal_result: unknown;
}

export interface DrainResultRow {
  readonly result: unknown;
}
export interface EgressIdRow {
  readonly egress_id: string;
}

export interface LiveKitIdentityRow {
  readonly livekit_identity: string;
}

export interface DispatchIdRow {
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
    id: row.id,
    aggregateId: row.aggregate_id,
    generation: safeDatabaseInteger(row.generation, "outbox.generation"),
    type: row.topic,
    payload: row.payload,
    attempts: safeDatabaseInteger(row.attempts, "outbox.attempts"),
  };
}

export function mapDeadline(row: DeadlineRow): Deadline {
  return {
    consultationId: row.consultation_id,
    generation: safeDatabaseInteger(row.generation, "orchestration_deadlines.generation"),
    kind: row.kind,
    dueAt: date(row.due_at),
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
  const timestamp =
    value instanceof Date
      ? value
      : typeof value === "string" || typeof value === "number"
        ? new Date(value)
        : null;
  if (timestamp === null) {
    throw new Error("database timestamp has invalid type");
  }
  if (Number.isNaN(timestamp.getTime())) {
    throw new Error("database timestamp is invalid");
  }
  return timestamp;
}

export function date(value: unknown): Date {
  const timestamp = nullableDate(value);
  if (timestamp === null) {
    throw new Error("database timestamp is required");
  }
  return timestamp;
}

export function mapEffect(row: ExternalEffectRow): Effect {
  if (row.result === null || typeof row.result !== "object" || Array.isArray(row.result)) {
    throw new Error("external_effects.result must be an object");
  }
  const result = row.result as Readonly<Record<string, unknown>>;
  if (result.plan === null || typeof result.plan !== "object" || Array.isArray(result.plan)) {
    throw new Error("external_effects.result.plan must be an object");
  }
  if (row.request_bytes !== null && !(row.request_bytes instanceof Uint8Array)) {
    throw new Error("external_effects.request_bytes must be bytea");
  }
  return {
    id: row.id,
    consultationId: row.consultation_id,
    generation: safeDatabaseInteger(row.generation, "external_effects.generation"),
    kind: row.effect_kind as Effect["kind"],
    subjectId: row.subject_id,
    occurrenceKey: row.occurrence_key,
    plan: result.plan as Readonly<Record<string, unknown>>,
    state: ExternalEffectStateSchema.parse(row.state),
    requestBytes: row.request_bytes,
    requestSha256: nullableString(row.request_hash),
    remoteId: typeof result.remoteId === "string" ? result.remoteId : null,
    appliedResult: "value" in result ? result.value : null,
    attempt: safeDatabaseInteger(row.attempts, "external_effects.attempts"),
    leaseOwner: nullableString(row.lease_owner),
    leaseExpiresAt: nullableDate(row.lease_expires_at),
  };
}

export function mapReservation(row: ReservationRow): WorkerReservation {
  return {
    consultationId: row.consultation_id,
    generation: safeDatabaseInteger(row.generation, "worker_reservations.generation"),
    workerId: row.worker_id,
    epoch: safeDatabaseInteger(row.epoch, "worker_reservations.epoch"),
    heartbeatAt: date(row.heartbeat_at),
    leaseExpiresAt: date(row.lease_expires_at),
    acceptingLoad: row.accepting_load,
  };
}
