export type {
  ArchiveState,
  ConsultationState,
  ExternalEffectState as EffectState,
  MagicLinkPurpose,
  ParticipantRole,
  StaffRole,
} from "@transhooter/contracts";
export type UUID = string;
export type Instant = Date;

export interface Clock {
  now(): Instant;
}
export interface IdGenerator {
  uuid(): UUID;
}
export interface TokenGenerator {
  bytes(length: number): Uint8Array;
}
export interface TokenHasher {
  sha256(value: Uint8Array | string): string;
}

export class DomainError extends Error {
  constructor(
    public readonly code: string,
    message = code,
  ) {
    super(message);
    this.name = "DomainError";
  }
}

export function assertUuid(value: string, field: string): asserts value is UUID {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) {
    throw new DomainError("INVALID_UUID", `${field} must be a UUID`);
  }
}

export function addMilliseconds(value: Date, milliseconds: number): Date {
  return new Date(value.getTime() + milliseconds);
}
