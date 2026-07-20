import { open, readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { z } from "zod";
import { EFFECT_KINDS, type EffectKind, type Uuid } from "../orchestration/model";

const effectKindSchema = z.enum(EFFECT_KINDS);
const consultationFaultSchema = z
  .object({
    failEffects: z.array(effectKindSchema).default([]),
    crashAfterPersistCalling: z.array(effectKindSchema).default([]),
    holdAfterPersistCalling: z.array(effectKindSchema).default([]),
    crashAfterRemoteSuccess: z.array(effectKindSchema).default([]),
    crashAfterMarkApplied: z.array(effectKindSchema).default([]),
    holdAfterRemoteSuccess: z.array(effectKindSchema).default([]),
  })
  .strict();
const faultSchema = z
  .object({
    consultations: z.record(z.uuid(), consultationFaultSchema),
  })
  .strict();

type FaultConfiguration = z.infer<typeof faultSchema>;

export interface EffectFaultControl {
  afterPersist(kind: EffectKind, consultationId: Uuid): Promise<void>;
  afterRemoteSuccess(kind: EffectKind, consultationId: Uuid): Promise<void>;
  afterMarkApplied(kind: EffectKind, consultationId: Uuid): Promise<void>;
  shouldFail(kind: EffectKind, consultationId: Uuid): Promise<void>;
}

export class FileEffectFaultControl implements EffectFaultControl {
  constructor(private readonly path: string) {}

  async afterPersist(kind: EffectKind, consultationId: Uuid): Promise<void> {
    const configured = await this.read();
    const scoped = configured.consultations[consultationId];
    if (scoped?.crashAfterPersistCalling.includes(kind) === true) {
      process.exit(86);
    }
    if (scoped?.holdAfterPersistCalling.includes(kind) !== true) {
      return;
    }
    await this.hold(consultationId, kind, "calling-owner", (current) =>
      current.holdAfterPersistCalling.includes(kind),
    );
  }
  async afterRemoteSuccess(kind: EffectKind, consultationId: Uuid): Promise<void> {
    const configured = await this.read();
    const scoped = configured.consultations[consultationId];
    if (scoped?.crashAfterRemoteSuccess.includes(kind) === true) {
      process.exit(87);
    }
    if (scoped?.holdAfterRemoteSuccess.includes(kind) !== true) {
      return;
    }
    await this.hold(consultationId, kind, "remote-success-owner", (current) =>
      current.holdAfterRemoteSuccess.includes(kind),
    );
  }

  async afterMarkApplied(kind: EffectKind, consultationId: Uuid): Promise<void> {
    const faults = await this.read();
    if (faults.consultations[consultationId]?.crashAfterMarkApplied.includes(kind) === true) {
      process.exit(88);
    }
  }

  async shouldFail(kind: EffectKind, consultationId: Uuid): Promise<void> {
    const faults = await this.read();
    if (faults.consultations[consultationId]?.failEffects.includes(kind) === true) {
      throw new Error(`test fault denied ${kind}`);
    }
  }

  private async hold(
    consultationId: Uuid,
    kind: EffectKind,
    suffix: string,
    remainsHeld: (current: z.infer<typeof consultationFaultSchema>) => boolean,
  ): Promise<void> {
    const markerPath = `${this.path}.${consultationId}.${kind}.${suffix}`;
    try {
      const marker = await open(markerPath, "wx", 0o600);
      await marker.close();
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "EEXIST") {
        return;
      }
      throw error;
    }
    while (true) {
      const current = (await this.read()).consultations[consultationId];
      if (current === undefined || !remainsHeld(current)) return;
      await delay(25);
    }
  }
  private async read(): Promise<FaultConfiguration> {
    const contents = await readFile(this.path, "utf8");
    const parsedContents = JSON.parse(contents) as unknown;
    return faultSchema.parse(parsedContents);
  }
}

export const noEffectFaults: EffectFaultControl = {
  afterPersist: () => Promise.resolve(),
  shouldFail: () => Promise.resolve(),
  afterRemoteSuccess: () => Promise.resolve(),
  afterMarkApplied: () => Promise.resolve(),
};
