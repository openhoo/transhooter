import { readFile } from "node:fs/promises";
import { z } from "zod";
import { EFFECT_KINDS, type EffectKind, type Uuid } from "../orchestration/model";

const effectKindSchema = z.enum(EFFECT_KINDS);
const consultationFaultSchema = z
  .object({
    failEffects: z.array(effectKindSchema).default([]),
    crashAfterPersistCalling: z.array(effectKindSchema).default([]),
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
  shouldFail(kind: EffectKind, consultationId: Uuid): Promise<void>;
}

export class FileEffectFaultControl implements EffectFaultControl {
  constructor(private readonly path: string) {}

  async afterPersist(kind: EffectKind, consultationId: Uuid): Promise<void> {
    const faults = await this.read();
    if (faults.consultations[consultationId]?.crashAfterPersistCalling.includes(kind) === true) {
      process.exit(86);
    }
  }

  async shouldFail(kind: EffectKind, consultationId: Uuid): Promise<void> {
    const faults = await this.read();
    if (faults.consultations[consultationId]?.failEffects.includes(kind) === true) {
      throw new Error(`test fault denied ${kind}`);
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
};
