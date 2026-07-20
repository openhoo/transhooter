import {
  addMilliseconds,
  type Clock,
  DomainError,
  type IdGenerator,
  type UUID,
} from "../domain/model";
import type { EffectRepository } from "../ports/index";

export interface CanonicalEffectCodec<Request, Result> {
  encode(request: Request): Uint8Array;
  hash(bytes: Uint8Array): string;
  serializeResult(result: Result): unknown;
  deserializeResult(value: unknown): Result;
}

interface DurableEffectInput<Request, Result> {
  effectId: UUID;
  generation: number;
  request: Request;
  codec: CanonicalEffectCodec<Request, Result>;
  adopt: (request: Request) => Promise<Result | null>;
  call: (request: Request) => Promise<Result>;
  compensate: (request: Request, result: Result) => Promise<unknown>;
  resultGeneration: (result: Result) => number;
}

interface EffectClaim<Result> {
  terminal: Result | null;
  compensating: Result | null;
  claimed: boolean;
}

export class DurableEffectExecutor {
  constructor(
    private readonly effects: EffectRepository,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
  ) {}

  async execute<Request, Result>(input: DurableEffectInput<Request, Result>): Promise<Result> {
    const requestBytes = input.codec.encode(input.request);
    const requestHash = input.codec.hash(requestBytes);
    const owner = this.ids.uuid();
    const claim = await this.claimEffect(input, requestBytes, requestHash, owner);

    if (claim.terminal) {
      return claim.terminal;
    }
    if (!claim.claimed) {
      throw new DomainError("EFFECT_ALREADY_CLAIMED");
    }
    if (claim.compensating) {
      await this.resumeCompensation(input, claim.compensating, owner, requestHash);
    }

    const result = await this.adoptOrCall(input, owner, requestHash);
    const late = input.resultGeneration(result) !== input.generation;
    const state = late ? "compensating" : "done";
    const persisted = await this.effects.transaction((tx) =>
      this.effects.complete(
        input.effectId,
        owner,
        requestHash,
        state,
        input.codec.serializeResult(result),
        tx,
      ),
    );

    if (!persisted) {
      const authoritative = await this.authoritativeResult(input, requestHash);
      if (authoritative) {
        return authoritative;
      }
      throw new DomainError("EFFECT_LEASE_LOST");
    }
    if (late) {
      await this.compensateLateResult(input, result, requestHash);
    }
    return result;
  }

  private async claimEffect<Request, Result>(
    input: DurableEffectInput<Request, Result>,
    requestBytes: Uint8Array,
    requestHash: string,
    owner: UUID,
  ): Promise<EffectClaim<Result>> {
    return this.effects.transaction(async (tx) => {
      const effect = await this.effects.lock(input.effectId, tx);
      if (!effect) {
        throw new DomainError("EFFECT_NOT_FOUND");
      }
      if (effect.generation !== input.generation) {
        throw new DomainError("FENCED_GENERATION");
      }
      if (effect.requestHash && effect.requestHash !== requestHash) {
        throw new DomainError("EFFECT_REQUEST_MISMATCH");
      }
      if (effect.state === "done" || effect.state === "applied") {
        return {
          terminal: input.codec.deserializeResult(effect.result),
          compensating: null,
          claimed: false,
        };
      }
      if (effect.state === "compensating") {
        const claimed = await this.effects.beginCompensation(
          effect.id,
          owner,
          addMilliseconds(this.clock.now(), 30_000),
          tx,
        );
        return {
          terminal: null,
          compensating: input.codec.deserializeResult(effect.result),
          claimed,
        };
      }

      const claimed = await this.effects.beginCall(
        effect.id,
        requestBytes,
        requestHash,
        owner,
        addMilliseconds(this.clock.now(), 30_000),
        tx,
      );
      return {
        terminal: null,
        compensating: null,
        claimed,
      };
    });
  }

  private async resumeCompensation<Request, Result>(
    input: DurableEffectInput<Request, Result>,
    result: Result,
    owner: UUID,
    requestHash: string,
  ): Promise<never> {
    const compensation = await input.compensate(input.request, result);
    const completed = await this.effects.transaction((tx) =>
      this.effects.completeCompensation(input.effectId, owner, requestHash, compensation, tx),
    );
    if (!completed) {
      throw new DomainError("EFFECT_COMPENSATION_LOST");
    }
    throw new DomainError("LATE_EFFECT_RESULT");
  }

  private async adoptOrCall<Request, Result>(
    input: DurableEffectInput<Request, Result>,
    owner: UUID,
    requestHash: string,
  ): Promise<Result> {
    const adopted = await input.adopt(input.request);
    if (adopted) {
      return adopted;
    }

    try {
      return await input.call(input.request);
    } catch (error) {
      const discovered = await input.adopt(input.request).catch(() => null);
      if (discovered) {
        if (input.resultGeneration(discovered) === input.generation) {
          return discovered;
        }
        await this.persistDiscoveredEffectAndCompensate(input, discovered, owner, requestHash);
      } else {
        await this.effects.transaction((tx) =>
          this.effects.complete(
            input.effectId,
            owner,
            requestHash,
            "failed",
            {
              error: error instanceof Error ? error.message : "effect call failed",
            },
            tx,
          ),
        );
      }
      throw error;
    }
  }

  private async persistDiscoveredEffectAndCompensate<Request, Result>(
    input: DurableEffectInput<Request, Result>,
    discovered: Result,
    owner: UUID,
    requestHash: string,
  ): Promise<void> {
    const persisted = await this.effects.transaction((tx) =>
      this.effects.complete(
        input.effectId,
        owner,
        requestHash,
        "compensating",
        input.codec.serializeResult(discovered),
        tx,
      ),
    );
    if (!persisted) {
      return;
    }

    const compensationOwner = this.ids.uuid();
    const began = await this.effects.transaction((tx) =>
      this.effects.beginCompensation(
        input.effectId,
        compensationOwner,
        addMilliseconds(this.clock.now(), 30_000),
        tx,
      ),
    );
    if (!began) {
      return;
    }

    const compensation = await input.compensate(input.request, discovered);
    await this.effects.transaction((tx) =>
      this.effects.recordCompensationAttempt(
        input.effectId,
        compensationOwner,
        requestHash,
        compensation,
        tx,
      ),
    );
    await this.effects.transaction((tx) =>
      this.effects.completeCompensation(
        input.effectId,
        compensationOwner,
        requestHash,
        compensation,
        tx,
      ),
    );
  }

  private async authoritativeResult<Request, Result>(
    input: DurableEffectInput<Request, Result>,
    requestHash: string,
  ): Promise<Result | null> {
    return this.effects.transaction(async (tx) => {
      const current = await this.effects.lock(input.effectId, tx);
      const isAuthoritative =
        current?.requestHash === requestHash &&
        (current.state === "done" || current.state === "applied");
      return isAuthoritative ? input.codec.deserializeResult(current.result) : null;
    });
  }

  private async compensateLateResult<Request, Result>(
    input: DurableEffectInput<Request, Result>,
    result: Result,
    requestHash: string,
  ): Promise<never> {
    const compensationOwner = this.ids.uuid();
    const began = await this.effects.transaction((tx) =>
      this.effects.beginCompensation(
        input.effectId,
        compensationOwner,
        addMilliseconds(this.clock.now(), 30_000),
        tx,
      ),
    );
    if (!began) {
      throw new DomainError("EFFECT_COMPENSATION_LOST");
    }

    const compensation = await input.compensate(input.request, result);
    await this.effects.transaction((tx) =>
      this.effects.recordCompensationAttempt(
        input.effectId,
        compensationOwner,
        requestHash,
        compensation,
        tx,
      ),
    );
    const completed = await this.effects.transaction((tx) =>
      this.effects.completeCompensation(
        input.effectId,
        compensationOwner,
        requestHash,
        compensation,
        tx,
      ),
    );
    if (!completed) {
      throw new DomainError("EFFECT_COMPENSATION_LOST");
    }
    throw new DomainError("LATE_EFFECT_RESULT");
  }
}
