export async function runProviderScenarios(ctx, proof) {
  const {
    translationFailureCases,
    shouldRunScenario,
    resetAuthenticationThrottle,
    runConsultation,
    setFaults,
    waitFor,
    consultationEvidence,
    cancelBeforeStartForCleanup,
    settleConsultation,
    checkpointScenario,
    terminateProcessTree,
    assertTranslationFailureEvidence,
    assertTerminalAttempt,
    settlementSummary,
    forceArchiveReconciliationDeadline,
    setWorkerScenario,
  } = ctx;
  if (shouldRunScenario("participant-egress-denied")) {
    // Denying Participant Egress after the durable effect row must keep publication
    // blocked and surface a durable retry/failure rather than silently joining.
    await resetAuthenticationThrottle();
    const deniedRun = await runConsultation({
      faults: { failEffects: ["PARTICIPANT_EGRESS"] },
      captureBarrierTimeoutMs: 10_000,
    });
    const denial = await deniedRun.completed;
    const deniedConsultationId = deniedRun.consultationId;
    await setFaults();
    if (denial.code === 0) {
      throw new Error("Participant Egress denial unexpectedly allowed a complete consultation");
    }
    await cancelBeforeStartForCleanup(deniedConsultationId);
    const deniedEvidence = await waitFor(
      "fenced durable Participant Egress failure",
      async () => {
        const evidence = await consultationEvidence(deniedConsultationId);
        return evidence.admission_fenced_at != null &&
          evidence.effects.some(
            (effect) =>
              effect.kind === "PARTICIPANT_EGRESS" &&
              effect.attempts >= 1 &&
              String(effect.result?.error ?? "").includes("test fault denied PARTICIPANT_EGRESS"),
          )
          ? evidence
          : null;
      },
      120_000,
    );
    if (
      deniedEvidence.publication_grants !== 0 ||
      deniedEvidence.participant_grant_effects !== 0 ||
      deniedEvidence.capture_ready_packets !== 0
    ) {
      throw new Error(
        `Participant Egress denial crossed publication barrier: ${JSON.stringify({
          publicationGrants: deniedEvidence.publication_grants,
          participantGrantEffects: deniedEvidence.participant_grant_effects,
          captureReadyPackets: deniedEvidence.capture_ready_packets,
        })}`,
      );
    }
    const deniedEffect = deniedEvidence.effects.find(
      (effect) =>
        effect.kind === "PARTICIPANT_EGRESS" &&
        String(effect.result?.error ?? "").includes("test fault denied PARTICIPANT_EGRESS"),
    );
    if (!deniedEffect?.result || deniedEffect.attempts < 1)
      throw new Error(
        `Participant Egress failure lacks durable terminal detail: ${JSON.stringify(deniedEffect)}`,
      );
    proof.scenarios.push({
      name: "participant-egress-denied",
      consultationId: deniedConsultationId,
      publicationBlocked: true,
      effectAttempts: deniedEffect.attempts,
      durableTerminal: deniedEffect.result,
    });
    await settleConsultation(deniedConsultationId);

    await checkpointScenario("participant-egress-denied");
  }
  // Exercise normalized provider errors and partial synthesis through the actual
  // fixture runtime. Each must degrade/fail rather than produce false complete proof.
  for (const expected of translationFailureCases) {
    const { name, failure } = expected;
    if (!shouldRunScenario(name)) continue;
    await resetAuthenticationThrottle();
    const run = await runConsultation({
      workerScenario: { translation: { failure } },
    });
    const consultationId = run.consultationId;
    const evidence = await waitFor(
      `${failure} provider terminal`,
      async () => {
        const current = await consultationEvidence(consultationId);
        const attempts = current.attempts
          .filter((attempt) => attempt.stage === "translation" && attempt.errorKind === failure)
          .toSorted((left, right) => Number(left.attemptNumber) - Number(right.attemptNumber));
        if (attempts.length === 0) return null;
        const lastAttempt = attempts.at(-1);
        return !expected.expectRetry ||
          (attempts.length >= 2 && lastAttempt?.retryDecision?.action !== "retry")
          ? current
          : null;
      },
      90_000,
    );
    const result = await terminateProcessTree(run);
    if (result.code === 0) throw new Error(`${failure} unexpectedly produced a complete archive`);
    const attempt = assertTranslationFailureEvidence(evidence, expected);
    proof.scenarios.push({
      name,
      consultationId,
      outcome: attempt.outcome,
      retryDecision: attempt.retryDecision,
      terminalHash: attempt.terminalHash,
    });
    await settleConsultation(consultationId);
    await checkpointScenario(name);
  }
  if (shouldRunScenario("tts-partial-finalization")) {
    await resetAuthenticationThrottle();
    const partialRun = await runConsultation({
      workerScenario: { tts: { partialSamples: 960, failAfterPartial: true } },
    });
    const partialConsultationId = partialRun.consultationId;
    const partial = await partialRun.completed;
    if (partial.code === 0) throw new Error("partial TTS unexpectedly produced a complete archive");
    const partialEvidence = await waitFor(
      "partial TTS terminal evidence",
      async () => {
        const evidence = await consultationEvidence(partialConsultationId);
        return evidence.attempts.some(
          (attempt) =>
            attempt.stage === "tts" &&
            attempt.outcome === "failed" &&
            Number(attempt.received ?? 0) > 0,
        )
          ? evidence
          : null;
      },
      90_000,
    );
    const partialAttempt = partialEvidence.attempts.find(
      (attempt) =>
        attempt.stage === "tts" &&
        attempt.outcome === "failed" &&
        Number(attempt.received ?? 0) > 0,
    );
    assertTerminalAttempt(partialAttempt, "transport");
    if (partialAttempt.retryDecision?.action === "retry")
      throw new Error(
        `partial output was incorrectly retryable: ${JSON.stringify(partialAttempt)}`,
      );
    proof.scenarios.push({
      name: "tts-partial-output",
      consultationId: partialConsultationId,
      receivedOutputWatermark: partialAttempt.received,
      emittedOutputWatermark: partialAttempt.emitted,
      retryDecision: partialAttempt.retryDecision,
    });
    const reconcilingSettlement = await settleConsultation(partialConsultationId, {
      stopAtReconciliation: true,
    });
    if (
      reconcilingSettlement.state !== "ended" ||
      reconcilingSettlement.archive_state !== "reconciling"
    ) {
      throw new Error(
        `partial TTS did not reach archive reconciliation: ${JSON.stringify(
          settlementSummary(reconcilingSettlement),
        )}`,
      );
    }
    await forceArchiveReconciliationDeadline(
      partialConsultationId,
      reconcilingSettlement.generation,
    );
    const deadlineEvidence = await settleConsultation(partialConsultationId);
    if (
      deadlineEvidence.state !== "ended" ||
      deadlineEvidence.archive_state !== "incomplete" ||
      deadlineEvidence.inventory?.status !== "incomplete" ||
      (deadlineEvidence.inventory?.missing ?? []).length === 0 ||
      deadlineEvidence.egress.length === 0 ||
      !deadlineEvidence.egress.every((job) => job.terminalAt && job.terminalResult)
    ) {
      throw new Error(
        `forced finalization omitted terminal evidence: ${JSON.stringify(
          settlementSummary(deadlineEvidence),
        )}`,
      );
    }
    proof.scenarios.push({
      name: "finalization-deadline",
      consultationId: partialConsultationId,
      consultationState: deadlineEvidence.state,
      archiveState: deadlineEvidence.archive_state,
      explicitGaps: deadlineEvidence.inventory.missing,
      egressTerminals: deadlineEvidence.egress.map((job) => ({
        egressId: job.egressId,
        state: job.state,
        terminalAt: job.terminalAt,
        terminalResult: job.terminalResult,
      })),
    });
    await setWorkerScenario();
    await checkpointScenario("tts-partial-finalization");
  }
}
