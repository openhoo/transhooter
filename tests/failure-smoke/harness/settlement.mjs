import { randomUUID } from "node:crypto";
import { restartCountIncremented, settlementProblems } from "../harness-contracts.mjs";

export function installSettlement(ctx) {
  const { database, ownerLeaseMs } = ctx;
  const { waitFor, persistOwnerLease, onlyContainer, inspect, setFaults, containerExitEvents } =
    ctx;
  const resetAuthenticationThrottle = (...args) => ctx.resetAuthenticationThrottle(...args);
  const runConsultation = (...args) => ctx.runConsultation(...args);
  async function sql(statement) {
    const rows = await database.unsafe(statement);
    return rows
      .map((row) => {
        const values = Object.values(row);
        return values.length === 1
          ? String(values[0] ?? "")
          : values.map((value) => String(value ?? "")).join("|");
      })
      .join("\n");
  }
  async function queryJson(statement) {
    const output = await sql(
      `SELECT COALESCE(json_agg(row_to_json(q)), '[]'::json)::text FROM (${statement}) q`,
    );
    return JSON.parse(output || "[]");
  }
  async function consultationStatus(consultationId) {
    const rows = await queryJson(`
      SELECT c.id,c.generation,c.state,c.admission_fenced_at,
        a.id AS archive_id,a.state AS archive_state,a.final_inventory_hash,
        COALESCE((SELECT count(*) FROM expected_archive_artifacts x WHERE x.archive_id=a.id),0)::int AS expected_count,
        COALESCE((SELECT count(*) FROM archive_objects o WHERE o.archive_id=a.id),0)::int AS object_count,
        COALESCE((SELECT count(*) FROM consultation_participants p
          WHERE p.consultation_id=c.id AND p.publication_granted),0)::int AS publication_grants,
        COALESCE((SELECT count(*) FROM external_effects e
          WHERE e.consultation_id=c.id AND e.effect_kind='PARTICIPANT_GRANT'),0)::int AS participant_grant_effects,
        COALESCE((SELECT count(*) FROM external_effects e
          WHERE e.consultation_id=c.id AND e.effect_kind='STATUS_PACKET'
            AND e.result->'plan'->>'reasonCode'='CAPTURE_READY'
            AND e.request_bytes IS NOT NULL),0)::int AS capture_ready_packets
      FROM consultations c JOIN archives a ON a.consultation_id=c.id
      WHERE c.id='${consultationId}'`);
    if (rows.length !== 1) throw new Error(`missing durable consultation ${consultationId}`);
    return rows[0];
  }

  async function effectEvidence(consultationId, effectKind) {
    return await queryJson(`
      SELECT id,effect_kind AS kind,generation,subject_id AS "subjectId",state,attempts,
        request_hash AS "requestHash",lease_owner AS "leaseOwner",
        lease_expires_at AS "leaseExpiresAt",result,compensation_result AS "compensationResult"
      FROM external_effects
      WHERE consultation_id='${consultationId}' AND effect_kind='${effectKind}'
      ORDER BY created_at`);
  }

  async function participantEgressDenialEvidence(consultationId) {
    const rows = await queryJson(`
      SELECT c.admission_fenced_at,
        COALESCE((SELECT count(*) FROM consultation_participants p
          WHERE p.consultation_id=c.id AND p.publication_granted),0)::int AS publication_grants,
        COALESCE((SELECT count(*) FROM external_effects grant_effect
          WHERE grant_effect.consultation_id=c.id
            AND grant_effect.effect_kind='PARTICIPANT_GRANT'),0)::int AS participant_grant_effects,
        COALESCE((SELECT count(*) FROM external_effects packet
          WHERE packet.consultation_id=c.id AND packet.effect_kind='STATUS_PACKET'
            AND packet.result->'plan'->>'reasonCode'='CAPTURE_READY'
            AND packet.request_bytes IS NOT NULL),0)::int AS capture_ready_packets,
        denied.effect AS denied_effect
      FROM consultations c
      LEFT JOIN LATERAL (
        SELECT json_build_object(
          'id',effect.id,'kind',effect.effect_kind,'generation',effect.generation,
          'subjectId',effect.subject_id,'state',effect.state,'attempts',effect.attempts,
          'requestHash',effect.request_hash,'leaseOwner',effect.lease_owner,
          'leaseExpiresAt',effect.lease_expires_at,'result',effect.result,
          'compensationResult',effect.compensation_result
        ) AS effect
        FROM external_effects effect
        WHERE effect.consultation_id=c.id
          AND effect.effect_kind='PARTICIPANT_EGRESS'
          AND COALESCE(effect.result->>'error','') LIKE '%test fault denied PARTICIPANT_EGRESS%'
        ORDER BY effect.created_at DESC
        LIMIT 1
      ) denied ON true
      WHERE c.id='${consultationId}'`);
    if (rows.length !== 1) throw new Error(`missing durable consultation ${consultationId}`);
    return rows[0];
  }

  async function providerAttemptEvidence(consultationId, stage) {
    return await queryJson(`
      SELECT id,stage,operation_id AS "operationId",attempt_number AS "attemptNumber",
        outcome,terminal_at AS "terminalAt",error_kind AS "errorKind",error_scope AS "errorScope",
        provider_retry_advice AS "providerRetryAdvice",retry_of AS "retryOf",
        retry_decision AS "retryDecision",accepted_input_watermark AS accepted,
        received_output_watermark AS received,emitted_output_watermark AS emitted,
        terminal_hash AS "terminalHash",num_nonnulls(raw_http,raw_websocket,raw_grpc) AS "rawRefs"
      FROM provider_attempts
      WHERE consultation_id='${consultationId}' AND stage='${stage}'
      ORDER BY started_at`);
  }

  async function workerSupervisionEvidence(consultationId) {
    const rows = await queryJson(`
      SELECT c.generation,c.state,
        COALESCE((SELECT json_agg(json_build_object(
          'workerId',w.worker_id,'generation',w.generation,'epoch',w.epoch,
          'fencedAt',w.fenced_at,'terminalOutcome',w.terminal_outcome,
          'terminalAt',w.terminal_at,'terminalCheckpointId',w.terminal_checkpoint_id
        ) ORDER BY w.epoch) FROM worker_job_epochs w WHERE w.consultation_id=c.id),'[]'::json) AS worker_epochs,
        COALESCE((SELECT json_agg(json_build_object(
          'workerId',r.worker_id,'generation',r.generation,'epoch',r.epoch,
          'heartbeatAt',r.heartbeat_at,'leaseExpiresAt',r.lease_expires_at,
          'fencedAt',r.fenced_at,'fenceReason',r.fence_reason
        ) ORDER BY r.epoch) FROM worker_reservations r WHERE r.consultation_id=c.id),'[]'::json) AS reservations
      FROM consultations c WHERE c.id='${consultationId}'`);
    if (rows.length !== 1) throw new Error(`missing durable consultation ${consultationId}`);
    return rows[0];
  }

  async function settlementObservation(consultationId) {
    const rows = await queryJson(`
      SELECT c.id,c.generation,c.state,a.state AS archive_state,
        COALESCE((SELECT count(*) FROM outbox o
          WHERE o.aggregate_id=c.id AND o.delivered_at IS NULL),0)::int AS pending_outbox,
        COALESCE((SELECT count(*) FROM external_effects e WHERE e.consultation_id=c.id
          AND e.state IN ('planned','calling','applied','compensating')),0)::int AS active_effects,
        COALESCE((SELECT count(*) FROM expected_archive_artifacts x WHERE x.archive_id=a.id
          AND x.disposition='expected' AND x.fulfilled_object_id IS NULL),0)::int AS unresolved_expectations,
        COALESCE((SELECT count(*) FROM orchestration_deadlines d
          WHERE d.consultation_id=c.id AND d.completed_at IS NULL),0)::int AS unfinished_deadlines,
        COALESCE((SELECT count(*) FROM egress_jobs j WHERE j.consultation_id=c.id AND
          (j.terminal_at IS NULL OR j.terminal_result IS NULL OR
           j.state NOT IN ('EGRESS_COMPLETE','EGRESS_FAILED','EGRESS_ABORTED','EGRESS_LIMIT_REACHED'))),0)::int AS active_egress,
        COALESCE((SELECT count(*) FROM worker_reservations r WHERE r.consultation_id=c.id
          AND r.fenced_at IS NULL AND r.released_at IS NULL),0)::int AS unfenced_reservations,
        COALESCE((SELECT count(*) FROM worker_job_epochs w WHERE w.consultation_id=c.id
          AND w.terminal_at IS NULL),0)::int AS unterminated_worker_epochs,
        COALESCE((SELECT count(*) FROM external_effects created WHERE created.consultation_id=c.id
          AND created.effect_kind='WORKER_DISPATCH' AND created.result->>'remoteId' IS NOT NULL
          AND created.compensation_result IS NULL AND NOT EXISTS (
            SELECT 1 FROM external_effects removed WHERE removed.consultation_id=c.id
              AND removed.effect_kind='DISPATCH_DELETE' AND removed.state='done'
              AND removed.result->'plan'->>'dispatchId'=created.result->>'remoteId')),0)::int AS unclean_dispatches,
        COALESCE((SELECT count(*) FROM external_effects created WHERE created.consultation_id=c.id
          AND created.effect_kind='ROOM_CREATE' AND created.result->>'remoteId' IS NOT NULL
          AND created.compensation_result IS NULL AND NOT EXISTS (
            SELECT 1 FROM external_effects removed WHERE removed.consultation_id=c.id
              AND removed.effect_kind='ROOM_DELETE' AND removed.state='done'
              AND (removed.result->'plan'->>'resourceGeneration')::integer=created.generation)),0)::int AS unclean_rooms,
        NOT EXISTS (SELECT 1 FROM external_effects created WHERE created.consultation_id=c.id
          AND created.effect_kind='ROOM_CREATE' AND created.result->>'remoteId' IS NOT NULL
          AND created.compensation_result IS NULL AND NOT EXISTS (
            SELECT 1 FROM external_effects removed WHERE removed.consultation_id=c.id
              AND removed.effect_kind='ROOM_DELETE' AND removed.state='done'
              AND (removed.result->'plan'->>'resourceGeneration')::integer=created.generation)) AS room_cleanup_confirmed
      FROM consultations c JOIN archives a ON a.consultation_id=c.id
      WHERE c.id='${consultationId}'`);
    if (rows.length !== 1) throw new Error(`missing durable consultation ${consultationId}`);
    return rows[0];
  }

  async function consultationEvidence(consultationId) {
    const rows = await queryJson(`
      SELECT c.id,c.generation,c.state,c.admission_fenced_at,
        a.id AS archive_id,a.state AS archive_state,a.final_inventory_hash,
        (SELECT row_to_json(f) FROM final_inventories f WHERE f.archive_id=a.id) AS inventory,
        COALESCE((SELECT json_agg(json_build_object(
          'id',e.id,'kind',e.effect_kind,'generation',e.generation,'subjectId',e.subject_id,
          'state',e.state,'attempts',e.attempts,'requestHash',e.request_hash,
          'leaseOwner',e.lease_owner,'leaseExpiresAt',e.lease_expires_at,
          'result',e.result,'compensationResult',e.compensation_result
        ) ORDER BY e.created_at) FROM external_effects e WHERE e.consultation_id=c.id),'[]'::json) AS effects,
        COALESCE((SELECT json_agg(json_build_object(
          'kind',j.kind,'state',j.state,'egressId',j.egress_id,'terminalAt',j.terminal_at,
          'terminalResult',j.terminal_result,'expectedArtifactId',j.expected_artifact_id
        ) ORDER BY j.created_at) FROM egress_jobs j WHERE j.consultation_id=c.id),'[]'::json) AS egress,
        COALESCE((SELECT json_agg(json_build_object(
          'id',p.id,'stage',p.stage,'operationId',p.operation_id,
          'attemptNumber',p.attempt_number,'outcome',p.outcome,'terminalAt',p.terminal_at,
          'errorKind',p.error_kind,'errorScope',p.error_scope,
          'providerRetryAdvice',p.provider_retry_advice,
          'retryOf',p.retry_of,'retryDecision',p.retry_decision,
          'accepted',p.accepted_input_watermark,'received',p.received_output_watermark,
          'emitted',p.emitted_output_watermark,'terminalHash',p.terminal_hash,
          'rawRefs',num_nonnulls(p.raw_http,p.raw_websocket,p.raw_grpc)
        ) ORDER BY p.started_at) FROM provider_attempts p
          WHERE p.consultation_id=c.id),'[]'::json) AS attempts,
        COALESCE((SELECT json_agg(json_build_object(
          'workerId',w.worker_id,'generation',w.generation,'epoch',w.epoch,
          'fencedAt',w.fenced_at,'terminalOutcome',w.terminal_outcome,
          'terminalAt',w.terminal_at,'terminalCheckpointId',w.terminal_checkpoint_id
        ) ORDER BY w.epoch) FROM worker_job_epochs w WHERE w.consultation_id=c.id),'[]'::json) AS worker_epochs,
        COALESCE((SELECT json_agg(json_build_object(
          'workerId',r.worker_id,'generation',r.generation,'epoch',r.epoch,
          'heartbeatAt',r.heartbeat_at,'leaseExpiresAt',r.lease_expires_at,
          'fencedAt',r.fenced_at,'fenceReason',r.fence_reason
        ) ORDER BY r.epoch) FROM worker_reservations r WHERE r.consultation_id=c.id),'[]'::json) AS reservations,
        COALESCE((SELECT count(*) FROM expected_archive_artifacts x WHERE x.archive_id=a.id),0)::int AS expected_count,
        COALESCE((SELECT count(*) FROM archive_objects o WHERE o.archive_id=a.id),0)::int AS object_count,
        COALESCE((SELECT count(*) FROM consultation_participants p
          WHERE p.consultation_id=c.id AND p.publication_granted),0)::int AS publication_grants,
        COALESCE((SELECT count(*) FROM external_effects e
          WHERE e.consultation_id=c.id AND e.effect_kind='PARTICIPANT_GRANT'),0)::int
          AS participant_grant_effects,
        COALESCE((SELECT count(*) FROM external_effects e
          WHERE e.consultation_id=c.id AND e.effect_kind='STATUS_PACKET'
            AND e.result->'plan'->>'reasonCode'='CAPTURE_READY'
            AND e.request_bytes IS NOT NULL),0)::int
          AS capture_ready_packets,
        COALESCE((SELECT count(*) FROM outbox o
          WHERE o.aggregate_id=c.id AND o.delivered_at IS NULL),0)::int AS pending_outbox,
        COALESCE((SELECT count(*) FROM outbox o
          WHERE o.aggregate_id=c.id AND o.topic='consultation.cancelled'
            AND o.generation=c.generation AND o.delivered_at IS NULL),0)::int
          AS pending_cancellation_outbox,
        COALESCE((SELECT count(*) FROM external_effects e
          WHERE e.consultation_id=c.id
            AND e.state IN ('planned','calling','applied','compensating')),0)::int
          AS active_effects,
        COALESCE((SELECT count(*) FROM expected_archive_artifacts x
          WHERE x.archive_id=a.id AND x.disposition='expected'
            AND x.fulfilled_object_id IS NULL),0)::int AS unresolved_expectations,
        COALESCE((SELECT count(*) FROM orchestration_deadlines d
          WHERE d.consultation_id=c.id AND d.completed_at IS NULL),0)::int
          AS unfinished_deadlines,
        COALESCE((SELECT count(*) FROM external_effects created
          WHERE created.consultation_id=c.id AND created.effect_kind='WORKER_DISPATCH'
            AND created.result->>'remoteId' IS NOT NULL
            AND created.compensation_result IS NULL
            AND NOT EXISTS (
              SELECT 1 FROM external_effects removed
              WHERE removed.consultation_id=c.id AND removed.effect_kind='DISPATCH_DELETE'
                AND removed.state='done'
                AND removed.result->'plan'->>'dispatchId'=created.result->>'remoteId'
            )),0)::int AS unclean_dispatches,
        COALESCE((SELECT count(*) FROM external_effects created
          WHERE created.consultation_id=c.id AND created.effect_kind='ROOM_CREATE'
            AND created.result->>'remoteId' IS NOT NULL
            AND created.compensation_result IS NULL
            AND NOT EXISTS (
              SELECT 1 FROM external_effects removed
              WHERE removed.consultation_id=c.id AND removed.effect_kind='ROOM_DELETE'
                AND removed.state='done'
                AND (removed.result->'plan'->>'resourceGeneration')::integer=created.generation
            )),0)::int AS unclean_rooms,
        COALESCE((SELECT count(*) FROM egress_jobs j
          WHERE j.consultation_id=c.id
            AND (
              j.terminal_at IS NULL OR j.terminal_result IS NULL
              OR j.state NOT IN ('EGRESS_COMPLETE','EGRESS_FAILED','EGRESS_ABORTED','EGRESS_LIMIT_REACHED')
            )),0)::int AS active_egress,
        COALESCE((SELECT count(*) FROM worker_reservations r
          WHERE r.consultation_id=c.id AND r.fenced_at IS NULL AND r.released_at IS NULL),0)::int
          AS unfenced_reservations,
        COALESCE((SELECT count(*) FROM worker_job_epochs w
          WHERE w.consultation_id=c.id AND w.terminal_at IS NULL),0)::int
          AS unterminated_worker_epochs,
        COALESCE((SELECT json_agg(json_build_object(
          'kind',d.kind,'generation',d.generation,'dueAt',d.due_at,'completedAt',d.completed_at,
          'leaseOwner',d.lease_owner,'leaseExpiresAt',d.lease_expires_at
        ) ORDER BY d.kind) FROM orchestration_deadlines d
          WHERE d.consultation_id=c.id),'[]'::json) AS deadlines,
        NOT EXISTS (
          SELECT 1 FROM external_effects created
          WHERE created.consultation_id=c.id AND created.effect_kind='ROOM_CREATE'
            AND created.result->>'remoteId' IS NOT NULL
            AND created.compensation_result IS NULL
            AND NOT EXISTS (
              SELECT 1 FROM external_effects removed
              WHERE removed.consultation_id=c.id AND removed.effect_kind='ROOM_DELETE'
                AND removed.state='done'
                AND (removed.result->'plan'->>'resourceGeneration')::integer=created.generation
            )
        ) AS room_cleanup_confirmed
      FROM consultations c JOIN archives a ON a.consultation_id=c.id WHERE c.id='${consultationId}'`);
    if (rows.length !== 1) throw new Error(`missing durable consultation ${consultationId}`);
    return rows[0];
  }

  function isCleanSettlement(evidence) {
    return settlementProblems(evidence).length === 0;
  }

  function settlementSummary(evidence) {
    return {
      problems: settlementProblems(evidence),
      state: evidence.state,
      generation: evidence.generation,
      archiveState: evidence.archive_state,
      pendingOutbox: evidence.pending_outbox,
      activeEffects: evidence.active_effects,
      unresolvedExpectations: evidence.unresolved_expectations,
      unfinishedDeadlines: evidence.unfinished_deadlines,
      uncleanDispatches: evidence.unclean_dispatches,
      uncleanRooms: evidence.unclean_rooms,
      activeEgress: evidence.active_egress,
      unfencedReservations: evidence.unfenced_reservations,
      unterminatedWorkerEpochs: evidence.unterminated_worker_epochs,
      roomCleanupConfirmed: evidence.room_cleanup_confirmed,
      effects: evidence.effects.slice(0, 10).map((effect) => ({
        kind: effect.kind,
        generation: effect.generation,
        state: effect.state,
        attempts: effect.attempts,
      })),
      egress: evidence.egress.slice(0, 10).map((job) => ({
        kind: job.kind,
        state: job.state,
        terminal: job.terminalAt != null && job.terminalResult != null,
      })),
      workerEpochs: evidence.worker_epochs.slice(0, 10).map((epoch) => ({
        generation: epoch.generation,
        epoch: epoch.epoch,
        terminal: epoch.terminalAt != null,
      })),
      reservations: evidence.reservations.slice(0, 10).map((reservation) => ({
        generation: reservation.generation,
        epoch: reservation.epoch,
        fenced: reservation.fencedAt != null,
      })),
      deadlines: evidence.deadlines.slice(0, 10).map((deadline) => ({
        generation: deadline.generation,
        kind: deadline.kind,
        completed: deadline.completedAt != null,
      })),
      truncated:
        evidence.effects.length > 10 ||
        evidence.egress.length > 10 ||
        evidence.worker_epochs.length > 10 ||
        evidence.reservations.length > 10 ||
        evidence.deadlines.length > 10,
    };
  }

  async function cancelBeforeStartForCleanup(consultationId) {
    await database.begin(async (transaction) => {
      const rows = await transaction`
        SELECT state,generation FROM consultations WHERE id=${consultationId} FOR UPDATE`;
      const current = rows[0];
      if (!current || current.state === "cancelled") return;
      if (current.state !== "invited" && current.state !== "ready") {
        throw new Error(
          `cleanup cancellation requires invited or ready, received ${String(current.state)}`,
        );
      }
      const generation = Number(current.generation) + 1;
      await transaction`
        UPDATE consultations
        SET state='cancelled',generation=${generation},
          admission_fenced_at=COALESCE(admission_fenced_at,now()),
          updated_at=GREATEST(now(),updated_at+interval '1 microsecond')
        WHERE id=${consultationId}`;
      await transaction`
        UPDATE magic_links SET revoked_at=now()
        WHERE consultation_id=${consultationId}
          AND consumed_at IS NULL AND revoked_at IS NULL`;
      await transaction`
        INSERT INTO outbox(id,topic,aggregate_id,generation,payload,available_at)
        VALUES (${randomUUID()},'consultation.cancelled',${consultationId},${generation},
          jsonb_build_object(
            'resourceGeneration',${current.generation}::integer,
            'consultationId',${consultationId}::uuid,
            'generation',${generation}::integer
          ),now())`;
    });
  }

  async function forceArchiveReconciliationDeadline(consultationId, generation) {
    await sql(`
      UPDATE archives
      SET reconciliation_deadline_at=now()-interval '1 second'
      WHERE consultation_id='${consultationId}' AND state='reconciling';
      INSERT INTO orchestration_deadlines(
        consultation_id,generation,kind,due_at,completed_at,lease_owner,lease_expires_at
      )
      VALUES (
        '${consultationId}',${generation},'archive-reconcile',
        now()-interval '1 second',NULL,NULL,NULL
      )
      ON CONFLICT (consultation_id,generation,kind) DO UPDATE
      SET due_at=excluded.due_at,completed_at=NULL,lease_owner=NULL,lease_expires_at=NULL
    `);
  }

  async function settleConsultation(consultationId, { stopAtReconciliation = false } = {}) {
    for (let transition = 0; transition < 6; transition += 1) {
      const evidence = await consultationStatus(consultationId);
      if (["ended", "cancelled", "deleted"].includes(evidence.state)) {
        if (stopAtReconciliation && evidence.archive_state === "reconciling") {
          return evidence;
        }
        let initialError;
        try {
          await waitFor(
            `${consultationId} terminal resource settlement`,
            async () => {
              const current = await settlementObservation(consultationId);
              return isCleanSettlement(current) ? current : null;
            },
            30_000,
          );
          return await consultationEvidence(consultationId);
        } catch (error) {
          initialError = error;
        }
        let current = await consultationStatus(consultationId);
        if (current.archive_state === "reconciling") {
          await forceArchiveReconciliationDeadline(consultationId, current.generation);
          try {
            await waitFor(
              `${consultationId} forced terminal resource settlement`,
              async () => {
                const forced = await settlementObservation(consultationId);
                return isCleanSettlement(forced) ? forced : null;
              },
              90_000,
            );
            return await consultationEvidence(consultationId);
          } catch (error) {
            initialError = error;
            current = await consultationEvidence(consultationId);
          }
        }
        if (!Array.isArray(current.effects)) current = await consultationEvidence(consultationId);
        throw new Error(
          `terminal resources did not settle: ${JSON.stringify(settlementSummary(current))}`,
          { cause: initialError },
        );
      }
      const generation = evidence.generation;
      if (evidence.state === "invited" || evidence.state === "ready") {
        await cancelBeforeStartForCleanup(consultationId);
      } else if (evidence.state === "active") {
        await sql(`
          UPDATE consultations SET both_absent_since=now()-interval '1 minute'
          WHERE id='${consultationId}' AND state='active';
          INSERT INTO orchestration_deadlines(consultation_id,generation,kind,due_at)
          VALUES ('${consultationId}',${generation},'absence',now()-interval '1 second')
          ON CONFLICT(consultation_id,generation,kind)
          DO UPDATE SET due_at=excluded.due_at,completed_at=NULL,lease_owner=NULL,lease_expires_at=NULL`);
      } else if (evidence.state === "finalizing") {
        await sql(`
          UPDATE consultations SET finalize_deadline_at=now()-interval '1 second'
          WHERE id='${consultationId}' AND state='finalizing';
          INSERT INTO orchestration_deadlines(consultation_id,generation,kind,due_at)
          VALUES ('${consultationId}',${generation},'finalize',now()-interval '1 second')
          ON CONFLICT(consultation_id,generation,kind)
          DO UPDATE SET due_at=excluded.due_at,completed_at=NULL,lease_owner=NULL,lease_expires_at=NULL`);
      } else {
        throw new Error(
          `cannot settle consultation ${consultationId} from state ${evidence.state}`,
        );
      }
      const previousState = evidence.state;
      try {
        await waitFor(
          `${consultationId} cleanup from ${previousState}`,
          async () => {
            const current = await consultationStatus(consultationId);
            return current.state !== previousState ? current : null;
          },
          90_000,
        );
      } catch (error) {
        const current = await consultationEvidence(consultationId);
        throw new Error(
          `cleanup did not advance from ${previousState}: ${JSON.stringify(settlementSummary(current))}`,
          { cause: error },
        );
      }
    }
    throw new Error(`consultation ${consultationId} did not release its scenario resources`);
  }

  async function settleConsultations(consultationIds, concurrency = 3) {
    const ids = [...new Set(consultationIds)];
    if (ids.length === 0) return;
    const failures = [];
    let nextIndex = 0;
    let leaseWrite = Promise.resolve();
    const renewLease = (leaseMs = ownerLeaseMs) => {
      leaseWrite = leaseWrite.then(() => persistOwnerLease(leaseMs));
      return leaseWrite;
    };
    const heartbeat = setInterval(
      () => {
        renewLease().catch((error) => failures.push(error));
      },
      Math.max(1_000, Math.floor(ownerLeaseMs / 3)),
    );
    try {
      const workers = Array.from({ length: Math.min(concurrency, ids.length) }, async () => {
        for (;;) {
          const index = nextIndex;
          nextIndex += 1;
          if (index >= ids.length) return;
          const consultationId = ids[index];
          try {
            await settleConsultation(consultationId);
            await renewLease();
          } catch (error) {
            failures.push(
              new Error(`${consultationId}: ${error?.message ?? String(error)}`, { cause: error }),
            );
          }
        }
      });
      await Promise.all(workers);
      await leaseWrite;
      if (failures.length > 0) {
        await renewLease(0);
        throw new AggregateError(
          failures,
          `failed to settle owned consultations: ${failures
            .map((failure) => failure.message)
            .join("; ")}`,
        );
      }
    } catch (error) {
      await renewLease(0).catch(() => undefined);
      throw error;
    } finally {
      clearInterval(heartbeat);
    }
  }

  function assertTerminalAttempt(attempt, expectedKind) {
    if (
      !/^[0-9a-f]{64}$/u.test(attempt?.terminalHash ?? "") ||
      !attempt.outcome ||
      !attempt.terminalAt ||
      attempt.rawRefs !== 1 ||
      typeof attempt.retryDecision?.action !== "string"
    ) {
      throw new Error(`provider terminal lacks exact evidence: ${JSON.stringify(attempt)}`);
    }
    if (expectedKind && attempt.errorKind !== expectedKind)
      throw new Error(`expected ${expectedKind}, received ${attempt.errorKind}`);
  }

  function assertTranslationFailureEvidence(evidence, expected) {
    const attempts = evidence.attempts.filter(
      (attempt) => attempt.stage === "translation" && attempt.errorKind === expected.failure,
    );
    if (attempts.length === 0) {
      throw new Error(`missing ${expected.failure} translation terminal`);
    }
    const attempt = attempts[0];
    assertTerminalAttempt(attempt, expected.failure);
    if (
      attempt.providerRetryAdvice !== expected.advice ||
      attempt.retryDecision?.action !== expected.action ||
      Number(attempt.accepted) !== expected.watermarks.accepted ||
      Number(attempt.received) !== expected.watermarks.received ||
      Number(attempt.emitted) !== expected.watermarks.emitted
    ) {
      throw new Error(
        `${expected.failure} terminal policy/watermark mismatch: ${JSON.stringify(attempt)}`,
      );
    }
    if (attempt.retryDecision?.previousAttemptId !== attempt.id) {
      throw new Error(
        `${expected.failure} retry decision is not bound to its terminal attempt: ` +
          JSON.stringify(attempt),
      );
    }
    const operationAttempts = evidence.attempts.filter(
      (candidate) =>
        candidate.stage === "translation" && candidate.operationId === attempt.operationId,
    );
    const linkedAttempts = operationAttempts.filter(
      (candidate) => candidate.retryOf === attempt.id,
    );
    if (expected.expectRetry) {
      if (operationAttempts.length < 2 || linkedAttempts.length !== 1) {
        throw new Error(
          `${expected.failure} retry policy/relation mismatch: ${JSON.stringify(operationAttempts)}`,
        );
      }
      const orderedAttempts = operationAttempts.toSorted(
        (left, right) => Number(left.attemptNumber) - Number(right.attemptNumber),
      );
      for (let index = 1; index < orderedAttempts.length; index += 1) {
        const previous = orderedAttempts[index - 1];
        const current = orderedAttempts[index];
        if (
          !previous ||
          !current ||
          previous.retryDecision?.action !== "retry" ||
          Number(current.attemptNumber) !== Number(previous.attemptNumber) + 1 ||
          current.operationId !== previous.operationId ||
          current.retryOf !== previous.id
        ) {
          throw new Error(
            `${expected.failure} retry chain is not contiguous: ${JSON.stringify(operationAttempts)}`,
          );
        }
      }
      if (orderedAttempts.at(-1)?.retryDecision?.action === "retry") {
        throw new Error(
          `${expected.failure} retry chain lacks a terminal decision: ${JSON.stringify(operationAttempts)}`,
        );
      }
    } else if (
      operationAttempts.length !== 1 ||
      operationAttempts.some((candidate) => candidate.retryOf !== null)
    ) {
      throw new Error(`${expected.failure} must never retry: ${JSON.stringify(operationAttempts)}`);
    }
    return attempt;
  }

  async function controlWorkerBaselines() {
    const controls = await Promise.all([
      onlyContainer("control-worker-1"),
      onlyContainer("control-worker-2"),
    ]);
    const baselines = new Map(
      await Promise.all(
        controls.map(async (container) => {
          const details = await inspect(container.Id);
          const workerId = details.Config.Env.find((entry) =>
            entry.startsWith("INSTANCE_ID="),
          )?.slice("INSTANCE_ID=".length);
          if (!workerId) throw new Error(`control worker ${container.Id} has no INSTANCE_ID`);
          return [
            workerId,
            {
              containerId: container.Id,
              workerId,
              restartCount: details.RestartCount,
              startedAt: details.State.StartedAt,
            },
          ];
        }),
      ),
    );
    return { controls, baselines };
  }

  async function runEffectBoundaryCrash({ scenario, fault, expectedState, exitCode }) {
    const { baselines } = await controlWorkerBaselines();
    await resetAuthenticationThrottle();
    const crashWatermarkMs = Date.now();
    const run = await runConsultation({ faults: { [fault]: ["ROOM_CREATE"] } });
    const crashedEffect = await waitFor(
      `${scenario} durable boundary`,
      async () => {
        const effects = await effectEvidence(run.consultationId, "ROOM_CREATE");
        return (
          effects.find(
            (candidate) =>
              candidate.state === expectedState && typeof candidate.leaseOwner === "string",
          ) ?? null
        );
      },
      60_000,
    );
    const crashedWorker = baselines.get(crashedEffect.leaseOwner);
    if (!crashedWorker) throw new Error(`${scenario} owner was not a live control replica`);
    const restarted = await waitFor(
      `${scenario} exact owner restart`,
      async (signal, deadline) => {
        const [current, exits] = await Promise.all([
          inspect(crashedWorker.containerId, { signal, deadline }),
          containerExitEvents(crashedWorker.containerId, crashWatermarkMs, { signal, deadline }),
        ]);
        const injectedExit = exits.some(
          (event) => String(event.Actor?.Attributes?.exitCode ?? "") === String(exitCode),
        );
        return injectedExit &&
          current.State.Running &&
          (!current.State.Health || current.State.Health.Status === "healthy") &&
          restartCountIncremented(crashedWorker.restartCount, current.RestartCount)
          ? { restartCount: current.RestartCount, startedAt: current.State.StartedAt }
          : null;
      },
      90_000,
    );
    await setFaults();
    const completed = await run.completed;
    if (completed.code !== 0) {
      throw new Error(`${scenario} consultation failed: ${completed.stderr}\n${completed.stdout}`);
    }
    await waitFor(
      `${scenario} clean durable settlement`,
      async () => {
        const current = await settlementObservation(run.consultationId);
        return isCleanSettlement(current) ? current : null;
      },
      120_000,
    );
    const evidence = await consultationEvidence(run.consultationId);
    const recoveredEffect = evidence.effects.find(
      (candidate) =>
        candidate.kind === "ROOM_CREATE" &&
        candidate.state === "done" &&
        typeof candidate.result?.remoteId === "string",
    );
    if (!recoveredEffect) {
      throw new Error(`${scenario} lacks one durable remote ROOM_CREATE outcome`);
    }
    if (expectedState === "calling" && recoveredEffect.attempts < 2) {
      throw new Error(`${scenario} did not recover by adopting the ambiguous remote success`);
    }
    if (expectedState === "applied" && recoveredEffect.attempts !== crashedEffect.attempts) {
      throw new Error(`${scenario} replayed a remote effect after durable applied evidence`);
    }
    return {
      name: scenario,
      consultationId: run.consultationId,
      remoteId: recoveredEffect.result.remoteId,
      effectAttempts: recoveredEffect.attempts,
      crashedWorkerId: crashedWorker.workerId,
      restartedEpoch: restarted.startedAt,
      restartCount: restarted.restartCount,
      cleanSettlement: true,
    };
  }

  Object.assign(ctx, {
    sql,
    queryJson,
    consultationEvidence,
    consultationStatus,
    effectEvidence,
    participantEgressDenialEvidence,
    providerAttemptEvidence,
    workerSupervisionEvidence,
    settlementObservation,
    isCleanSettlement,
    settlementSummary,
    cancelBeforeStartForCleanup,
    forceArchiveReconciliationDeadline,
    settleConsultation,
    settleConsultations,
    assertTerminalAttempt,
    assertTranslationFailureEvidence,
    controlWorkerBaselines,
    runEffectBoundaryCrash,
  });
}
