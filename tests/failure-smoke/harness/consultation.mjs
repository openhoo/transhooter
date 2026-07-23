import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { chromium } from "playwright";
import { archiveLockHierarchyBlocked, archiveRaceWinner } from "../harness-contracts.mjs";

export function installConsultation(ctx) {
  const {
    expectedProfile,
    capabilityMinimumRemainingMs,
    consultationRuns,
    trackedConsultations,
    releaseDirectory,
    consultationHarness,
    baseUrl,
    livekitUrl,
    mailpitUrl,
    adminEmail,
    database,
  } = ctx;
  const {
    rerunOneShot,
    waitFor,
    persistOwnerLease,
    operationDeadline,
    setFaults,
    setWorkerScenario,
    onlyContainer,
    inspect,
    withAbsoluteDeadline,
    sql,
    queryJson,
  } = ctx;
  async function refreshFixtureCapabilities() {
    if (expectedProfile !== "fixture" || process.env.APP_ENV !== "test") {
      throw new Error("failure smoke requires the hermetic fixture profile in APP_ENV=test");
    }
    await rerunOneShot("language-refresh");
    ctx.capabilityLease = await waitFor(
      "fresh fixture language capability publication",
      async () => {
        const rows = await queryJson(`
          SELECT p.name,p.current_revision,count(l.id)::int AS fresh_rows,min(l.fresh_until) AS fresh_until
          FROM provider_profiles p
          JOIN language_capabilities l
            ON l.profile_id=p.id AND l.revision=p.current_revision
          WHERE p.name='fixture' AND p.enabled AND l.enabled
          GROUP BY p.name,p.current_revision`);
        const lease = rows[0];
        const freshUntilMs = Date.parse(lease?.fresh_until);
        return Number(lease?.fresh_rows ?? 0) > 0 &&
          Number.isFinite(freshUntilMs) &&
          freshUntilMs - Date.now() >= capabilityMinimumRemainingMs
          ? lease
          : null;
      },
      30_000,
    );
  }

  async function assertFixtureCapabilityLease() {
    if (!ctx.capabilityLease) throw new Error("fixture capability lease was not initialized");
    const rows = await queryJson(`
      SELECT p.current_revision,count(l.id)::int AS fresh_rows,min(l.fresh_until) AS fresh_until
      FROM provider_profiles p
      JOIN language_capabilities l
        ON l.profile_id=p.id AND l.revision=p.current_revision
      WHERE p.name='fixture' AND p.enabled AND l.enabled
      GROUP BY p.current_revision`);
    const current = rows[0];
    const freshUntilMs = Date.parse(current?.fresh_until);
    if (
      current?.current_revision !== ctx.capabilityLease.current_revision ||
      Number(current?.fresh_rows ?? 0) !== Number(ctx.capabilityLease.fresh_rows) ||
      !Number.isFinite(freshUntilMs) ||
      freshUntilMs - Date.now() < capabilityMinimumRemainingMs
    ) {
      throw new Error(
        `fixture capability revision lease changed or expires too soon: ${JSON.stringify({
          leased: ctx.capabilityLease,
          current,
        })}`,
      );
    }
  }

  function customerEmailForRun(runId) {
    return `customer-${runId}@example.test`;
  }

  async function trackConsultationsForRun(runId) {
    const customerEmail = customerEmailForRun(runId);
    const candidates = (
      await queryJson(`
        SELECT DISTINCT c.id
        FROM consultations c
        JOIN consultation_participants p ON p.consultation_id=c.id AND p.role='customer'
        JOIN users u ON u.id=p.user_id
        JOIN magic_links m ON m.consultation_id=c.id AND m.user_id=u.id
          AND m.purpose='consultation_invite'
        WHERE u.email='${customerEmail}'
        ORDER BY c.id`)
    ).map((row) => row.id);
    for (const consultationId of candidates) {
      trackedConsultations.add(consultationId);
    }
    if (candidates.length > 0) await persistOwnerLease();
    return candidates;
  }

  function signalProcessGroup(child, signal) {
    try {
      process.kill(-child.pid, signal);
    } catch (error) {
      if (error?.code !== "ESRCH") throw error;
    }
  }

  async function settleBefore(promise, deadline) {
    let timer;
    try {
      return await Promise.race([
        promise.then((result) => ({ closed: true, result })),
        new Promise((resolve) => {
          timer = setTimeout(
            () => resolve({ closed: false }),
            Math.max(0, operationDeadline(deadline) - Date.now()),
          );
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  async function terminateProcessTree(run) {
    if (run.treeTerminated) return await run.completed;
    signalProcessGroup(run.child, "SIGTERM");
    const graceful = run.closed
      ? { closed: true, result: await run.completed }
      : await settleBefore(run.completed, Date.now() + 10_000);
    signalProcessGroup(run.child, "SIGKILL");
    run.treeTerminated = true;
    if (graceful.closed) return graceful.result;
    const forced = await settleBefore(run.completed, Date.now() + 10_000);
    if (!forced.closed) {
      run.closed = true;
      const result = {
        code: null,
        signal: "SIGKILL",
        stdout: run.stdout,
        stderr: `${run.stderr}\nprocess group did not close after SIGKILL`,
      };
      run.child.stdout?.destroy();
      run.child.stderr?.destroy();
      run.child.stdin?.destroy();
      run.child.unref();
      run.completion.resolve(result);
      return result;
    }
    return forced.result;
  }

  async function runConsultation({
    faults = {},
    workerScenario = {},
    captureBarrierTimeoutMs = null,
    skipMediaOutputProof = true,
  } = {}) {
    await assertFixtureCapabilityLease();
    const runId = randomUUID();
    await mkdir(releaseDirectory, { recursive: true });
    const releaseFile = `${releaseDirectory}/${randomUUID()}.release`;
    await rm(releaseFile, { force: true });
    const runDeadline = operationDeadline(Date.now() + 8 * 60_000);
    const child = spawn(
      "bun",
      [
        consultationHarness,
        "--base-url",
        baseUrl,
        "--livekit-url",
        livekitUrl,
        "--mailpit-url",
        process.env.MAILPIT_URL ?? "http://mailpit:8025",
        "--expected-profile",
        expectedProfile,
        "--expected-profile-revision",
        String(ctx.capabilityLease.current_revision),
        "--deadline-epoch-ms",
        String(runDeadline),
        "--emit-proof-json",
        ...(skipMediaOutputProof ? ["--skip-media-output-proof"] : []),
        "--failure-harness-release-file",
        releaseFile,
        "--failure-harness-release-timeout-ms",
        "30000",
        ...(captureBarrierTimeoutMs === null
          ? []
          : ["--capture-barrier-timeout-ms", String(captureBarrierTimeoutMs)]),
      ],
      {
        detached: true,
        env: { ...process.env, E2E_RUN_ID: runId },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    const progress = Promise.withResolvers();
    const completion = Promise.withResolvers();
    const run = {
      child,
      consultationId: null,
      releaseFile,
      stdout: "",
      stderr: "",
      closed: false,
      treeTerminated: false,
      completion,
      completed: null,
    };
    consultationRuns.add(run);
    let stdoutLines = "";
    child.stdout.on("data", (chunk) => {
      const text = chunk.toString();
      run.stdout += text;
      stdoutLines += text;
      for (;;) {
        const newline = stdoutLines.indexOf("\n");
        if (newline < 0) break;
        const line = stdoutLines.slice(0, newline);
        stdoutLines = stdoutLines.slice(newline + 1);
        try {
          const record = JSON.parse(line);
          if (
            record?.phase === "consultation-created" &&
            record.runId === runId &&
            /^[0-9a-f-]{36}$/u.test(record.consultationId)
          ) {
            progress.resolve(record);
          }
        } catch {
          // Non-JSON Playwright output remains part of the child result.
        }
      }
    });
    child.stderr.on("data", (chunk) => {
      const text = chunk.toString();
      run.stderr += text;
      process.stderr.write(text);
    });
    run.completed = completion.promise;
    child.on("close", (code, signal) => {
      run.closed = true;
      clearTimeout(run.absoluteTimer);
      progress.reject(
        new Error(`consultation exited before creation progress: ${run.stderr}\n${run.stdout}`),
      );
      completion.resolve({ code, signal, stdout: run.stdout, stderr: run.stderr });
    });
    run.absoluteTimer = setTimeout(
      () => {
        terminateProcessTree(run).catch(() => undefined);
      },
      Math.max(0, runDeadline - Date.now()),
    );
    let created;
    try {
      created = await progress.promise;
    } catch (error) {
      const candidates = await trackConsultationsForRun(runId);
      throw new Error(
        `consultation exited before progress; recovered run-correlated candidate IDs: ` +
          `${candidates.join(", ") || "none"}`,
        { cause: error },
      );
    }
    const candidates = await trackConsultationsForRun(runId);
    if (candidates.length !== 1 || candidates[0] !== created.consultationId) {
      throw new Error(
        `created consultation ${created.consultationId} did not exactly match durable invite identity ` +
          `${customerEmailForRun(runId)}: ${candidates.join(", ") || "none"}`,
      );
    }
    run.consultationId = created.consultationId;
    trackedConsultations.add(created.consultationId);
    await persistOwnerLease();
    try {
      await assertFixtureCapabilityLease();
      await setFaults(created.consultationId, faults);
      await setWorkerScenario(created.consultationId, workerScenario);
      await writeFile(releaseFile, `${created.runId}\n`, { mode: 0o600 });
    } catch (error) {
      await terminateProcessTree(run).catch(() => undefined);
      throw error;
    }
    return run;
  }
  async function assertServiceHealthy(service, timeoutMs = 120_000) {
    const container = await onlyContainer(service);
    return await waitFor(
      `${service} healthy`,
      async (signal, deadline) => {
        const state = (await inspect(container.Id, { signal, deadline })).State;
        return state.Running && (!state.Health || state.Health.Status === "healthy");
      },
      timeoutMs,
    );
  }

  async function resetAuthenticationThrottle() {
    await sql("DELETE FROM magic_link_requests");
  }

  async function fetchWithDeadline(url, init = {}, deadline = Date.now() + 30_000, parentSignal) {
    return await withAbsoluteDeadline(
      `fetch ${url}`,
      deadline,
      (signal) => fetch(url, { ...init, signal }),
      parentSignal ?? init.signal,
    );
  }

  async function latestLink(recipient, signal, deadline = Date.now() + 15_000) {
    const listResponse = await fetchWithDeadline(
      `${mailpitUrl}/api/v1/messages?limit=100`,
      {},
      deadline,
      signal,
    );
    if (!listResponse.ok) throw new Error(`Mailpit list failed: ${listResponse.status}`);
    const payload = await listResponse.json();
    const messages = (payload.messages ?? payload.Messages ?? [])
      .filter((message) => {
        const recipients = message.To ?? message.to ?? [];
        return recipients.some((entry) => (entry.Address ?? entry.address) === recipient);
      })
      .sort(
        (left, right) =>
          Date.parse(right.Created ?? right.created ?? 0) -
          Date.parse(left.Created ?? left.created ?? 0),
      );
    const message = messages[0];
    if (!message) return null;
    const id = message.ID ?? message.Id ?? message.id;
    const detailResponse = await fetchWithDeadline(
      `${mailpitUrl}/api/v1/message/${encodeURIComponent(id)}`,
      {},
      deadline,
      signal,
    );
    if (!detailResponse.ok) throw new Error(`Mailpit message failed: ${detailResponse.status}`);
    const detail = await detailResponse.json();
    const content = `${detail.HTML ?? detail.Html ?? ""}\n${detail.Text ?? detail.text ?? ""}`;
    const match = content.match(/https?:\/\/[^\s"'<>]+\/auth\/exchange\?[^\s"'<>]+/u);
    return match?.[0]?.replaceAll("&amp;", "&") ?? null;
  }

  function internalizeLink(link) {
    const url = new URL(link);
    const internal = new URL(baseUrl);
    url.protocol = internal.protocol;
    url.host = internal.host;
    return url.toString();
  }

  async function authenticateAdmin(context) {
    const page = await context.newPage();
    await page.goto(`${baseUrl}/sign-in`, { waitUntil: "domcontentloaded" });
    await page.getByLabel("Email address").fill(adminEmail);
    const previousLink = await latestLink(adminEmail);
    await page.getByRole("button", { name: "Email me a sign-in link" }).click();
    const link = await waitFor(
      "admin magic link",
      async (signal, deadline) => {
        const candidate = await latestLink(adminEmail, signal, deadline);
        return candidate && candidate !== previousLink ? candidate : null;
      },
      30_000,
    );
    await page.goto(internalizeLink(link), { waitUntil: "domcontentloaded" });
    await page.getByRole("button", { name: "Continue securely" }).click();
    await page.waitForURL(/\/consultations(?:\?|$)/u);
    return page;
  }

  async function pagePost(page, path, body, deadline = operationDeadline(Date.now() + 30_000)) {
    try {
      return await page.evaluate(
        async ({ path, body, deadline }) => {
          const csrf = document.cookie
            .split("; ")
            .find((part) => part.startsWith("csrf="))
            ?.slice(5);
          if (!csrf) throw new Error("CSRF cookie unavailable");
          const response = await fetch(path, {
            method: "POST",
            credentials: "same-origin",
            cache: "no-store",
            headers: { "content-type": "application/json", "x-csrf-token": csrf },
            body: JSON.stringify(body),
            signal: AbortSignal.timeout(Math.max(1, deadline - Date.now())),
          });
          return {
            ok: response.ok,
            status: response.status,
            body: await response.text(),
          };
        },
        { path, body, deadline },
      );
    } catch (error) {
      throw new Error(
        `${path} request failed: ${error instanceof Error ? error.message : String(error)}`,
        { cause: error },
      );
    }
  }

  async function reauthenticateForArchive(page, consultationId) {
    const previousLink = await latestLink(adminEmail);
    const requested = await pagePost(page, "/api/auth/archive-delete-reauth", { consultationId });
    if (!requested.ok) {
      throw new Error(`archive reauthentication request failed: ${JSON.stringify(requested)}`);
    }
    const link = await waitFor(
      "archive-bound reauthentication link",
      async (signal, deadline) => {
        const candidate = await latestLink(adminEmail, signal, deadline);
        return candidate && candidate !== previousLink ? candidate : null;
      },
      30_000,
    );
    await page.goto(internalizeLink(link), { waitUntil: "domcontentloaded" });
    await page.getByRole("button", { name: "Continue securely" }).click();
    await page.waitForURL((url) => !url.pathname.startsWith("/auth/"), { timeout: 30_000 });
  }

  async function raceArchiveHoldAndDelete(archiveId, consultationId) {
    const browser = await chromium.launch({
      headless: true,
      args: ["--host-resolver-rules=MAP app.localhost web, MAP rtc.localhost livekit"],
    });
    const holdContext = await browser.newContext();
    const deleteContext = await browser.newContext();
    try {
      const holdPage = await authenticateAdmin(holdContext);
      const deletePage = await authenticateAdmin(deleteContext);
      await reauthenticateForArchive(holdPage, consultationId);
      await reauthenticateForArchive(deletePage, consultationId);
      let responses;
      const settledResponses = {};
      const observeResponse = (name, response) =>
        response.then(
          (value) => {
            settledResponses[name] = value;
            return value;
          },
          (error) => {
            settledResponses[name] = {
              error: error instanceof Error ? error.message : String(error),
            };
            throw error;
          },
        );
      const raceDeadline = operationDeadline(Date.now() + 120_000);
      await database.begin(async (transaction) => {
        await transaction`SELECT id FROM archives WHERE id=${archiveId} FOR UPDATE`;
        responses = Promise.all([
          observeResponse(
            "hold",
            pagePost(
              holdPage,
              `/api/archives/${archiveId}/hold`,
              {
                archiveId,
                consultationId,
                reason: "failure-smoke-race",
                enabled: true,
              },
              raceDeadline,
            ),
          ),
          observeResponse(
            "delete",
            pagePost(
              deletePage,
              `/api/archives/${archiveId}/delete`,
              {
                archiveId,
                consultationId,
                reason: "failure-smoke-race",
              },
              raceDeadline,
            ),
          ),
        ]);
        responses.catch(() => undefined);
        await waitFor(
          "both archive race requests blocked on the durable consultation/archive lock hierarchy",
          async (signal, deadline) => {
            const observations = await withAbsoluteDeadline(
              "archive lock observation",
              deadline,
              async () => {
                const rows = await database.unsafe(`
                  SELECT 'Lock' AS "waitEventType",relation.relname AS query
                  FROM pg_locks waiting
                  JOIN pg_locks held_by_waiter
                    ON held_by_waiter.pid=waiting.pid
                    AND held_by_waiter.locktype='relation'
                    AND held_by_waiter.granted
                  JOIN pg_class relation ON relation.oid=held_by_waiter.relation
                  WHERE NOT waiting.granted
                    AND waiting.pid <> pg_backend_pid()
                    AND relation.relname IN ('archives','consultations')`);
                return rows;
              },
              signal,
            );
            if (Object.keys(settledResponses).length > 0) {
              throw new Error(
                `archive race request bypassed the durable lock hierarchy: ${JSON.stringify(settledResponses)}`,
              );
            }
            return archiveLockHierarchyBlocked(observations) ? observations : null;
          },
          30_000,
        );
      });
      let hold;
      let deletion;
      try {
        [hold, deletion] = await responses;
      } catch (error) {
        const [activity, archiveState] = await Promise.all([
          database.unsafe(`
            SELECT lock.locktype,lock.mode,lock.granted,relation.relname AS relation
            FROM pg_locks lock
            LEFT JOIN pg_class relation ON relation.oid=lock.relation
            WHERE lock.pid <> pg_backend_pid()
              AND (NOT lock.granted OR relation.relname IN ('archives','consultations'))`),
          queryJson(`
            SELECT a.state,a.hold_operation_id,a.hold_operation_kind,a.hold_operation_owner,
              a.hold_operation_lease_expires_at,
              COALESCE(jsonb_agg(jsonb_build_object('id',h.id,'state',h.state))
                FILTER (WHERE h.id IS NOT NULL AND h.released_at IS NULL),'[]'::jsonb) AS active_holds
            FROM archives a
            LEFT JOIN legal_holds h ON h.archive_id=a.id
            WHERE a.id='${archiveId}'
            GROUP BY a.id`),
        ]);
        throw new Error(
          `archive race requests did not settle: ${JSON.stringify({
            settledResponses,
            activity,
            archiveState,
          })}`,
          { cause: error },
        );
      }
      const outcome = await queryJson(`
        SELECT a.state,
          COALESCE(jsonb_agg(jsonb_build_object('id',h.id,'reason',h.reason))
            FILTER (WHERE h.id IS NOT NULL AND h.released_at IS NULL),'[]'::jsonb) AS active_holds
        FROM archives a
        LEFT JOIN legal_holds h ON h.archive_id=a.id
        WHERE a.id='${archiveId}'
        GROUP BY a.state`);
      const row = outcome[0];
      const activeHolds = row?.active_holds ?? [];
      const winner = archiveRaceWinner({
        holdOk: hold.ok,
        deleteOk: deletion.ok,
        archiveState: row?.state,
        activeHoldCount: activeHolds.length,
      });
      if (!winner) {
        throw new Error(
          `production hold/delete operations did not serialize: ${JSON.stringify({
            hold,
            deletion,
            outcome,
          })}`,
        );
      }
      if (winner === "hold") {
        await reauthenticateForArchive(holdPage, consultationId);
        const release = await pagePost(
          holdPage,
          `/api/archives/${archiveId}/hold`,
          {
            archiveId,
            consultationId,
            holdId: activeHolds[0].id,
            enabled: false,
          },
          operationDeadline(Date.now() + 120_000),
        );
        if (!release.ok) {
          throw new Error(`production hold cleanup failed: ${JSON.stringify(release)}`);
        }
      }
      return {
        archiveState: row.state,
        winner,
        holdStatus: hold.status,
        deleteStatus: deletion.status,
      };
    } finally {
      await Promise.allSettled([holdContext.close(), deleteContext.close()]);
      await browser.close();
    }
  }

  Object.assign(ctx, {
    refreshFixtureCapabilities,
    assertFixtureCapabilityLease,
    customerEmailForRun,
    trackConsultationsForRun,
    signalProcessGroup,
    settleBefore,
    terminateProcessTree,
    runConsultation,
    assertServiceHealthy,
    resetAuthenticationThrottle,
    fetchWithDeadline,
    latestLink,
    internalizeLink,
    authenticateAdmin,
    pagePost,
    reauthenticateForArchive,
    raceArchiveHoldAndDelete,
  });
}
