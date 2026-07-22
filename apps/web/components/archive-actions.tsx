"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import type { FormEvent, MouseEvent } from "react";
import { useEffect, useId, useRef, useState } from "react";
import { api } from "@/lib/browser/browser-api";

type DownloadButtonProps = {
  archiveId: string;
  objectId: string;
  label: string;
};

export function DownloadButton({ archiveId, label, objectId }: DownloadButtonProps) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const errorId = useId();
  async function download() {
    setBusy(true);
    setError("");

    try {
      const result = await api<{ url: string }>(`/api/archives/${archiveId}/download`, {
        method: "POST",
        body: JSON.stringify({ archiveId, objectId }),
      });
      window.location.assign(result.url);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Download unavailable");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="grid min-w-0 gap-2">
      <button
        aria-label={`Download ${label}`}
        aria-describedby={error ? errorId : undefined}
        aria-busy={busy}
        className="inline-flex min-h-9 items-center justify-center rounded-md border border-border bg-background px-3 text-sm font-medium text-foreground transition-colors hover:bg-secondary disabled:cursor-not-allowed disabled:opacity-50"
        disabled={busy}
        type="button"
        onClick={() => {
          void download();
        }}
      >
        {busy ? "Preparing…" : "Download"}
      </button>
      {error && (
        <p className="text-xs text-destructive" id={errorId} role="alert">
          {error}
        </p>
      )}
    </div>
  );
}

type ActionFeedback = { kind: "status" | "error"; text: string } | null;

export function RefreshArchiveStatus() {
  const [refreshing, setRefreshing] = useState(false);

  return (
    <button
      aria-busy={refreshing}
      className="inline-flex min-h-9 items-center justify-center rounded-md border border-border bg-background px-3 text-sm font-medium text-foreground transition-colors hover:bg-secondary disabled:cursor-not-allowed disabled:opacity-50"
      disabled={refreshing}
      type="button"
      onClick={() => {
        setRefreshing(true);
        window.location.reload();
      }}
    >
      {refreshing ? "Refreshing…" : "Refresh archive status"}
    </button>
  );
}

type CursorLink = {
  cursor: string;
  previous: string;
};

const CURSOR_HISTORY_LIMIT = 128;
const FIRST_CURSOR = "__first__";

function readCursorHistory(storage: Pick<Storage, "getItem">, archiveId: string): CursorLink[] {
  try {
    const value: unknown = JSON.parse(
      storage.getItem(`transhooter.archive-cursors.${archiveId}`) ?? "[]",
    );
    if (!Array.isArray(value)) {
      return [];
    }
    return value
      .filter(
        (entry): entry is CursorLink =>
          typeof entry === "object" &&
          entry !== null &&
          typeof Reflect.get(entry, "cursor") === "string" &&
          typeof Reflect.get(entry, "previous") === "string",
      )
      .slice(-CURSOR_HISTORY_LIMIT);
  } catch {
    return [];
  }
}

export function rememberArchiveCursor(
  storage: Pick<Storage, "getItem" | "setItem">,
  archiveId: string,
  cursor: string,
  previous: string,
): void {
  const history = readCursorHistory(storage, archiveId).filter((entry) => entry.cursor !== cursor);
  history.push({ cursor, previous });
  try {
    storage.setItem(
      `transhooter.archive-cursors.${archiveId}`,
      JSON.stringify(history.slice(-CURSOR_HISTORY_LIMIT)),
    );
  } catch {
    // The links still retain one server-rendered step when session storage is unavailable.
  }
}

export function previousArchiveCursor(
  storage: Pick<Storage, "getItem">,
  archiveId: string,
  cursor: string,
): string | undefined {
  return readCursorHistory(storage, archiveId).findLast((entry) => entry.cursor === cursor)
    ?.previous;
}

type ArchivePaginationProps = {
  archiveId: string;
  currentCursor: string | undefined;
  nextCursor: string | null;
  previousCursor: string | undefined;
};

export function ArchivePagination({
  archiveId,
  currentCursor,
  nextCursor,
  previousCursor,
}: ArchivePaginationProps) {
  const [earlierCursor, setEarlierCursor] = useState<string>();

  useEffect(() => {
    setEarlierCursor(
      previousCursor && previousCursor !== FIRST_CURSOR
        ? previousArchiveCursor(window.sessionStorage, archiveId, previousCursor)
        : undefined,
    );
  }, [archiveId, previousCursor]);

  if (!currentCursor && !nextCursor) {
    return null;
  }

  const previousHref =
    previousCursor && previousCursor !== FIRST_CURSOR
      ? {
          pathname: `/archives/${archiveId}`,
          query: {
            cursor: previousCursor,
            ...(earlierCursor ? { previous: earlierCursor } : {}),
          },
        }
      : { pathname: `/archives/${archiveId}` };

  function rememberNext(event: MouseEvent<HTMLAnchorElement>) {
    if (
      event.button !== 0 ||
      event.altKey ||
      event.ctrlKey ||
      event.metaKey ||
      event.shiftKey ||
      !nextCursor
    ) {
      return;
    }
    rememberArchiveCursor(
      window.sessionStorage,
      archiveId,
      nextCursor,
      currentCursor ?? FIRST_CURSOR,
    );
  }

  return (
    <nav className="archivePagination" aria-label="Archive object pages">
      <p className="meta">Each page replaces the objects shown above.</p>
      <div className="actions">
        {currentCursor && (
          <>
            <Link className="button secondary" href={`/archives/${archiveId}`}>
              First objects page
            </Link>
            <Link className="button secondary" href={previousHref} rel="prev">
              Previous objects page
            </Link>
          </>
        )}
        {nextCursor && (
          <Link
            className="button secondary"
            href={{
              pathname: `/archives/${archiveId}`,
              query: {
                cursor: nextCursor,
                previous: currentCursor ?? FIRST_CURSOR,
              },
            }}
            rel="next"
            onClick={rememberNext}
          >
            Next objects page
          </Link>
        )}
      </div>
    </nav>
  );
}

type ArchiveAdminActionsProps = {
  archiveId: string;
  consultationId: string;
  activeHolds: ReadonlyArray<{
    id: string;
    reason: string;
  }>;
};

export function ArchiveAdminActions({
  archiveId,
  consultationId,
  activeHolds,
}: ArchiveAdminActionsProps) {
  const router = useRouter();
  const [feedback, setFeedback] = useState<ActionFeedback>(null);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [deleteAcknowledged, setDeleteAcknowledged] = useState(false);
  const [confirmingHoldId, setConfirmingHoldId] = useState<string | null>(null);
  const [deleteReason, setDeleteReason] = useState("");
  const deletionBlockedByHold = activeHolds.length > 0;
  const keepHoldRef = useRef<HTMLButtonElement>(null);
  const releaseTriggerRefs = useRef(new Map<string, HTMLButtonElement>());
  const restoreHoldTrigger = useRef<string | null>(null);
  const feedbackRef = useRef<HTMLDivElement>(null);
  const focusDestructiveSuccess = useRef(false);

  useEffect(() => {
    if (confirmingHoldId) {
      keepHoldRef.current?.focus();
      return;
    }

    if (restoreHoldTrigger.current) {
      releaseTriggerRefs.current.get(restoreHoldTrigger.current)?.focus();
      restoreHoldTrigger.current = null;
    }
  }, [confirmingHoldId]);
  useEffect(() => {
    if (feedback?.kind !== "status" || !focusDestructiveSuccess.current) {
      return;
    }
    focusDestructiveSuccess.current = false;
    feedbackRef.current?.focus();
  }, [feedback]);

  async function requestReauth() {
    setBusyAction("reauth");
    setFeedback(null);
    try {
      await api("/api/auth/archive-delete-reauth", {
        method: "POST",
        body: JSON.stringify({ consultationId }),
      });
      setFeedback({
        kind: "status",
        text: "A consultation-bound reauthentication link was sent. Open it, then return here within five minutes.",
      });
    } catch (cause) {
      setFeedback({
        kind: "error",
        text: cause instanceof Error ? cause.message : "Reauthentication could not be started",
      });
    } finally {
      setBusyAction(null);
    }
  }

  async function mutate(event: FormEvent<HTMLFormElement>, action: "hold" | "delete") {
    event.preventDefault();
    setBusyAction(action);
    setFeedback(null);
    const values = new FormData(event.currentTarget);
    try {
      await api(`/api/archives/${archiveId}/${action}`, {
        method: "POST",
        body: JSON.stringify({
          archiveId,
          consultationId,
          reason: values.get("reason"),
          ...(action === "hold" ? { enabled: true } : {}),
        }),
      });
      if (action === "delete") {
        setDeleteAcknowledged(false);
        setDeleteReason("");
        focusDestructiveSuccess.current = true;
      }
      router.refresh();
      setFeedback({
        kind: "status",
        text:
          action === "hold"
            ? "Legal hold results were verified for every object version."
            : "Deletion admitted. Storage emptiness will be verified before the tombstone is written.",
      });
    } catch (cause) {
      setFeedback({
        kind: "error",
        text: cause instanceof Error ? cause.message : "Action could not be completed",
      });
    } finally {
      setBusyAction(null);
    }
  }

  async function releaseHold(holdId: string) {
    setBusyAction(`release:${holdId}`);
    setFeedback(null);
    try {
      await api(`/api/archives/${archiveId}/hold`, {
        method: "POST",
        body: JSON.stringify({
          archiveId,
          consultationId,
          holdId,
          enabled: false,
        }),
      });
      setConfirmingHoldId(null);
      focusDestructiveSuccess.current = true;
      router.refresh();
      setFeedback({
        kind: "status",
        text: "The legal hold release was verified for every object version.",
      });
    } catch (cause) {
      setFeedback({
        kind: "error",
        text: cause instanceof Error ? cause.message : "The legal hold could not be released",
      });
    } finally {
      setBusyAction(null);
    }
  }

  return (
    <section
      aria-busy={busyAction !== null}
      aria-describedby={feedback ? "archive-action-feedback" : undefined}
      className="flex flex-col gap-5 rounded-md border border-border bg-card p-5"
    >
      <div className="border-b border-border pb-4">
        <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">Restricted archive controls</p>
        <h2 className="mt-1 font-serif text-lg font-semibold">Hold or delete</h2>
        <p className="mt-1 text-sm leading-relaxed text-muted-foreground">
          Both actions require a fresh, consultation-bound administrator sign-in and are recorded in the audit register.
        </p>
      </div>

      <button
        className="inline-flex min-h-9 w-fit items-center justify-center rounded-md border border-border bg-background px-3 text-sm font-medium transition-colors hover:bg-secondary disabled:cursor-not-allowed disabled:opacity-50"
        disabled={busyAction !== null}
        type="button"
        onClick={() => {
          void requestReauth();
        }}
      >
        {busyAction === "reauth" ? "Sending…" : "Send reauthentication link"}
      </button>

      <form
        aria-busy={busyAction === "hold"}
        className="grid gap-3 rounded-md border border-border p-4"
        onSubmit={(event) => {
          void mutate(event, "hold");
        }}
      >
        <div className="grid gap-2">
          <label className="text-sm font-medium" htmlFor="hold-reason">Legal hold reason</label>
          <input className="min-h-10 rounded-md border border-input bg-background px-3 text-sm" id="hold-reason" name="reason" required />
        </div>
        <button className="inline-flex min-h-9 w-fit items-center justify-center rounded-md border border-border bg-background px-3 text-sm font-medium transition-colors hover:bg-secondary disabled:cursor-not-allowed disabled:opacity-50" disabled={busyAction !== null} type="submit">
          {busyAction === "hold" ? "Applying…" : "Apply legal hold"}
        </button>
      </form>

      {activeHolds.length > 0 && (
        <div className="grid gap-3 rounded-md border border-attention/70 bg-attention/20 p-4">
          <h3 className="font-serif text-base font-semibold">Release an active hold</h3>
          {activeHolds.map((hold) => (
            <div className="grid gap-3 border-t border-border pt-3 first:border-0 first:pt-0" key={hold.id}>
              <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                <span className="archiveHoldReason">{hold.reason}</span>
                {confirmingHoldId !== hold.id && (
                  <button
                    ref={(element) => {
                      if (element) {
                        releaseTriggerRefs.current.set(hold.id, element);
                      } else {
                        releaseTriggerRefs.current.delete(hold.id);
                      }
                    }}
                    aria-label={`Release legal hold: ${hold.reason}`}
                    className="inline-flex min-h-9 items-center justify-center rounded-md border border-border bg-background px-3 text-sm font-medium transition-colors hover:bg-secondary disabled:opacity-50"
                    disabled={busyAction !== null}
                    type="button"
                    onClick={() => {
                      setConfirmingHoldId(hold.id);
                    }}
                  >
                    Release hold
                  </button>
                )}
              </div>
              {confirmingHoldId === hold.id && (
                <div className="grid gap-3 rounded-md border border-attention bg-attention/30 p-3">
                  <p id={`release-hold-${hold.id}`}>
                    Release the legal hold “{hold.reason}”? Protected versions may then become
                    eligible for deletion.
                  </p>
                  <div className="flex flex-wrap gap-2">
                    <button
                      ref={keepHoldRef}
                      className="inline-flex min-h-9 items-center justify-center rounded-md border border-border bg-background px-3 text-sm font-medium hover:bg-secondary disabled:opacity-50"
                      disabled={busyAction !== null}
                      type="button"
                      onClick={() => {
                        restoreHoldTrigger.current = hold.id;
                        setConfirmingHoldId(null);
                      }}
                    >
                      Keep hold
                    </button>
                    <button
                      aria-describedby={`release-hold-${hold.id}`}
                      className="inline-flex min-h-9 items-center justify-center rounded-md bg-destructive px-3 text-sm font-medium text-destructive-foreground disabled:opacity-50"
                      disabled={busyAction !== null}
                      type="button"
                      onClick={() => {
                        void releaseHold(hold.id);
                      }}
                    >
                      {busyAction === `release:${hold.id}` ? "Releasing…" : "Confirm release"}
                    </button>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      <form
        aria-busy={busyAction === "delete"}
        className="grid gap-3 rounded-md border border-destructive/30 bg-destructive/5 p-4"
        onSubmit={(event) => {
          void mutate(event, "delete");
        }}
      >
        <div className="grid gap-2">
          <label className="text-sm font-medium" htmlFor="delete-reason">Deletion reason</label>
          <input
            id="delete-reason"
            name="reason"
            required
            value={deleteReason}
            onChange={(event) => {
              setDeleteReason(event.currentTarget.value);
            }}
            className="min-h-10 rounded-md border border-input bg-background px-3 text-sm"
          />
        </div>
        <p className="text-sm text-muted-foreground" id="delete-scope">
          This requests irreversible deletion of every stored object version.
        </p>
        {deletionBlockedByHold && (
          <p className="rounded-md border border-attention bg-attention/30 p-3 text-sm text-attention-foreground" id="delete-hold-block">
            Release and verify every active legal hold before requesting deletion.
          </p>
        )}
        <label className="grid min-h-11 cursor-pointer grid-cols-[auto_minmax(0,1fr)] items-start gap-3 text-sm">
          <input
            checked={deleteAcknowledged}
            disabled={busyAction !== null || deletionBlockedByHold}
            required
            type="checkbox"
            onChange={(event) => {
              setDeleteAcknowledged(event.currentTarget.checked);
            }}
          />
          <span>I understand this requests deletion of every stored version.</span>
        </label>
        <button
          aria-describedby={`delete-scope${deletionBlockedByHold ? " delete-hold-block" : ""}`}
          className="inline-flex min-h-10 w-fit items-center justify-center rounded-md bg-destructive px-4 text-sm font-semibold text-destructive-foreground disabled:cursor-not-allowed disabled:opacity-50"
          disabled={
            busyAction !== null ||
            deletionBlockedByHold ||
            deleteReason.trim() === "" ||
            !deleteAcknowledged
          }
          type="submit"
        >
          {busyAction === "delete" ? "Deleting…" : "Verify and delete every version"}
        </button>
      </form>

      {feedback && (
        <div
          ref={feedbackRef}
          tabIndex={-1}
          className={feedback.kind === "error" ? "rounded-md border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive" : "rounded-md border border-verified/40 bg-verified/5 p-3 text-sm text-foreground"}
          id="archive-action-feedback"
          role={feedback.kind === "error" ? "alert" : "status"}
        >
          {feedback.text}
        </div>
      )}
    </section>
  );
}
