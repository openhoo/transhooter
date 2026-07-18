"use client";

import { useRouter } from "next/navigation";
import type { FormEvent } from "react";
import { useId, useState } from "react";
import { api } from "@/lib/browser-api";

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
    <div className="archiveDownload">
      <button
        aria-label={`Download ${label}`}
        aria-describedby={error ? errorId : undefined}
        aria-busy={busy}
        className="button secondary"
        disabled={busy}
        type="button"
        onClick={() => {
          void download();
        }}
      >
        {busy ? "Preparing…" : "Download"}
      </button>
      {error && (
        <p className="error" id={errorId} role="alert">
          {error}
        </p>
      )}
    </div>
  );
}

type ActionFeedback = { kind: "status" | "error"; text: string } | null;

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
  const [deleteReason, setDeleteReason] = useState("");
  const deletionBlockedByHold = activeHolds.length > 0;

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
      className="stack panel archiveActions"
    >
      <div>
        <p className="eyebrow">Restricted archive controls</p>
        <h2>Hold or delete</h2>
        <p className="muted">
          Both actions require a fresh, consultation-bound administrator sign-in.
        </p>
      </div>

      <button
        className="button secondary"
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
        className="stack"
        onSubmit={(event) => {
          void mutate(event, "hold");
        }}
      >
        <div className="field">
          <label htmlFor="hold-reason">Legal hold reason</label>
          <input id="hold-reason" name="reason" required />
        </div>
        <button className="button secondary" disabled={busyAction !== null} type="submit">
          {busyAction === "hold" ? "Applying…" : "Apply legal hold"}
        </button>
      </form>

      {activeHolds.length > 0 && (
        <div className="stack">
          <h3>Release an active hold</h3>
          {activeHolds.map((hold) => (
            <div className="row archiveHoldRow" key={hold.id}>
              <span className="archiveHoldReason">{hold.reason}</span>
              <button
                aria-label={`Release legal hold: ${hold.reason}`}
                className="button secondary"
                disabled={busyAction !== null}
                type="button"
                onClick={() => {
                  void releaseHold(hold.id);
                }}
              >
                {busyAction === `release:${hold.id}` ? "Releasing…" : "Release hold"}
              </button>
            </div>
          ))}
        </div>
      )}

      <form
        aria-busy={busyAction === "delete"}
        className="stack"
        onSubmit={(event) => {
          void mutate(event, "delete");
        }}
      >
        <div className="field">
          <label htmlFor="delete-reason">Deletion reason</label>
          <input
            id="delete-reason"
            name="reason"
            required
            value={deleteReason}
            onChange={(event) => {
              setDeleteReason(event.currentTarget.value);
            }}
          />
        </div>
        <p className="muted" id="delete-scope">
          This requests irreversible deletion of every stored object version.
        </p>
        {deletionBlockedByHold && (
          <p className="notice warning" id="delete-hold-block">
            Release and verify every active legal hold before requesting deletion.
          </p>
        )}
        <label className="archiveDeleteConfirmation">
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
          className="button danger"
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
          className={feedback.kind === "error" ? "error" : "notice"}
          id="archive-action-feedback"
          role={feedback.kind === "error" ? "alert" : "status"}
        >
          {feedback.text}
        </div>
      )}
    </section>
  );
}
