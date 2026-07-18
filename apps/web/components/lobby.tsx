"use client";

import { Room } from "livekit-client";
import type { FormEvent, ReactNode, RefObject } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "@/lib/browser-api";
import { describeLobbyPhase, type LobbyPhase } from "@/lib/lobby-phase";
import styles from "./lobby.module.css";

const CONSENT_COPY =
  "By joining, I agree that this consultation, including my audio/video, live captions, translations, synthesized interpretation, and data sent to and returned by the listed speech, translation, and voice providers, will be recorded and stored until an administrator deletes it. Media is encrypted in transit but is not end-to-end encrypted; the self-hosted translation and recording services receive decrypted media to translate and record it.";

type Direction = {
  sourceLabel: string;
  destinationLabel: string;
  speech: string;
  translation: string;
  voice: string;
  region: string;
};

type LanguageOption = {
  code: string;
  label: string;
};

type LobbyState = {
  phase: LobbyPhase;
  snapshotHash: string | null;
  profileName: string;
  profileRevision: number;
  directions: Direction[];
  languages: LanguageOption[];
  consented: boolean;
  redirectTo?: string;
};

type LobbyProps = {
  consultationId: string;
  initial: LobbyState;
};

type PreferencesFormProps = {
  busy: boolean;
  devices: MediaDeviceInfo[];
  languages: LanguageOption[];
  onPreview: () => Promise<void>;
  onSubmit: (event: FormEvent<HTMLFormElement>) => Promise<void>;
  videoRef: RefObject<HTMLVideoElement | null>;
};

type ProviderConsentFormProps = {
  busy: boolean;
  data: LobbyState;
  onSubmit: (event: FormEvent<HTMLFormElement>) => Promise<void>;
};

function useLobbyPolling(
  consultationId: string,
  shouldPoll: boolean,
  pollGeneration: RefObject<number>,
  setData: (data: LobbyState) => void,
) {
  const [pollError, setPollError] = useState("");

  useEffect(() => {
    if (!shouldPoll) {
      return;
    }

    let active = true;
    let timer: number | undefined;

    async function poll() {
      const requestGeneration = pollGeneration.current;
      try {
        const next = await api<LobbyState>(`/api/consultations/${consultationId}`);
        if (!active || requestGeneration !== pollGeneration.current) {
          return;
        }

        setPollError("");
        if (next.redirectTo) {
          window.location.replace(next.redirectTo);
        } else {
          setData(next);
        }
      } catch {
        if (active && requestGeneration === pollGeneration.current) {
          setPollError("Live status is temporarily unavailable. Retrying automatically.");
        }
      } finally {
        if (active) {
          timer = window.setTimeout(() => {
            void poll();
          }, 2000);
        }
      }
    }

    void poll();

    return () => {
      active = false;
      if (timer !== undefined) {
        window.clearTimeout(timer);
      }
    };
  }, [consultationId, pollGeneration, setData, shouldPoll]);

  return pollError;
}

function useLocalDevicePreview(setError: (message: string) => void) {
  const videoRef = useRef<HTMLVideoElement>(null);
  const stream = useRef<MediaStream | null>(null);
  const audioUnlockRoom = useRef<Room | null>(null);
  const [devices, setDevices] = useState<MediaDeviceInfo[]>([]);

  const stopPreview = useCallback(() => {
    stream.current?.getTracks().forEach((track) => {
      track.stop();
    });
  }, []);

  useEffect(() => {
    return () => {
      stopPreview();
      void audioUnlockRoom.current?.disconnect();
    };
  }, [stopPreview]);

  const preview = useCallback(async () => {
    setError("");
    stopPreview();

    try {
      const media = await navigator.mediaDevices.getUserMedia({
        audio: true,
        video: true,
      });
      stream.current = media;
      media.getTracks().forEach((track) => {
        track.addEventListener(
          "ended",
          () => {
            setError("Camera or microphone access ended. Preview again before joining.");
          },
          { once: true },
        );
      });
      media.addEventListener(
        "inactive",
        () => {
          setError("Camera and microphone preview stopped. Preview again before joining.");
        },
        { once: true },
      );
      if (videoRef.current) {
        videoRef.current.srcObject = media;
      }
      setDevices(await navigator.mediaDevices.enumerateDevices());
    } catch {
      setError(
        "Camera or microphone access is blocked. Allow access in your browser, then try again.",
      );
    }
  }, [setError, stopPreview]);

  const unlockAudio = useCallback(async () => {
    audioUnlockRoom.current ??= new Room();
    await audioUnlockRoom.current.startAudio();
  }, []);

  return {
    devices,
    preview,
    stopPreview,
    unlockAudio,
    videoRef,
  };
}

function LobbySteps({ stage }: { stage: 1 | 2 | null }) {
  return (
    <nav className={styles.steps} aria-label="Consultation preparation">
      <ol>
        <li className={`${styles.step ?? ""} ${stage === 1 ? (styles.current ?? "") : ""}`}>
          <span aria-current={stage === 1 ? "step" : undefined}>1 · Devices and language</span>
        </li>
        <li className={`${styles.step ?? ""} ${stage === 2 ? (styles.current ?? "") : ""}`}>
          <span aria-current={stage === 2 ? "step" : undefined}>2 · Providers and consent</span>
        </li>
      </ol>
    </nav>
  );
}

function PreferencesForm({
  busy,
  devices,
  languages,
  onPreview,
  onSubmit,
  videoRef,
}: PreferencesFormProps) {
  const microphones = devices.filter((device) => device.kind === "audioinput");
  const cameras = devices.filter((device) => device.kind === "videoinput");

  return (
    <form
      aria-busy={busy}
      aria-describedby="lobby-feedback"
      className={`${styles.preferences ?? ""} grid`}
      onSubmit={(event) => {
        void onSubmit(event);
      }}
    >
      <fieldset className={`${styles.preferenceGroup ?? ""} stack`} disabled={busy}>
        <legend className="srOnly">Media preview</legend>
        <video
          className={styles.preview}
          ref={videoRef}
          autoPlay
          muted
          playsInline
          aria-label="Local camera preview"
        />
        <button
          className="button secondary"
          type="button"
          onClick={() => {
            void onPreview();
          }}
        >
          Preview camera and microphone
        </button>
      </fieldset>
      <fieldset className={`${styles.preferenceGroup ?? ""} stack panel`} disabled={busy}>
        <legend className="srOnly">Consultation preferences</legend>
        <div className="field">
          <label htmlFor="displayName">Display name</label>
          <input id="displayName" name="displayName" autoComplete="name" required />
        </div>
        <div className="field">
          <label htmlFor="language">Your spoken language</label>
          <select id="language" name="language" required>
            <option value="">Choose language</option>
            {languages.map((language) => (
              <option value={language.code} key={language.code}>
                {language.label}
              </option>
            ))}
          </select>
        </div>
        <div className="field">
          <label htmlFor="microphone">Microphone</label>
          <select aria-describedby="device-preference-hint" id="microphone" name="microphone">
            <option value="">System default microphone</option>
            {microphones.map((device) => (
              <option value={device.deviceId} key={device.deviceId}>
                {device.label || "Available microphone"}
              </option>
            ))}
          </select>
        </div>
        <div className="field">
          <label htmlFor="camera">Camera</label>
          <select aria-describedby="device-preference-hint" id="camera" name="camera">
            <option value="">System default camera</option>
            {cameras.map((device) => (
              <option value={device.deviceId} key={device.deviceId}>
                {device.label || "Available camera"}
              </option>
            ))}
          </select>
        </div>
        <p className="muted" id="device-preference-hint">
          Preview to choose a specific device. Otherwise, the browser uses your system defaults.
        </p>
        <button className="button" type="submit">
          {busy ? "Saving…" : "Save and continue"}
        </button>
      </fieldset>
    </form>
  );
}

function WaitingPanel() {
  return (
    <section className="panel">
      <span className="status warning">Waiting for the other participant’s preferences</span>
      <p>You can leave this page open. The provider details will appear here automatically.</p>
    </section>
  );
}

function ConsentWaitingPanel() {
  return (
    <section className="panel">
      <span className="status warning">Waiting for the other participant’s consent</span>
      <p>
        Your consent is recorded. Recording preparation begins only after both participants agree to
        the same frozen provider profile.
      </p>
    </section>
  );
}

function TerminalPanel() {
  return (
    <section className="empty flat">
      <h2>This consultation is closed.</h2>
      <p className="muted">
        It can no longer be joined. Contact the employee if you need another invitation.
      </p>
    </section>
  );
}

function ProviderConsentForm({ busy, data, onSubmit }: ProviderConsentFormProps) {
  return (
    <form
      aria-busy={busy}
      aria-describedby="lobby-feedback"
      className="stack"
      onSubmit={(event) => {
        void onSubmit(event);
      }}
    >
      <section className="panel">
        <div className="row">
          <div>
            <p className="eyebrow">Frozen provider profile</p>
            <h2>{data.profileName}</h2>
          </div>
          <span className="meta">Revision {data.profileRevision}</span>
        </div>
        <section aria-label="Frozen provider direction details">
          <table className={styles.providerTable}>
            <caption className="srOnly">
              Speech, translation, voice, and processing region for each language direction
            </caption>
            <thead>
              <tr>
                <th>Direction</th>
                <th>Speech</th>
                <th>Translation</th>
                <th>Voice</th>
                <th>Region</th>
              </tr>
            </thead>
            <tbody>
              {data.directions.map((direction) => (
                <tr key={`${direction.sourceLabel}:${direction.destinationLabel}`}>
                  <td>
                    <span className={styles.mobileFieldLabel}>Direction</span>
                    {direction.sourceLabel} → {direction.destinationLabel}
                  </td>
                  <td>
                    <span className={styles.mobileFieldLabel}>Speech</span>
                    {direction.speech}
                  </td>
                  <td>
                    <span className={styles.mobileFieldLabel}>Translation</span>
                    {direction.translation}
                  </td>
                  <td>
                    <span className={styles.mobileFieldLabel}>Voice</span>
                    {direction.voice}
                  </td>
                  <td>
                    <span className={styles.mobileFieldLabel}>Region</span>
                    {direction.region}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </section>
      <section aria-label="Recording and provider consent, version 1" className={styles.consent}>
        <strong>Recording and provider consent · version 1</strong>
        <p>{CONSENT_COPY}</p>
      </section>
      <label className={styles.check}>
        <input
          key={`${data.snapshotHash ?? "missing"}:${data.consented ? "consented" : "pending"}`}
          type="checkbox"
          defaultChecked={data.consented}
          disabled={data.consented}
          required={!data.consented}
        />
        <span>I have read and agree to the recording and provider consent above.</span>
      </label>
      {data.phase === "ready" && (
        <div className="notice">
          Recording is being prepared. This page will enter the room when the capture barrier is
          ready.
        </div>
      )}
      <button className="button" disabled={busy || data.phase === "ready"} type="submit">
        {busy ? "Preparing consultation…" : "Agree and join"}
      </button>
    </form>
  );
}

export function Lobby({ consultationId, initial }: LobbyProps) {
  const [data, setData] = useState(initial);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const pollGeneration = useRef(0);
  const [focusPhaseRequest, setFocusPhaseRequest] = useState(0);
  const phaseStatusRef = useRef<HTMLParagraphElement>(null);
  const phaseDescriptor = describeLobbyPhase(data.phase);
  const pollError = useLobbyPolling(consultationId, phaseDescriptor.polls, pollGeneration, setData);
  const { devices, preview, stopPreview, unlockAudio, videoRef } = useLocalDevicePreview(setError);

  useEffect(() => {
    if (focusPhaseRequest > 0) {
      phaseStatusRef.current?.focus();
    }
  }, [focusPhaseRequest]);

  async function savePreferences(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError("");
    pollGeneration.current += 1;

    const values = new FormData(event.currentTarget);
    const microphoneValue = values.get("microphone");
    const cameraValue = values.get("camera");
    const microphoneId = typeof microphoneValue === "string" ? microphoneValue : "";
    const cameraId = typeof cameraValue === "string" ? cameraValue : "";

    if (microphoneId) {
      window.sessionStorage.setItem("transhooter.microphone", microphoneId);
    }
    if (cameraId) {
      window.sessionStorage.setItem("transhooter.camera", cameraId);
    }

    try {
      const next = await api<LobbyState>(`/api/consultations/${consultationId}/preferences`, {
        method: "POST",
        body: JSON.stringify({
          displayName: values.get("displayName"),
          language: values.get("language"),
          microphoneId,
          cameraId,
        }),
      });
      stopPreview();
      if (describeLobbyPhase(next.phase).contentKind !== phaseDescriptor.contentKind) {
        setFocusPhaseRequest((current) => current + 1);
      }
      pollGeneration.current += 1;
      setData(next);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Preferences could not be saved");
    } finally {
      setBusy(false);
    }
  }

  async function consent(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!data.snapshotHash) {
      return;
    }

    setBusy(true);
    setError("");
    pollGeneration.current += 1;

    try {
      await unlockAudio();
      await api(`/api/consultations/${consultationId}/consent`, {
        method: "POST",
        body: JSON.stringify({
          snapshotHash: data.snapshotHash,
          consentVersion: 1,
          accepted: true,
        }),
      });
      const result = await api<{ status: "ready" | "provisioning"; redirectTo?: string }>(
        `/api/consultations/${consultationId}/join`,
        {
          method: "POST",
          body: JSON.stringify({ snapshotHash: data.snapshotHash }),
        },
      );

      if (result.redirectTo) {
        window.location.replace(result.redirectTo);
      } else {
        const next = await api<LobbyState>(`/api/consultations/${consultationId}`);
        if (next.redirectTo) {
          window.location.replace(next.redirectTo);
        } else {
          if (describeLobbyPhase(next.phase).contentKind !== phaseDescriptor.contentKind) {
            setFocusPhaseRequest((current) => current + 1);
          }
          pollGeneration.current += 1;
          setData(next);
        }
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Consent could not be recorded");
      setBusy(false);
    }
  }

  let phaseContent: ReactNode;
  switch (phaseDescriptor.contentKind) {
    case "preferences":
      phaseContent = (
        <PreferencesForm
          busy={busy}
          devices={devices}
          languages={data.languages}
          onPreview={preview}
          onSubmit={savePreferences}
          videoRef={videoRef}
        />
      );
      break;
    case "waiting":
      phaseContent = <WaitingPanel />;
      break;
    case "provider-consent":
      phaseContent = <ProviderConsentForm busy={busy} data={data} onSubmit={consent} />;
      break;
    case "consent-waiting":
      phaseContent = <ConsentWaitingPanel />;
      break;
    case "terminal":
      phaseContent = <TerminalPanel />;
      break;
    default: {
      const exhaustiveContent: never = phaseDescriptor.contentKind;
      throw new Error(`Unhandled lobby content: ${exhaustiveContent}`);
    }
  }

  return (
    <div className="stack">
      <div>
        <p className="eyebrow">Consultation lobby</p>
        <h1>Prepare before joining.</h1>
        <p className="lede">
          This lobby is disconnected from the meeting. Your camera and microphone remain local until
          recording is ready and the server grants publication.
        </p>
      </div>
      <LobbySteps stage={phaseDescriptor.stage} />
      <p
        className={styles.feedback}
        ref={phaseStatusRef}
        role="status"
        aria-live="polite"
        aria-atomic="true"
        tabIndex={-1}
      >
        {phaseDescriptor.announcement}
      </p>
      {phaseContent}
      <div id="lobby-feedback" className={styles.feedback}>
        {pollError && (
          <p className="notice" role="alert">
            {pollError}
          </p>
        )}
        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}
      </div>
    </div>
  );
}
