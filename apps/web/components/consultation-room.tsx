"use client";

import {
  CAPTION_TOPIC,
  type CaptionPacket,
  CaptionPacketSchema,
  STATUS_TOPIC,
  StatusPacketSchema,
} from "@transhooter/contracts";
import {
  ConnectionState,
  createLocalAudioTrack,
  createLocalVideoTrack,
  DataPacket_Kind,
  type LocalAudioTrack,
  type LocalVideoTrack,
  type RemoteParticipant,
  type RemoteTrack,
  type RemoteTrackPublication,
  Room,
  RoomEvent,
  Track,
  TrackEvent,
} from "livekit-client";
import type { MutableRefObject, RefObject } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "@/lib/browser-api";
import {
  type AudioMode,
  acceptsCaption,
  acceptsStatus,
  audioGains,
  releaseLocalTrack,
} from "@/lib/room-policy";
import {
  createWithDeviceFallback,
  persistDevicePreference,
  readDevicePreference,
} from "./interface-state";
import styles from "./room.module.css";

type InitialRoom = {
  consultationId: string;
  participantId: string;
  participantIdentity: string;
  otherParticipantId: string;
  otherIdentity: string;
  generation: number;
  liveKitUrl: string;
  displayName: string;
  otherDisplayName: string;
  role: "employee" | "customer";
  state: "ready" | "active";
};

type ConsultationRoomProps = {
  initial: InitialRoom;
};

type CallState = "ready" | "active" | "finalizing";

type MediaElementRefs = {
  localVideo: RefObject<HTMLVideoElement | null>;
  remoteVideo: RefObject<HTMLVideoElement | null>;
  originalAudio: RefObject<HTMLAudioElement | null>;
  interpretationAudio: RefObject<HTMLAudioElement | null>;
};

type RoomControllerState = {
  setArchiveFailed: (failed: boolean) => void;
  setCallState: (state: CallState) => void;
  setCaption: (caption: CaptionPacket) => void;
  setCaptureReady: (ready: boolean) => void;
  setConnected: (connected: boolean) => void;
  setConnecting: (connecting: boolean) => void;
  setError: (message: string) => void;
  setInterpretationReady: (ready: boolean) => void;
  setSameLanguage: (sameLanguage: boolean) => void;
  setShutdownAt: (shutdownAt: number) => void;
};

type CaptionHandlerContext = {
  captions: MutableRefObject<Map<string, CaptionPacket>>;
  initial: InitialRoom;
  participant: RemoteParticipant | undefined;
  setCaption: (caption: CaptionPacket) => void;
  visibleCaption: MutableRefObject<CaptionPacket | null>;
};

type StatusHandlerContext = {
  generation: MutableRefObject<number>;
  initial: InitialRoom;
  participant: RemoteParticipant | undefined;
  publishDevices: (room: Room) => Promise<void>;
  room: Room;
  state: RoomControllerState;
};

function decodeReliablePacket(payload: Uint8Array) {
  try {
    return JSON.parse(new TextDecoder().decode(payload)) as unknown;
  } catch {
    return undefined;
  }
}

function handleCaptionPacket(decoded: unknown, context: CaptionHandlerContext) {
  const parsed = CaptionPacketSchema.safeParse(decoded);
  if (!parsed.success) {
    return;
  }

  const accepted = acceptsCaption(
    parsed.data,
    context.captions.current.get(parsed.data.utteranceId),
    {
      consultationId: context.initial.consultationId,
      destinationParticipantId: context.initial.participantId,
      sourceParticipantId: context.initial.otherParticipantId,
    },
    context.participant?.isAgent === true,
  );
  if (!accepted) {
    return;
  }

  context.captions.current.set(parsed.data.utteranceId, parsed.data);
  const visible = context.visibleCaption.current;
  const shouldShow =
    !visible ||
    visible.utteranceId === parsed.data.utteranceId ||
    parsed.data.occurredAtMs >= visible.occurredAtMs;

  if (shouldShow) {
    context.visibleCaption.current = parsed.data;
    context.setCaption(parsed.data);
  }
}

function handleStatusPacket(decoded: unknown, context: StatusHandlerContext) {
  const parsed = StatusPacketSchema.safeParse(decoded);
  if (!parsed.success) {
    return;
  }

  const accepted = acceptsStatus(parsed.data, context.participant !== undefined, {
    consultationId: context.initial.consultationId,
    generation: context.generation.current,
  });
  if (!accepted) {
    return;
  }

  const status = parsed.data;
  context.state.setCallState(status.state);

  if (
    status.reasonCode === "CAPTURE_READY" &&
    status.subjectParticipantId === context.initial.participantId
  ) {
    void context.publishDevices(context.room).then(() => context.state.setCaptureReady(true));
    return;
  }

  if (
    status.reasonCode === "SAME_LANGUAGE_BYPASS" &&
    status.destinationParticipantId === context.initial.participantId
  ) {
    context.state.setSameLanguage(true);
    context.state.setInterpretationReady(false);
    return;
  }

  if (status.reasonCode === "ARCHIVE_FAILED" || status.reasonCode === "SHUTDOWN") {
    context.state.setInterpretationReady(false);
    if (status.reasonCode === "ARCHIVE_FAILED") {
      context.state.setArchiveFailed(true);
    }
    context.state.setShutdownAt(status.shutdownAtMs);
  }
}

function useAudioGainRouting(
  mode: AudioMode,
  interpretationReady: boolean,
  sameLanguage: boolean,
  originalAudio: RefObject<HTMLAudioElement | null>,
  interpretationAudio: RefObject<HTMLAudioElement | null>,
) {
  useEffect(() => {
    const original = originalAudio.current;
    const interpreted = interpretationAudio.current;
    if (!original || !interpreted) {
      return;
    }

    const gains = audioGains(mode, interpretationReady, sameLanguage);
    original.volume = gains.original;
    interpreted.volume = gains.interpretation;
  }, [interpretationAudio, interpretationReady, mode, originalAudio, sameLanguage]);
}

function useShutdownCountdown(shutdownAt: number | null) {
  const [seconds, setSeconds] = useState<number | null>(null);

  useEffect(() => {
    if (shutdownAt === null) {
      return;
    }
    const deadline = shutdownAt;

    function tick() {
      setSeconds(Math.max(0, Math.ceil((deadline - Date.now()) / 1000)));
    }

    tick();
    const timer = window.setInterval(tick, 200);
    return () => {
      window.clearInterval(timer);
    };
  }, [shutdownAt]);

  return seconds;
}

function useFinalizationRedirect(
  callState: CallState,
  consultationId: string,
  seconds: number | null,
) {
  useEffect(() => {
    if (callState !== "finalizing" || seconds !== 0) {
      return;
    }

    async function poll() {
      try {
        const result = await api<{ redirectTo?: string }>(`/api/consultations/${consultationId}`);
        if (result.redirectTo) {
          window.location.replace(result.redirectTo);
        }
      } catch {
        // A transient poll failure must not interrupt the server-owned shutdown.
      }
    }

    void poll();
    const timer = window.setInterval(() => {
      void poll();
    }, 1000);
    return () => {
      window.clearInterval(timer);
    };
  }, [callState, consultationId, seconds]);
}

function useRoomMediaController(
  initial: InitialRoom,
  media: MediaElementRefs,
  generation: MutableRefObject<number>,
  state: RoomControllerState,
) {
  const roomRef = useRef<Room | null>(null);
  const published = useRef(false);
  const localTracks = useRef<Array<LocalAudioTrack | LocalVideoTrack>>([]);
  const cleanupPromise = useRef<Promise<void>>(Promise.resolve());
  const publicationEpoch = useRef(0);
  const publicationPromise = useRef<Promise<void>>(Promise.resolve());
  const connectingRef = useRef(false);
  const leaving = useRef(false);
  const reconnectTimer = useRef<number | null>(null);
  const reconnect = useRef<(() => Promise<void>) | null>(null);
  const captions = useRef(new Map<string, CaptionPacket>());
  const visibleCaption = useRef<CaptionPacket | null>(null);

  const shouldSubscribe = useCallback(
    (publication: RemoteTrackPublication, participant: RemoteParticipant) => {
      const isOtherHumanTrack =
        participant.identity === initial.otherIdentity &&
        (publication.source === Track.Source.Camera ||
          publication.source === Track.Source.Microphone);
      const isInterpretationTrack =
        participant.isAgent && publication.trackName === `interpretation:${initial.participantId}`;
      return isOtherHumanTrack || isInterpretationTrack;
    },
    [initial.otherIdentity, initial.participantId],
  );

  const attachTrack = useCallback(
    (track: RemoteTrack, publication: RemoteTrackPublication, participant: RemoteParticipant) => {
      if (
        participant.identity === initial.otherIdentity &&
        publication.source === Track.Source.Camera &&
        media.remoteVideo.current
      ) {
        track.attach(media.remoteVideo.current);
      }
      if (
        participant.identity === initial.otherIdentity &&
        publication.source === Track.Source.Microphone &&
        media.originalAudio.current
      ) {
        track.attach(media.originalAudio.current);
      }
      if (
        publication.trackName === `interpretation:${initial.participantId}` &&
        media.interpretationAudio.current
      ) {
        state.setInterpretationReady(false);
        track.on(TrackEvent.Ended, () => {
          state.setInterpretationReady(false);
        });
        track.attach(media.interpretationAudio.current);
        track.on(TrackEvent.AudioPlaybackStarted, () => {
          state.setInterpretationReady(true);
        });
      }
    },
    [initial.otherIdentity, initial.participantId, media, state],
  );

  const stopPublishedDevices = useCallback((room: Room) => {
    publicationEpoch.current += 1;
    published.current = false;
    const tracks = localTracks.current.splice(0);
    const cleanup = Promise.allSettled(
      tracks.map((track) =>
        releaseLocalTrack(track, () =>
          room.localParticipant.unpublishTrack(track.mediaStreamTrack),
        ),
      ),
    ).then(() => undefined);
    cleanupPromise.current = Promise.allSettled([cleanupPromise.current, cleanup]).then(
      () => undefined,
    );
    return cleanupPromise.current;
  }, []);

  const publishDevices = useCallback(
    (room: Room) => {
      const publish = async () => {
        await cleanupPromise.current;
        if (published.current || roomRef.current !== room || leaving.current) {
          return;
        }

        const attempt = publicationEpoch.current;
        const isCurrentAttempt = () =>
          published.current &&
          publicationEpoch.current === attempt &&
          roomRef.current === room &&
          !leaving.current;
        published.current = true;
        try {
          const microphoneId = readDevicePreference(
            () => window.sessionStorage,
            "transhooter.microphone",
          );
          const cameraId = readDevicePreference(() => window.sessionStorage, "transhooter.camera");
          const audioTrack = await createWithDeviceFallback(
            microphoneId,
            (deviceId) => createLocalAudioTrack(deviceId ? { deviceId } : {}),
            () =>
              persistDevicePreference(() => window.sessionStorage, "transhooter.microphone", ""),
          );
          localTracks.current.push(audioTrack);
          if (!isCurrentAttempt()) {
            await stopPublishedDevices(room);
            return;
          }

          const videoTrack = await createWithDeviceFallback(
            cameraId,
            (deviceId) => createLocalVideoTrack(deviceId ? { deviceId } : {}),
            () => persistDevicePreference(() => window.sessionStorage, "transhooter.camera", ""),
          );
          localTracks.current.push(videoTrack);
          if (!isCurrentAttempt()) {
            await stopPublishedDevices(room);
            return;
          }

          if (media.localVideo.current) {
            videoTrack.attach(media.localVideo.current);
          }
          await room.localParticipant.publishTrack(audioTrack.mediaStreamTrack, {
            source: Track.Source.Microphone,
          });
          if (!isCurrentAttempt()) {
            await stopPublishedDevices(room);
            return;
          }
          await room.localParticipant.publishTrack(videoTrack.mediaStreamTrack, {
            source: Track.Source.Camera,
          });
        } catch (cause) {
          await stopPublishedDevices(room);
          const errorName =
            typeof cause === "object" && cause !== null && "name" in cause ? cause.name : undefined;
          state.setError(
            errorName === "NotAllowedError" ||
              errorName === "PermissionDeniedError" ||
              errorName === "SecurityError"
              ? "Camera or microphone access was denied. Allow access in your browser and try reconnecting."
              : "Camera or microphone could not be published. Check that a device is available and try reconnecting.",
          );
        }
      };

      const task = publicationPromise.current.then(publish, publish);
      publicationPromise.current = task;
      return task;
    },
    [media.localVideo, state, stopPublishedDevices],
  );

  const reattachCurrentMedia = useCallback(() => {
    const room = roomRef.current;
    if (!room) {
      return;
    }

    const localVideoTrack = localTracks.current.find((track) => track.kind === Track.Kind.Video);
    if (localVideoTrack && media.localVideo.current) {
      localVideoTrack.attach(media.localVideo.current);
    }
    room.remoteParticipants.forEach((participant) => {
      participant.trackPublications.forEach((publication) => {
        if (publication.track && shouldSubscribe(publication, participant)) {
          attachTrack(publication.track, publication, participant);
        }
      });
    });
  }, [attachTrack, media.localVideo, shouldSubscribe]);

  const connect = useCallback(async () => {
    if (connectingRef.current || roomRef.current?.state === ConnectionState.Connected) {
      return;
    }

    connectingRef.current = true;
    state.setConnecting(true);
    state.setError("");

    try {
      const tokenResult = await api<{ token: string }>(
        `/api/consultations/${initial.consultationId}/livekit-token`,
        { method: "POST", body: "{}" },
      );
      const room = new Room({ adaptiveStream: true, dynacast: true });
      roomRef.current = room;

      room.on(RoomEvent.TrackPublished, (publication, participant) => {
        if (shouldSubscribe(publication, participant)) {
          publication.setSubscribed(true);
        }
      });
      room.on(RoomEvent.TrackSubscribed, attachTrack);
      room.on(RoomEvent.TrackUnsubscribed, (_track, publication) => {
        if (publication.trackName === `interpretation:${initial.participantId}`) {
          state.setInterpretationReady(false);
        }
      });
      room.on(RoomEvent.TrackUnpublished, (publication) => {
        if (publication.trackName === `interpretation:${initial.participantId}`) {
          state.setInterpretationReady(false);
        }
      });
      room.on(RoomEvent.DataReceived, (payload, participant, kind, topic) => {
        if (roomRef.current !== room) {
          return;
        }

        if (kind !== DataPacket_Kind.RELIABLE) {
          return;
        }

        const decoded = decodeReliablePacket(payload);
        if (decoded === undefined) {
          return;
        }

        if (topic === CAPTION_TOPIC) {
          handleCaptionPacket(decoded, {
            captions,
            initial,
            participant,
            setCaption: state.setCaption,
            visibleCaption,
          });
        }
        if (topic === STATUS_TOPIC) {
          handleStatusPacket(decoded, {
            generation,
            initial,
            participant,
            publishDevices,
            room,
            state,
          });
        }
      });
      room.on(RoomEvent.Disconnected, () => {
        published.current = false;
        if (roomRef.current === room) {
          roomRef.current = null;
        }
        void (async () => {
          await stopPublishedDevices(room);
          state.setCaptureReady(false);
          state.setConnected(false);
          state.setInterpretationReady(false);
          if (!leaving.current) {
            if (reconnectTimer.current !== null) {
              window.clearTimeout(reconnectTimer.current);
            }
            reconnectTimer.current = window.setTimeout(() => {
              reconnectTimer.current = null;
              void reconnect.current?.();
            }, 1000);
          }
        })();
      });

      await room.connect(initial.liveKitUrl, tokenResult.token, {
        autoSubscribe: false,
      });
      if (room.localParticipant.identity !== initial.participantIdentity) {
        throw new Error("The room identity does not match this consultation");
      }
      await room.startAudio();
      room.remoteParticipants.forEach((participant) => {
        participant.trackPublications.forEach((publication) => {
          if (shouldSubscribe(publication, participant)) {
            publication.setSubscribed(true);
          }
        });
      });
      state.setConnected(true);
    } catch (cause) {
      state.setError(
        cause instanceof Error ? cause.message : "The secure room could not be reached",
      );
      if (roomRef.current) {
        await stopPublishedDevices(roomRef.current);
      }
      state.setCaptureReady(false);
      roomRef.current?.removeAllListeners();
      void roomRef.current?.disconnect();
      roomRef.current = null;
    } finally {
      connectingRef.current = false;
      state.setConnecting(false);
    }
  }, [
    attachTrack,
    generation,
    initial,
    publishDevices,
    shouldSubscribe,
    state,
    stopPublishedDevices,
  ]);

  reconnect.current = connect;

  useEffect(() => {
    return () => {
      leaving.current = true;
      if (reconnectTimer.current !== null) {
        window.clearTimeout(reconnectTimer.current);
      }
      if (roomRef.current) {
        void stopPublishedDevices(roomRef.current);
      }
      void roomRef.current?.disconnect();
    };
  }, [stopPublishedDevices]);

  const leaveLocally = useCallback(async () => {
    leaving.current = true;
    published.current = false;
    if (reconnectTimer.current !== null) {
      window.clearTimeout(reconnectTimer.current);
      reconnectTimer.current = null;
    }
    const room = roomRef.current;
    if (room) {
      await stopPublishedDevices(room);
      try {
        await room.disconnect();
      } catch {
        // Local media is already stopped; a transport shutdown failure cannot keep the UI joined.
      }
    }
    state.setCaptureReady(false);
  }, [state, stopPublishedDevices]);

  return { connect, leaveLocally, reattachCurrentMedia };
}

function JoinPanel({
  connecting,
  error,
  onConnect,
}: {
  connecting: boolean;
  error: string;
  onConnect: () => Promise<void>;
}) {
  return (
    <section className={styles.join} aria-labelledby="room-join-title">
      <p className="eyebrow">Recording is prepared</p>
      <h1 id="room-join-title">Enter the consultation.</h1>
      <p>
        Camera and microphone publication stays blocked until the server confirms your isolated
        recording is active.
      </p>
      {error && (
        <p className={`error ${styles.error}`} role="alert">
          {error}
        </p>
      )}
      <button
        type="button"
        className="button"
        aria-busy={connecting}
        disabled={connecting}
        onClick={() => {
          void onConnect();
        }}
      >
        {connecting ? "Connecting…" : "Enter room"}
      </button>
    </section>
  );
}

function LeftPanel({ consultationId }: { consultationId: string }) {
  return (
    <section className={styles.join} aria-labelledby="room-left-title">
      <p className="eyebrow">Left locally</p>
      <h1 id="room-left-title">You have left the consultation.</h1>
      <p>Your camera and microphone are off. The employee’s consultation continues without you.</p>
      <a className="button secondary" href={`/consultations/${consultationId}/room`}>
        Rejoin safely
      </a>
    </section>
  );
}

function RecordingStatus({
  archiveFailed,
  callState,
  captureReady,
}: {
  archiveFailed: boolean;
  callState: CallState;
  captureReady: boolean;
}) {
  let recordingMessage = "Recording and secure storage preparing";
  if (archiveFailed) {
    recordingMessage = "Recording preservation failed — consultation ending";
  } else if (callState === "finalizing") {
    recordingMessage = "Recording and secure storage finalizing";
  } else if (captureReady) {
    recordingMessage = "Recording and secure storage active";
  }

  return (
    <header className={styles.chrome}>
      <output
        aria-label="Recording and storage status"
        className={`${styles.recording ?? ""} ${
          archiveFailed ? (styles.recordingWarning ?? "") : ""
        }`}
        aria-atomic="true"
        aria-live="polite"
      >
        {recordingMessage}
      </output>
      <span className="meta">{captureReady ? "Media enabled" : "Waiting for capture"}</span>
    </header>
  );
}

function VideoGrid({
  displayName,
  localVideo,
  otherDisplayName,
  remoteVideo,
}: {
  displayName: string;
  localVideo: RefObject<HTMLVideoElement | null>;
  otherDisplayName: string;
  remoteVideo: RefObject<HTMLVideoElement | null>;
}) {
  return (
    <section className={styles.videoRegion} aria-labelledby="participant-video-title">
      <h2 className="srOnly" id="participant-video-title">
        Participant video
      </h2>
      <div className={styles.videos}>
        <div className={styles.pane}>
          <video
            ref={localVideo}
            aria-label={`${displayName} self-view`}
            autoPlay
            muted
            playsInline
          />
          <bdi className={styles.person} dir="auto">
            {displayName} · you
          </bdi>
        </div>
        <div className={styles.pane}>
          {/* biome-ignore lint/a11y/useMediaCaption: This is a live remote WebRTC stream with no prerecorded caption track; translated captions render in the room ribbon. */}
          <video
            ref={remoteVideo}
            aria-label={`${otherDisplayName} live video`}
            autoPlay
            playsInline
          />
          <bdi className={styles.person} dir="auto">
            {otherDisplayName}
          </bdi>
        </div>
      </div>
    </section>
  );
}

function TranslationRibbon({
  caption,
  otherDisplayName,
}: {
  caption: CaptionPacket | null;
  otherDisplayName: string;
}) {
  return (
    <section className={styles.ribbon} aria-labelledby="translation-title">
      <h2 className="srOnly" id="translation-title">
        Live translation
      </h2>
      <div className={styles.speaker}>
        {caption ? (
          <>
            <strong>
              <bdi dir="auto">{otherDisplayName}</bdi>
            </strong>
            <br />
            {caption.sourceLanguage}
          </>
        ) : (
          "Translation desk"
        )}
      </div>
      <section
        className={styles.caption}
        aria-label="Current translated and source caption. Scroll for longer captions."
        // biome-ignore lint/a11y/noNoninteractiveTabindex: The fixed-height caption viewport must be keyboard-scrollable.
        tabIndex={0}
      >
        <p className={styles.translation} dir="auto">
          {caption?.translatedText ?? "Listening for the other speaker…"}
        </p>
        <p className={styles.source} dir="auto">
          {caption?.sourceText ?? "The source line remains stable while speech is translated."}
        </p>
      </section>
      <p className="srOnly" aria-atomic="true" aria-live="polite">
        {caption?.finality === "final"
          ? `Final translation from ${otherDisplayName}, ${caption.sourceLanguage} to ${caption.targetLanguage}: ${caption.translatedText}`
          : ""}
      </p>
    </section>
  );
}

function RecoveryStatus({
  fallback,
  sameLanguage,
  seconds,
}: {
  fallback: boolean;
  sameLanguage: boolean;
  seconds: number | null;
}) {
  return (
    <>
      {fallback && (
        <output className={styles.recovery} aria-live="polite">
          Interpretation reconnecting — original audio remains available.
        </output>
      )}
      {sameLanguage && (
        <output className={styles.recovery} aria-live="polite">
          Both participants use the same language. Original audio is playing.
        </output>
      )}
      {seconds !== null && (
        <div className={styles.recovery}>
          <output className="srOnly" aria-live="polite">
            Consultation ending. Media controls are disabled.
          </output>
          <span role="timer">
            Consultation ending in {seconds} second{seconds === 1 ? "" : "s"}. Media controls are
            disabled.
          </span>
        </div>
      )}
    </>
  );
}

function RoomControls({
  callState,
  mode,
  onEnd,
  onLeave,
  onModeChange,
  role,
  seconds,
}: {
  callState: CallState;
  mode: AudioMode;
  onEnd: () => Promise<void>;
  onLeave: () => Promise<void>;
  onModeChange: (mode: AudioMode) => void;
  role: "employee" | "customer";
  seconds: number | null;
}) {
  const choices: readonly AudioMode[] = ["interpreted", "overlay", "original"];

  return (
    <section className={styles.controls} aria-labelledby="listening-controls-title">
      <h2 className="srOnly" id="listening-controls-title">
        Listening controls
      </h2>
      <fieldset className={styles.audioModeGroup}>
        <legend className={styles.controlLabel}>Audio mode</legend>
        <div className={styles.modes}>
          {choices.map((choice) => (
            <button
              type="button"
              className={`${styles.mode ?? ""} ${mode === choice ? (styles.selected ?? "") : ""}`}
              disabled={seconds !== null}
              aria-pressed={mode === choice}
              key={choice}
              onClick={() => {
                onModeChange(choice);
              }}
            >
              {choice === "interpreted"
                ? "Interpreted"
                : choice === "overlay"
                  ? "Overlay"
                  : "Original"}
            </button>
          ))}
        </div>
      </fieldset>
      {role === "employee" && (
        <button
          type="button"
          className="button danger"
          disabled={callState !== "active" || seconds !== null}
          onClick={() => {
            void onEnd();
          }}
        >
          End consultation
        </button>
      )}
      {role === "customer" && (
        <button
          type="button"
          className="button secondary"
          disabled={seconds !== null}
          onClick={() => {
            void onLeave();
          }}
        >
          Leave locally
        </button>
      )}
    </section>
  );
}

export function ConsultationRoom({ initial }: ConsultationRoomProps) {
  const localVideo = useRef<HTMLVideoElement>(null);
  const remoteVideo = useRef<HTMLVideoElement>(null);
  const originalAudio = useRef<HTMLAudioElement>(null);
  const interpretationAudio = useRef<HTMLAudioElement>(null);
  const generation = useRef(initial.generation);
  const [connected, setConnected] = useState(false);
  const [connecting, setConnecting] = useState(false);
  const [captureReady, setCaptureReady] = useState(false);
  const [callState, setCallState] = useState<CallState>(initial.state);
  const [mode, setMode] = useState<AudioMode>("interpreted");
  const [interpretationReady, setInterpretationReady] = useState(false);
  const [sameLanguage, setSameLanguage] = useState(false);
  const [archiveFailed, setArchiveFailed] = useState(false);
  const [caption, setCaption] = useState<CaptionPacket | null>(null);
  const [shutdownAt, setShutdownAt] = useState<number | null>(null);
  const [error, setError] = useState("");
  const [leftLocally, setLeftLocally] = useState(false);
  const media = useRef({
    localVideo,
    remoteVideo,
    originalAudio,
    interpretationAudio,
  }).current;
  const controllerState = useRef({
    setArchiveFailed,
    setCallState,
    setCaption,
    setCaptureReady,
    setConnected,
    setConnecting,
    setError,
    setInterpretationReady,
    setSameLanguage,
    setShutdownAt,
  }).current;

  useAudioGainRouting(mode, interpretationReady, sameLanguage, originalAudio, interpretationAudio);
  const seconds = useShutdownCountdown(shutdownAt);
  useFinalizationRedirect(callState, initial.consultationId, seconds);
  const { connect, leaveLocally, reattachCurrentMedia } = useRoomMediaController(
    initial,
    media,
    generation,
    controllerState,
  );
  useEffect(() => {
    if (connected) {
      reattachCurrentMedia();
    }
  }, [connected, reattachCurrentMedia]);

  async function endConsultation() {
    try {
      const result = await api<{ generation: number; shutdownAtMs: number }>(
        `/api/consultations/${initial.consultationId}/end`,
        { method: "POST", body: "{}" },
      );
      generation.current = result.generation;
      setCallState("finalizing");
      setShutdownAt(result.shutdownAtMs);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "The consultation could not be ended");
    }
  }

  async function leaveConsultationLocally() {
    await leaveLocally();
    setLeftLocally(true);
  }

  if (leftLocally) {
    return <LeftPanel consultationId={initial.consultationId} />;
  }

  if (!connected) {
    return <JoinPanel connecting={connecting} error={error} onConnect={connect} />;
  }

  const fallback = !sameLanguage && !interpretationReady && mode !== "original";

  return (
    <section className={styles.room} aria-labelledby="live-consultation-title">
      <h1 className="srOnly" id="live-consultation-title">
        Live consultation
      </h1>
      <RecordingStatus
        archiveFailed={archiveFailed}
        callState={callState}
        captureReady={captureReady}
      />
      <VideoGrid
        displayName={initial.displayName}
        localVideo={localVideo}
        otherDisplayName={initial.otherDisplayName}
        remoteVideo={remoteVideo}
      />
      <TranslationRibbon caption={caption} otherDisplayName={initial.otherDisplayName} />
      <RecoveryStatus fallback={fallback} sameLanguage={sameLanguage} seconds={seconds} />
      <RoomControls
        callState={callState}
        mode={mode}
        onEnd={endConsultation}
        onLeave={leaveConsultationLocally}
        onModeChange={setMode}
        role={initial.role}
        seconds={seconds}
      />
      {/* biome-ignore lint/a11y/useMediaCaption: This element plays the other participant's live WebRTC microphone; translated captions render in the room ribbon. */}
      <audio className={styles.hiddenMedia} ref={originalAudio} autoPlay />
      {/* biome-ignore lint/a11y/useMediaCaption: This element plays live synthesized interpretation audio; translated captions render in the room ribbon. */}
      <audio
        className={styles.hiddenMedia}
        ref={interpretationAudio}
        autoPlay
        onError={() => {
          setInterpretationReady(false);
        }}
        onStalled={() => {
          setInterpretationReady(false);
        }}
        onEnded={() => {
          setInterpretationReady(false);
        }}
      />
      {error && (
        <p className={`error ${styles.error}`} role="alert">
          {error}
        </p>
      )}
    </section>
  );
}
