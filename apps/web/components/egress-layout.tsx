"use client";

import EgressHelper from "@livekit/egress-sdk";
import {
  type RemoteParticipant,
  type RemoteTrack,
  type RemoteTrackPublication,
  Room,
  RoomEvent,
  Track,
} from "livekit-client";
import type { MutableRefObject } from "react";
import { useEffect, useRef, useState } from "react";
import styles from "./egress-layout.module.css";

type Participant = {
  identity: string;
  displayName: string;
};

type EgressLayoutProps = {
  participants: readonly [Participant, Participant];
};

type MediaElements = {
  videos: MutableRefObject<Map<string, HTMLVideoElement>>;
  audios: MutableRefObject<Map<string, HTMLAudioElement>>;
};

function isEligibleEgressTrack(
  publication: RemoteTrackPublication,
  participant: RemoteParticipant,
  allowedIdentities: ReadonlySet<string>,
) {
  const isAllowedParticipant = allowedIdentities.has(participant.identity);
  const isStandardMedia =
    publication.source === Track.Source.Camera || publication.source === Track.Source.Microphone;
  return isAllowedParticipant && isStandardMedia;
}

function useEgressRoom(
  allowedIdentities: ReadonlySet<string>,
  mediaElements: MediaElements,
  setFailed: (failed: boolean) => void,
) {
  useEffect(() => {
    const current = new Room({ adaptiveStream: false, dynacast: false });
    let active = true;
    let recordingStarted = false;

    function subscribeIfEligible(
      publication: RemoteTrackPublication,
      participant: RemoteParticipant,
    ) {
      if (active && isEligibleEgressTrack(publication, participant, allowedIdentities)) {
        publication.setSubscribed(true);
      }
    }

    function attachTrack(
      track: RemoteTrack,
      publication: RemoteTrackPublication,
      participant: RemoteParticipant,
    ) {
      if (!active || !allowedIdentities.has(participant.identity)) {
        return;
      }

      let element: HTMLVideoElement | HTMLAudioElement | undefined;
      if (publication.source === Track.Source.Camera) {
        element = mediaElements.videos.current.get(participant.identity);
      } else if (publication.source === Track.Source.Microphone) {
        element = mediaElements.audios.current.get(participant.identity);
      }

      if (element) {
        track.attach(element);
      }
    }

    function handleDisconnected() {
      if (active) {
        setFailed(true);
      }
    }

    current.on(RoomEvent.TrackPublished, subscribeIfEligible);
    current.on(RoomEvent.TrackSubscribed, attachTrack);
    current.on(RoomEvent.Disconnected, handleDisconnected);

    async function connectAndRecord() {
      try {
        await current.connect(EgressHelper.getLiveKitURL(), EgressHelper.getAccessToken(), {
          autoSubscribe: false,
        });
        if (!active) {
          void current.disconnect();
          return;
        }
        current.remoteParticipants.forEach((participant) => {
          participant.trackPublications.forEach((publication) => {
            subscribeIfEligible(publication, participant);
          });
        });
        if (!active) {
          void current.disconnect();
          return;
        }
        EgressHelper.setRoom(current);
        EgressHelper.startRecording();
        recordingStarted = true;
      } catch {
        if (active) {
          setFailed(true);
        }
      }
    }

    void connectAndRecord();

    return () => {
      active = false;
      current.off(RoomEvent.TrackPublished, subscribeIfEligible);
      current.off(RoomEvent.TrackSubscribed, attachTrack);
      current.off(RoomEvent.Disconnected, handleDisconnected);
      if (recordingStarted) {
        EgressHelper.endRecording();
      }
      void current.disconnect();
    };
  }, [allowedIdentities, mediaElements, setFailed]);
}

export function EgressLayout({ participants }: EgressLayoutProps) {
  const videoElements = useRef(new Map<string, HTMLVideoElement>());
  const audioElements = useRef(new Map<string, HTMLAudioElement>());
  const allowedIdentities = useRef(
    new Set(participants.map((participant) => participant.identity)),
  ).current;
  const mediaElements = useRef({
    videos: videoElements,
    audios: audioElements,
  }).current;
  const [failed, setFailed] = useState(false);

  useEgressRoom(allowedIdentities, mediaElements, setFailed);

  return (
    <main className={styles.layout} aria-label="Private consultation recording layout">
      {participants.map((participant) => (
        <section
          className={styles.pane}
          key={participant.identity}
          aria-label={`${participant.displayName} recording pane`}
        >
          <video
            ref={(element) => {
              if (element) {
                videoElements.current.set(participant.identity, element);
              } else {
                videoElements.current.delete(participant.identity);
              }
            }}
            autoPlay
            muted
            playsInline
            aria-label={`${participant.displayName} live video`}
          />
          {/* biome-ignore lint/a11y/useMediaCaption: The compositor receives live WebRTC microphone media with no prerecorded caption track; translated captions are recorded separately. */}
          <audio
            ref={(element) => {
              if (element) {
                audioElements.current.set(participant.identity, element);
              } else {
                audioElements.current.delete(participant.identity);
              }
            }}
            autoPlay
          />
          <span>{participant.displayName}</span>
        </section>
      ))}
      {failed && (
        <div className={styles.failure} role="alert">
          Recording layout disconnected
        </div>
      )}
    </main>
  );
}
