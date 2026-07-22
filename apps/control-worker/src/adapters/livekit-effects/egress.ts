import { createHmac } from "node:crypto";
import type { EgressInfo } from "@livekit/protocol";
import { StopEgressRequest } from "@livekit/protocol";
import {
  type EgressClient,
  EgressStatus,
  S3Upload,
  SegmentedFileOutput,
  SegmentedFileProtocol,
} from "livekit-server-sdk";
import type { Effect } from "../../orchestration/model";
import { canonicalRequest as encodeCanonicalRequest } from "../../orchestration/model";
import type { Adoption, RemoteResult } from "../../orchestration/remote";
import type { LiveKitConfig } from "./shared";
import { requiredOptionalString, requiredString, roomNameFor, twirpBytes } from "./shared";

export type EgressStatusName =
  | "EGRESS_STARTING"
  | "EGRESS_ACTIVE"
  | "EGRESS_ENDING"
  | "EGRESS_COMPLETE"
  | "EGRESS_FAILED"
  | "EGRESS_ABORTED"
  | "EGRESS_LIMIT_REACHED";

type EgressTerminalResult = {
  readonly egressId: string;
  readonly status: EgressStatusName;
  readonly error: string;
  readonly errorCode: number;
  readonly details: string;
  readonly fileResults: EgressInfo["fileResults"];
  readonly segmentResults: EgressInfo["segmentResults"];
};

export function isTerminalEgress(status: EgressStatus): boolean {
  return (
    status === EgressStatus.EGRESS_COMPLETE ||
    status === EgressStatus.EGRESS_FAILED ||
    status === EgressStatus.EGRESS_ABORTED ||
    status === EgressStatus.EGRESS_LIMIT_REACHED
  );
}

export function isViableEgressAdoption(status: EgressStatus): boolean {
  return status === EgressStatus.EGRESS_STARTING || status === EgressStatus.EGRESS_ACTIVE;
}

export function egressStatusName(status: EgressStatus): EgressStatusName {
  switch (status) {
    case EgressStatus.EGRESS_STARTING:
      return "EGRESS_STARTING";
    case EgressStatus.EGRESS_ACTIVE:
      return "EGRESS_ACTIVE";
    case EgressStatus.EGRESS_ENDING:
      return "EGRESS_ENDING";
    case EgressStatus.EGRESS_COMPLETE:
      return "EGRESS_COMPLETE";
    case EgressStatus.EGRESS_FAILED:
      return "EGRESS_FAILED";
    case EgressStatus.EGRESS_ABORTED:
      return "EGRESS_ABORTED";
    case EgressStatus.EGRESS_LIMIT_REACHED:
      return "EGRESS_LIMIT_REACHED";
    default:
      throw new Error(`unsupported LiveKit Egress status ${String(status)}`);
  }
}

export function terminalEgressResult(info: EgressInfo): EgressTerminalResult {
  return {
    egressId: info.egressId,
    status: egressStatusName(info.status),
    error: info.error,
    errorCode: info.errorCode,
    details: info.details,
    fileResults: info.fileResults,
    segmentResults: info.segmentResults,
  };
}

function egressMatches(
  info: { readonly roomName?: string; readonly request: unknown },
  effect: Effect,
  request: Readonly<Record<string, unknown>>,
): boolean {
  const roomName = roomNameFor(effect, request);
  const participantIdentity = requiredOptionalString(request, "participantIdentity");
  const encoded = JSON.stringify(info.request);
  return (
    info.roomName === roomName &&
    encoded.includes(effect.id) &&
    (participantIdentity === undefined || encoded.includes(participantIdentity))
  );
}

export class EgressEffects {
  constructor(
    private readonly getEgress: () => EgressClient,
    private readonly config: LiveKitConfig,
  ) {}

  async adopt(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
  ): Promise<Adoption | null> {
    if (effect.kind === "ROOM_COMPOSITE_EGRESS" || effect.kind === "PARTICIPANT_EGRESS") {
      const roomName = roomNameFor(effect, request);
      const found = (await this.getEgress().listEgress({ roomName })).find(
        (candidate) =>
          egressMatches(candidate, effect, request) && isViableEgressAdoption(candidate.status),
      );
      return found === undefined
        ? null
        : {
            remoteId: found.egressId,
            matchesRequest: true,
            terminal: false,
            result: { egressId: found.egressId, status: egressStatusName(found.status) },
          };
    }
    const egressId = requiredString(request, "egressId");
    const found = (await this.getEgress().listEgress({ egressId }))[0];
    if (found === undefined || !isTerminalEgress(found.status)) return null;
    return {
      remoteId: egressId,
      matchesRequest: true,
      terminal: true,
      result: terminalEgressResult(found),
    };
  }

  canonical(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Uint8Array {
    if (effect.kind === "EGRESS_STOP") {
      return twirpBytes(new StopEgressRequest({ egressId: requiredString(request, "egressId") }));
    }
    return encodeCanonicalRequest({
      kind: effect.kind,
      roomName,
      egressIdentity: effect.id,
      consultationId: effect.consultationId,
      generation: effect.generation,
      output: { format: "segmented_hls", segmentDurationSeconds: 2 },
      ...(effect.kind === "ROOM_COMPOSITE_EGRESS"
        ? { render: { layout: "speaker", audioOnly: false, videoOnly: false } }
        : {
            participantIdentity: requiredString(request, "participantIdentity"),
            screenShare: false,
          }),
    }).bytes;
  }

  async execute(
    effect: Effect,
    request: Readonly<Record<string, unknown>>,
    roomName: string,
  ): Promise<RemoteResult> {
    if (effect.kind === "EGRESS_STOP") {
      const info = await this.getEgress().stopEgress(requiredString(request, "egressId"));
      if (!isTerminalEgress(info.status)) {
        throw new Error("Egress stop has not reached terminal state");
      }
      return { remoteId: info.egressId, result: terminalEgressResult(info) };
    }
    const output = this.segmentedOutput(`${requiredString(request, "outputPrefix")}/${effect.id}`);
    const started =
      effect.kind === "ROOM_COMPOSITE_EGRESS"
        ? await this.getEgress().startRoomCompositeEgress(roomName, output, {
            layout: "speaker",
            customBaseUrl: this.signedLayoutUrl(effect),
          })
        : await this.getEgress().startParticipantEgress(
            roomName,
            requiredString(request, "participantIdentity"),
            { segments: output },
            { screenShare: false },
          );
    if (!isViableEgressAdoption(started.status)) {
      throw new Error(
        `Egress start returned non-viable status ${egressStatusName(started.status)}`,
      );
    }
    return {
      remoteId: started.egressId,
      result: { egressId: started.egressId, status: egressStatusName(started.status) },
    };
  }

  async compensate(remoteId: string): Promise<void> {
    const egress = this.getEgress();
    const found = (await egress.listEgress({ egressId: remoteId }))[0];
    if (found !== undefined && !isTerminalEgress(found.status)) await egress.stopEgress(remoteId);
  }

  private signedLayoutUrl(effect: Effect): string {
    const expires = Date.now() + 10 * 60_000;
    const message = `${effect.consultationId}\n${String(effect.generation)}\n${String(expires)}`;
    const signature = createHmac("sha256", this.config.egressLayoutSigningKey)
      .update(message)
      .digest("hex");
    const url = new URL(this.config.egressLayoutUrl);
    url.searchParams.set("consultationId", effect.consultationId);
    url.searchParams.set("generation", String(effect.generation));
    url.searchParams.set("expires", String(expires));
    url.searchParams.set("signature", signature);
    return url.toString();
  }

  private segmentedOutput(prefix: string): SegmentedFileOutput {
    return new SegmentedFileOutput({
      protocol: SegmentedFileProtocol.HLS_PROTOCOL,
      filenamePrefix: prefix,
      playlistName: `${prefix}/index.m3u8`,
      livePlaylistName: `${prefix}/live.m3u8`,
      segmentDuration: 2,
      output: {
        case: "s3",
        value: new S3Upload({
          accessKey: this.config.s3.accessKey,
          secret: this.config.s3.secretKey,
          endpoint: this.config.s3.endpoint,
          bucket: this.config.s3.bucket,
          region: this.config.s3.region,
          forcePathStyle: this.config.s3.forcePathStyle,
        }),
      },
    });
  }
}
