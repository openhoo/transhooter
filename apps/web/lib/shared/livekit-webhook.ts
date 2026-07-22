import { type EgressInfo, EgressStatus } from "@livekit/protocol";
import type { EgressEventEarlySource } from "@transhooter/server-core";

export type NormalizedEgressWebhookKind =
  | "egress_active"
  | "egress_complete"
  | "egress_failed"
  | "ignored"
  | null;

export function normalizeEgressRequestSource(
  egressInfo: Pick<EgressInfo, "request"> | undefined,
): EgressEventEarlySource | null {
  const request = egressInfo?.request;
  if (request?.case === "roomComposite" && request.value.roomName) {
    return { kind: "room_composite", roomName: request.value.roomName };
  }
  if (request?.case === "participant" && request.value.roomName && request.value.identity) {
    return {
      kind: "participant",
      roomName: request.value.roomName,
      identity: request.value.identity,
    };
  }
  return null;
}

export function normalizedEgressWebhookKind(
  eventName: string | undefined,
  status: EgressStatus | undefined,
): NormalizedEgressWebhookKind {
  switch (eventName) {
    case "egress_started":
    case "egress_updated":
      if (status === EgressStatus.EGRESS_ACTIVE) {
        return "egress_active";
      }
      return "ignored";

    case "egress_ended":
      if (status === EgressStatus.EGRESS_COMPLETE) {
        return "egress_complete";
      }

      switch (status) {
        case EgressStatus.EGRESS_FAILED:
        case EgressStatus.EGRESS_ABORTED:
        case EgressStatus.EGRESS_LIMIT_REACHED:
          return "egress_failed";
        default:
          return "ignored";
      }

    default:
      return null;
  }
}
