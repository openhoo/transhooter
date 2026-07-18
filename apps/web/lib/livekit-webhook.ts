import { EgressStatus } from "@livekit/protocol";

export type NormalizedEgressWebhookKind =
  | "egress_active"
  | "egress_complete"
  | "egress_failed"
  | "ignored"
  | null;

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
