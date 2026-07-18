export type RoutedConsultation = {
  id: string;
  state: "invited" | "ready" | "active" | "finalizing" | "ended" | "cancelled" | "deleted";
  roomName: string | null;
  roomSid: string | null;
  dispatchId: string | null;
  compositeEgressId: string | null;
};

export function durableConsultationDestination(
  consultation: RoutedConsultation,
): `/consultations/${string}/room` | `/archives/${string}` | null {
  const mediaReady = Boolean(
    consultation.roomName &&
      consultation.roomSid &&
      consultation.dispatchId &&
      consultation.compositeEgressId,
  );
  const isRoomState = consultation.state === "ready" || consultation.state === "active";

  if (isRoomState && mediaReady) {
    return `/consultations/${consultation.id}/room`;
  }

  const isArchiveState = consultation.state === "finalizing" || consultation.state === "ended";
  if (isArchiveState) {
    return `/archives/${consultation.id}`;
  }

  return null;
}
