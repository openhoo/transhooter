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
  const roomReady = Boolean(consultation.roomName && consultation.roomSid);
  const isRoomState = consultation.state === "ready" || consultation.state === "active";

  if (isRoomState && roomReady) {
    return `/consultations/${consultation.id}/room`;
  }

  const isArchiveState = consultation.state === "finalizing" || consultation.state === "ended";
  if (isArchiveState) {
    return `/archives/${consultation.id}`;
  }

  return null;
}
