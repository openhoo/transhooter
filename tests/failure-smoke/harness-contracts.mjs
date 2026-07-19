export function restartCountIncremented(baseline, current) {
  return Number.isInteger(baseline) && Number.isInteger(current) && current > baseline;
}

export function archiveRaceWinner({ holdOk, deleteOk, archiveState, activeHoldCount }) {
  if (holdOk && !deleteOk && archiveState === "complete" && activeHoldCount === 1) {
    return "hold";
  }
  if (
    deleteOk &&
    !holdOk &&
    (archiveState === "deleting" || archiveState === "deleted") &&
    activeHoldCount === 0
  ) {
    return "delete";
  }
  return null;
}
