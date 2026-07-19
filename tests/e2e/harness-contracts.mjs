export const MODE_GAIN_PAIRS = Object.freeze([
  Object.freeze(["Interpreted", 0, 1]),
  Object.freeze(["Overlay", 0.18, 1]),
  Object.freeze(["Original", 1, 0]),
]);

export function acceptedCaptionMatchesRender({
  candidate,
  consultationId,
  destinationParticipantId,
  sourceParticipantId,
  sourceLanguage,
  targetLanguage,
  renderedTranslation,
  renderedSource,
  finalAnnouncement,
  otherDisplayName,
}) {
  if (
    candidate?.schemaVersion !== 1 ||
    candidate.finality !== "final" ||
    candidate.consultationId !== consultationId ||
    candidate.destinationParticipantId !== destinationParticipantId ||
    candidate.sourceParticipantId !== sourceParticipantId ||
    candidate.sourceLanguage !== sourceLanguage ||
    candidate.targetLanguage !== targetLanguage ||
    typeof candidate.sourceText !== "string" ||
    candidate.sourceText.trim() === "" ||
    typeof candidate.translatedText !== "string" ||
    candidate.translatedText.trim() === ""
  ) {
    return false;
  }
  return (
    renderedTranslation === candidate.translatedText &&
    renderedSource === candidate.sourceText &&
    finalAnnouncement ===
      `Final translation from ${otherDisplayName}, ${sourceLanguage} to ${targetLanguage}: ${candidate.translatedText}`
  );
}
