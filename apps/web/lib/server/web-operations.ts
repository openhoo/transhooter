export const WEB_OPERATION_REGISTRY = {
  "auth.magicLink.request": true,
  "auth.exchange.prepare": true,
  "auth.exchange.verify": true,
  "auth.logout": true,
  "auth.archiveDeleteReauth.request": true,
  "consultations.list": true,
  "consultations.create.options": true,
  "consultations.create": true,
  "consultations.get": true,
  "consultations.preferences.update": true,
  "consultations.consent.record": true,
  "consultations.join": true,
  "consultations.livekitToken": true,
  "consultations.room": true,
  "consultations.end": true,
  "consultations.cancel": true,
  "consultations.invitation.resend": true,
  "archives.list": true,
  "archives.get": true,
  "archives.objects.list": true,
  "archives.object.download": true,
  "archives.hold.update": true,
  "archives.delete": true,
  "languages.catalog": true,
  "admin.failures.list": true,
  "admin.languages.list": true,
  "admin.languages.update": true,
  "internal.capabilities.update": true,
  "internal.worker.heartbeat": true,
  "internal.archive.checkpoint": true,
  "internal.providerAttempt": true,
  "internal.archiveObject": true,
  "internal.archive.finalize": true,
  "internal.expiredWorkerEpochs": true,
  "internal.completeWorkerEpoch": true,
  "internal.abandonWorkerEpoch": true,
  "internal.egressLayout.authorize": true,
  "internal.archiveRecording": true,
  "internal.deleteDrain": true,
  "webhooks.livekit.receive": true,
} as const satisfies Record<string, true>;

export type WebOperation = keyof typeof WEB_OPERATION_REGISTRY;

export const WEB_OPERATIONS = Object.keys(WEB_OPERATION_REGISTRY) as WebOperation[];

export function isWebOperation(operation: string): operation is WebOperation {
  return Object.hasOwn(WEB_OPERATION_REGISTRY, operation);
}

export function boundedWebOperation(operation: string): WebOperation | "unknown" {
  return isWebOperation(operation) ? operation : "unknown";
}
