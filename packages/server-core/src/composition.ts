import { PrismaApplicationOperations } from "./application-operations";
import { ArchiveService, type InventoryHasher } from "./archives/service";
import { type AuthSecrets, AuthService, type MagicLinkTokenSealer } from "./auth/service";
import { type ConsultationHasher, ConsultationService } from "./consultations/service";
import type { Clock, IdGenerator, TokenGenerator, TokenHasher } from "./domain/model";
import { LanguageService } from "./languages/service";
import {
  PrismaArchiveRepository,
  PrismaAuditRepository,
  PrismaConsultationRepository,
  PrismaLanguageRepository,
  PrismaProviderSnapshotRepository,
} from "./persistence/application-repositories";
import {
  PrismaAuthRepository,
  type PrismaClient,
  PrismaEffectRepository,
} from "./persistence/repositories";
import type {
  EgressPort,
  InternalPrincipalVerifier,
  LiveKitRoomPort,
  LiveKitTokenPort,
  MailPort,
  ObjectStoragePort,
} from "./ports/index";
import { RoomService, type WebhookVerifier } from "./rooms/service";
import { createWebApplication, type WebApplication } from "./web-application";

export interface CoreHashing extends TokenHasher, ConsultationHasher, InventoryHasher {}

export interface ConfiguredWebApplication extends WebApplication {
  readonly roomService: RoomService;
}

export interface ConfiguredWebApplicationConfig {
  database: PrismaClient;
  mail: MailPort;
  storage: ObjectStoragePort;
  livekitRooms: LiveKitRoomPort;
  livekitTokens: LiveKitTokenPort;
  egress: EgressPort;
  webhookVerifier: WebhookVerifier;
  internalPrincipalVerifier: InternalPrincipalVerifier;
  clock: Clock;
  ids: IdGenerator;
  tokens: TokenGenerator;
  hashing: CoreHashing;
  authSecrets: AuthSecrets;
  magicLinkTokenSealer: MagicLinkTokenSealer;
  publicBaseUrl: string;
  clientIp: (request: Request) => string;
  readiness: () => Promise<boolean>;
}

export function createConfiguredWebApplication(
  config: ConfiguredWebApplicationConfig,
): ConfiguredWebApplication {
  const authRepository = new PrismaAuthRepository(config.database);
  const consultationRepository = new PrismaConsultationRepository(config.database);
  const effectRepository = new PrismaEffectRepository(config.database);
  const archiveRepository = new PrismaArchiveRepository(config.database);
  const languageRepository = new PrismaLanguageRepository(config.database);
  const auditRepository = new PrismaAuditRepository(config.database);
  const snapshotRepository = new PrismaProviderSnapshotRepository(config.database, (value) =>
    config.hashing.sha256Canonical(value),
  );

  const auth = new AuthService(
    authRepository,
    config.mail,
    config.clock,
    config.ids,
    config.tokens,
    config.hashing,
    config.authSecrets,
    config.magicLinkTokenSealer,
  );
  const consultations = new ConsultationService(
    consultationRepository,
    effectRepository,
    snapshotRepository,
    config.livekitTokens,
    auditRepository,
    config.clock,
    config.ids,
    config.hashing,
    authRepository,
  );
  const archives = new ArchiveService(
    archiveRepository,
    config.storage,
    auditRepository,
    config.clock,
    config.ids,
    config.hashing,
    effectRepository,
  );
  const languages = new LanguageService(languageRepository, config.clock, config.ids);
  const operations = new PrismaApplicationOperations(config.database, config.storage, config.clock);

  const application = createWebApplication({
    auth,
    consultations,
    archives,
    languages,
    operations,
    internalPrincipalVerifier: config.internalPrincipalVerifier,
    publicBaseUrl: config.publicBaseUrl,
    clientIp: config.clientIp,
    ready: config.readiness,
  });
  const roomService = new RoomService(
    consultationRepository,
    effectRepository,
    config.livekitRooms,
    config.egress,
    config.webhookVerifier,
    auditRepository,
    config.clock,
    config.ids,
    config.hashing,
  );

  return { ...application, roomService };
}
