import { sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import type {
  AdminLanguageRow,
  ArchiveDetail,
  ArchiveDetailProof,
  ArchiveObjectPage,
  StaffPrincipal,
} from "./application-operations";
import { DomainError, type UUID } from "./domain/model";
import type { DrizzleSchema } from "./persistence/repositories";
import type { ObjectStoragePort } from "./ports/index";

function mapArchiveDetail(row: Record<string, unknown>): ArchiveDetail {
  return {
    id: row.id as UUID,
    consultationId: row.consultation_id as UUID,
    state: String(row.state),
    inventory: row.inventory ?? null,
    activeHolds: row.active_holds as ArchiveDetailProof["activeHolds"],
    inventoryVersionId: row.inventory_version_id as string | null,
    inventorySha256: row.sha256 as string | null,
    egressIds: row.egress_ids as string[],
    providerAttemptIds: row.provider_attempt_ids as UUID[],
    providerAttemptGroups:
      row.provider_attempt_groups as ArchiveDetailProof["providerAttemptGroups"],
  };
}

function mapAdminLanguage(row: Record<string, unknown>): AdminLanguageRow {
  return {
    id: row.id as UUID,
    profileId: row.profile_id as UUID,
    sourceLocale: String(row.source_locale),
    targetLocale: String(row.target_locale),
    mode: row.mode as AdminLanguageRow["mode"],
    snapshot: row.snapshot,
    profileName: String(row.profile_name),
    revision: Number(row.revision),
    freshUntil: row.fresh_until as Date,
    enabled: Boolean(row.enabled),
  };
}

function presentArchiveObjectPage(
  rows: readonly Record<string, unknown>[],
  limit: number,
): ArchiveObjectPage {
  const hasMore = rows.length > limit;
  const objects = hasMore ? rows.slice(0, limit) : rows;
  return {
    objects,
    cursor: hasMore ? String(objects.at(-1)?.id) : null,
  };
}

export class ApplicationOperationsQueries {
  constructor(
    private readonly database: NodePgDatabase<DrizzleSchema>,
    private readonly storage: ObjectStoragePort,
  ) {}

  async consultationOptions(profileId: string): Promise<readonly Record<string, unknown>[]> {
    const result = await this.database.execute(
      sql`SELECT l.id,p.id AS profile_id,l.source_locale,l.target_locale,l.mode,l.snapshot,p.name AS profile_name,l.revision,l.fresh_until FROM language_capabilities l JOIN provider_profiles p ON p.id=l.profile_id WHERE (p.id::text=${profileId} OR p.name=${profileId}) AND p.enabled AND l.enabled AND l.revision=p.current_revision AND l.fresh_until>now() ORDER BY l.source_locale,l.target_locale`,
    );
    return result.rows;
  }

  async consultationRoom(consultationId: UUID, userId: UUID): Promise<Record<string, unknown>> {
    const result = await this.database.execute(
      sql`SELECT c.id AS consultation_id,c.state,c.generation,c.worker_identity,c.room_sid,c.dispatch_id,c.composite_egress_id,mine.id AS participant_id,mine.livekit_identity AS participant_identity,mine.role,mine.display_name,other.id AS other_participant_id,other.livekit_identity AS other_identity,other.display_name AS other_display_name FROM consultations c JOIN consultation_participants mine ON mine.consultation_id=c.id AND mine.user_id=${userId} JOIN consultation_participants other ON other.consultation_id=c.id AND other.id<>mine.id WHERE c.id=${consultationId}`,
    );
    const row = result.rows[0];
    if (!row) {
      throw new DomainError("NOT_FOUND");
    }
    if (!row.worker_identity || !row.room_sid) {
      throw new DomainError("PROVISIONING");
    }
    return row;
  }

  async consultationInviteRecipient(consultationId: UUID, employeeUserId: UUID): Promise<string> {
    const result = await this.database.execute(
      sql`SELECT customer.email FROM consultation_participants employee JOIN consultation_participants slot ON slot.consultation_id=employee.consultation_id AND slot.role='customer' JOIN users customer ON customer.id=slot.user_id JOIN consultations c ON c.id=employee.consultation_id WHERE c.id=${consultationId} AND c.state='invited' AND employee.user_id=${employeeUserId} AND employee.role='employee'`,
    );
    const email = result.rows[0]?.email;
    if (typeof email !== "string") {
      throw new DomainError("NOT_FOUND");
    }
    return email;
  }

  async archiveList(principal: StaffPrincipal): Promise<readonly Record<string, unknown>[]> {
    const result = await this.database.execute(
      sql`SELECT DISTINCT a.id,a.consultation_id,a.state,a.final_inventory_hash,a.updated_at FROM archives a LEFT JOIN consultation_participants p ON p.consultation_id=a.consultation_id AND p.user_id=${principal.userId} AND p.role='employee' WHERE ${principal.role}='admin' OR p.id IS NOT NULL ORDER BY a.updated_at DESC`,
    );
    return result.rows;
  }

  async archiveGet(principal: StaffPrincipal, routeId: UUID): Promise<ArchiveDetail> {
    const result = await this.database.execute(
      sql`SELECT a.id,a.consultation_id,a.state,f.inventory,f.sha256,inventory_object.version_id AS inventory_version_id,COALESCE((SELECT jsonb_agg(jsonb_build_object('id',h.id,'reason',h.reason) ORDER BY h.placed_at,h.id) FROM legal_holds h WHERE h.archive_id=a.id AND h.released_at IS NULL),'[]'::jsonb) AS active_holds,COALESCE((SELECT jsonb_agg(j.egress_id ORDER BY j.id) FROM egress_jobs j WHERE j.consultation_id=a.consultation_id AND j.egress_id IS NOT NULL),'[]'::jsonb) AS egress_ids,COALESCE((SELECT jsonb_agg(pa.id ORDER BY pa.id) FROM provider_attempts pa WHERE pa.archive_id=a.id),'[]'::jsonb) AS provider_attempt_ids,COALESCE((SELECT jsonb_agg(jsonb_build_object('stage',g.stage,'provider',g.provider,'direction',g.direction_id,'attemptIds',g.attempt_ids) ORDER BY g.stage,g.provider,g.direction_id) FROM (SELECT stage,provider,direction_id,jsonb_agg(id ORDER BY attempt_number,id) AS attempt_ids FROM provider_attempts WHERE archive_id=a.id GROUP BY stage,provider,direction_id) g),'[]'::jsonb) AS provider_attempt_groups FROM archives a LEFT JOIN consultation_participants p ON p.consultation_id=a.consultation_id AND p.user_id=${principal.userId} AND p.role='employee' LEFT JOIN final_inventories f ON f.archive_id=a.id LEFT JOIN archive_objects inventory_object ON inventory_object.id=f.object_id WHERE (a.id=${routeId} OR a.consultation_id=${routeId}) AND (${principal.role}='admin' OR p.id IS NOT NULL)`,
    );
    const row = result.rows[0];
    if (!row) {
      throw new DomainError("NOT_FOUND");
    }
    return mapArchiveDetail(row);
  }

  async archiveObjects(
    principal: StaffPrincipal,
    routeId: UUID,
    cursor: string | null,
    limit: number,
  ): Promise<ArchiveObjectPage> {
    const boundedLimit = Math.max(1, Math.min(limit, 100));
    const result = await this.database.execute(
      sql`SELECT DISTINCT o.* FROM archive_objects o JOIN archives a ON a.id=o.archive_id LEFT JOIN consultation_participants p ON p.consultation_id=a.consultation_id AND p.user_id=${principal.userId} AND p.role='employee' WHERE (a.id=${routeId} OR a.consultation_id=${routeId}) AND (${principal.role}='admin' OR p.id IS NOT NULL) AND (${cursor}::uuid IS NULL OR o.id>${cursor}::uuid) ORDER BY o.id LIMIT ${boundedLimit + 1}`,
    );
    return presentArchiveObjectPage(result.rows, boundedLimit);
  }

  async archiveDownload(principal: StaffPrincipal, routeId: UUID, objectId: UUID): Promise<string> {
    const result = await this.database.execute(
      sql`SELECT o.key,o.version_id FROM archive_objects o JOIN archives a ON a.id=o.archive_id LEFT JOIN consultation_participants p ON p.consultation_id=a.consultation_id AND p.user_id=${principal.userId} AND p.role='employee' WHERE (a.id=${routeId} OR a.consultation_id=${routeId}) AND o.id=${objectId} AND (${principal.role}='admin' OR p.id IS NOT NULL)`,
    );
    const row = result.rows[0];
    if (!row) {
      throw new DomainError("NOT_FOUND");
    }
    return this.storage.presignGet(String(row.key), String(row.version_id), 300);
  }

  async adminFailures(principal: StaffPrincipal): Promise<readonly Record<string, unknown>[]> {
    this.assertAdmin(principal);
    const result = await this.database.execute(
      sql`SELECT id,consultation_id,state,result,updated_at FROM external_effects WHERE state='failed' UNION ALL SELECT id,consultation_id,'failed',terminal_result,terminal_at FROM egress_jobs WHERE state IN ('EGRESS_FAILED','EGRESS_ABORTED','EGRESS_LIMIT_REACHED') ORDER BY updated_at DESC NULLS LAST`,
    );
    return result.rows;
  }

  async adminLanguages(
    principal: StaffPrincipal,
    profileId: string,
  ): Promise<readonly AdminLanguageRow[]> {
    this.assertAdmin(principal);
    const result = await this.database.execute(
      sql`SELECT l.id,l.profile_id,l.source_locale,l.target_locale,l.mode,l.snapshot,p.name AS profile_name,l.revision,l.fresh_until,l.enabled FROM language_capabilities l JOIN provider_profiles p ON p.id=l.profile_id WHERE p.name=${profileId} AND l.revision=p.current_revision ORDER BY l.source_locale,l.target_locale,l.mode`,
    );
    return result.rows.map(mapAdminLanguage);
  }

  async egressLayout(consultationId: UUID, generation: number): Promise<Record<string, unknown>> {
    const result = await this.database.execute(
      sql`SELECT c.id,c.room_name,c.generation,jsonb_agg(jsonb_build_object('identity',p.livekit_identity,'role',p.role,'displayName',p.display_name) ORDER BY p.role) AS participants FROM consultations c JOIN consultation_participants p ON p.consultation_id=c.id WHERE c.id=${consultationId} AND c.generation=${generation} GROUP BY c.id`,
    );
    const row = result.rows[0];
    if (!row) {
      throw new DomainError("FENCED_GENERATION");
    }
    return row;
  }

  private assertAdmin(principal: StaffPrincipal): void {
    if (principal.role !== "admin") {
      throw new DomainError("FORBIDDEN");
    }
  }
}
