import type { Metadata } from "next";
import Link from "next/link";
import { ArchiveAdminActions, DownloadButton } from "@/components/archive-actions";
import { requirePageData } from "@/lib/server-application";

export const metadata: Metadata = { title: "Consultation archive" };
export const dynamic = "force-dynamic";

type ArchiveObjectGroup =
  | "composite"
  | "original"
  | "interpretation"
  | "captions"
  | "pipeline"
  | "inventory";

type ArchiveObject = {
  id: string;
  group: ArchiveObjectGroup;
  label: string;
  contentType: string;
  size: number;
  sha256: string;
  versionId: string;
};

type ArchiveGap = {
  class: string;
  detail: string;
};

type ArchiveHold = {
  id: string;
  reason: string;
};

type ProviderAttemptGroup = {
  stage: "stt" | "translation" | "tts";
  provider: string;
  direction: string;
  attemptIds: string[];
};

type ArchiveView = {
  id: string;
  consultationId: string;
  status: string;
  objects: ArchiveObject[];
  gaps: ArchiveGap[];
  nextCursor: string | null;
  canAdminister: boolean;
  activeHolds: ArchiveHold[];
  inventoryVersion: string | null;
  inventorySha256: string | null;
  egressIds: string[];
  providerAttemptIds: string[];
  providerAttemptGroups: ProviderAttemptGroup[];
};

type ArchivePageProps = {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ cursor?: string }>;
};

type GroupedArchiveObjects = Record<ArchiveObjectGroup, ArchiveObject[]>;

const ARCHIVE_GROUPS: ReadonlyArray<{
  group: ArchiveObjectGroup;
  label: string;
}> = [
  { group: "composite", label: "Progressive and composite media" },
  { group: "original", label: "Isolated originals" },
  { group: "interpretation", label: "Interpreted tracks" },
  { group: "captions", label: "Captions" },
  { group: "pipeline", label: "Provider exchanges and outcomes" },
  { group: "inventory", label: "Inventory and checksums" },
];

const byteFormatter = new Intl.NumberFormat("en", {
  style: "unit",
  unit: "byte",
  unitDisplay: "narrow",
});

function groupArchiveObjects(objects: ArchiveObject[]): GroupedArchiveObjects {
  const groupedObjects: GroupedArchiveObjects = {
    composite: [],
    original: [],
    interpretation: [],
    captions: [],
    pipeline: [],
    inventory: [],
  };

  for (const object of objects) {
    groupedObjects[object.group].push(object);
  }

  return groupedObjects;
}

function ArchiveHeader({ status }: { status: string }) {
  return (
    <div className="row">
      <div>
        <p className="eyebrow">Recorded consultation</p>
        <h1>Archive evidence</h1>
      </div>
      <span className={`status ${status !== "complete" ? "warning" : ""}`}>{status}</span>
    </div>
  );
}

function ArchiveFinalizingNotice({ status }: { status: string }) {
  const isFinalizing = status === "pending" || status === "recording" || status === "reconciling";

  if (!isFinalizing) {
    return null;
  }

  return (
    <div className="notice warning" role="status">
      <strong>Finalizing the archive</strong>
      <p>
        Original recordings and provider evidence are still being reconciled. This page will show
        verified versions and checksums when available.
      </p>
    </div>
  );
}

function ActiveLegalHolds({ holds }: { holds: ArchiveHold[] }) {
  if (holds.length === 0) {
    return null;
  }

  return (
    <section className="notice warning" aria-labelledby="active-holds-title">
      <h2 id="active-holds-title">Active legal holds</h2>
      <ul>
        {holds.map((hold) => (
          <li key={hold.id}>
            {hold.reason} <span className="meta">({hold.id})</span>
          </li>
        ))}
      </ul>
    </section>
  );
}

function ArchiveProof({ archive }: { archive: ArchiveView }) {
  const hasProof =
    archive.inventoryVersion ||
    archive.inventorySha256 ||
    archive.egressIds.length > 0 ||
    archive.providerAttemptIds.length > 0;

  if (!hasProof) {
    return null;
  }

  return (
    <section className="panel" aria-labelledby="archive-proof-title">
      <h2 id="archive-proof-title">Archive proof</h2>
      <dl className="proofGrid">
        <div>
          <dt>Inventory version</dt>
          <dd>
            <code>{archive.inventoryVersion ?? "Pending"}</code>
          </dd>
        </div>
        <div>
          <dt>Inventory SHA-256</dt>
          <dd>
            <code>{archive.inventorySha256 ?? "Pending"}</code>
          </dd>
        </div>
        <div>
          <dt>Egress jobs</dt>
          <dd>{archive.egressIds.length}</dd>
        </div>
        <div>
          <dt>Provider attempts</dt>
          <dd>{archive.providerAttemptIds.length}</dd>
        </div>
      </dl>
    </section>
  );
}

function InventoryGaps({ gaps }: { gaps: ArchiveGap[] }) {
  if (gaps.length === 0) {
    return null;
  }

  return (
    <section className="panel">
      <h2>Inventory gaps</h2>
      <ul>
        {gaps.map((gap, index) => (
          <li key={`${gap.class}:${String(index)}`}>
            <strong>{gap.class}</strong> — {gap.detail}
          </li>
        ))}
      </ul>
    </section>
  );
}

function ArchiveObjectRow({ archiveId, object }: { archiveId: string; object: ArchiveObject }) {
  return (
    <tr>
      <td>
        <span className="mobileFieldLabel">Artifact</span>
        {object.label}
      </td>
      <td>
        <span className="mobileFieldLabel">Type and size</span>
        {object.contentType}
        <br />
        <span className="meta">{byteFormatter.format(object.size)}</span>
      </td>
      <td>
        <span className="mobileFieldLabel">Version and SHA-256</span>
        <code>{object.versionId}</code>
        <br />
        <code>{object.sha256}</code>
      </td>
      <td>
        <span className="mobileFieldLabel">Download</span>
        <DownloadButton archiveId={archiveId} label={object.label} objectId={object.id} />
      </td>
    </tr>
  );
}

function ArchiveObjectSection({
  archiveId,
  label,
  objects,
}: {
  archiveId: string;
  label: string;
  objects: ArchiveObject[];
}) {
  const artifactCount = `${String(objects.length)} ${objects.length === 1 ? "artifact" : "artifacts"} on this page`;

  return (
    <details className="archiveGroup">
      <summary className="archiveGroupSummary">
        <span className="archiveGroupSummaryContent">
          <strong>{label}</strong>
          <span className="meta">{artifactCount}</span>
        </span>
      </summary>
      {objects.length === 0 ? (
        <p className="muted archiveGroupEmpty">No artifacts in this group on this page.</p>
      ) : (
        <div className="tableWrap archiveTableWrap">
          <table className="archiveObjectTable">
            <caption className="srOnly">
              {label}, {artifactCount}
            </caption>
            <thead>
              <tr>
                <th>Artifact</th>
                <th>Type / size</th>
                <th>Version / SHA-256</th>
                <th>
                  <span className="srOnly">Action</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {objects.map((object) => (
                <ArchiveObjectRow archiveId={archiveId} object={object} key={object.id} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </details>
  );
}

function ArchiveObjectGroups({
  archiveId,
  groupedObjects,
}: {
  archiveId: string;
  groupedObjects: GroupedArchiveObjects;
}) {
  return ARCHIVE_GROUPS.map(({ group, label }) => (
    <ArchiveObjectSection
      archiveId={archiveId}
      key={group}
      label={label}
      objects={groupedObjects[group]}
    />
  ));
}

function ArchivePagination({
  archiveId,
  nextCursor,
}: {
  archiveId: string;
  nextCursor: string | null;
}) {
  if (!nextCursor) {
    return null;
  }

  return (
    <nav className="archivePagination" aria-label="Archive object pages">
      <p className="meta">Each page replaces the objects shown above.</p>
      <Link
        className="button secondary"
        href={`/archives/${archiveId}?cursor=${encodeURIComponent(nextCursor)}`}
        rel="next"
      >
        View next objects page
      </Link>
    </nav>
  );
}

export default async function ArchivePage({ params, searchParams }: ArchivePageProps) {
  const { id } = await params;
  const { cursor } = await searchParams;
  const query = cursor ? { cursor } : {};
  const archive = await requirePageData<ArchiveView>("archives.get", { id }, query);
  const groupedObjects = groupArchiveObjects(archive.objects);

  return (
    <div className="stack">
      <ArchiveHeader status={archive.status} />
      <ArchiveFinalizingNotice status={archive.status} />
      <ActiveLegalHolds holds={archive.activeHolds} />
      <ArchiveProof archive={archive} />
      <InventoryGaps gaps={archive.gaps} />
      <ArchiveObjectGroups archiveId={archive.id} groupedObjects={groupedObjects} />
      <ArchivePagination archiveId={id} nextCursor={archive.nextCursor} />
      {archive.canAdminister && (
        <ArchiveAdminActions
          archiveId={archive.id}
          consultationId={archive.consultationId}
          activeHolds={archive.activeHolds}
        />
      )}
    </div>
  );
}
