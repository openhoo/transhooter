import type { Metadata } from "next";
import {
  ArchiveAdminActions,
  ArchivePagination,
  DownloadButton,
  RefreshArchiveStatus,
} from "@/components/archive-actions";
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
  searchParams: Promise<{
    cursor?: string | string[];
    previous?: string | string[];
  }>;
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
  const complete = status === "complete";

  return (
    <header className="flex flex-col gap-3 border-b border-border pb-5 lg:flex-row lg:items-end lg:justify-between">
      <div>
        <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">
          Recorded consultation
        </p>
        <h1 className="mt-1 font-serif text-2xl font-semibold tracking-tight text-foreground md:text-3xl">
          Archive evidence
        </h1>
        <p className="mt-2 max-w-2xl text-sm leading-relaxed text-muted-foreground">
          Reconciled recordings, captions, provider exchanges, and the signed inventory for this
          consultation.
        </p>
      </div>
      <span
        className={`inline-flex w-fit items-center rounded-full px-3 py-1 text-xs font-semibold ${complete ? "bg-verified text-verified-foreground" : "bg-attention text-attention-foreground"}`}
      >
        {status}
      </span>
    </header>
  );
}

function ArchiveFinalizingNotice({ status }: { status: string }) {
  const isFinalizing = status === "pending" || status === "recording" || status === "reconciling";

  if (!isFinalizing) return null;

  return (
    <section
      className="rounded-md border border-attention/70 bg-attention/30 p-4"
      aria-labelledby="archive-finalizing-title"
    >
      <h2 className="font-serif text-base font-semibold" id="archive-finalizing-title">
        Finalizing the archive
      </h2>
      <p className="mt-1 text-sm leading-relaxed text-muted-foreground">
        Original recordings and provider evidence are still being reconciled. This page will show
        verified versions and checksums when available.
      </p>
      <div className="mt-3">
        <RefreshArchiveStatus />
      </div>
    </section>
  );
}

function ActiveLegalHolds({ holds }: { holds: ArchiveHold[] }) {
  if (holds.length === 0) return null;

  return (
    <section
      className="rounded-md border border-attention/70 bg-attention/30 p-4"
      aria-labelledby="active-holds-title"
    >
      <h2 className="font-serif text-base font-semibold" id="active-holds-title">
        Active legal holds
      </h2>
      <ul className="mt-2 space-y-1 text-sm text-muted-foreground">
        {holds.map((hold) => (
          <li key={hold.id}>
            {hold.reason} <span className="font-mono text-xs">({hold.id})</span>
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
  if (!hasProof) return null;

  const proof = [
    ["Inventory version", archive.inventoryVersion ?? "Pending"],
    ["Inventory SHA-256", archive.inventorySha256 ?? "Pending"],
    ["Egress jobs", String(archive.egressIds.length)],
    ["Provider attempts", String(archive.providerAttemptIds.length)],
  ] as const;

  return (
    <section
      className="rounded-md border border-border bg-card"
      aria-labelledby="archive-proof-title"
    >
      <div className="border-b border-border px-5 py-4">
        <h2 className="font-serif text-lg font-semibold" id="archive-proof-title">
          Archive proof
        </h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Signed inventory and persisted processing identifiers.
        </p>
      </div>
      <dl className="grid gap-px bg-border sm:grid-cols-2">
        {proof.map(([label, value]) => (
          <div className="min-w-0 bg-card px-5 py-4" key={label}>
            <dt className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">
              {label}
            </dt>
            <dd className="mt-2 break-all font-mono text-sm text-foreground">{value}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}

function InventoryGaps({ gaps }: { gaps: ArchiveGap[] }) {
  if (gaps.length === 0) return null;

  return (
    <section
      className="rounded-md border border-destructive/40 bg-destructive/5 p-5"
      aria-labelledby="inventory-gaps-title"
    >
      <h2 className="font-serif text-lg font-semibold text-destructive" id="inventory-gaps-title">
        Inventory gaps
      </h2>
      <ul className="mt-3 space-y-2 text-sm leading-relaxed text-muted-foreground">
        {gaps.map((gap, index) => (
          <li key={`${gap.class}:${String(index)}`}>
            <strong className="text-foreground">{gap.class}</strong> — {gap.detail}
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
    <details
      className="group overflow-hidden rounded-md border border-border bg-card"
      open={objects.length > 0}
    >
      <summary className="flex min-h-16 cursor-pointer list-none items-center justify-between gap-4 px-5 py-4 marker:hidden">
        <span className="font-serif text-base font-semibold text-foreground">{label}</span>
        <span className="shrink-0 text-xs text-muted-foreground">{artifactCount}</span>
      </summary>
      {objects.length === 0 ? (
        <p className="border-t border-border px-5 py-4 text-sm text-muted-foreground">
          No artifacts in this group on this page.
        </p>
      ) : (
        <div className="overflow-x-auto border-t border-border">
          <table className="archiveObjectTable w-full text-sm">
            <caption className="srOnly">
              {label}, {artifactCount}
            </caption>
            <thead className="bg-secondary">
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

export function lastArchiveQueryValue(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value.at(-1) : value;
}

export default async function ArchivePage({ params, searchParams }: ArchivePageProps) {
  const { id } = await params;
  const { cursor, previous } = await searchParams;
  const currentCursor = lastArchiveQueryValue(cursor);
  const previousCursor = lastArchiveQueryValue(previous);
  const query = currentCursor ? { cursor: currentCursor } : {};
  const archive = await requirePageData<ArchiveView>("archives.get", { id }, query);
  const groupedObjects = groupArchiveObjects(archive.objects);

  return (
    <div className="flex flex-col gap-6">
      <ArchiveHeader status={archive.status} />
      <ArchiveFinalizingNotice status={archive.status} />
      <ActiveLegalHolds holds={archive.activeHolds} />
      <ArchiveProof archive={archive} />
      <InventoryGaps gaps={archive.gaps} />
      <div className="flex flex-col gap-4">
        <ArchiveObjectGroups archiveId={archive.id} groupedObjects={groupedObjects} />
      </div>
      <ArchivePagination
        archiveId={id}
        currentCursor={currentCursor}
        nextCursor={archive.nextCursor}
        previousCursor={previousCursor}
      />
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
