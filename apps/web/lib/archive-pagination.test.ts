import { describe, expect, mock, test } from "bun:test";
import { previousArchiveCursor, rememberArchiveCursor } from "../components/archive-actions.tsx";

mock.module("server-only", () => ({}));
// Import after replacing Next's server-only guard so the page boundary can run in Bun.
const { lastArchiveQueryValue } = await import("../app/archives/[id]/page.tsx");

class MemoryStorage {
  readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }
}

describe("archive query normalization", () => {
  test("uses the last repeated cursor value", () => {
    expect(lastArchiveQueryValue(["stale-cursor", "current-cursor"])).toBe("current-cursor");
  });

  test("preserves single and absent cursor values", () => {
    expect(lastArchiveQueryValue("current-cursor")).toBe("current-cursor");
    expect(lastArchiveQueryValue(undefined)).toBeUndefined();
  });
});

describe("archive pagination cursor state", () => {
  test("retains immediate predecessors without growing query-string history", () => {
    const storage = new MemoryStorage();
    const archiveId = "10000000-0000-4000-8000-000000000001";

    rememberArchiveCursor(storage, archiveId, "cursor-2", "__first__");
    rememberArchiveCursor(storage, archiveId, "cursor-3", "cursor-2");
    rememberArchiveCursor(storage, archiveId, "cursor-4", "cursor-3");

    expect(previousArchiveCursor(storage, archiveId, "cursor-4")).toBe("cursor-3");
    expect(previousArchiveCursor(storage, archiveId, "cursor-3")).toBe("cursor-2");
  });

  test("bounds session state across arbitrarily many forward pages", () => {
    const storage = new MemoryStorage();
    const archiveId = "10000000-0000-4000-8000-000000000002";

    for (let page = 1; page <= 400; page += 1) {
      rememberArchiveCursor(
        storage,
        archiveId,
        `cursor-${String(page + 1)}`,
        page === 1 ? "__first__" : `cursor-${String(page)}`,
      );
    }

    const serialized = storage.getItem(`transhooter.archive-cursors.${archiveId}`);
    expect(serialized).not.toBeNull();
    expect(JSON.parse(serialized ?? "[]")).toHaveLength(128);
    expect(previousArchiveCursor(storage, archiveId, "cursor-401")).toBe("cursor-400");
  });
});
