"use client";

import { useMemo, useState } from "react";
import { api } from "@/lib/browser/browser-api";
import { createExclusiveActionGate } from "./interface-state";

type Direction = {
  id: string;
  profileId: string;
  profile: string;
  revision: number;
  source: string;
  target: string;
  providers: string;
  region: string;
  enabled: boolean;
  freshAt: string;
};

type LanguageAdminProps = {
  directions: Direction[];
};

const refreshedAtFormatter = new Intl.DateTimeFormat("en", {
  dateStyle: "medium",
  timeStyle: "short",
});

type ProviderStageParser = (value: string) => unknown;

function computeProviderStageLabel(value: string, parse: ProviderStageParser): string {
  try {
    const snapshot = parse(value) as Record<string, unknown>;
    const labels: string[] = [];
    for (const [stage, details] of Object.entries(snapshot)) {
      if (!details || typeof details !== "object") continue;
      const provider = Reflect.get(details, "provider");
      const model = Reflect.get(details, "model");
      const bypass = Reflect.get(details, "bypass");
      if (bypass === true) {
        labels.push(`${stage.toUpperCase()}: bypass`);
      } else if (typeof provider === "string") {
        labels.push(
          `${stage.toUpperCase()}: ${provider}${typeof model === "string" ? ` · ${model}` : ""}`,
        );
      }
    }
    return labels.length > 0 ? labels.join("\n") : "Configured provider stages";
  } catch {
    return value;
  }
}

export function createProviderStageFormatter(
  parse: ProviderStageParser = JSON.parse,
): (value: string) => string {
  const cache = new Map<string, string>();
  return (value) => {
    const cached = cache.get(value);
    if (cached !== undefined) return cached;

    const label = computeProviderStageLabel(value, parse);
    cache.set(value, label);
    return label;
  };
}

export function LanguageAdmin({ directions }: LanguageAdminProps) {
  const [items, setItems] = useState(directions);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState<string | null>(null);
  const [actionGate] = useState(createExclusiveActionGate);
  const [formatProviderStages] = useState(createProviderStageFormatter);
  const renderedItems = useMemo(
    () =>
      items.map((item) => ({
        item,
        providerStages: formatProviderStages(item.providers),
      })),
    [formatProviderStages, items],
  );

  async function toggle(item: Direction) {
    if (!actionGate.tryEnter()) {
      return;
    }
    setBusy(item.id);
    setError("");

    try {
      await api("/api/admin/languages", {
        method: "POST",
        body: JSON.stringify({
          directionId: item.id,
          profileId: item.profileId,
          enabled: !item.enabled,
          profileRevision: item.revision,
        }),
      });
      setItems((current) =>
        current.map((candidate) =>
          candidate.id === item.id ? { ...candidate, enabled: !candidate.enabled } : candidate,
        ),
      );
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Language direction could not be updated");
    } finally {
      setBusy(null);
      actionGate.leave();
    }
  }

  return (
    <div className="flex flex-col gap-4" aria-busy={busy !== null}>
      <div className="overflow-x-auto rounded-md border border-border bg-card">
        <table className="w-full text-sm">
          <thead className="bg-secondary">
            <tr>
              <th className="font-semibold text-foreground">Profile</th>
              <th className="font-semibold text-foreground">Direction</th>
              <th className="font-semibold text-foreground">Provider stages</th>
              <th className="font-semibold text-foreground">Region / last verified</th>
              <th className="text-right font-semibold text-foreground">Offered</th>
            </tr>
          </thead>
          <tbody>
            {renderedItems.map(({ item, providerStages }) => (
              <tr className={item.enabled ? "" : "opacity-60"} key={item.id}>
                <td>
                  <span className="font-medium">{item.profile}</span>
                  <br />
                  <span className="font-mono text-xs text-muted-foreground">
                    Revision {item.revision}
                  </span>
                </td>
                <td className="font-medium">
                  {item.source} → {item.target}
                </td>
                <td>
                  <span className="whitespace-pre-line text-xs leading-relaxed text-muted-foreground">
                    {providerStages}
                  </span>
                </td>
                <td>
                  <span className="inline-flex rounded-full border border-border px-2 py-0.5 font-mono text-xs">
                    {item.region}
                  </span>
                  <br />
                  <span className="mt-1 inline-block text-xs text-muted-foreground">
                    {refreshedAtFormatter.format(new Date(item.freshAt))}
                  </span>
                </td>
                <td className="text-right">
                  <button
                    aria-label={`${item.enabled ? "Disable" : "Enable"} ${item.source} to ${item.target} for ${item.profile}`}
                    aria-pressed={item.enabled}
                    className={`inline-flex min-h-9 min-w-20 items-center justify-center rounded-md px-3 text-sm font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${item.enabled ? "bg-primary text-primary-foreground" : "border border-border bg-background text-foreground hover:bg-secondary"}`}
                    disabled={busy !== null}
                    type="button"
                    onClick={() => {
                      void toggle(item);
                    }}
                  >
                    {busy === item.id ? "Updating…" : item.enabled ? "Enabled" : "Enable"}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {items.length === 0 && (
        <section className="rounded-md border border-border bg-card p-6">
          <h2 className="font-serif text-lg font-semibold">No complete provider directions</h2>
          <p className="mt-2 text-sm text-muted-foreground">
            Refresh provider capabilities, then reload this catalog before enabling a language.
          </p>
          <button
            className="mt-4 inline-flex min-h-9 items-center justify-center rounded-md border border-border bg-background px-3 text-sm font-medium hover:bg-secondary"
            type="button"
            onClick={() => {
              window.location.reload();
            }}
          >
            Reload language catalog
          </button>
        </section>
      )}

      {error && (
        <p
          className="rounded-md border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive"
          role="alert"
        >
          {error}
        </p>
      )}
    </div>
  );
}
