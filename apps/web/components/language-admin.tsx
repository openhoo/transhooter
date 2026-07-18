"use client";

import { useState } from "react";
import { api } from "@/lib/browser-api";

type Direction = {
  id: string;
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

export function LanguageAdmin({ directions }: LanguageAdminProps) {
  const [items, setItems] = useState(directions);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState<string | null>(null);

  async function toggle(item: Direction) {
    setBusy(item.id);
    setError("");

    try {
      await api("/api/admin/languages", {
        method: "POST",
        body: JSON.stringify({
          directionId: item.id,
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
    }
  }

  return (
    <div className="stack">
      <div className="tableWrap">
        <table>
          <thead>
            <tr>
              <th>Profile</th>
              <th>Direction</th>
              <th>Stages</th>
              <th>Region / refreshed</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.id}>
                <td>
                  {item.profile}
                  <br />
                  <span className="meta">Revision {item.revision}</span>
                </td>
                <td>
                  {item.source} → {item.target}
                </td>
                <td>{item.providers}</td>
                <td>
                  {item.region}
                  <br />
                  <span className="meta">
                    {refreshedAtFormatter.format(new Date(item.freshAt))}
                  </span>
                </td>
                <td>
                  <button
                    className={`button ${item.enabled ? "secondary" : ""}`}
                    disabled={busy === item.id}
                    type="button"
                    onClick={() => {
                      void toggle(item);
                    }}
                  >
                    {busy === item.id ? "Updating…" : item.enabled ? "Disable" : "Enable"}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {items.length === 0 && (
        <section className="empty flat">
          <h2>No complete provider directions</h2>
          <p>Refresh provider capabilities before enabling a language.</p>
        </section>
      )}

      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
    </div>
  );
}
