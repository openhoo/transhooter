import { describe, expect, it } from "bun:test";
import { createHash } from "node:crypto";
import { type BearerRegistration, ComposeBearerVerifier } from "../src/auth/internal-principal";

const NOW = new Date("2026-01-01T00:00:00.000Z");
const hasher = {
  sha256(value: Uint8Array | string): string {
    return createHash("sha256").update(value).digest("hex");
  },
};
const ACTIVE_CONTROL_BEARER = {
  service: "control-worker",
  subject: "control-a",
  permissions: ["effects:dispatch"],
  tokenHash: hasher.sha256("secret"),
  notBefore: new Date(NOW.getTime() - 1),
  expiresAt: new Date(NOW.getTime() + 1),
} satisfies BearerRegistration;

describe("ComposeBearerVerifier", () => {
  it("maps only an active pre-hashed service bearer", async () => {
    const verifier = new ComposeBearerVerifier([ACTIVE_CONTROL_BEARER], hasher, { now: () => NOW });

    await expect(verifier.verify({ authorization: "Bearer secret" })).resolves.toEqual({
      service: "control-worker",
      subject: "control-a",
      permissions: ["effects:dispatch"],
    });
    await expect(verifier.verify({ authorization: "Bearer wrong" })).rejects.toThrowError(
      /UNAUTHORIZED_INTERNAL/,
    );
  });
});
