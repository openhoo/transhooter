import { type Clock, DomainError, type TokenHasher } from "../domain/model";
import type { InternalPrincipal, InternalPrincipalVerifier } from "../ports/index";

type HeaderMap = Readonly<Record<string, string | readonly string[] | undefined>>;

interface ServiceJwtClaims {
  issuer: string;
  audience: string | readonly string[];
  subject: string;
  expiresAt: Date;
}

interface KubernetesSubjectRegistration {
  service: InternalPrincipal["service"];
  permissions: readonly string[];
}

export interface BearerRegistration {
  service: InternalPrincipal["service"];
  tokenHash: string;
  subject: string;
  permissions: readonly string[];
  notBefore: Date;
  expiresAt: Date;
}

export interface ServiceJwtVerifier {
  verify(jwt: string): Promise<ServiceJwtClaims>;
}

export class ComposeBearerVerifier implements InternalPrincipalVerifier {
  constructor(
    private readonly registrations: readonly BearerRegistration[],
    private readonly hasher: TokenHasher,
    private readonly clock: Clock,
  ) {}

  async verify(headers: HeaderMap): Promise<InternalPrincipal> {
    const authorization = header(headers, "authorization");
    if (!authorization?.startsWith("Bearer ")) {
      throw new DomainError("UNAUTHORIZED_INTERNAL");
    }

    const tokenHash = this.hasher.sha256(authorization.slice(7));
    const now = this.clock.now();
    const registration = this.registrations.find(
      (candidate) =>
        candidate.tokenHash === tokenHash &&
        candidate.notBefore <= now &&
        candidate.expiresAt > now,
    );
    if (!registration) {
      throw new DomainError("UNAUTHORIZED_INTERNAL");
    }

    return {
      service: registration.service,
      subject: registration.subject,
      permissions: registration.permissions,
    };
  }
}

export class KubernetesServiceAccountVerifier implements InternalPrincipalVerifier {
  constructor(
    private readonly jwt: ServiceJwtVerifier,
    private readonly issuer: string,
    private readonly audience: string,
    private readonly subjects: Readonly<Record<string, KubernetesSubjectRegistration>>,
    private readonly clock: Clock,
  ) {}

  async verify(headers: HeaderMap): Promise<InternalPrincipal> {
    const authorization = header(headers, "authorization");
    if (!authorization?.startsWith("Bearer ")) {
      throw new DomainError("UNAUTHORIZED_INTERNAL");
    }

    const claims = await this.jwt.verify(authorization.slice(7));
    const audiences = typeof claims.audience === "string" ? [claims.audience] : claims.audience;
    const registration = this.subjects[claims.subject];
    if (claims.issuer !== this.issuer) {
      throw new DomainError("UNAUTHORIZED_INTERNAL");
    }
    if (!audiences.includes(this.audience)) {
      throw new DomainError("UNAUTHORIZED_INTERNAL");
    }
    if (claims.expiresAt <= this.clock.now()) {
      throw new DomainError("UNAUTHORIZED_INTERNAL");
    }
    if (!registration) {
      throw new DomainError("UNAUTHORIZED_INTERNAL");
    }

    return {
      service: registration.service,
      subject: claims.subject,
      permissions: registration.permissions,
    };
  }
}

function header(headers: HeaderMap, name: string): string | undefined {
  const value = headers[name] ?? headers[name.toLowerCase()];
  if (typeof value === "string" || value === undefined) {
    return value;
  }
  return value[0];
}
