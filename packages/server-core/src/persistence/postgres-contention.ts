export interface PostgresContentionDependencies {
  readonly sleep: (milliseconds: number) => Promise<void>;
  readonly jitter: (exclusiveMaximum: number) => number;
}

const defaultDependencies: PostgresContentionDependencies = {
  sleep: (milliseconds) => new Promise<void>((resolve) => setTimeout(resolve, milliseconds)),
  jitter: (exclusiveMaximum) => Math.floor(Math.random() * exclusiveMaximum),
};

export function postgresSqlState(error: unknown, inspected = new Set<object>()): string | null {
  if (typeof error !== "object" || error === null || inspected.has(error)) {
    return null;
  }
  inspected.add(error);

  const record = error as Record<string, unknown>;
  const meta =
    typeof record.meta === "object" && record.meta !== null
      ? (record.meta as Record<string, unknown>)
      : null;
  const driverAdapterError =
    typeof meta?.driverAdapterError === "object" && meta.driverAdapterError !== null
      ? (meta.driverAdapterError as Record<string, unknown>)
      : null;
  const driverCause =
    typeof driverAdapterError?.cause === "object" && driverAdapterError.cause !== null
      ? (driverAdapterError.cause as Record<string, unknown>)
      : null;
  if (typeof driverCause?.originalCode === "string") {
    return driverCause.originalCode;
  }
  if (typeof record.originalCode === "string") {
    return record.originalCode;
  }
  if (typeof record.code === "string" && record.code !== "P2010") {
    return record.code;
  }

  const causeCode = postgresSqlState(record.cause, inspected);
  return causeCode ?? (typeof record.code === "string" ? record.code : null);
}

export async function retryPostgresContention<T>(
  operation: () => Promise<T>,
  dependencies: PostgresContentionDependencies = defaultDependencies,
): Promise<T> {
  for (let attempt = 1; ; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      const code = postgresSqlState(error);
      if (attempt >= 5 || (code !== "40P01" && code !== "40001")) {
        throw error;
      }
      const backoffMs = Math.min(250, 10 * 2 ** (attempt - 1)) + dependencies.jitter(10);
      await dependencies.sleep(backoffMs);
    }
  }
}
