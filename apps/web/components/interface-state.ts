export type DevicePreferenceKey = "transhooter.camera" | "transhooter.microphone";

type SessionStoragePort = Pick<Storage, "getItem" | "removeItem" | "setItem">;

type StorageAccess<T> = T | (() => T);

export function readDevicePreference(
  storage: StorageAccess<Pick<SessionStoragePort, "getItem">>,
  key: DevicePreferenceKey,
): string | undefined {
  try {
    const sessionStorage = typeof storage === "function" ? storage() : storage;
    return sessionStorage.getItem(key) || undefined;
  } catch {
    return undefined;
  }
}

export function persistDevicePreference(
  storage: StorageAccess<Pick<SessionStoragePort, "removeItem" | "setItem">>,
  key: DevicePreferenceKey,
  deviceId: string,
): void {
  try {
    const sessionStorage = typeof storage === "function" ? storage() : storage;
    if (deviceId) {
      sessionStorage.setItem(key, deviceId);
      return;
    }
    sessionStorage.removeItem(key);
  } catch {
    // Storage is an optional convenience and can be blocked by browser privacy settings.
  }
}

export function isUnavailableSelectedDeviceError(cause: unknown): boolean {
  if (typeof cause !== "object" || cause === null || !("name" in cause)) {
    return false;
  }
  return cause.name === "NotFoundError" || cause.name === "OverconstrainedError";
}

export async function createWithDeviceFallback<T>(
  deviceId: string | undefined,
  create: (deviceId: string | undefined) => Promise<T>,
  forgetSelection: () => void,
): Promise<T> {
  try {
    return await create(deviceId);
  } catch (cause) {
    if (!deviceId || !isUnavailableSelectedDeviceError(cause)) {
      throw cause;
    }
    forgetSelection();
    return create(undefined);
  }
}

export type ExclusiveActionGate = {
  leave(): void;
  tryEnter(): boolean;
};

export function createExclusiveActionGate(): ExclusiveActionGate {
  let active = false;
  return {
    tryEnter() {
      if (active) {
        return false;
      }
      active = true;
      return true;
    },
    leave() {
      active = false;
    },
  };
}
