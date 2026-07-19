export type DevicePreferenceKey = "transhooter.camera" | "transhooter.microphone";

type SessionStoragePort = Pick<Storage, "removeItem" | "setItem">;

export function persistDevicePreference(
  storage: SessionStoragePort,
  key: DevicePreferenceKey,
  deviceId: string,
): void {
  if (deviceId) {
    storage.setItem(key, deviceId);
    return;
  }
  storage.removeItem(key);
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
