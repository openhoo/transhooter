import { createHash } from "node:crypto";
import { lookup } from "node:dns";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";

export function createConsultationArchiveProof(context) {
  const {
    apiJson,
    archiveObjectCeiling,
    archivePageCeiling,
    bounded,
    boundedPage,
    objectDownloadTimeoutMs,
  } = context;

  function archiveObjectGroup(objectClass) {
    if (objectClass.includes("composite")) return "composite";
    if (objectClass.includes("participant") || objectClass.includes("original")) return "original";
    if (objectClass.includes("interpret") || objectClass.includes("tts")) return "interpretation";
    if (objectClass.includes("caption") || objectClass.includes("vtt")) return "captions";
    if (objectClass.includes("inventory") || objectClass.includes("checkpoint")) return "inventory";
    return "pipeline";
  }

  async function allArchiveObjects(page, archiveId) {
    const objects = [];
    const seenCursors = new Set();
    let cursor = null;
    let pageCount = 0;
    do {
      if (pageCount >= archivePageCeiling) {
        throw new Error(`archive pagination exceeded ${archivePageCeiling} pages`);
      }
      pageCount += 1;
      const suffix = cursor ? `?cursor=${encodeURIComponent(cursor)}` : "";
      const result = await apiJson(page, `/api/archives/${archiveId}/objects${suffix}`);
      if (result.status !== 200 || !Array.isArray(result.body?.objects)) {
        throw new Error(`archive object listing failed: ${result.status}`);
      }
      if (objects.length + result.body.objects.length > archiveObjectCeiling) {
        throw new Error(`archive object listing exceeded ${archiveObjectCeiling} objects`);
      }
      for (const object of result.body.objects) {
        const objectClass = object.objectClass ?? object.object_class;
        objects.push({
          id: object.id,
          key: object.key,
          group: archiveObjectGroup(objectClass),
          label: objectClass,
          contentType: object.contentType ?? object.content_type,
          size: Number(object.size),
          sha256: object.sha256,
          s3Checksum: object.s3Checksum ?? object.s3_checksum,
          versionId: object.versionId ?? object.version_id,
        });
      }
      cursor = result.body.cursor;
      if (cursor !== null && cursor !== undefined) {
        if (typeof cursor !== "string" || cursor.length === 0) {
          throw new Error("archive pagination returned an invalid cursor");
        }
        if (seenCursors.has(cursor)) {
          throw new Error(`archive pagination repeated cursor ${cursor}`);
        }
        seenCursors.add(cursor);
      }
    } while (cursor);
    if (new Set(objects.map(({ id }) => id)).size !== objects.length) {
      throw new Error("archive object pagination returned duplicate object IDs");
    }
    return objects;
  }

  async function presignedObjectUrl(page, archiveId, objectId) {
    return await boundedPage(page, `authorize archive object ${objectId}`, ({ timeoutMs }) =>
      page.evaluate(
        async ({ id, archiveObjectId, timeoutMs }) => {
          const csrf = document.cookie
            .split("; ")
            .find((part) => part.startsWith("csrf="))
            ?.slice(5);
          if (!csrf) throw new Error("CSRF cookie is unavailable for archive verification");
          const response = await fetch(`/api/archives/${id}/download`, {
            method: "POST",
            credentials: "same-origin",
            cache: "no-store",
            headers: {
              "content-type": "application/json",
              "x-csrf-token": decodeURIComponent(csrf),
            },
            body: JSON.stringify({ archiveId: id, objectId: archiveObjectId }),
            signal: AbortSignal.timeout(timeoutMs),
          });
          const body = await response.json();
          if (!response.ok || typeof body.url !== "string") {
            throw new Error(`archive download authorization failed (${response.status})`);
          }
          return body.url;
        },
        { id: archiveId, archiveObjectId: objectId, timeoutMs },
      ),
    );
  }

  const crc64NvmeTable = Object.freeze(
    Array.from({ length: 256 }, (_, index) => {
      let value = BigInt(index);
      for (let bit = 0; bit < 8; bit += 1) {
        value = (value & 1n) === 1n ? (value >> 1n) ^ 0x9a6c9329ac4bc9b5n : value >> 1n;
      }
      return value;
    }),
  );

  function updateCrc64Nvme(crc, chunk) {
    let current = crc;
    for (const byte of chunk) {
      current = (current >> 8n) ^ crc64NvmeTable[Number((current ^ BigInt(byte)) & 0xffn)];
    }
    return current;
  }

  function encodeCrc64Nvme(crc) {
    let value = crc ^ 0xffffffffffffffffn;
    const bytes = Buffer.allocUnsafe(8);
    for (let index = 7; index >= 0; index -= 1) {
      bytes[index] = Number(value & 0xffn);
      value >>= 8n;
    }
    return bytes.toString("base64");
  }

  function crc32Table(polynomial) {
    return Object.freeze(
      Array.from({ length: 256 }, (_, index) => {
        let value = index;
        for (let bit = 0; bit < 8; bit += 1) {
          value = (value & 1) === 1 ? (value >>> 1) ^ polynomial : value >>> 1;
        }
        return value >>> 0;
      }),
    );
  }

  const crc32IeeeTable = crc32Table(0xedb88320);
  const crc32cTable = crc32Table(0x82f63b78);

  function updateCrc32(crc, chunk, table) {
    let current = crc;
    for (const byte of chunk) {
      current = (table[(current ^ byte) & 0xff] ^ (current >>> 8)) >>> 0;
    }
    return current;
  }

  function encodeCrc32(crc) {
    const bytes = Buffer.allocUnsafe(4);
    bytes.writeUInt32BE((crc ^ 0xffffffff) >>> 0);
    return bytes.toString("base64");
  }

  function headerValue(headers, name) {
    const value = headers[name];
    return Array.isArray(value) ? value[0] : value;
  }
  function canonicalBase64(value, byteLength) {
    try {
      const decoded = Buffer.from(value, "base64");
      return decoded.length === byteLength && decoded.toString("base64") === value;
    } catch {
      return false;
    }
  }

  function validS3Checksum(value) {
    if (canonicalBase64(value, 8)) return true;
    const separator = value.indexOf(":");
    if (separator < 1 || separator !== value.lastIndexOf(":")) return false;
    const algorithm = value.slice(0, separator);
    const encoded = value.slice(separator + 1);
    const byteLength = {
      CRC32: 4,
      CRC32C: 4,
      SHA256: 32,
    }[algorithm];
    return byteLength !== undefined && canonicalBase64(encoded, byteLength);
  }

  function checksumEvidence(declared, downloaded) {
    if (!declared.includes(":")) {
      return {
        computed: downloaded.checksums.crc64nvme,
        response: headerValue(downloaded.headers, "x-amz-checksum-crc64nvme"),
      };
    }
    const separator = declared.indexOf(":");
    const algorithm = declared.slice(0, separator);
    const headers = {
      CRC32: "x-amz-checksum-crc32",
      CRC32C: "x-amz-checksum-crc32c",
      SHA256: "x-amz-checksum-sha256",
    };
    const computed = {
      CRC32: downloaded.checksums.crc32,
      CRC32C: downloaded.checksums.crc32c,
      SHA256: downloaded.checksums.sha256,
    };
    const header = headers[algorithm];
    const digest = computed[algorithm];
    if (header === undefined || digest === undefined) {
      throw new Error(`unsupported S3 checksum algorithm: ${algorithm}`);
    }
    const responseDigest = headerValue(downloaded.headers, header);
    return {
      computed: `${algorithm}:${digest}`,
      response: responseDigest === undefined ? undefined : `${algorithm}:${responseDigest}`,
    };
  }

  function download(url, declaredSize, mapLocalhostToMinio = false, captureBody = false) {
    let outgoing;
    return bounded(
      "presigned archive object GET",
      ({ signal, timeoutMs }) =>
        new Promise((resolve, reject) => {
          const parsed = new URL(url);
          const request = parsed.protocol === "https:" ? httpsRequest : httpRequest;
          const operationTimeoutMs = Math.min(objectDownloadTimeoutMs, timeoutMs);
          const options = {
            headers: { "x-amz-checksum-mode": "ENABLED" },
            signal,
            ...(mapLocalhostToMinio
              ? {
                  lookup(hostname, lookupOptions, callback) {
                    lookup(hostname === "localhost" ? "minio" : hostname, lookupOptions, callback);
                  },
                }
              : {}),
          };
          let settled = false;
          const finish = (operation, value) => {
            if (settled) return;
            settled = true;
            operation(value);
          };
          outgoing = request(parsed, options, (response) => {
            let size = 0;
            const sha256 = createHash("sha256");
            let crc64Nvme = 0xffffffffffffffffn;
            let crc32 = 0xffffffff;
            let crc32c = 0xffffffff;
            const body = captureBody ? [] : null;
            response.on("data", (chunk) => {
              size += chunk.length;
              if (size > declaredSize) {
                outgoing.destroy(
                  new Error(`presigned object GET exceeded declared size ${String(declaredSize)}`),
                );
                return;
              }
              sha256.update(chunk);
              crc64Nvme = updateCrc64Nvme(crc64Nvme, chunk);
              crc32 = updateCrc32(crc32, chunk, crc32IeeeTable);
              crc32c = updateCrc32(crc32c, chunk, crc32cTable);
              body?.push(chunk);
            });
            response.on("aborted", () =>
              finish(reject, new Error("presigned object GET response was aborted")),
            );
            response.on("error", (error) => finish(reject, error));
            response.on("end", () => {
              if (response.statusCode !== 200) {
                finish(
                  reject,
                  new Error(`presigned object GET failed (${String(response.statusCode)})`),
                );
                return;
              }
              if (size === 0) {
                finish(reject, new Error("presigned archive object body is empty"));
                return;
              }
              const sha256Hex = sha256.digest("hex");
              finish(resolve, {
                size,
                sha256: sha256Hex,
                checksums: {
                  crc64nvme: encodeCrc64Nvme(crc64Nvme),
                  crc32: encodeCrc32(crc32),
                  crc32c: encodeCrc32(crc32c),
                  sha256: Buffer.from(sha256Hex, "hex").toString("base64"),
                },
                headers: response.headers,
                body: body === null ? null : Buffer.concat(body, size),
              });
            });
          });
          outgoing.setTimeout(operationTimeoutMs, () => {
            outgoing.destroy(
              new Error(`presigned object GET timed out after ${operationTimeoutMs}ms`),
            );
          });
          outgoing.on("error", (error) => finish(reject, error));
          outgoing.end();
        }),
      (error) => outgoing?.destroy(error),
    );
  }

  async function independentlyVerifyObject(page, archiveId, object, captureBody = false) {
    const url = await presignedObjectUrl(page, archiveId, object.id);
    let downloaded;
    try {
      downloaded = await download(url, object.size, false, captureBody);
    } catch (error) {
      if (new URL(url).hostname !== "localhost") throw error;
      downloaded = await download(url, object.size, true, captureBody);
    }
    const checksum = checksumEvidence(object.s3Checksum, downloaded);
    const metadataHash = headerValue(downloaded.headers, "x-amz-meta-sha256");
    const responseVersion = headerValue(downloaded.headers, "x-amz-version-id");
    const responseChecksum = checksum.response;
    const contentLength = headerValue(downloaded.headers, "content-length");
    const contentType = headerValue(downloaded.headers, "content-type");
    if (
      downloaded.size !== object.size ||
      Number(contentLength) !== object.size ||
      downloaded.sha256 !== object.sha256 ||
      checksum.computed !== object.s3Checksum ||
      responseChecksum !== object.s3Checksum ||
      (metadataHash !== undefined && metadataHash !== object.sha256) ||
      contentType !== object.contentType ||
      responseVersion !== object.versionId
    ) {
      throw new Error(
        `independent object verification failed for ${object.id}: ` +
          JSON.stringify({
            expectedSize: object.size,
            downloadedSize: downloaded.size,
            contentLength,
            expectedContentType: object.contentType,
            responseContentType: contentType,
            expectedHash: object.sha256,
            actualHash: downloaded.sha256,
            metadataHash,
            expectedChecksum: object.s3Checksum,
            actualChecksum: checksum.computed,
            responseChecksum,
            expectedVersion: object.versionId,
            responseVersion,
          }),
      );
    }
    return downloaded;
  }

  async function independentlyVerifyObjects(page, archiveId, objects, concurrency = 4) {
    const failures = new Array(objects.length);
    let nextIndex = 0;
    const workers = Array.from({ length: Math.min(concurrency, objects.length) }, async () => {
      for (;;) {
        const index = nextIndex;
        nextIndex += 1;
        if (index >= objects.length) return;
        const object = objects[index];
        try {
          await independentlyVerifyObject(page, archiveId, object);
        } catch (error) {
          failures[index] = new Error(`${object.id}: ${error?.message ?? String(error)}`, {
            cause: error,
          });
        }
      }
    });
    await Promise.all(workers);
    const objectFailures = failures.filter(Boolean);
    if (objectFailures.length > 0) {
      throw new AggregateError(
        objectFailures,
        `independent archive verification failed for object IDs: ${objectFailures
          .map((failure) => failure.message.split(":", 1)[0])
          .join(", ")}`,
      );
    }
    return objects.length;
  }

  function archiveObjectProof(object) {
    return {
      id: object.id ?? object.objectId,
      key: object.key,
      label: object.label ?? object.objectClass ?? object.class,
      versionId: object.versionId,
      size: Number(object.size),
      sha256: object.sha256,
      s3Checksum: object.s3Checksum ?? object.checksum,
      contentType: object.contentType,
    };
  }

  function assertFinalInventoryBinding(
    archive,
    consultationId,
    listedObjects,
    finalObject,
    inventory,
  ) {
    if (
      inventory?.status !== "complete" ||
      !Array.isArray(inventory.objects) ||
      inventory.objects.length === 0 ||
      !Array.isArray(inventory.missing) ||
      !Array.isArray(inventory.errors)
    ) {
      throw new Error("downloaded final inventory has an invalid terminal shape");
    }
    if (inventory.missing.length !== 0 || inventory.errors.length !== 0) {
      throw new Error(
        `complete final inventory contains missing/errors: ${JSON.stringify({
          missing: inventory.missing,
          errors: inventory.errors,
        })}`,
      );
    }
    if ((archive.gaps ?? []).length !== inventory.missing.length) {
      throw new Error("archive detail gaps diverge from downloaded final inventory");
    }
    if (inventory.consultationId !== consultationId) {
      throw new Error("downloaded final inventory belongs to another consultation");
    }
    const listedMembers = listedObjects.filter((object) => object.id !== finalObject.id);
    const listedById = new Map(
      listedMembers.map((object) => {
        const proof = archiveObjectProof(object);
        return [proof.id, proof];
      }),
    );
    const inventoryProofs = inventory.objects.map(archiveObjectProof);
    if (
      listedById.size !== listedMembers.length ||
      new Set(inventoryProofs.map((object) => object.id)).size !== inventoryProofs.length ||
      inventoryProofs.length !== listedMembers.length
    ) {
      throw new Error(
        "final inventory and downloaded listing do not have the same unique object IDs",
      );
    }
    for (const proof of inventoryProofs) {
      const listed = listedById.get(proof.id);
      if (!listed || JSON.stringify(listed) !== JSON.stringify(proof)) {
        throw new Error(
          `final inventory object does not exactly bind listing object ${String(proof.id)}: ` +
            JSON.stringify({ inventory: proof, listed }),
        );
      }
    }
    return inventoryProofs;
  }

  function assertAttemptArchiveEvidence(providerAttemptGroups, inventoryObjects, consultationId) {
    const meetingPrefix = `v1/meetings/${consultationId}/`;
    for (const group of providerAttemptGroups) {
      for (const attemptId of group.attemptIds ?? []) {
        const terminalPath = `/pipeline/terminal/raw/${attemptId}/`;
        const exchangePath = `/pipeline/${group.stage}/raw/${attemptId}/`;
        const terminal = inventoryObjects.find(
          (object) => object.key.startsWith(meetingPrefix) && object.key.includes(terminalPath),
        );
        const exchange = inventoryObjects.find(
          (object) => object.key.startsWith(meetingPrefix) && object.key.includes(exchangePath),
        );
        if (!terminal || !exchange || terminal.id === exchange.id) {
          throw new Error(
            `attempt ${attemptId} lacks distinct archived terminal/raw ${group.stage} evidence`,
          );
        }
      }
    }
  }

  return {
    allArchiveObjects,
    assertAttemptArchiveEvidence,
    assertFinalInventoryBinding,
    independentlyVerifyObject,
    independentlyVerifyObjects,
    validS3Checksum,
  };
}
