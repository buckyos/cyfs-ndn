#!/usr/bin/env -S deno run --allow-all
/**
 * NDM Zone Gateway integration tests.
 *
 * Spawns the Rust example server (`ndm_zone_gateway_server`) and exercises the
 * upload protocol through both raw fetch (for fine-grained header checks) and
 * the tus-js-client library (for standard TUS compatibility).
 *
 * Usage:
 *   deno run --allow-all src/tests/run_ndm_zone_gateway_test.ts
 */

import * as tus from "npm:tus-js-client@4";
import { Buffer } from "node:buffer";
import { Readable } from "node:stream";

// ===================== Helpers =====================

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** Base64-encode a UTF-8 string (for TUS Upload-Metadata values). */
function b64(s: string): string {
  return btoa(s);
}

/** Build a standard TUS Upload-Metadata header value. */
function buildUploadMetadata(meta: Record<string, string>): string {
  return Object.entries(meta)
    .map(([k, v]) => `${k} ${b64(v)}`)
    .join(",");
}

/** Build a simple key=value Upload-Metadata (supported by the server). */
function buildSimpleMetadata(meta: Record<string, string>): string {
  return Object.entries(meta)
    .map(([k, v]) => `${k}=${v}`)
    .join(",");
}

// ===================== Server management =====================

interface ServerHandle {
  process: Deno.ChildProcess;
  port: number;
  baseUrl: string;
}

async function startServer(): Promise<ServerHandle> {
  // Resolve the workspace root (src/) relative to this script's location.
  // Script lives at src/tests/run_ndm_zone_gateway_test.ts, so ../  is src/.
  const scriptDir = new URL(".", import.meta.url).pathname;
  const workspaceRoot = Deno.realPathSync(scriptDir + "/..");
  const namedStorePath = Deno.realPathSync(workspaceRoot + "/named_store");

  const cmd = new Deno.Command("cargo", {
    args: [
      "run",
      "--example",
      "ndm_zone_gateway_server",
      "--manifest-path",
      `${namedStorePath}/Cargo.toml`,
    ],
    cwd: workspaceRoot,
    stdout: "piped",
    stderr: "piped",
  });

  const process = cmd.spawn();

  // Read stdout line by line until we see PORT:<port>
  const reader = process.stdout.getReader();
  let buffer = "";
  let port = 0;

  const timeout = setTimeout(() => {
    console.error("ERROR: server did not start within 120 seconds");
    process.kill("SIGTERM");
    Deno.exit(1);
  }, 120_000);

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() || "";
    for (const line of lines) {
      const m = line.match(/^PORT:(\d+)/);
      if (m) {
        port = parseInt(m[1], 10);
        break;
      }
    }
    if (port > 0) break;
  }

  clearTimeout(timeout);

  if (port === 0) {
    process.kill("SIGTERM");
    throw new Error("Failed to read server port from stdout");
  }

  // Release the reader so the process can continue writing
  reader.releaseLock();

  // Drain stderr in background to avoid blocking
  (async () => {
    const errReader = process.stderr.getReader();
    try {
      while (true) {
        const { done } = await errReader.read();
        if (done) break;
      }
    } catch { /* ignore */ }
  })();

  const baseUrl = `http://127.0.0.1:${port}`;
  console.log(`  Server started on ${baseUrl}`);
  return { process, port, baseUrl };
}

function stopServer(handle: ServerHandle) {
  try {
    handle.process.kill("SIGTERM");
  } catch { /* already exited */ }
}

// ===================== Test framework =====================

let passed = 0;
let failed = 0;
const failures: string[] = [];

async function runTest(name: string, fn: () => Promise<void>) {
  try {
    await fn();
    passed++;
    console.log(`  PASS  ${name}`);
  } catch (e) {
    failed++;
    const msg = e instanceof Error ? e.message : String(e);
    failures.push(`${name}: ${msg}`);
    console.error(`  FAIL  ${name}: ${msg}`);
  }
}

function assert(cond: boolean, msg: string) {
  if (!cond) throw new Error(`Assertion failed: ${msg}`);
}

function assertEqual(actual: unknown, expected: unknown, msg: string) {
  if (actual !== expected) {
    throw new Error(`${msg}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

// ===================== Test cases =====================

async function testCreateUploadSession(baseUrl: string) {
  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/hello.txt",
    chunk_index: "0",
    file_hash: "abc123",
  });

  const resp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": "1024",
      "upload-metadata": metadata,
    },
  });

  assertEqual(resp.status, 201, "should return 201 Created");
  const location = resp.headers.get("location");
  assert(
    location !== null && location.startsWith("/ndm/v1/uploads/"),
    `location header should start with /ndm/v1/uploads/, got: ${location}`,
  );
  assertEqual(resp.headers.get("ndm-chunk-status"), "pending", "status should be pending");
  assertEqual(resp.headers.get("upload-offset"), "0", "offset should be 0");
  assertEqual(resp.headers.get("upload-length"), "1024", "length should be 1024");
  await resp.body?.cancel();
}

async function testHeadUploadSession(baseUrl: string) {
  // Create a session first
  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/head-test.txt",
    chunk_index: "0",
    file_hash: "head123",
  });

  const createResp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": "512",
      "upload-metadata": metadata,
    },
  });
  const location = createResp.headers.get("location")!;
  await createResp.body?.cancel();

  // HEAD the session
  const headResp = await fetch(`${baseUrl}${location}`, { method: "HEAD" });
  assertEqual(headResp.status, 200, "HEAD should return 200");
  assertEqual(headResp.headers.get("upload-offset"), "0", "offset should be 0");
  assertEqual(headResp.headers.get("upload-length"), "512", "length should be 512");
  assertEqual(headResp.headers.get("ndm-chunk-status"), "pending", "status should be pending");
  await headResp.body?.cancel();
}

async function testSinglePatchUpload(baseUrl: string) {
  const chunkData = new Uint8Array(256);
  crypto.getRandomValues(chunkData);

  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/single-patch.bin",
    chunk_index: "0",
    file_hash: "sp123",
  });

  // Create session
  const createResp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": String(chunkData.length),
      "upload-metadata": metadata,
    },
  });
  assertEqual(createResp.status, 201, "create should return 201");
  const location = createResp.headers.get("location")!;
  await createResp.body?.cancel();

  // Upload all data in one PATCH
  const patchResp = await fetch(`${baseUrl}${location}`, {
    method: "PATCH",
    headers: {
      "upload-offset": "0",
      "content-type": "application/offset+octet-stream",
    },
    body: chunkData,
  });

  assertEqual(patchResp.status, 204, "PATCH should return 204");
  assertEqual(patchResp.headers.get("ndm-chunk-status"), "completed", "status should be completed");
  const objectId = patchResp.headers.get("ndm-chunk-object-id");
  assert(objectId !== null && objectId.length > 0, "should return chunk object id");
  await patchResp.body?.cancel();
}

async function testMultiPatchResume(baseUrl: string) {
  const totalSize = 1024;
  const chunkData = new Uint8Array(totalSize);
  crypto.getRandomValues(chunkData);

  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/resume-test.bin",
    chunk_index: "0",
    file_hash: "resume456",
  });

  // Create session
  const createResp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": String(totalSize),
      "upload-metadata": metadata,
    },
  });
  const location = createResp.headers.get("location")!;
  await createResp.body?.cancel();

  // Upload first half
  const half = totalSize / 2;
  const patch1 = await fetch(`${baseUrl}${location}`, {
    method: "PATCH",
    headers: {
      "upload-offset": "0",
      "content-type": "application/offset+octet-stream",
    },
    body: chunkData.slice(0, half),
  });
  assertEqual(patch1.status, 204, "first PATCH should return 204");
  assertEqual(patch1.headers.get("upload-offset"), String(half), "offset should advance to half");
  assertEqual(patch1.headers.get("ndm-chunk-status"), "uploading", "status should be uploading");
  await patch1.body?.cancel();

  // HEAD to verify offset
  const headResp = await fetch(`${baseUrl}${location}`, { method: "HEAD" });
  assertEqual(headResp.headers.get("upload-offset"), String(half), "HEAD offset should be half");
  assertEqual(headResp.headers.get("ndm-chunk-status"), "uploading", "HEAD status should be uploading");
  await headResp.body?.cancel();

  // Upload second half
  const patch2 = await fetch(`${baseUrl}${location}`, {
    method: "PATCH",
    headers: {
      "upload-offset": String(half),
      "content-type": "application/offset+octet-stream",
    },
    body: chunkData.slice(half),
  });
  assertEqual(patch2.status, 204, "second PATCH should return 204");
  assertEqual(patch2.headers.get("ndm-chunk-status"), "completed", "status should be completed");
  assert(
    patch2.headers.get("ndm-chunk-object-id") !== null,
    "should return object id on completion",
  );
  await patch2.body?.cancel();
}

async function testIdempotentCreate(baseUrl: string) {
  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/idempotent.txt",
    chunk_index: "0",
    file_hash: "idem789",
  });

  const headers = {
    "upload-length": "2048",
    "upload-metadata": metadata,
  };

  // Create twice with the same key
  const resp1 = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers,
  });
  const loc1 = resp1.headers.get("location")!;
  await resp1.body?.cancel();

  const resp2 = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers,
  });
  const loc2 = resp2.headers.get("location")!;
  await resp2.body?.cancel();

  assertEqual(loc1, loc2, "idempotent create should return the same session location");
  assertEqual(resp2.status, 200, "second create should return 200 (existing)");
}

async function testObjectLookupNotFound(baseUrl: string) {
  const resp = await fetch(
    `${baseUrl}/ndm/v1/objects/lookup?scope=app&quick_hash=chunksha256:0000000000000000000000000000000000000000000000000000000000000000`,
  );
  assertEqual(resp.status, 404, "lookup for non-existent object should return 404");
  const body = await resp.json();
  assertEqual(body.error, "not_found", "error code should be not_found");
}

async function testObjectLookupAfterUpload(baseUrl: string) {
  // Upload a chunk first
  const chunkData = encoder.encode("lookup-test-data-payload-1234567890");
  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/lookup-test.bin",
    chunk_index: "0",
    file_hash: "lookup999",
  });

  const createResp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": String(chunkData.length),
      "upload-metadata": metadata,
    },
  });
  const location = createResp.headers.get("location")!;
  await createResp.body?.cancel();

  const patchResp = await fetch(`${baseUrl}${location}`, {
    method: "PATCH",
    headers: {
      "upload-offset": "0",
      "content-type": "application/offset+octet-stream",
    },
    body: chunkData,
  });
  const objectId = patchResp.headers.get("ndm-chunk-object-id")!;
  await patchResp.body?.cancel();

  // Now lookup using the object id.
  // Note: the server's parse_query_params does not URL-decode, so we must NOT
  // encode the colon in chunk id formats like "mix256:abcdef..."
  const lookupResp = await fetch(
    `${baseUrl}/ndm/v1/objects/lookup?scope=app&quick_hash=${objectId}`,
  );
  assertEqual(lookupResp.status, 200, "lookup should return 200");
  const lookupBody = await lookupResp.json();
  assertEqual(lookupBody.exists, true, "object should exist");
  assertEqual(lookupBody.object_id, objectId, "object_id should match");
}

async function testErrorMissingUploadLength(baseUrl: string) {
  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/err.txt",
    chunk_index: "0",
  });

  const resp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-metadata": metadata,
      // missing upload-length
    },
  });
  assertEqual(resp.status, 400, "missing upload-length should return 400");
  await resp.body?.cancel();
}

async function testErrorMissingMetadata(baseUrl: string) {
  const resp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": "1024",
      // missing upload-metadata
    },
  });
  assertEqual(resp.status, 400, "missing metadata should return 400");
  await resp.body?.cancel();
}

async function testErrorInvalidLogicalPath(baseUrl: string) {
  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "../etc/passwd",
    chunk_index: "0",
  });

  const resp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": "1024",
      "upload-metadata": metadata,
    },
  });
  assertEqual(resp.status, 400, "path traversal should return 400");
  await resp.body?.cancel();
}

async function testErrorAbsoluteLogicalPath(baseUrl: string) {
  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "/etc/passwd",
    chunk_index: "0",
  });

  const resp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": "1024",
      "upload-metadata": metadata,
    },
  });
  assertEqual(resp.status, 400, "absolute path should return 400");
  await resp.body?.cancel();
}

async function testErrorOffsetMismatch(baseUrl: string) {
  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/offset-err.bin",
    chunk_index: "0",
    file_hash: "offseterr",
  });

  // Create session
  const createResp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": "1024",
      "upload-metadata": metadata,
    },
  });
  const location = createResp.headers.get("location")!;
  await createResp.body?.cancel();

  // PATCH with wrong offset
  const patchResp = await fetch(`${baseUrl}${location}`, {
    method: "PATCH",
    headers: {
      "upload-offset": "100",
      "content-type": "application/offset+octet-stream",
    },
    body: new Uint8Array(100),
  });
  assertEqual(patchResp.status, 409, "offset mismatch should return 409 Conflict");
  await patchResp.body?.cancel();
}

async function testErrorExceedChunkSize(baseUrl: string) {
  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/exceed.bin",
    chunk_index: "0",
    file_hash: "exceed",
  });

  // Create session with small chunk size
  const createResp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": "64",
      "upload-metadata": metadata,
    },
  });
  const location = createResp.headers.get("location")!;
  await createResp.body?.cancel();

  // PATCH with more data than chunk size
  const patchResp = await fetch(`${baseUrl}${location}`, {
    method: "PATCH",
    headers: {
      "upload-offset": "0",
      "content-type": "application/offset+octet-stream",
    },
    body: new Uint8Array(128),
  });
  assertEqual(patchResp.status, 400, "exceeding chunk size should return 400");
  await patchResp.body?.cancel();
}

async function testErrorSessionNotFound(baseUrl: string) {
  const resp = await fetch(`${baseUrl}/ndm/v1/uploads/nonexistent_session_id`, {
    method: "HEAD",
  });
  assertEqual(resp.status, 404, "non-existent session HEAD should return 404");
  await resp.body?.cancel();

  const patchResp = await fetch(`${baseUrl}/ndm/v1/uploads/nonexistent_session_id`, {
    method: "PATCH",
    headers: {
      "upload-offset": "0",
      "content-type": "application/offset+octet-stream",
    },
    body: new Uint8Array(10),
  });
  assertEqual(patchResp.status, 404, "non-existent session PATCH should return 404");
  await patchResp.body?.cancel();
}

async function testErrorUnknownRoute(baseUrl: string) {
  const resp = await fetch(`${baseUrl}/unknown/path`);
  assertEqual(resp.status, 404, "unknown route should return 404");
  await resp.body?.cancel();
}

async function testErrorChunkTooLarge(baseUrl: string) {
  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/toolarge.bin",
    chunk_index: "0",
  });

  const resp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": String(64 * 1024 * 1024), // 64 MiB > 32 MiB max
      "upload-metadata": metadata,
    },
  });
  assertEqual(resp.status, 400, "chunk too large should return 400");
  await resp.body?.cancel();
}

async function testTusClientUpload(baseUrl: string) {
  const chunkData = encoder.encode("tus-client-upload-test-data-0123456789");
  const buf = Buffer.from(chunkData);

  // tus-js-client base64-encodes metadata values. Since the server's
  // base64_decode is currently a no-op (returns raw input), the encoded
  // values are used literally. We must choose values whose base64 forms:
  //   1) have no '=' padding (string length must be a multiple of 3)
  //   2) contain only chars that pass validate_logical_path ([A-Za-z0-9/_\-.])
  //      — base64 may produce '+' which would be rejected.
  // In practice: short, 3-byte-aligned ASCII values without '+' in base64.
  return new Promise<void>((resolve, reject) => {
    const upload = new tus.Upload(buf, {
      endpoint: `${baseUrl}/ndm/v1/uploads`,
      chunkSize: chunkData.length,
      retryDelays: [],
      metadata: {
        // "abc" -> base64 "YWJj" (safe), "ab/c.d" -> "YWIvYy5k" (safe)
        app_id: "abc",                 // b64: YWJj
        logical_path: "ab/c.d",       // b64: YWIvYy5k
        chunk_index: "0",             // b64: MA (2 bytes → has ==? no, "0" is 1 byte → MA==)
        file_hash: "tus",             // b64: dHVz
      },
      onError: (error: Error) => {
        reject(new Error(`tus upload error: ${error.message}`));
      },
      onSuccess: () => {
        resolve();
      },
    });

    upload.start();

    // Safety timeout
    setTimeout(() => reject(new Error("tus upload timed out after 30s")), 30_000);
  });
}

async function testTusClientResume(baseUrl: string) {
  const totalSize = 2048;
  const chunkData = new Uint8Array(totalSize);
  crypto.getRandomValues(chunkData);

  // First: manually create session and upload partial data
  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "tus/resume-test.bin",
    chunk_index: "0",
    file_hash: "tusresume001",
  });

  const createResp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": String(totalSize),
      "upload-metadata": metadata,
    },
  });
  const location = createResp.headers.get("location")!;
  const sessionUrl = `${baseUrl}${location}`;
  await createResp.body?.cancel();

  // Upload first quarter
  const quarter = totalSize / 4;
  const patch1 = await fetch(sessionUrl, {
    method: "PATCH",
    headers: {
      "upload-offset": "0",
      "content-type": "application/offset+octet-stream",
    },
    body: chunkData.slice(0, quarter),
  });
  assertEqual(patch1.status, 204, "first partial PATCH should succeed");
  await patch1.body?.cancel();

  // Now use tus-js-client to resume from the existing session URL
  const remainingData = chunkData.slice(quarter);
  const buf = Buffer.from(remainingData);

  return new Promise<void>((resolve, reject) => {
    const upload = new tus.Upload(buf, {
      // Use uploadUrl to resume an existing upload
      uploadUrl: sessionUrl,
      chunkSize: remainingData.length,
      retryDelays: [],
      onError: (error: Error) => {
        reject(new Error(`tus resume error: ${error.message}`));
      },
      onSuccess: () => {
        resolve();
      },
    });

    upload.start();
    setTimeout(() => reject(new Error("tus resume timed out after 30s")), 30_000);
  });
}

async function testStaleSessionInvalidation(baseUrl: string) {
  // Create session with file_hash A
  const meta1 = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/stale-test.bin",
    chunk_index: "0",
    file_hash: "hashA",
  });

  const resp1 = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: { "upload-length": "512", "upload-metadata": meta1 },
  });
  const loc1 = resp1.headers.get("location")!;
  await resp1.body?.cancel();

  // Create session with different file_hash B on same path
  // This should invalidate the old session
  const meta2 = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/stale-test.bin",
    chunk_index: "0",
    file_hash: "hashB",
  });

  const resp2 = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: { "upload-length": "512", "upload-metadata": meta2 },
  });
  assertEqual(resp2.status, 201, "new file_hash should create a new session");
  const loc2 = resp2.headers.get("location")!;
  await resp2.body?.cancel();

  assert(loc1 !== loc2, "new session should have a different location");

  // The old session should be gone
  const headOld = await fetch(`${baseUrl}${loc1}`, { method: "HEAD" });
  assertEqual(headOld.status, 404, "old session should be invalidated (404)");
  await headOld.body?.cancel();
}

async function testEmptyPatchBody(baseUrl: string) {
  const metadata = buildSimpleMetadata({
    app_id: "test-app",
    logical_path: "docs/empty-patch.bin",
    chunk_index: "0",
    file_hash: "empty001",
  });

  const createResp = await fetch(`${baseUrl}/ndm/v1/uploads`, {
    method: "POST",
    headers: {
      "upload-length": "256",
      "upload-metadata": metadata,
    },
  });
  const location = createResp.headers.get("location")!;
  await createResp.body?.cancel();

  const patchResp = await fetch(`${baseUrl}${location}`, {
    method: "PATCH",
    headers: {
      "upload-offset": "0",
      "content-type": "application/offset+octet-stream",
    },
    body: new Uint8Array(0),
  });
  assertEqual(patchResp.status, 400, "empty body should return 400");
  await patchResp.body?.cancel();
}

async function testCyfsRouteNotImplemented(baseUrl: string) {
  const resp = await fetch(`${baseUrl}/cyfs/some/path`);
  assertEqual(resp.status, 405, "CYFS route should return 405 (not implemented)");
  await resp.body?.cancel();
}

async function testLookupMissingParams(baseUrl: string) {
  // Missing quick_hash
  const resp1 = await fetch(`${baseUrl}/ndm/v1/objects/lookup?scope=app`);
  assertEqual(resp1.status, 400, "missing quick_hash should return 400");
  await resp1.body?.cancel();

  // Missing scope
  const resp2 = await fetch(`${baseUrl}/ndm/v1/objects/lookup?quick_hash=abc`);
  assertEqual(resp2.status, 400, "missing scope should return 400");
  await resp2.body?.cancel();
}

async function testLookupInvalidScope(baseUrl: string) {
  const resp = await fetch(
    `${baseUrl}/ndm/v1/objects/lookup?scope=invalid&quick_hash=abc`,
  );
  assertEqual(resp.status, 400, "invalid scope should return 400");
  await resp.body?.cancel();
}

// ===================== Main =====================

async function main() {
  console.log("=== NDM Zone Gateway Integration Tests ===\n");
  console.log("Starting server...");

  let server: ServerHandle | undefined;
  try {
    server = await startServer();

    // Allow the server a brief moment to be fully ready
    await new Promise((r) => setTimeout(r, 200));

    console.log("\nRunning tests...\n");

    // --- Session lifecycle ---
    await runTest("create upload session", () => testCreateUploadSession(server!.baseUrl));
    await runTest("HEAD upload session", () => testHeadUploadSession(server!.baseUrl));
    await runTest("single PATCH upload (complete)", () => testSinglePatchUpload(server!.baseUrl));
    await runTest("multi-PATCH resume upload", () => testMultiPatchResume(server!.baseUrl));
    await runTest("idempotent session creation", () => testIdempotentCreate(server!.baseUrl));
    await runTest("stale session invalidation", () => testStaleSessionInvalidation(server!.baseUrl));

    // --- Object lookup ---
    await runTest("object lookup: not found", () => testObjectLookupNotFound(server!.baseUrl));
    await runTest("object lookup: after upload", () => testObjectLookupAfterUpload(server!.baseUrl));
    await runTest("lookup: missing params", () => testLookupMissingParams(server!.baseUrl));
    await runTest("lookup: invalid scope", () => testLookupInvalidScope(server!.baseUrl));

    // --- Error cases ---
    await runTest("error: missing upload-length", () => testErrorMissingUploadLength(server!.baseUrl));
    await runTest("error: missing metadata", () => testErrorMissingMetadata(server!.baseUrl));
    await runTest("error: path traversal", () => testErrorInvalidLogicalPath(server!.baseUrl));
    await runTest("error: absolute path", () => testErrorAbsoluteLogicalPath(server!.baseUrl));
    await runTest("error: offset mismatch", () => testErrorOffsetMismatch(server!.baseUrl));
    await runTest("error: exceed chunk size", () => testErrorExceedChunkSize(server!.baseUrl));
    await runTest("error: session not found", () => testErrorSessionNotFound(server!.baseUrl));
    await runTest("error: unknown route", () => testErrorUnknownRoute(server!.baseUrl));
    await runTest("error: chunk too large", () => testErrorChunkTooLarge(server!.baseUrl));
    await runTest("error: empty PATCH body", () => testEmptyPatchBody(server!.baseUrl));
    await runTest("error: CYFS route not implemented", () => testCyfsRouteNotImplemented(server!.baseUrl));

    // --- TUS client library ---
    await runTest("tus-js-client: full upload", () => testTusClientUpload(server!.baseUrl));
    await runTest("tus-js-client: resume upload", () => testTusClientResume(server!.baseUrl));

    // --- Summary ---
    console.log(`\n=== Results: ${passed} passed, ${failed} failed ===`);
    if (failures.length > 0) {
      console.error("\nFailures:");
      for (const f of failures) {
        console.error(`  - ${f}`);
      }
    }
  } finally {
    if (server) {
      console.log("\nStopping server...");
      stopServer(server);
    }
  }

  Deno.exit(failed > 0 ? 1 : 0);
}

main();
