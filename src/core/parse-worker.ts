/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See LICENSE in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

/* Parse worker entry point.
 * Supports both worker_threads and child-process IPC so parsing can run
 * in an isolated process with its own heap limit.
 */

import { parentPort } from 'worker_threads';
import { stripSessionsForMemory } from './cache';
import { parseAllLogsAsyncDetailed, type LoadProgress } from './parser';
import { installRuntimeDebugHooks, runtimeDebug } from './runtime-debug';

interface ParseWorkerRequest {
  logsDirs?: string[];
}

/** Number of sessions per streamed IPC chunk (issue #106, S1). */
const SESSION_CHUNK_SIZE = 250;

interface ProgressMessage {
  type: 'progress';
  progress: LoadProgress;
}

const port = parentPort;
const canUseProcessChannel = typeof process.send === 'function';

if (!port && !canUseProcessChannel) throw new Error('parse-worker: no parent channel');

installRuntimeDebugHooks('parse-worker');
runtimeDebug('parse-worker', port ? 'thread-started' : 'process-started');

function send(msg: unknown): void {
  if (port) port.postMessage(msg);
  else if (canUseProcessChannel) process.send?.(msg);
}

function parseWorkerRequest(msg: unknown): ParseWorkerRequest {
  if (typeof msg !== 'object' || msg === null) return {};
  const candidate = msg as { logsDirs?: unknown };
  return {
    logsDirs: Array.isArray(candidate.logsDirs)
      ? candidate.logsDirs.filter((dir): dir is string => typeof dir === 'string')
      : undefined,
  };
}

function onMessage(handler: (msg: ParseWorkerRequest) => void | Promise<void>): void {
  if (port) {
    port.on('message', (msg) => {
      void handler(parseWorkerRequest(msg));
    });
    return;
  }
  process.on('message', (msg) => {
    void handler(parseWorkerRequest(msg));
  });
}

onMessage(async (msg) => {
  try {
    const logsDirs = Array.isArray(msg.logsDirs) ? msg.logsDirs : [];
    runtimeDebug('parse-worker', 'message-start', `logsDirs=${logsDirs.length}`);

    // Throttle verbose intra-workspace progress messages, but always send
    // phase changes, workspace grid plans, and workspace completion updates.
    let lastSendTime = 0;
    let lastPhase = -1;
    let pending: ProgressMessage | null = null;
    const flushPending = () => {
      if (pending) {
        send(pending);
        pending = null;
        lastSendTime = Date.now();
      }
    };

    const { result, dirMetas } = await parseAllLogsAsyncDetailed(logsDirs, (progress) => {
      const progressMessage: ProgressMessage = { type: 'progress', progress };
      const now = Date.now();
      // Always send immediately for phase changes, workspace grid updates, or >= 100%.
      if (progress.phase !== lastPhase || progress.workspacePlan || progress.workspaceDone || progress.pct >= 100) {
        flushPending();
        send(progressMessage);
        lastPhase = progress.phase;
        lastSendTime = now;
        return;
      }
      if (now - lastSendTime >= 200) {
        send(progressMessage);
        lastSendTime = now;
        pending = null;
      } else {
        pending = progressMessage;
      }
    });
    // Flush any final pending progress before sending result.
    flushPending();

    // Keep full text only in the disk cache written by parseAllLogsAsyncDetailed.
    // The parent process receives the memory-efficient representation only.
    // (VS Code / CLI sessions are already stripped eagerly during parse; this also strips
    // external-harness sessions collected after the main loop.)
    stripSessionsForMemory(result.sessions);

    runtimeDebug('parse-worker', 'message-result', `workspaces=${result.workspaces.size} sessions=${result.sessions.length}`);

    // Stream the result to the parent in per-session-batch chunks (issue #106, S1). Sending
    // one giant payload allocates a single large JSON string; streaming keeps each serialized
    // chunk small so it can be GC'd before the next is built. editLocIndex / sessionSourceIndex
    // entries travel with the chunk that owns their sessions; anything left over (edits with no
    // matching chat request, or sources for filtered sessions) is flushed in the `done` message
    // so nothing is dropped.
    const emittedEditLocKeys = new Set<string>();
    const emittedSessionIds = new Set<string>();
    for (let i = 0; i < result.sessions.length; i += SESSION_CHUNK_SIZE) {
      const slice = result.sessions.slice(i, i + SESSION_CHUNK_SIZE);
      const editLocEntries: [string, [string, number][]][] = [];
      const sourceEntries: [string, unknown][] = [];
      for (const s of slice) {
        emittedSessionIds.add(s.sessionId);
        const src = result.sessionSourceIndex.get(s.sessionId);
        if (src) sourceEntries.push([s.sessionId, src]);
        for (const r of s.requests) {
          if (emittedEditLocKeys.has(r.requestId)) continue;
          const fileMap = result.editLocIndex.get(r.requestId);
          if (fileMap) {
            emittedEditLocKeys.add(r.requestId);
            editLocEntries.push([r.requestId, Array.from(fileMap.entries())]);
          }
        }
      }
      send({ type: 'chunk', payload: { sessions: slice, editLocEntries, sourceEntries } });
    }

    const orphanEditLoc: [string, [string, number][]][] = [];
    for (const [reqId, fileMap] of result.editLocIndex) {
      if (!emittedEditLocKeys.has(reqId)) orphanEditLoc.push([reqId, Array.from(fileMap.entries())]);
    }
    const orphanSources: [string, unknown][] = [];
    for (const [sessionId, src] of result.sessionSourceIndex) {
      if (!emittedSessionIds.has(sessionId)) orphanSources.push([sessionId, src]);
    }

    send({
      type: 'done',
      payload: {
        workspaces: Array.from(result.workspaces.entries()),
        orphanEditLoc,
        orphanSources,
        dirMetas,
      },
    });
  } catch (e) {
    runtimeDebug('parse-worker', 'message-error', e);
    send({
      type: 'error',
      message: e instanceof Error ? e.message : String(e),
    });
  }
});