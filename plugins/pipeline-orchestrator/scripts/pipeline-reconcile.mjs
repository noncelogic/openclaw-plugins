#!/usr/bin/env node

import { readFile, writeFile, readdir } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn } from 'node:child_process';

const __dirname = dirname(fileURLToPath(import.meta.url));
const SKILL_DIR = join(__dirname, '..');
const WORKSPACE_DIR = process.env.OPENCLAW_WORKSPACE_DIR || join(process.env.HOME || '', '.openclaw', 'workspace');
const JOBS_DIR = process.env.CLAUDE_SKILL_JOBS_DIR || join(SKILL_DIR, 'jobs');
const PIPELINE_STATE_PATH =
  process.env.PIPELINE_STATE_PATH || join(WORKSPACE_DIR, 'memory', 'pipeline-state.json');
const PIPELINE_HEALTH_PATH =
  process.env.PIPELINE_HEALTH_PATH || join(WORKSPACE_DIR, 'memory', 'pipeline-health.json');
const OPENCLAW_BIN = process.env.OPENCLAW_BINARY_PATH || 'openclaw';

const HOURLY_WINDOW_MS = 60 * 60 * 1000;
const AUTOPICK_COOLDOWN_MS = 10 * 60 * 1000;
const MAX_METRIC_EVENTS = 200;

function parseArgs(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg.startsWith('--')) continue;
    const key = arg.slice(2);
    const next = argv[i + 1];
    if (next && !next.startsWith('--')) {
      out[key] = next;
      i++;
    } else {
      out[key] = true;
    }
  }
  return out;
}

async function readJson(path) {
  return JSON.parse(await readFile(path, 'utf-8'));
}

async function readJsonSafe(path) {
  try {
    return await readJson(path);
  } catch {
    return null;
  }
}

async function writeJson(path, value) {
  await writeFile(path, JSON.stringify(value, null, 2) + '\n');
}

function parseIssueFromJobId(jobId) {
  const m = String(jobId || '').match(/^issue-(\d+)$/);
  return m ? Number(m[1]) : null;
}

function terminalStatus(status) {
  return status === 'completed' || status === 'failed' || status === 'killed';
}

function eventForStatus(jobId, status) {
  if (status === 'completed') return `JOB_DONE:${jobId}`;
  if (status === 'failed' || status === 'killed') return `JOB_FAILED:${jobId}`;
  return null;
}

function toMs(value) {
  const ms = Date.parse(String(value || ''));
  return Number.isFinite(ms) ? ms : null;
}

function shouldReconcileIdleAutopick(state) {
  return state?.status === 'idle' && state?.autoPickup === true && Array.isArray(state?.queue) && state.queue.length > 0;
}

function ensureMetrics(state) {
  const existing = state?.pipelineMetrics || {};
  const events = Array.isArray(existing.events) ? existing.events.slice(-MAX_METRIC_EVENTS) : [];
  return {
    ...existing,
    events,
  };
}

function pushMetricEvent(metrics, event) {
  metrics.events.push(event);
  if (metrics.events.length > MAX_METRIC_EVENTS) {
    metrics.events.splice(0, metrics.events.length - MAX_METRIC_EVENTS);
  }
}

function computeHourlyStats(metrics, nowMs) {
  const cutoff = nowMs - HOURLY_WINDOW_MS;
  let jobsCompleted = 0;
  let idleWithQueueIncidents = 0;
  let autoRecoveriesTriggered = 0;

  for (const event of metrics.events) {
    const atMs = toMs(event?.at);
    if (atMs == null || atMs < cutoff) continue;
    if (event.type === 'job_completed') jobsCompleted++;
    if (event.type === 'idle_with_queue_incident') idleWithQueueIncidents++;
    if (event.type === 'auto_recovery_triggered') autoRecoveriesTriggered++;
  }

  return {
    windowMinutes: 60,
    jobsCompleted,
    idleWithQueueIncidents,
    autoRecoveriesTriggered,
  };
}

function inferCandidateJobId(state) {
  if (state?.currentJob) return state.currentJob;
  const orphanMatch = String(state?.lastError || '').match(/\b(issue-\d+)\b/);
  if (orphanMatch) return orphanMatch[1];
  return null;
}

function alreadyReconciled(state, meta) {
  if (!meta?.jobId) return false;
  if (state?.lastReconciledJob !== meta.jobId) return false;
  const endedAtMs = toMs(meta.endedAt);
  const lastSourceEndedAtMs = toMs(state?.lastReconciledSourceEndedAt);
  if (endedAtMs != null && lastSourceEndedAtMs != null) {
    return endedAtMs <= lastSourceEndedAtMs;
  }
  return true;
}

async function emitSystemEvent(text) {
  return await new Promise((resolve) => {
    const proc = spawn(OPENCLAW_BIN, ['system', 'event', '--text', text, '--mode', 'now'], {
      cwd: SKILL_DIR,
      env: { ...process.env },
      stdio: 'ignore',
    });
    proc.once('error', () => resolve(false));
    proc.once('close', (code) => resolve(code === 0));
  });
}

async function listJobIds() {
  try {
    const entries = await readdir(JOBS_DIR, { withFileTypes: true });
    return entries.filter((e) => e.isDirectory()).map((e) => e.name);
  } catch {
    return [];
  }
}

async function readMetaSafe(jobId) {
  try {
    const meta = await readJson(join(JOBS_DIR, jobId, 'meta.json'));
    return meta;
  } catch {
    return null;
  }
}

async function findTerminalJob(jobId) {
  if (!jobId) return null;
  const meta = await readMetaSafe(jobId);
  if (!meta || !terminalStatus(meta.status)) return null;
  return meta;
}

async function findLatestTerminalIssueJob() {
  const ids = await listJobIds();
  let best = null;

  for (const jobId of ids) {
    const issueNum = parseIssueFromJobId(jobId);
    if (!issueNum) continue;
    const meta = await readMetaSafe(jobId);
    if (!meta || !terminalStatus(meta.status)) continue;
    const endedAtMs = toMs(meta.endedAt) || 0;
    if (!best || endedAtMs > best.endedAtMs) {
      best = { meta, endedAtMs };
    }
  }

  return best ? best.meta : null;
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  const dryRun = Boolean(args['dry-run']);

  const now = new Date();
  const nowIso = now.toISOString();
  const nowMs = now.getTime();

  const state = await readJson(PIPELINE_STATE_PATH);
  const previousHealth = (await readJsonSafe(PIPELINE_HEALTH_PATH)) || {};
  const nextState = { ...state };
  const metrics = ensureMetrics(previousHealth);
  let stateChanged = false;

  const out = {
    dryRun,
    reconciled: false,
    reconcileReason: null,
    jobId: null,
    status: null,
    eventText: null,
    eventEmitted: false,
    autoRecovery: {
      checked: false,
      triggered: false,
      eventEmitted: false,
      queueHeadIssue: null,
      reason: null,
    },
    statePath: PIPELINE_STATE_PATH,
    healthPath: PIPELINE_HEALTH_PATH,
    stateWrite: false,
  };
  const currentJobMeta = await readMetaSafe(state?.currentJob || null);

  let reconcileMeta = null;
  if (state?.currentJob && currentJobMeta && terminalStatus(currentJobMeta.status)) {
    reconcileMeta = currentJobMeta;
    out.reconcileReason = 'current-job-terminal';
  } else {
    const candidateJobId = inferCandidateJobId(state);
    reconcileMeta = await findTerminalJob(candidateJobId);
    if (reconcileMeta) {
      out.reconcileReason = 'candidate-terminal';
    }
    if (!reconcileMeta && shouldReconcileIdleAutopick(state)) {
      reconcileMeta = await findLatestTerminalIssueJob();
      if (reconcileMeta) {
        out.reconcileReason = 'latest-terminal-idle-autopick';
      }
    }
  }

  if (reconcileMeta && !alreadyReconciled(state, reconcileMeta)) {
    stateChanged = true;
    const jobId = reconcileMeta.jobId;
    const issueNum = parseIssueFromJobId(jobId);
    const eventText = eventForStatus(jobId, reconcileMeta.status);

    out.reconciled = true;
    out.jobId = jobId;
    out.status = reconcileMeta.status;
    out.eventText = eventText;

    nextState.lastError = null;
    nextState.lastReconciledAt = nowIso;
    nextState.lastReconciledJob = jobId;
    nextState.lastReconciledStatus = reconcileMeta.status;
    nextState.lastReconciledSourceEndedAt = reconcileMeta.endedAt || nowIso;
    nextState.notes = `${state.notes || ''}\n[reconcile] ${jobId} (${reconcileMeta.status}) reconciled at ${nowIso}`.trim();

    if (reconcileMeta.status === 'completed' && issueNum) {
      const existingCompleted = nextState.lastCompleted || {};
      nextState.lastCompleted = {
        ...existingCompleted,
        issue: issueNum,
        completedAt: reconcileMeta.endedAt || existingCompleted.completedAt || nowIso,
      };
      pushMetricEvent(metrics, { at: nowIso, type: 'job_completed', issue: issueNum, jobId });
    }

    if ((reconcileMeta.status === 'failed' || reconcileMeta.status === 'killed') && issueNum) {
      pushMetricEvent(metrics, {
        at: nowIso,
        type: 'job_terminal_non_success',
        issue: issueNum,
        jobId,
        status: reconcileMeta.status,
      });
    }

    if (state?.currentJob === jobId) {
      nextState.status = 'idle';
      nextState.currentJob = null;
      nextState.currentIssue = null;
      nextState.jobPid = null;
      nextState.startedAt = null;
      nextState.currentJobStartedAt = null;
    }

    if (!dryRun && eventText) {
      out.eventEmitted = await emitSystemEvent(eventText);
    }
  }

  if (shouldReconcileIdleAutopick(nextState)) {
    out.autoRecovery.checked = true;
    const queueHeadIssue = Number(nextState.queue?.[0]) || null;
    out.autoRecovery.queueHeadIssue = queueHeadIssue;
    if (queueHeadIssue != null) {
      const lastIssue = Number(nextState.lastAutoRecoveryIssue) || null;
      const lastAtMs = toMs(nextState.lastAutoRecoveryAt);
      const cooldownOk = lastAtMs == null || nowMs - lastAtMs >= AUTOPICK_COOLDOWN_MS;
      const issueChanged = lastIssue == null || lastIssue !== queueHeadIssue;

      if (issueChanged || cooldownOk) {
        pushMetricEvent(metrics, {
          at: nowIso,
          type: 'idle_with_queue_incident',
          queueHeadIssue,
          queueLength: Array.isArray(nextState.queue) ? nextState.queue.length : null,
        });
        const recoveryEvent =
          `PIPELINE_AUTORECOVER: idle-with-queue detected (autoPickup=true, status=idle, queueHead=#${queueHeadIssue}). ` +
          `Reconcile terminal job state and start exactly one next queued ticket (#${queueHeadIssue}) via normal flow.`;
        out.autoRecovery.triggered = true;
        if (!dryRun) {
          out.autoRecovery.eventEmitted = await emitSystemEvent(recoveryEvent);
          if (out.autoRecovery.eventEmitted) {
            stateChanged = true;
            nextState.lastAutoRecoveryAt = nowIso;
            nextState.lastAutoRecoveryIssue = queueHeadIssue;
            nextState.autoRecoveriesTriggered = Number(nextState.autoRecoveriesTriggered || 0) + 1;
            pushMetricEvent(metrics, {
              at: nowIso,
              type: 'auto_recovery_triggered',
              queueHeadIssue,
              idleCycles: 1,
            });
          }
        }
        if (!out.autoRecovery.eventEmitted && !dryRun) {
          out.autoRecovery.reason = 'event-emit-failed';
        }
      } else {
        out.autoRecovery.reason = 'cooldown-active';
      }
    }
  }

  const hourlyStats = computeHourlyStats(metrics, nowMs);
  const healthSnapshot = {
    updatedAt: nowIso,
    lastReconciledJob: nextState.lastReconciledJob || null,
    lastReconciledAt: nextState.lastReconciledAt || null,
    currentJob: nextState.currentJob || null,
    currentJobStartedAt: nextState.currentJobStartedAt || null,
    idleWithQueueCycles: shouldReconcileIdleAutopick(nextState) ? 1 : 0,
    autoRecoveriesTriggeredTotal: Number(nextState.autoRecoveriesTriggered || 0),
    pipelineMetrics: metrics,
    ...hourlyStats,
  };

  if (!dryRun) {
    if (stateChanged) {
      await writeJson(PIPELINE_STATE_PATH, nextState);
      out.stateWrite = true;
    }
    await writeJson(PIPELINE_HEALTH_PATH, healthSnapshot);
  }

  out.pipelineHealth = {
    ...hourlyStats,
    updatedAt: nowIso,
  };
  out.idleWithQueueCycles = shouldReconcileIdleAutopick(nextState) ? 1 : 0;
  process.stdout.write(JSON.stringify(out, null, 2) + '\n');
}

run().catch((err) => {
  process.stderr.write(`${err?.stack || err?.message || String(err)}\n`);
  process.exit(1);
});
