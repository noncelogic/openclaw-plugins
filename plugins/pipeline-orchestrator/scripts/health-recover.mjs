#!/usr/bin/env node

import { readFile, writeFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';

const PIPELINE_STATE_PATH =
  process.env.PIPELINE_STATE_PATH || `${process.env.HOME || ''}/.openclaw/workspace/memory/pipeline-state.json`;
const PIPELINE_REPO = process.env.PIPELINE_REPO || 'noncelogic/cortex-plane';
const BOT_AUTHOR_LOGINS = String(process.env.PIPELINE_BOT_AUTHORS || 'jg-noncelogic,openclaw-bot,copilot-swe-agent')
  .split(',')
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

const ORCH_SERVICE = process.env.PIPELINE_ORCH_SERVICE || 'pipeline-orchestrator.service';
const ORCH_TIMER = process.env.PIPELINE_ORCH_TIMER || 'pipeline-orchestrator.timer';
const GATEWAY_SERVICE = process.env.PIPELINE_GATEWAY_SERVICE || 'openclaw-gateway.service';
const ENABLE_GATEWAY_RECOVER = String(process.env.PIPELINE_GATEWAY_RECOVER || 'true').toLowerCase() === 'true';
const GATEWAY_RECOVER_COOLDOWN_MIN = Math.max(1, Number(process.env.PIPELINE_GATEWAY_RECOVER_COOLDOWN_MIN || 15));
const RECOVER_NOTIFY_ENABLED = String(process.env.PIPELINE_RECOVER_NOTIFY_ENABLED || 'true').toLowerCase() === 'true';
const NOTIFY_BIN = process.env.PIPELINE_NOTIFY_BIN || 'openclaw';
const NOTIFY_CHANNEL = process.env.PIPELINE_NOTIFY_CHANNEL || 'telegram';
const NOTIFY_TARGET = process.env.PIPELINE_NOTIFY_TARGET || '';
const RECOVER_NOTIFY_COOLDOWN_MIN = Math.max(1, Number(process.env.PIPELINE_RECOVER_NOTIFY_COOLDOWN_MIN || 10));

function toIso(ms) {
  return new Date(ms).toISOString();
}

function parseIssue(v) {
  const m = String(v || '').match(/#?(\d+)/);
  return m ? Number(m[1]) : null;
}

async function runProc(bin, args) {
  return await new Promise((resolve) => {
    const p = spawn(bin, args, { stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env } });
    let stdout = '';
    let stderr = '';
    p.stdout.on('data', (d) => (stdout += d.toString()));
    p.stderr.on('data', (d) => (stderr += d.toString()));
    p.on('error', (err) => resolve({ code: 1, stdout, stderr: `${stderr}\n${err.message}`.trim() }));
    p.on('close', (code) => resolve({ code: code ?? 1, stdout, stderr }));
  });
}

async function readJson(path) {
  return JSON.parse(await readFile(path, 'utf-8'));
}

async function writeJson(path, data) {
  await writeFile(path, `${JSON.stringify(data, null, 2)}\n`);
}

function issueMentioned(text, issueNum) {
  if (!issueNum) return false;
  const s = String(text || '').toLowerCase();
  return s.includes(`#${issueNum}`) || s.includes(`issue-${issueNum}`) || s.includes(`issue ${issueNum}`);
}

function setIdle(state) {
  state.status = 'idle';
  state.currentIssue = null;
  state.currentJob = null;
  state.jobId = null;
  state.jobPid = null;
  state.currentWorker = null;
  state.startedAt = null;
  state.currentJobStartedAt = null;
}

async function ghJson(args) {
  const r = await runProc('gh', args);
  if (r.code !== 0) throw new Error(r.stderr || r.stdout || 'gh failed');
  return JSON.parse(r.stdout || '[]');
}

async function serviceActive(unit) {
  const r = await runProc('systemctl', ['--user', 'is-active', unit]);
  return r.code === 0 && String(r.stdout || '').trim() === 'active';
}

function buildRecoverNotification(repo, actions, nowIso) {
  return ['Pipeline self-heal', `repo=${repo}`, ...actions.map((a) => `- ${a}`), `at=${nowIso}`].join('\n');
}

async function maybeSendRecoverNotification(state, actions, nowIso) {
  if (!RECOVER_NOTIFY_ENABLED || !NOTIFY_TARGET || actions.length === 0) return false;
  if (state.status === "paused" || state.autoPickup === false) return false;
  const actionKey = actions.join(' | ');
  const lastAtMs = Date.parse(String(state.lastRecoverNotifiedAt || ''));
  const cooldownMs = RECOVER_NOTIFY_COOLDOWN_MIN * 60 * 1000;
  if (
    state.lastRecoverNotifiedActionKey === actionKey &&
    Number.isFinite(lastAtMs) &&
    Date.now() - lastAtMs < cooldownMs
  ) {
    return false;
  }

  const msg = buildRecoverNotification(PIPELINE_REPO, actions, nowIso);
  const send = await runProc(NOTIFY_BIN, [
    'message',
    'send',
    '--channel',
    NOTIFY_CHANNEL,
    '--target',
    NOTIFY_TARGET,
    '--message',
    msg,
  ]);
  if (send.code !== 0) {
    state.lastRecoverNotifyError = `recover notify failed: ${send.stderr || send.stdout}`;
    return true;
  }
  state.lastRecoverNotifiedAt = nowIso;
  state.lastRecoverNotifiedActionKey = actionKey;
  state.lastRecoverNotifyError = null;
  return true;
}

async function main() {
  const nowIso = toIso(Date.now());
  const nowMs = Date.now();
  const actions = [];
  let changed = false;
  let stateUpdated = false;

  let state;
  try {
    state = await readJson(PIPELINE_STATE_PATH);
  } catch (err) {
    process.stdout.write(JSON.stringify({ ok: false, error: `read state failed: ${String(err.message || err)}` }, null, 2) + '\n');
    process.exit(1);
  }

  if (!Array.isArray(state.queue)) state.queue = [];
  const isPaused = state.status === 'paused' || state.autoPickup === false;

  if (isPaused) {
    if (state.watchdogPauseActive !== true) {
      state.watchdogPauseActive = true;
      state.watchdogPausedAt = nowIso;
      actions.push('watchdog hold (pipeline paused)');
      stateUpdated = true;
    }
  } else if (state.watchdogPauseActive === true) {
    state.watchdogPauseActive = false;
    state.watchdogResumedAt = nowIso;
    actions.push('watchdog resume (pipeline unpaused)');
    stateUpdated = true;
  }

  if (!isPaused && state.status === 'running') {
    const jobId = String(state.currentJob || state.jobId || '');
    if (!jobId) {
      setIdle(state);
      state.lastError = `self-heal: running without currentJob at ${nowIso}`;
      actions.push('running->idle (missing currentJob)');
      changed = true;
    } else {
      const probe = await runProc('bash', ['-lc', `pgrep -fa "cli-worker\\.mjs ${jobId}" | head -n 1`]);
      const alive = String(probe.stdout || '').trim().length > 0;
      if (!alive) {
        setIdle(state);
        state.lastError = `self-heal: orphaned ${jobId} reset to idle at ${nowIso}`;
        actions.push(`running->idle (orphaned ${jobId})`);
        changed = true;
      }
    }
  }

  try {
    if (isPaused) throw new Error('pipeline paused');
    const prs = await ghJson([
      'pr',
      'list',
      '--repo',
      PIPELINE_REPO,
      '--state',
      'open',
      '--json',
      'number,author,title,headRefName,body',
      '--limit',
      '100',
    ]);
    const pipelinePrs = prs.filter((pr) => BOT_AUTHOR_LOGINS.includes(String(pr?.author?.login || '').toLowerCase()));

    if (state.status === 'waiting-on-pr' && pipelinePrs.length === 0) {
      setIdle(state);
      state.lastError = null;
      actions.push('waiting-on-pr->idle (no open pipeline PR)');
      changed = true;
    }

    if (state.status === 'pr-pending') {
      const issue = parseIssue(state.currentIssue || state.currentTicket);
      const matched = pipelinePrs.find((pr) =>
        issueMentioned(pr.headRefName, issue) || issueMentioned(pr.title, issue) || issueMentioned(pr.body, issue),
      );
      if (!matched) {
        setIdle(state);
        state.lastError = `self-heal: pr-pending without matching open PR for issue ${issue ?? 'unknown'} at ${nowIso}`;
        actions.push('pr-pending->idle (missing matching open PR)');
        changed = true;
      }
    }
  } catch (err) {
    const why = String(err.message || err);
    if (why !== 'pipeline paused') {
      actions.push(`skipped gh checks: ${why}`);
    }
  }

  if (ENABLE_GATEWAY_RECOVER && !isPaused) {
    const gatewayIsActive = await serviceActive(GATEWAY_SERVICE);
    if (gatewayIsActive) {
      const recent = await runProc('journalctl', ['--user', '-u', GATEWAY_SERVICE, '--since', '4 min ago', '--no-pager']);
      const text = `${recent.stdout || ''}\n${recent.stderr || ''}`;
      const stuck = text.includes('typing TTL reached') || text.includes('stuck session:');
      if (stuck) {
        const cooldownMs = GATEWAY_RECOVER_COOLDOWN_MIN * 60 * 1000;
        const lastRestartMs = Date.parse(String(state.lastGatewayRecoverAt || ''));
        if (Number.isFinite(lastRestartMs) && nowMs - lastRestartMs < cooldownMs) {
          actions.push('gateway recover skipped (cooldown active)');
        } else {
          await runProc('systemctl', ['--user', 'restart', GATEWAY_SERVICE]);
          state.lastGatewayRecoverAt = nowIso;
          state.lastGatewayRecoverReason = 'stuck telegram session';
          state.lastGatewayRecoverCooldownMin = GATEWAY_RECOVER_COOLDOWN_MIN;
          stateUpdated = true;
          actions.push('restarted gateway (stuck telegram session)');
        }
      }
    }
  }

  const timerIsActive = await serviceActive(ORCH_TIMER);
  if (!timerIsActive) {
    await runProc('systemctl', ['--user', 'reset-failed', ORCH_SERVICE]);
    await runProc('systemctl', ['--user', 'start', ORCH_TIMER]);
    actions.push('started orchestrator timer');
  }

  if (changed) {
    state.lastRecoveredAt = nowIso;
    state.lastRecoverActions = actions;
    stateUpdated = true;
  }
  if (await maybeSendRecoverNotification(state, actions, nowIso)) {
    stateUpdated = true;
  }
  if (stateUpdated) {
    await writeJson(PIPELINE_STATE_PATH, state);
  }

  if (!isPaused) {
    await runProc('systemctl', ['--user', 'start', ORCH_SERVICE]);
  }

  process.stdout.write(
    JSON.stringify(
      {
        ok: true,
        changed,
        actions,
        state: {
          status: state.status,
          currentIssue: state.currentIssue || null,
          currentJob: state.currentJob || null,
          lastError: state.lastError || null,
          lastOrchestratedAt: state.lastOrchestratedAt || null,
        },
      },
      null,
      2,
    ) + '\n',
  );
}

main().catch((err) => {
  process.stderr.write(String(err?.stack || err) + '\n');
  process.exit(1);
});
