#!/usr/bin/env node

import { readFile, writeFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const SKILL_DIR = join(__dirname, '..');
const WORKSPACE_DIR = process.env.OPENCLAW_WORKSPACE_DIR || join(process.env.HOME || '', '.openclaw', 'workspace');

const PIPELINE_STATE_PATH =
  process.env.PIPELINE_STATE_PATH || join(WORKSPACE_DIR, 'memory', 'pipeline-state.json');
const PIPELINE_REPO = process.env.PIPELINE_REPO || 'noncelogic/cortex-plane';
const PIPELINE_PROJECT_PATH = process.env.PIPELINE_PROJECT_PATH || join(WORKSPACE_DIR, 'projects', 'cortex-plane');
const MAX_WIP = Number(process.env.PIPELINE_MAX_WIP || 1);
const CLAUDE_MODEL = process.env.PIPELINE_CLAUDE_MODEL || '';
const CODEX_MODEL = process.env.PIPELINE_CODEX_MODEL || 'gpt-5.3-codex';
const COOLDOWN_MINUTES = Number(process.env.PIPELINE_CLAUDE_COOLDOWN_MINUTES || 120);
const DEFAULT_WORKER = ['claude', 'codex'].includes(String(process.env.PIPELINE_DEFAULT_WORKER || '').toLowerCase())
  ? String(process.env.PIPELINE_DEFAULT_WORKER || '').toLowerCase()
  : 'codex';
const SELF_HEAL_DIRTY_WORKTREE = String(process.env.PIPELINE_SELF_HEAL_DIRTY_WORKTREE || 'true').toLowerCase() === 'true';
const NOTIFY_ENABLED = String(process.env.PIPELINE_NOTIFY_ENABLED || '').toLowerCase() === 'true';
const NOTIFY_BIN = process.env.PIPELINE_NOTIFY_BIN || 'openclaw';
const NOTIFY_CHANNEL = process.env.PIPELINE_NOTIFY_CHANNEL || 'telegram';
const NOTIFY_TARGET = process.env.PIPELINE_NOTIFY_TARGET || '';
const BOT_AUTHOR_LOGINS = String(process.env.PIPELINE_BOT_AUTHORS || 'jg-noncelogic')
  .split(',')
  .map((v) => v.trim().toLowerCase())
  .filter(Boolean);
const PIPELINE_STATUS = Object.freeze({
  IDLE: 'idle',
  RUNNING: 'running',
  PR_PENDING: 'pr-pending',
  WAITING_ON_PR: 'waiting-on-pr',
  BLOCKED: 'blocked',
  PAUSED: 'paused',
});

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

async function writeJson(path, data) {
  await writeFile(path, JSON.stringify(data, null, 2) + '\n');
}

function toIso(ms) {
  return new Date(ms).toISOString();
}

function parseIssue(v) {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  const m = String(v || '').match(/#?(\d+)/);
  return m ? Number(m[1]) : null;
}

function priorityRank(labels) {
  for (const n of labels) {
    const m = n.match(/^(?:priority:\s*)?p([0-3])$/i);
    if (m) return Number(m[1]);
  }
  return null;
}

function sizeRank(labels) {
  const order = ['s', 'm', 'l', 'xl'];
  for (const n of labels) {
    const m = n.match(/^size:\s*(s|m|l|xl)$/i);
    if (m) return order.indexOf(m[1].toLowerCase());
  }
  return 9;
}

function isEpicAggregator(labels) {
  for (const raw of labels) {
    const n = String(raw || '').trim().toLowerCase();
    if (n === 'epic' || n.startsWith('epic:') || n === 'type: epic' || n === 'kind: epic') return true;
    if (n === 'meta' || n.startsWith('meta:')) return true;
  }
  return false;
}

function blockedReasons(labels) {
  const blocked = new Set();
  const blockedSet = new Set(['needs-joe', 'blocked', 'status:blocked', 'needs-design', 'status:backlog', 'epic: business-ops']);
  for (const n of labels) {
    if (blockedSet.has(n.toLowerCase())) blocked.add(n);
  }
  if (isEpicAggregator(labels)) blocked.add('epic-aggregator');
  return Array.from(blocked);
}

function checksGreen(statusCheckRollup) {
  if (!Array.isArray(statusCheckRollup) || statusCheckRollup.length === 0) return true;
  for (const check of statusCheckRollup) {
    const typename = String(check?.__typename || '');
    if (typename === 'StatusContext') {
      const state = String(check?.state || '').toUpperCase();
      if (state !== 'SUCCESS') return false;
      continue;
    }
    if (check?.status !== 'COMPLETED') return false;
    const conclusion = String(check?.conclusion || '').toUpperCase();
    if (!['SUCCESS', 'NEUTRAL', 'SKIPPED'].includes(conclusion)) return false;
  }
  return true;
}

function isBranchOutdatedMergeError(msg) {
  const s = String(msg || '').toLowerCase();
  return (
    s.includes('head branch is not up to date with the base branch') ||
    (s.includes('not mergeable') && s.includes('not up to date with the base branch'))
  );
}

function isIssueMentioned(text, issue) {
  if (!issue) return false;
  const s = String(text || '').toLowerCase();
  return s.includes(`#${issue}`) || s.includes(`issue-${issue}`) || s.includes(`issue ${issue}`);
}

function findTargetPr(prs, targetIssue, state) {
  if (!Array.isArray(prs) || prs.length === 0) return null;
  if (targetIssue) {
    const direct = prs.find((pr) =>
      isIssueMentioned(pr.headRefName, targetIssue) ||
      isIssueMentioned(pr.title, targetIssue) ||
      isIssueMentioned(pr.body, targetIssue),
    );
    if (direct) return direct;
  }

  const lastCompletedMs = Date.parse(String(state?.lastWorkerCompletedAt || ''));
  const lowerBound = Number.isFinite(lastCompletedMs) ? lastCompletedMs - 2 * 60 * 60 * 1000 : Date.now() - 30 * 60 * 1000;
  const recentBotCandidates = prs.filter((pr) => {
    const author = String(pr?.author?.login || '').toLowerCase();
    const updated = Date.parse(String(pr?.updatedAt || ''));
    return BOT_AUTHOR_LOGINS.includes(author) && Number.isFinite(updated) && updated >= lowerBound;
  });
  if (recentBotCandidates.length === 1) return recentBotCandidates[0];
  return null;
}

async function runProc(bin, args, options = {}) {
  return await new Promise((resolve) => {
    const child = spawn(bin, args, {
      cwd: options.cwd || process.cwd(),
      env: { ...process.env, ...(options.env || {}) },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => {
      stdout += d.toString();
    });
    child.stderr.on('data', (d) => {
      stderr += d.toString();
    });
    child.on('error', (err) => resolve({ code: 1, stdout, stderr: `${stderr}\n${err.message}`.trim() }));
    child.on('close', (code) => resolve({ code: code ?? 1, stdout, stderr }));
  });
}

async function ghJson(args) {
  const res = await runProc('gh', args, { cwd: PIPELINE_PROJECT_PATH });
  if (res.code !== 0) throw new Error(`gh ${args.join(' ')} failed: ${res.stderr || res.stdout}`);
  try {
    return JSON.parse(res.stdout || '[]');
  } catch {
    throw new Error(`Failed to parse JSON from gh ${args.join(' ')}`);
  }
}

async function runManagerJson(args) {
  const res = await runProc(process.execPath, [join(__dirname, 'run.mjs'), ...args], { cwd: __dirname });
  if (res.code !== 0) throw new Error(`run.mjs ${args.join(' ')} failed: ${res.stderr || res.stdout}`);
  return JSON.parse(res.stdout || '{}');
}

async function hasCodexBinary() {
  const bin = process.env.CODEX_BINARY_PATH || 'codex';
  const probe = await runProc(bin, ['--help']);
  const combined = `${probe.stderr || ''}\n${probe.stdout || ''}`;
  return !combined.includes('ENOENT');
}

function buildJobPrompt(issueNum) {
  return [
    `Implement GitHub issue #${issueNum} in ${PIPELINE_REPO}.`,
    `Work tree: ${PIPELINE_PROJECT_PATH}`,
    'Requirements:',
    '0) Preflight repo hygiene: git fetch origin, git checkout main, git pull --ff-only origin main, then create/switch to issue branch from origin/main.',
    '1) Read issue details with gh issue view.',
    '2) Implement complete fix with tests.',
    '3) Run lint/typecheck/test/build and fix failures.',
    '4) Open a PR targeting main with issue reference.',
    '5) Wait for CI and address quick follow-up issues if obvious.',
    '6) Before finishing, stop any processes started by this job.',
    '7) Leave concise summary of files changed and validation run.',
    '8) After handoff is complete, return repository to main and pull --ff-only.',
  ].join('\n');
}

async function prepareBranchForIssue(issueNum) {
  const branch = `issue-${issueNum}`;
  const healNotes = [];
  const dirty = await runProc('git', ['status', '--porcelain'], { cwd: PIPELINE_PROJECT_PATH });
  if (dirty.code !== 0) {
    return { ok: false, branch, error: `git status failed: ${dirty.stderr || dirty.stdout}`, healed: false };
  }
  if (String(dirty.stdout || '').trim().length > 0) {
    if (!SELF_HEAL_DIRTY_WORKTREE) {
      return { ok: false, branch, error: 'worktree is dirty; refusing to create branch from stale state', healed: false };
    }
    const stamp = toIso(Date.now()).replace(/[:.]/g, '-');
    const stashMsg = `pipeline-self-heal-${stamp}-issue-${issueNum}`;
    const stashed = await runProc('git', ['stash', 'push', '-u', '-m', stashMsg], { cwd: PIPELINE_PROJECT_PATH });
    if (stashed.code !== 0) {
      return {
        ok: false,
        branch,
        error: `worktree dirty and self-heal stash failed: ${stashed.stderr || stashed.stdout}`,
        healed: false,
      };
    }
    healNotes.push(`stashed dirty worktree (${stashMsg})`);
  }

  const steps = [
    ['fetch', 'origin'],
    ['checkout', 'main'],
    ['pull', '--ff-only', 'origin', 'main'],
  ];
  for (const step of steps) {
    const res = await runProc('git', step, { cwd: PIPELINE_PROJECT_PATH });
    if (res.code !== 0) {
      return {
        ok: false,
        branch,
        error: `git ${step.join(' ')} failed: ${res.stderr || res.stdout}`,
        healed: false,
      };
    }
  }
  const checkoutPrimary = await runProc('git', ['checkout', '-B', branch, 'origin/main'], { cwd: PIPELINE_PROJECT_PATH });
  if (checkoutPrimary.code === 0) {
    return {
      ok: true,
      branch,
      healed: healNotes.length > 0,
      healNote: healNotes.join('; '),
    };
  }

  const primaryErr = String(checkoutPrimary.stderr || checkoutPrimary.stdout || '');
  const refCollision =
    primaryErr.includes(`cannot lock ref 'refs/heads/${branch}'`) &&
    primaryErr.includes(`'refs/heads/${branch}/`);
  if (refCollision) {
    const fallback = `issue-${issueNum}-auto`;
    const checkoutFallback = await runProc('git', ['checkout', '-B', fallback, 'origin/main'], {
      cwd: PIPELINE_PROJECT_PATH,
    });
    if (checkoutFallback.code === 0) {
      healNotes.push(`branch ref collision; used ${fallback}`);
      return { ok: true, branch: fallback, healed: true, healNote: healNotes.join('; ') };
    }
    return {
      ok: false,
      branch: fallback,
      error: `git checkout fallback ${fallback} failed: ${checkoutFallback.stderr || checkoutFallback.stdout}`,
      healed: false,
    };
  }

  return {
    ok: false,
    branch,
    error: `git checkout -B ${branch} origin/main failed: ${primaryErr}`,
    healed: false,
  };
}

function ensureQueue(state) {
  if (!Array.isArray(state.queue)) state.queue = [];
  state.queue = state.queue.map((v) => parseIssue(v)).filter((v) => Number.isFinite(v));
}

function ensureStatus(state) {
  const valid = new Set(Object.values(PIPELINE_STATUS));
  if (!valid.has(state.status)) state.status = PIPELINE_STATUS.IDLE;
}

function ensureSubagents(state) {
  if (!state.subagents || typeof state.subagents !== 'object') {
    state.subagents = { active: [] };
  }
  if (!Array.isArray(state.subagents.active)) state.subagents.active = [];
}

function setIdle(state) {
  state.status = PIPELINE_STATUS.IDLE;
  state.currentJob = null;
  state.currentIssue = null;
  state.jobId = null;
  state.jobPid = null;
  state.startedAt = null;
  state.currentJobStartedAt = null;
  clearCiGateSubagent(state);
}

function setWaitingOnPr(state) {
  state.status = PIPELINE_STATUS.WAITING_ON_PR;
  state.currentJob = null;
  state.currentIssue = null;
  state.jobId = null;
  state.jobPid = null;
  state.startedAt = null;
  state.currentJobStartedAt = null;
  clearCiGateSubagent(state);
}

function setBlocked(state) {
  state.status = PIPELINE_STATUS.BLOCKED;
  state.currentJob = null;
  state.currentIssue = null;
  state.jobId = null;
  state.jobPid = null;
  state.startedAt = null;
  state.currentJobStartedAt = null;
  clearCiGateSubagent(state);
}

function setPaused(state) {
  state.status = PIPELINE_STATUS.PAUSED;
  state.currentJob = null;
  state.currentIssue = null;
  state.jobId = null;
  state.jobPid = null;
  state.startedAt = null;
  state.currentJobStartedAt = null;
  clearCiGateSubagent(state);
}

function touchSubagent(state, jobId, worker, nowIso) {
  ensureSubagents(state);
  const existing = state.subagents.active.find((s) => s.jobId === jobId);
  if (existing) {
    existing.lastHeartbeatAt = nowIso;
    existing.state = 'running';
    return;
  }
  state.subagents.active.push({
    jobId,
    worker,
    state: 'running',
    startedAt: nowIso,
    lastHeartbeatAt: nowIso,
  });
}

function finishSubagent(state, jobId, status, nowIso) {
  ensureSubagents(state);
  state.subagents.active = state.subagents.active.filter((s) => s.jobId !== jobId);
  state.subagents.lastFinished = { jobId, status, at: nowIso };
}

function clearCiGateSubagent(state) {
  ensureSubagents(state);
  const before = state.subagents.active.length;
  state.subagents.active = state.subagents.active.filter((s) => s.role !== 'ci-gate');
  return state.subagents.active.length !== before;
}

function touchCiGateSubagent(state, issue, nowIso) {
  ensureSubagents(state);
  const id = `ci-gate-${issue ?? 'unknown'}`;
  const existing = state.subagents.active.find((s) => s.jobId === id);
  if (existing) {
    existing.lastHeartbeatAt = nowIso;
    existing.state = 'waiting-ci';
    return false;
  }
  state.subagents.active.push({
    jobId: id,
    worker: 'orchestrator',
    role: 'ci-gate',
    issue: issue ?? null,
    state: 'waiting-ci',
    startedAt: nowIso,
    lastHeartbeatAt: nowIso,
  });
  return true;
}

function buildTransitionKey(next, out, prioritizedOpen, openPrs) {
  const parts = [
    `status:${next.status || ''}`,
    `issue:${next.currentIssue || ''}`,
    `job:${next.currentJob || ''}`,
    `prio:${prioritizedOpen.length}`,
    `prs:${openPrs.length}`,
    `t:${(out.transitions || []).join(' || ')}`,
  ];
  return parts.join('|');
}

function buildTransitionMessage(next, out, prioritizedOpen, openPrs, nowIso) {
  const first = out.transitions[0] || 'state transition';
  const more = out.transitions.length > 1 ? ` (+${out.transitions.length - 1} more)` : '';
  const issue = next.currentIssue ? `#${next.currentIssue}` : 'none';
  return [
    `Pipeline transition (${PIPELINE_REPO})`,
    `${first}${more}`,
    `status=${next.status} issue=${issue} openPRs=${openPrs.length} prioritized=${prioritizedOpen.length}`,
    `at=${nowIso}`,
  ].join('\n');
}

function isRateLimitText(text) {
  const normalized = String(text || '').toLowerCase();
  return (
    normalized.includes('rate limit') ||
    normalized.includes('rate-limited') ||
    normalized.includes('429') ||
    normalized.includes('quota') ||
    normalized.includes('usage limit') ||
    normalized.includes('too many requests') ||
    normalized.includes('out of extra usage')
  );
}

// Transitions worth notifying about — actual state changes the user or agent should know about.
// Everything else (self-heal, branch prep, heartbeats) is internal bookkeeping.
const SIGNIFICANT_TRANSITION_PATTERNS = [
  /-> running/,         // job started
  /-> pr-pending/,      // job completed, waiting for PR merge
  /-> idle/,            // job finished (completed, failed, or PR merged)
  /-> blocked/,         // something broke
  /-> paused/,          // pipeline paused
  /paused ->/,          // pipeline resumed
  /auto-merge enabled/, // PR auto-merge triggered
  /direct-merged/,      // PR merged directly
  /start failed/,       // job failed to start
];

function hasSignificantTransition(transitions) {
  return transitions.some((t) =>
    SIGNIFICANT_TRANSITION_PATTERNS.some((pattern) => pattern.test(t)),
  );
}

async function maybeNotifyTransition(next, out, prioritizedOpen, openPrs, nowIso) {
  if (!NOTIFY_ENABLED || out.transitions.length === 0) return false;
  if (!hasSignificantTransition(out.transitions)) return false;
  const paused = next.status === PIPELINE_STATUS.PAUSED || next.autoPickup === false;
  const pauseTransition = out.transitions.some(
    (t) => String(t).includes('-> paused') || String(t).includes('paused ->'),
  );
  if (paused && !pauseTransition) return false;
  if (!NOTIFY_TARGET) {
    next.lastNotifyError = 'notify enabled but PIPELINE_NOTIFY_TARGET missing';
    return false;
  }
  const key = buildTransitionKey(next, out, prioritizedOpen, openPrs);
  if (next.lastNotifiedTransitionKey === key) return false;

  const message = buildTransitionMessage(next, out, prioritizedOpen, openPrs, nowIso);
  const send = await runProc(
    NOTIFY_BIN,
    ['message', 'send', '--channel', NOTIFY_CHANNEL, '--target', NOTIFY_TARGET, '--message', message],
    { cwd: PIPELINE_PROJECT_PATH },
  );
  if (send.code === 0) {
    next.lastNotifiedTransitionKey = key;
    next.lastNotifiedAt = nowIso;
    next.lastNotifyError = null;
    out.notified = true;
    return true;
  }
  next.lastNotifyError = `notify failed: ${send.stderr || send.stdout}`;
  out.notified = false;
  return true;
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  const dryRun = Boolean(args['dry-run']);
  const nowMs = Date.now();
  const nowIso = toIso(nowMs);

  const out = {
    dryRun,
    repo: PIPELINE_REPO,
    projectPath: PIPELINE_PROJECT_PATH,
    maxWip: MAX_WIP,
    changed: false,
    transitions: [],
    endState: null,
    workerDecision: null,
    notified: null,
  };

  const state = await readJson(PIPELINE_STATE_PATH);
  const next = { ...state };
  ensureQueue(next);
  ensureStatus(next);
  next.maxWip = MAX_WIP;
  if (typeof next.autoPickup !== 'boolean') next.autoPickup = true;
  if (!next.desiredEndState) next.desiredEndState = 'idle-and-no-prioritized-or-prs';

  const currentIssue = parseIssue(next.currentIssue || next.currentTicket);
  const currentJob = next.currentJob || next.jobId || null;
  if (currentIssue != null && next.currentIssue !== currentIssue) {
    next.currentIssue = currentIssue;
    out.changed = true;
  }
  if (
    (next.status === PIPELINE_STATUS.IDLE ||
      next.status === PIPELINE_STATUS.WAITING_ON_PR ||
      next.status === PIPELINE_STATUS.BLOCKED ||
      next.status === PIPELINE_STATUS.PAUSED) &&
    (next.currentJob || next.jobId || next.jobPid || next.currentIssue || next.currentTicket || next.currentWorker)
  ) {
    const prev = next.status;
    if (prev === PIPELINE_STATUS.WAITING_ON_PR) setWaitingOnPr(next);
    else if (prev === PIPELINE_STATUS.BLOCKED) setBlocked(next);
    else if (prev === PIPELINE_STATUS.PAUSED) setPaused(next);
    else setIdle(next);
    out.changed = true;
  }

  if (next.status === PIPELINE_STATUS.PR_PENDING) {
    // Normalize legacy runtime fields so stale watchdog-era values do not leak across phases.
    const hadRuntimeFields = Boolean(next.currentJob || next.jobId || next.jobPid || next.currentWorker);
    next.currentJob = null;
    next.jobId = null;
    next.jobPid = null;
    next.currentWorker = null;
    next.currentJobStartedAt = null;
    if (hadRuntimeFields) out.changed = true;
  }

  if (next.status === PIPELINE_STATUS.RUNNING && !currentJob) {
    setBlocked(next);
    next.lastError = `running without currentJob at ${nowIso}`;
    out.transitions.push('running -> blocked (missing currentJob)');
    out.changed = true;
  }

  if (next.status === PIPELINE_STATUS.RUNNING && currentJob) {
    const s = await runManagerJson(['status', '--job-id', currentJob]);
    touchSubagent(next, currentJob, next.currentWorker || 'claude', nowIso);
    if (clearCiGateSubagent(next)) out.changed = true;
    if (s.status === 'completed' || s.status === 'failed' || s.status === 'killed' || s.status === 'not_found') {
      const r = await runManagerJson(['result', '--job-id', currentJob]);
      const rateLimited = Boolean(s.rateLimited || r.rateLimited);
      finishSubagent(next, currentJob, s.status, nowIso);
      if (s.status === 'completed') {
        next.status = PIPELINE_STATUS.PR_PENDING;
        next.currentIssue = currentIssue;
        next.currentTicket = currentIssue ? `#${currentIssue}` : next.currentTicket || null;
        next.lastError = null;
        next.lastWorkerCompleted = next.currentWorker || 'claude';
        next.lastWorkerCompletedAt = nowIso;
        next.currentJob = null;
        next.jobId = null;
        next.jobPid = null;
        next.startedAt = null;
        next.currentJobStartedAt = null;
        out.transitions.push(`running -> pr-pending (${currentJob})`);
      } else {
        setIdle(next);
        next.lastError = `${currentJob} ${s.status} at ${nowIso}${rateLimited ? ' (rate-limited)' : ''}`;
        if (rateLimited) {
          next.claudeCooldownUntil = toIso(nowMs + COOLDOWN_MINUTES * 60 * 1000);
          next.nextWorker = 'codex';
        }
        out.transitions.push(`running -> idle (${currentJob} ${s.status})`);
      }
      out.changed = true;
    }
  }

  if (next.status === PIPELINE_STATUS.PR_PENDING) {
    if (touchCiGateSubagent(next, parseIssue(next.currentIssue || next.currentTicket), nowIso)) {
      out.changed = true;
    }
    const prs = await ghJson([
      'pr',
      'list',
      '--repo',
      PIPELINE_REPO,
      '--state',
      'open',
      '--json',
      'number,title,body,headRefName,mergeable,statusCheckRollup,updatedAt,author',
      '--limit',
      '50',
    ]);
    const targetIssue = parseIssue(next.currentIssue || next.currentTicket);
    const target = findTargetPr(prs, targetIssue, next);

    if (!target) {
      const merged = await ghJson([
        'pr',
        'list',
        '--repo',
        PIPELINE_REPO,
        '--state',
        'merged',
        '--json',
        'number,title,body,headRefName,mergedAt',
        '--limit',
        '50',
      ]);
      const mergedMatch = merged.find((pr) =>
        isIssueMentioned(pr.headRefName, targetIssue) ||
        isIssueMentioned(pr.title, targetIssue) ||
        isIssueMentioned(pr.body, targetIssue),
      );
      if (mergedMatch) {
        if (targetIssue) {
          await runProc('gh', ['issue', 'close', String(targetIssue), '--repo', PIPELINE_REPO], {
            cwd: PIPELINE_PROJECT_PATH,
          });
        }
        setIdle(next);
        next.lastCompleted = {
          issue: targetIssue,
          pr: mergedMatch.number,
          completedAt: String(mergedMatch.mergedAt || nowIso),
        };
        next.lastError = null;
        out.transitions.push(`pr-pending -> idle (detected already-merged PR #${mergedMatch.number})`);
        out.changed = true;
      } else {
        // No open or merged PR found. Check if the branch exists with commits
        // ahead of main — if so, the agent completed work but failed to create
        // the PR (common during GitHub rate limiting). Create it now.
        const branch = next.currentBranch || (targetIssue ? `issue-${targetIssue}` : null);
        let recovered = false;
        if (branch) {
          const diff = await runProc(
            'gh', ['api', `repos/${PIPELINE_REPO}/compare/main...${branch}`, '--jq', '.ahead_by // 0'],
            { cwd: PIPELINE_PROJECT_PATH },
          );
          const aheadBy = Number(String(diff.stdout).trim());
          if (diff.code === 0 && aheadBy > 0) {
            const title = targetIssue
              ? `fix: automated PR for issue #${targetIssue} (fixes #${targetIssue})`
              : `fix: automated PR from branch ${branch}`;
            const create = await runProc(
              'gh',
              ['pr', 'create', '--repo', PIPELINE_REPO, '--head', branch, '--base', 'main',
               '--title', title, '--body', `Closes #${targetIssue || ''}\n\nAutomated PR created by pipeline orchestrator (branch had ${aheadBy} commit(s) ahead of main but no PR).`],
              { cwd: PIPELINE_PROJECT_PATH },
            );
            if (create.code === 0) {
              const prUrl = String(create.stdout).trim();
              const prNum = Number(String(prUrl).match(/\/(\d+)$/)?.[1] || 0);
              next.lastError = null;
              out.transitions.push(`pr-pending -> pr-pending (auto-created PR #${prNum || prUrl} for ${branch})`);
              out.changed = true;
              recovered = true;
            }
          }
        }
        if (!recovered) {
          setIdle(next);
          next.lastError = `pr-pending without matching open PR for issue ${targetIssue ?? 'unknown'} at ${nowIso}`;
          out.transitions.push('pr-pending -> idle (missing open PR)');
          out.changed = true;
        }
      }
    } else {
      // Policy: never direct-merge from orchestrator.
      // Always request GitHub auto-merge so required reviews/checks gate the merge.
      if (next.autoMergeRequestedFor !== target.number) {
        const auto = await runProc(
          'gh',
          ['pr', 'merge', String(target.number), '--repo', PIPELINE_REPO, '--auto', '--squash', '--delete-branch'],
          { cwd: PIPELINE_PROJECT_PATH },
        );
        if (auto.code === 0) {
          next.autoMergeRequestedFor = target.number;
          next.autoMergeRequestedAt = nowIso;
          next.lastError = null;
          out.transitions.push(`pr-pending auto-merge enabled (#${target.number})`);
          out.changed = true;
        } else {
          next.lastError = `auto-merge setup failed for PR #${target.number}: ${auto.stderr || auto.stdout}`;
          out.changed = true;
        }
      } else if (String(target.mergeable || '').toUpperCase() === 'MERGEABLE' && checksGreen(target.statusCheckRollup)) {
        // Auto-merge should complete shortly once checks/approvals are satisfied.
        next.lastError = null;
      }
    }
  }

  if (next.status === PIPELINE_STATUS.IDLE && String(next.lastError || '').includes('pr-pending without matching open PR')) {
    next.status = PIPELINE_STATUS.PR_PENDING;
    out.transitions.push('idle -> pr-pending (missing open PR hold)');
    out.changed = true;
  }

  const issues = await ghJson([
    'issue',
    'list',
    '--repo',
    PIPELINE_REPO,
    '--state',
    'open',
    '--json',
    'number,title,labels',
    '--limit',
    '200',
  ]);

  const eligible = issues
    .map((i) => {
      const labels = (i.labels || []).map((l) => l.name);
      return {
        number: i.number,
        title: i.title,
        labels,
        priority: priorityRank(labels),
        size: sizeRank(labels),
        blocked: blockedReasons(labels),
      };
    })
    .filter((i) => i.priority != null && i.blocked.length === 0)
    .sort((a, b) => a.priority - b.priority || a.size - b.size || a.number - b.number);

  const prioritizedOpen = eligible.map((i) => i.number);
  const prioritizedSet = new Set(prioritizedOpen);
  const openPrs = await ghJson([
    'pr',
    'list',
    '--repo',
    PIPELINE_REPO,
    '--state',
    'open',
    '--json',
    'number,author,title,headRefName,updatedAt,autoMergeRequest,mergeable,statusCheckRollup,isDraft',
    '--limit',
    '100',
  ]);
  const openPipelinePrs = openPrs.filter((pr) => {
    const author = String(pr?.author?.login || '').toLowerCase();
    return BOT_AUTHOR_LOGINS.includes(author);
  });

  if (openPipelinePrs.length === 0 && next.status === PIPELINE_STATUS.WAITING_ON_PR) {
    setIdle(next);
    out.transitions.push('waiting-on-pr -> idle (open PR gate cleared)');
    out.changed = true;
  }

  if (
    (next.status === PIPELINE_STATUS.IDLE ||
      next.status === PIPELINE_STATUS.WAITING_ON_PR ||
      next.status === PIPELINE_STATUS.BLOCKED ||
      next.status === PIPELINE_STATUS.PAUSED ||
      next.status === PIPELINE_STATUS.RUNNING) &&
    next.autoPickup === true
  ) {
    if (clearCiGateSubagent(next)) out.changed = true;
    if (openPipelinePrs.length > 0) {
      // Enforce strict serial flow: never start next issue while pipeline PR(s) remain open.
      if (next.status !== PIPELINE_STATUS.WAITING_ON_PR && next.status !== PIPELINE_STATUS.RUNNING) {
        const from = next.status;
        setWaitingOnPr(next);
        out.transitions.push(`${from} -> waiting-on-pr (open PR gate)`);
        out.changed = true;
      }
      out.workerDecision = { worker: null, reason: 'open-pr-gate' };
      let autoMergeErrors = 0;
      for (const pr of openPipelinePrs) {
        if (pr?.autoMergeRequest) continue;
        const auto = await runProc(
          'gh',
          ['pr', 'merge', String(pr.number), '--repo', PIPELINE_REPO, '--auto', '--squash', '--delete-branch'],
          { cwd: PIPELINE_PROJECT_PATH },
        );
        if (auto.code === 0) {
          out.transitions.push(`open-pr-gate auto-merge enabled (#${pr.number})`);
          out.changed = true;
        } else {
          const msg = String(auto.stderr || auto.stdout || '');
          const autoMergeUnsupported = msg.includes('Auto merge is not allowed for this repository');
          const readyForDirectMerge =
            !Boolean(pr?.isDraft) &&
            String(pr?.mergeable || '').toUpperCase() === 'MERGEABLE' &&
            checksGreen(pr?.statusCheckRollup);
          if (autoMergeUnsupported && readyForDirectMerge) {
            const merge = await runProc(
              'gh',
              ['pr', 'merge', String(pr.number), '--repo', PIPELINE_REPO, '--squash', '--delete-branch'],
              { cwd: PIPELINE_PROJECT_PATH },
            );
            if (merge.code === 0) {
              out.transitions.push(`open-pr-gate direct-merged (#${pr.number})`);
              out.changed = true;
              continue;
            }
            const mergeMsg = String(merge.stderr || merge.stdout || '');
            if (isBranchOutdatedMergeError(mergeMsg)) {
              const updated = await runProc(
                'gh',
                ['pr', 'update-branch', String(pr.number), '--repo', PIPELINE_REPO],
                { cwd: PIPELINE_PROJECT_PATH },
              );
              if (updated.code === 0) {
                out.transitions.push(`open-pr-gate updated branch (#${pr.number})`);
                next.lastError = null;
                out.changed = true;
                continue;
              }
              autoMergeErrors++;
              next.lastError = `update-branch failed for PR #${pr.number}: ${updated.stderr || updated.stdout}`;
              out.changed = true;
            } else {
              autoMergeErrors++;
              next.lastError = `direct merge failed for PR #${pr.number}: ${mergeMsg}`;
              out.changed = true;
            }
          } else if (autoMergeUnsupported) {
            // Repository-level auto-merge disabled; stay gated until checks/review allow direct merge.
            next.lastError = null;
          } else {
            autoMergeErrors++;
            next.lastError = `auto-merge setup failed for PR #${pr.number}: ${msg}`;
            out.changed = true;
          }
        }
      }
      if (autoMergeErrors === 0) {
        next.lastError = null;
      }
      out.endState = {
        desired: next.desiredEndState,
        achieved: false,
        prioritizedOpen: prioritizedOpen.length,
        openPrs: openPrs.length,
        status: next.status,
        currentWorker: next.currentWorker || null,
      };
      next.lastOrchestratedAt = nowIso;
      next.maxWip = MAX_WIP;
      next.defaultWorker = DEFAULT_WORKER;
      next.fallbackWorker = 'codex';
      if (await maybeNotifyTransition(next, out, prioritizedOpen, openPrs, nowIso)) {
        out.changed = true;
      }
      if (!dryRun && out.changed) {
        await writeJson(PIPELINE_STATE_PATH, next);
      }
      process.stdout.write(JSON.stringify(out, null, 2) + '\n');
      return;
    }
    if (next.queue.length > 0) {
      const before = next.queue.length;
      const activeIssue = parseIssue(next.currentIssue) ?? parseIssue(next.currentJob);
      next.queue = next.queue
        .map((n) => parseIssue(n))
        .filter((n) => Number.isFinite(n) && prioritizedSet.has(n) && n !== activeIssue);
      if (next.queue.length !== before) {
        out.transitions.push('self-heal (dropped non-eligible queue items)');
        out.changed = true;
      }
    }
    if (next.queue.length === 0) {
      next.queue = prioritizedOpen.slice();
      out.changed = true;
    }
    if (next.queue.length > 0) {
      const head = parseIssue(next.queue[0]);
      const cooldownUntil = Date.parse(String(next.claudeCooldownUntil || ''));
      const cooldownActive = Number.isFinite(cooldownUntil) && cooldownUntil > nowMs;
      let worker = next.nextWorker === 'codex' || cooldownActive ? 'codex' : DEFAULT_WORKER;
      if (worker === 'codex' && !(await hasCodexBinary())) {
        next.lastError = `Codex fallback requested but codex CLI missing on host`;
        if (next.status !== PIPELINE_STATUS.BLOCKED) {
          const from = next.status;
          next.status = PIPELINE_STATUS.BLOCKED;
          out.transitions.push(`${from} -> blocked (codex binary missing)`);
        }
        out.changed = true;
        out.workerDecision = { worker, reason: 'binary-missing' };
      } else if (head != null && MAX_WIP === 1 && !next.currentJob) {
        const prep = await prepareBranchForIssue(head);
        if (!prep.ok) {
          next.lastError = `branch prep failed for issue-${head}: ${prep.error}`;
          next.status = PIPELINE_STATUS.BLOCKED;
          out.transitions.push(`idle -> blocked (branch prep failed issue-${head})`);
          out.changed = true;
        } else {
          if (prep.healed) {
            out.transitions.push(`self-heal (${prep.healNote || `prepared branch state for issue-${head}`})`);
            out.changed = true;
          }
          const jobId = `issue-${head}`;
          const startArgs = [
            'start',
            '--job-id',
            jobId,
            '--cwd',
            PIPELINE_PROJECT_PATH,
            '--orchestrated',
            'true',
            '--worker',
            worker,
            '--prompt',
            buildJobPrompt(head),
          ];
          if (worker === 'codex') {
            startArgs.push('--model', CODEX_MODEL);
          } else if (CLAUDE_MODEL) {
            startArgs.push('--model', CLAUDE_MODEL);
          }
          try {
            const started = await runManagerJson(startArgs);
            if (started.status === 'running') {
              next.status = PIPELINE_STATUS.RUNNING;
              next.currentJob = jobId;
              next.jobId = jobId;
              next.currentIssue = head;
              next.currentTicket = `#${head}`;
              next.currentWorker = worker;
              next.startedAt = nowIso;
              next.currentJobStartedAt = nowIso;
              next.currentBranch = prep.branch;
              next.nextWorker = null;
              next.queue = next.queue.filter((n) => n !== head);
              touchSubagent(next, jobId, worker, nowIso);
              out.transitions.push(`idle -> running (${jobId}, worker=${worker})`);
              out.workerDecision = { worker, reason: cooldownActive ? 'claude-cooldown' : 'default' };
              out.changed = true;
            }
          } catch (err) {
            const msg = String(err?.message || err);
            const startRateLimited =
              isRateLimitText(msg);
            next.lastError = `start failed for ${jobId} (${worker}): ${msg}`;
            if (worker === 'claude' && startRateLimited) {
              next.claudeCooldownUntil = toIso(nowMs + COOLDOWN_MINUTES * 60 * 1000);
              next.nextWorker = 'codex';
            }
            next.status = PIPELINE_STATUS.BLOCKED;
            out.transitions.push(`idle start failed (${jobId}, worker=${worker})`);
            out.changed = true;
          }
        }
      }
    }
  }

  if (next.autoPickup !== true && prioritizedOpen.length > 0 && next.status === PIPELINE_STATUS.IDLE) {
    setPaused(next);
    out.transitions.push('idle -> paused (autoPickup disabled with prioritized queue)');
    out.changed = true;
  }

  if (next.status === PIPELINE_STATUS.IDLE && prioritizedOpen.length > 0) {
    setBlocked(next);
    next.lastError = next.lastError || `idle with prioritized queue at ${nowIso}`;
    out.transitions.push('idle -> blocked (prioritized queue present)');
    out.changed = true;
  }
  if (next.status === PIPELINE_STATUS.IDLE && prioritizedOpen.length === 0 && openPrs.length === 0 && next.lastError) {
    next.lastError = null;
    out.changed = true;
  }

  out.endState = {
    desired: next.desiredEndState,
    achieved:
      prioritizedOpen.length === 0 &&
      openPrs.length === 0 &&
      next.status === PIPELINE_STATUS.IDLE,
    prioritizedOpen: prioritizedOpen.length,
    openPrs: openPrs.length,
    status: next.status,
    currentWorker: next.currentWorker || null,
  };
  next.lastOrchestratedAt = nowIso;
  next.maxWip = MAX_WIP;
  next.defaultWorker = DEFAULT_WORKER;
  next.fallbackWorker = 'codex';
  if (await maybeNotifyTransition(next, out, prioritizedOpen, openPrs, nowIso)) {
    out.changed = true;
  }

  if (!dryRun && out.changed) {
    await writeJson(PIPELINE_STATE_PATH, next);
  }

  process.stdout.write(JSON.stringify(out, null, 2) + '\n');
}

run().catch((err) => {
  process.stderr.write(`${err?.stack || err?.message || String(err)}\n`);
  process.exit(1);
});
