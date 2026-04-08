import { spawn } from 'node:child_process';
import { readFile, writeFile, appendFile, mkdir, open, rm } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const SKILL_DIR = join(__dirname, '..');
const JOBS_DIR = process.env.CLAUDE_SKILL_JOBS_DIR || join(SKILL_DIR, 'jobs');

const jobId = process.argv[2];
if (!jobId) {
  console.error('Usage: node cli-worker.mjs <jobId>');
  process.exit(1);
}

const metaFile = join(JOBS_DIR, jobId, 'meta.json');
const outputFile = join(JOBS_DIR, jobId, 'output.log');
const resultFile = join(JOBS_DIR, jobId, 'result.json');
const CODEX_LOCK_DIR = process.env.PIPELINE_LOCK_DIR || `${process.env.HOME || ''}/.openclaw/locks`;
const CODEX_LOCK_FILE = join(CODEX_LOCK_DIR, 'openai-codex-refresh.lock');
const CODEX_LOCK_WAIT_MS = Math.max(250, Number(process.env.CODEX_REFRESH_LOCK_WAIT_MS || 2000));
const CODEX_LOCK_STALE_MS = Math.max(60_000, Number(process.env.CODEX_REFRESH_LOCK_STALE_MS || 20 * 60 * 1000));

async function readMeta() {
  return JSON.parse(await readFile(metaFile, 'utf-8'));
}

async function writeMeta(meta) {
  await writeFile(metaFile, JSON.stringify(meta, null, 2) + '\n');
}

async function log(text) {
  await appendFile(outputFile, text);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Markers that indicate the agent has finished its work (PR merged, repo cleaned up).
// The openclaw agent --local process stays alive after completing, so we detect
// completion from output and gracefully terminate.
const COMPLETION_MARKERS = [
  /PR is (?:already )?\*?\*?MERGED\*?\*?/i,
  /Returned repo to main/i,
  /Returned to.*main/i,
  /git checkout main.*\n.*git pull --ff-only origin main/s,
  /Final repo state/i,
  /Process cleanup/i,
  /could\s+\*?\*?not\*?\*?\s+open the PR/i,
  /Resource not accessible by personal access token/i,
  /No (?:background |long-running )?processes? left running/i,
];
const COMPLETION_GRACE_MS = Number(process.env.PIPELINE_COMPLETION_GRACE_MS || 30_000);

function hasCompletionMarker(text) {
  return COMPLETION_MARKERS.some((re) => re.test(text));
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

async function emitSystemEvent(text, attempts = 3) {
  const openclawBin = process.env.OPENCLAW_BINARY_PATH || 'openclaw';

  for (let attempt = 1; attempt <= attempts; attempt++) {
    const ok = await new Promise((resolve) => {
      const proc = spawn(openclawBin, ['system', 'event', '--text', text, '--mode', 'now'], {
        cwd: SKILL_DIR,
        env: { ...process.env },
        stdio: 'ignore',
      });
      proc.once('error', () => resolve(false));
      proc.once('close', (code) => resolve(code === 0));
    });

    if (ok) {
      await log(`[worker] Emitted system event: ${text}\n`);
      return true;
    }

    await log(`[worker] WARN: system event emit failed (${attempt}/${attempts}): ${text}\n`);
    if (attempt < attempts) {
      await sleep(attempt * 1500);
    }
  }

  return false;
}

async function acquireCodexLock() {
  await mkdir(CODEX_LOCK_DIR, { recursive: true });
  while (true) {
    try {
      const handle = await open(CODEX_LOCK_FILE, 'wx');
      await handle.writeFile(`${process.pid}\n${Date.now()}\n`);
      await handle.close();
      await log(`[worker] Acquired codex lock: ${CODEX_LOCK_FILE}\n`);
      return true;
    } catch {
      try {
        const lockBody = await readFile(CODEX_LOCK_FILE, 'utf-8');
        const parts = String(lockBody || '').trim().split('\n');
        const lockTs = Number(parts[1] || 0);
        if (Number.isFinite(lockTs) && Date.now() - lockTs > CODEX_LOCK_STALE_MS) {
          await rm(CODEX_LOCK_FILE, { force: true });
          await log(`[worker] Removed stale codex lock (${Date.now() - lockTs}ms old)\n`);
          continue;
        }
      } catch {
        // Lock vanished or unreadable; retry.
      }
      await log(`[worker] Waiting for codex lock...\n`);
      await sleep(CODEX_LOCK_WAIT_MS);
    }
  }
}

async function releaseCodexLock() {
  await rm(CODEX_LOCK_FILE, { force: true });
}

let shuttingDown = false;
let completionDetected = false;
let childProcess = null;
let codexLockHeld = false;

process.on('SIGTERM', async () => {
  shuttingDown = true;
  await log('\n[worker] Received SIGTERM, stopping child...\n');
  if (childProcess) {
    childProcess.kill('SIGTERM');
  }
  if (codexLockHeld) {
    await releaseCodexLock();
    codexLockHeld = false;
  }
  const meta = await readMeta();
  meta.status = 'killed';
  meta.endedAt = new Date().toISOString();
  await writeMeta(meta);
  process.exit(0);
});

async function run() {
  const meta = await readMeta();
  await log(`[worker] Starting CLI job ${jobId}\n`);

  const worker = String(meta.worker || 'codex').toLowerCase();
  let bin;
  let args;
  if (worker === 'codex') {
    bin = process.env.OPENCLAW_BINARY_PATH || 'openclaw';
    args = [
      'agent',
      '--agent', 'pipeline-codex',
      '--local',
      '--message', meta.prompt,
    ];
  } else {
    bin = process.env.CLAUDE_BINARY_PATH || 'claude';
    args = [
      '--no-session-persistence',
      '--dangerously-skip-permissions',
      '--print',
    ];
    if (meta.model) {
      args.push('--model', meta.model);
    }
    args.push(meta.prompt);
  }

  await log(`[worker] Worker: ${worker}\n`);
  await log(`[worker] Cmd: ${bin} ${args.join(' ')}\n`);
  await log(`[worker] CWD: ${meta.cwd}\n`);

  childProcess = spawn(bin, args, {
    cwd: meta.cwd,
    env: { ...process.env, CI: 'true', FORCE_COLOR: '0' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let outputBuffer = '';

  childProcess.stdout.on('data', async (data) => {
    const text = data.toString();
    outputBuffer += text;
    await log(text);

    // Detect when the agent has finished its work (PR merged, repo cleaned up)
    // and schedule a graceful shutdown since --local agents don't exit on their own.
    if (!completionDetected && hasCompletionMarker(outputBuffer)) {
      completionDetected = true;
      await log(`\n[worker] Completion marker detected, waiting ${COMPLETION_GRACE_MS / 1000}s before terminating agent...\n`);
      setTimeout(async () => {
        if (childProcess && childProcess.exitCode === null) {
          await log(`[worker] Grace period elapsed, sending SIGTERM to agent\n`);
          childProcess.kill('SIGTERM');
          // Force kill after 10s if SIGTERM doesn't work — check exitCode
          // instead of .killed since .killed is true after signal is sent,
          // regardless of whether the process actually exited.
          setTimeout(async () => {
            if (childProcess && childProcess.exitCode === null) {
              await log(`[worker] SIGTERM did not terminate agent, sending SIGKILL\n`);
              childProcess.kill('SIGKILL');
            }
          }, 10_000);
        }
      }, COMPLETION_GRACE_MS);
    }
  });

  childProcess.stderr.on('data', async (data) => {
    const text = data.toString();
    await log(`[stderr] ${text}`);
  });

  childProcess.on('error', async (err) => {
    const msg = `Failed to start ${worker}: ${err.message}`;
    await log(`\n[worker] ERROR: ${msg}\n`);
    meta.status = 'failed';
    meta.error = msg;
    meta.endedAt = new Date().toISOString();
    await writeMeta(meta);
  });

  childProcess.on('close', async (code) => {
    const endTime = new Date().toISOString();
    await log(`\n[worker] Process exited with code ${code}\n`);

    if (shuttingDown) return;
    if (codexLockHeld) {
      await releaseCodexLock();
      codexLockHeld = false;
    }

    let eventText = '';
    // Treat completion-marker-triggered termination as success regardless of exit code
    if (code === 0 || completionDetected) {
      meta.status = 'completed';

      // Try to parse cost/duration from output if Claude prints it
      // Claude Code output often ends with cost summary.
      // We'll just save the raw text as result for now.

      const resultData = {
        jobId,
        status: 'completed',
        result: outputBuffer,
        exitCode: code,
      };
      await writeFile(resultFile, JSON.stringify(resultData, null, 2) + '\n');
      eventText = `JOB_DONE:${jobId}`;
    } else {
      const combined = outputBuffer.toLowerCase();
      const rateLimited = isRateLimitText(combined);
      meta.status = 'failed';
      meta.error = `CLI exited with code ${code}`;
      meta.rateLimited = rateLimited;

      const resultData = {
        jobId,
        status: 'failed',
        worker,
        rateLimited,
        result: outputBuffer,
        exitCode: code,
      };
      await writeFile(resultFile, JSON.stringify(resultData, null, 2) + '\n');
      eventText = `JOB_FAILED:${jobId}`;
    }

    meta.endedAt = endTime;
    await writeMeta(meta);
    if (eventText) {
      await emitSystemEvent(eventText);
    }
  });
}

run().catch(async (err) => {
  if (codexLockHeld) {
    await releaseCodexLock();
    codexLockHeld = false;
  }
  await log(`[worker] Unhandled exception: ${err.message}\n`);
  process.exit(1);
});
