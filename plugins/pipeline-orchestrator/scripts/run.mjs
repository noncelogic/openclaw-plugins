#!/usr/bin/env node

import { start, status, result, logs, list, kill, wait } from './job-manager.mjs';
import { spawn } from 'node:child_process';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const STRICT_PIPELINE_START = String(process.env.PIPELINE_STRICT_START || 'false').toLowerCase() === 'true';

function parseArgs(args) {
  const parsed = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith('--')) {
      const key = args[i].slice(2);
      const next = args[i + 1];
      if (next && !next.startsWith('--')) {
        parsed[key] = next;
        i++;
      } else {
        parsed[key] = true;
      }
    }
  }
  return parsed;
}

function output(data) {
  process.stdout.write(JSON.stringify(data, null, 2) + '\n');
}

function die(message) {
  output({ error: message });
  process.exit(1);
}

async function runPipelineReconcile(options = {}) {
  const args = ['pipeline-reconcile.mjs'];
  if (options.dryRun) args.push('--dry-run');
  if (options.noStateWrite) args.push('--no-state-write');
  return await new Promise((resolve, reject) => {
    const proc = spawn(process.execPath, args, {
      cwd: __dirname,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env },
    });
    let stdout = '';
    let stderr = '';
    proc.stdout.on('data', (d) => {
      stdout += d.toString();
    });
    proc.stderr.on('data', (d) => {
      stderr += d.toString();
    });
    proc.on('error', reject);
    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(stderr || `pipeline-reconcile exited ${code}`));
        return;
      }
      try {
        resolve(JSON.parse(stdout));
      } catch {
        resolve({ raw: stdout.trim() });
      }
    });
  });
}

async function runPipelineOrchestrator(options = {}) {
  const args = ['pipeline-orchestrator.mjs'];
  if (options.dryRun) args.push('--dry-run');
  return await new Promise((resolve, reject) => {
    const proc = spawn(process.execPath, args, {
      cwd: __dirname,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env },
    });
    let stdout = '';
    let stderr = '';
    proc.stdout.on('data', (d) => {
      stdout += d.toString();
    });
    proc.stderr.on('data', (d) => {
      stderr += d.toString();
    });
    proc.on('error', reject);
    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(stderr || `pipeline-orchestrator exited ${code}`));
        return;
      }
      try {
        resolve(JSON.parse(stdout));
      } catch {
        resolve({ raw: stdout.trim() });
      }
    });
  });
}

const [command, ...rest] = process.argv.slice(2);
const args = parseArgs(rest);

switch (command) {
  case 'start': {
    const prompt = args.prompt;
    const cwd = args.cwd || process.cwd();
    const jobId = args['job-id'];

    if (!prompt) die('--prompt is required');
    if (!jobId) die('--job-id is required');
    const isIssueJob = /^issue-\d+$/i.test(jobId);
    const orchestrated = String(args.orchestrated || '').toLowerCase() === 'true';
    if (STRICT_PIPELINE_START && isIssueJob && !orchestrated) {
      die(`strict pipeline start enabled: refusing non-orchestrated start for ${jobId}`);
    }

    const options = {};
    if (args.worker) options.worker = args.worker;
    if (args.model) options.model = args.model;

    const res = await start(jobId, prompt, cwd, options);
    output(res);
    break;
  }

  case 'status': {
    const jobId = args['job-id'];
    if (!jobId) die('--job-id is required');
    output(await status(jobId));
    break;
  }

  case 'result': {
    const jobId = args['job-id'];
    if (!jobId) die('--job-id is required');
    output(await result(jobId));
    break;
  }

  case 'logs': {
    const jobId = args['job-id'];
    if (!jobId) die('--job-id is required');
    const tail = parseInt(args.tail, 10) || 50;
    output(await logs(jobId, tail));
    break;
  }

  case 'list': {
    output(await list());
    break;
  }

  case 'kill': {
    const jobId = args['job-id'];
    if (!jobId) die('--job-id is required');
    output(await kill(jobId));
    break;
  }

  case 'wait': {
    const jobId = args['job-id'];
    if (!jobId) die('--job-id is required');
    const intervalMs = args['interval-ms'] ? Number(args['interval-ms']) : undefined;
    const timeoutSeconds = args['timeout-seconds'] ? Number(args['timeout-seconds']) : undefined;
    const timeoutMs = Number.isFinite(timeoutSeconds) ? timeoutSeconds * 1000 : undefined;
    output(await wait(jobId, { intervalMs, timeoutMs }));
    break;
  }

  case 'reconcile': {
    const dryRun = Boolean(args['dry-run']);
    const noStateWrite = Boolean(args['no-state-write']);
    output(await runPipelineReconcile({ dryRun, noStateWrite }));
    break;
  }

  case 'orchestrate': {
    const dryRun = Boolean(args['dry-run']);
    output(await runPipelineOrchestrator({ dryRun }));
    break;
  }

  default:
    die(
      `Unknown command: ${command || '(none)'}. ` +
        `Available: start, status, result, logs, list, kill, wait, reconcile, orchestrate`,
    );
}
