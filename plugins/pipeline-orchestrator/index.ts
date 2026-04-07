/**
 * OpenClaw Pipeline Orchestrator Plugin
 *
 * In-process pipeline management for GitHub issues, PRs, and CI.
 * Wraps the existing pipeline-orchestrator.mjs scripts and adds:
 * - Session-aware notifications via before_agent_start hook
 * - Pipeline tools (status, pause, resume) accessible to the agent
 * - CLI commands for manual pipeline management
 *
 * The orchestration logic lives in scripts/pipeline-orchestrator.mjs
 * and is invoked via scripts/run.mjs (same as the systemd timer).
 */

import { spawn } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { Type } from "@sinclair/typebox";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SCRIPTS_DIR = join(__dirname, "scripts");

// ============================================================================
// Registration Guard
// ============================================================================

// File-based singleton guard
import { existsSync as _plExists, writeFileSync as _plWrite, unlinkSync as _plUnlink } from "node:fs";
const _plGuardPath = `/tmp/openclaw-pl-${process.pid}.lock`;
process.once("exit", () => { try { _plUnlink(_plGuardPath); } catch {} });

// ============================================================================
// Helpers
// ============================================================================

async function runScript(
  scriptArgs: string[],
  env: Record<string, string> = {},
): Promise<{ code: number; stdout: string; stderr: string }> {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, scriptArgs, {
      cwd: SCRIPTS_DIR,
      env: { ...process.env, ...env },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d: Buffer) => {
      stdout += d.toString();
    });
    child.stderr.on("data", (d: Buffer) => {
      stderr += d.toString();
    });
    child.on("error", (err) =>
      resolve({ code: 1, stdout, stderr: `${stderr}\n${err.message}`.trim() }),
    );
    child.on("close", (code) => resolve({ code: code ?? 1, stdout, stderr }));
  });
}

async function readStateJson(statePath: string): Promise<Record<string, unknown>> {
  try {
    return JSON.parse(await readFile(statePath, "utf-8"));
  } catch {
    return {};
  }
}

// ============================================================================
// Plugin Definition
// ============================================================================

const pipelinePlugin = {
  id: "pipeline-orchestrator",
  name: "Pipeline Orchestrator",
  description:
    "In-process GitHub issue/PR pipeline management with session-aware notifications",
  configSchema: {
    type: "object" as const,
    additionalProperties: false,
    properties: {
      repo: { type: "string" },
      projectPath: { type: "string" },
      statePath: { type: "string" },
      intervalSec: { type: "number" },
      maxWip: { type: "number" },
      defaultWorker: { type: "string" },
      cooldownMinutes: { type: "number" },
      notifyEnabled: { type: "boolean" },
      notifyChannel: { type: "string" },
      notifyTarget: { type: "string" },
      selfHealDirtyWorktree: { type: "boolean" },
      telegramBotToken: { type: "string" },
    },
    required: ["repo", "projectPath"] as const,
  },

  register(api: OpenClawPluginApi) {
    // Always resolve config for hooks — hooks must register on every load
    // because the gateway may only bind hooks from the final registration pass.
    const cfg = api.pluginConfig as Record<string, unknown>;
    const STATE_PATH_FOR_HOOK = String(
      cfg.statePath ||
        join(process.env.HOME || "", ".openclaw", "workspace", "memory", "pipeline-state.json"),
    );
    const REPO_FOR_HOOK = String(cfg.repo || "noncelogic/signalflow");
    const pendingNotifications: string[] = [];

    api.on("before_agent_start", async (_event) => {
      // Inject rich pipeline state so the agent can reason about pipeline progress
      const state = await readStateJson(STATE_PATH_FOR_HOOK);
      api.logger.info?.(`pipeline-orchestrator: injecting pipeline context (status=${state.status || "unknown"})`);
      const status = String(state.status || "unknown");
      const currentJob = state.currentJob || null;
      const queue = Array.isArray(state.queue) ? state.queue : [];

      const lines: string[] = [];
      lines.push(`Pipeline (${REPO_FOR_HOOK})`);
      lines.push(`Status: ${status}`);

      if (currentJob) {
        const issue = state.currentIssue || "?";
        const worker = state.currentWorker || "unknown";
        const startedAt = state.currentJobStartedAt || state.startedAt || "?";
        lines.push(`Active job: ${currentJob} (issue #${issue}, worker: ${worker}, started: ${startedAt})`);
      }

      if (queue.length > 0) {
        lines.push(`Queue: ${queue.map((n: unknown) => `#${n}`).join(", ")} (${queue.length} pending)`);
      } else {
        lines.push("Queue: empty");
      }

      const lc = state.lastCompleted as Record<string, unknown> | undefined;
      if (lc?.issue) {
        lines.push(`Last completed: issue #${lc.issue} → PR #${lc.pr || "?"} (${lc.completedAt || "?"})`);
      }

      if (state.lastError) {
        lines.push(`Last error: ${String(state.lastError).slice(0, 200)}`);
      }

      if (pendingNotifications.length > 0) {
        lines.push("Recent events:");
        lines.push(...pendingNotifications.splice(0));
      }

      return {
        prependContext: `<pipeline-context>\n${lines.join("\n")}\n</pipeline-context>`,
      };
    });

    if (_plExists(_plGuardPath)) {
      api.logger.info("pipeline-orchestrator: skipping duplicate registration (pid-lock)");
      return;
    }
    try { _plWrite(_plGuardPath, String(Date.now())); } catch {}

    const REPO = String(cfg.repo || "noncelogic/signalflow");
    const PROJECT_PATH = String(cfg.projectPath || "");
    const STATE_PATH = STATE_PATH_FOR_HOOK;
    const INTERVAL = (Number(cfg.intervalSec) || 30) * 1000;
    const NOTIFY = cfg.notifyEnabled !== false;

    // Build env for scripts
    const scriptEnv: Record<string, string> = {
      PIPELINE_REPO: REPO,
      PIPELINE_PROJECT_PATH: PROJECT_PATH,
      PIPELINE_STATE_PATH: STATE_PATH,
      PIPELINE_MAX_WIP: String(cfg.maxWip || 1),
      PIPELINE_CLAUDE_COOLDOWN_MINUTES: String(cfg.cooldownMinutes || 120),
      PIPELINE_DEFAULT_WORKER: String(cfg.defaultWorker || "codex"),
      PIPELINE_SELF_HEAL_DIRTY_WORKTREE: String(
        cfg.selfHealDirtyWorktree !== false,
      ),
      PIPELINE_NOTIFY_ENABLED: String(NOTIFY),
      PIPELINE_NOTIFY_CHANNEL: String(cfg.notifyChannel || "telegram"),
      PIPELINE_NOTIFY_TARGET: String(cfg.notifyTarget || ""),
      PIPELINE_STRICT_START: "true",
    };
    if (cfg.telegramBotToken) {
      scriptEnv.PIPELINE_TELEGRAM_BOT_TOKEN = String(cfg.telegramBotToken);
    }

    let orchestrateTimer: ReturnType<typeof setInterval> | null = null;
    let orchestrating = false;

    api.logger.info(
      `pipeline-orchestrator: registered (repo=${REPO}, interval=${INTERVAL / 1000}s)`,
    );

    // ========================================================================
    // Session-aware notifications
    // ========================================================================

    function queueNotification(message: string) {
      pendingNotifications.push(`[${new Date().toISOString()}] ${message}`);
      while (pendingNotifications.length > 20) pendingNotifications.shift();
    }

    // ========================================================================
    // Orchestration loop
    // ========================================================================

    async function orchestrate() {
      if (orchestrating) return;
      orchestrating = true;
      try {
        const result = await runScript(
          [join(SCRIPTS_DIR, "run.mjs"), "orchestrate"],
          scriptEnv,
        );
        if (result.code !== 0) {
          api.logger.warn(
            `pipeline-orchestrator: cycle failed (code=${result.code}): ${result.stderr.slice(0, 200)}`,
          );
        } else {
          // Parse transitions from stdout for notifications
          try {
            const out = JSON.parse(result.stdout);
            if (Array.isArray(out.transitions) && out.transitions.length > 0) {
              for (const t of out.transitions) {
                queueNotification(String(t));
              }
            }
          } catch {
            // stdout may not be JSON if the script logged other things
          }
        }
      } catch (err) {
        api.logger.warn(
          `pipeline-orchestrator: cycle error: ${err instanceof Error ? err.message : String(err)}`,
        );
      } finally {
        orchestrating = false;
      }
    }

    // ========================================================================
    // Tools
    // ========================================================================

    api.registerTool(
      {
        name: "pipeline_status",
        label: "Pipeline Status",
        description:
          "Show current pipeline orchestrator status: running jobs, queued issues, open PRs, and recent transitions.",
        parameters: Type.Object({}),
        async execute(_toolCallId, _params) {
          const state = await readStateJson(STATE_PATH);
          const summary = [
            `Status: ${state.status || "unknown"}`,
            `Current job: ${state.currentJob || "none"}`,
            `Current worker: ${state.currentWorker || "none"}`,
            `Queue: ${Array.isArray(state.queue) ? state.queue.join(", ") : "empty"}`,
            `Auto-pickup: ${state.autoPickup !== false}`,
            `Last orchestrated: ${state.lastOrchestratedAt || "never"}`,
            `Last completed: ${JSON.stringify(state.lastCompleted || null)}`,
            `Last error: ${state.lastError || "none"}`,
            `Pending notifications: ${pendingNotifications.length}`,
          ];
          return {
            content: [{ type: "text", text: summary.join("\n") }],
          };
        },
      },
      { name: "pipeline_status" },
    );

    api.registerTool(
      {
        name: "pipeline_pause",
        label: "Pipeline Pause",
        description:
          "Pause the pipeline orchestrator. No new issues will be picked up.",
        parameters: Type.Object({
          reason: Type.Optional(
            Type.String({ description: "Reason for pausing" }),
          ),
        }),
        async execute(_toolCallId, params) {
          const { reason } = params as { reason?: string };
          const state = await readStateJson(STATE_PATH);
          state.autoPickup = false;
          state.status = "paused";
          if (reason) state.pauseReason = reason;
          await writeFile(STATE_PATH, JSON.stringify(state, null, 2) + "\n");
          queueNotification(
            `Pipeline paused${reason ? `: ${reason}` : ""}`,
          );
          return {
            content: [
              {
                type: "text",
                text: `Pipeline paused.${reason ? ` Reason: ${reason}` : ""}`,
              },
            ],
          };
        },
      },
      { name: "pipeline_pause" },
    );

    api.registerTool(
      {
        name: "pipeline_resume",
        label: "Pipeline Resume",
        description: "Resume the pipeline orchestrator after a pause.",
        parameters: Type.Object({}),
        async execute(_toolCallId, _params) {
          const state = await readStateJson(STATE_PATH);
          state.autoPickup = true;
          state.status = "idle";
          delete state.pauseReason;
          await writeFile(STATE_PATH, JSON.stringify(state, null, 2) + "\n");
          queueNotification("Pipeline resumed");
          return {
            content: [{ type: "text", text: "Pipeline resumed." }],
          };
        },
      },
      { name: "pipeline_resume" },
    );

    api.registerTool(
      {
        name: "pipeline_run_reconcile",
        label: "Pipeline Reconcile",
        description:
          "Run a pipeline reconciliation cycle: check PR states, merge ready PRs, update queue.",
        parameters: Type.Object({
          dryRun: Type.Optional(
            Type.Boolean({ description: "Preview without making changes" }),
          ),
        }),
        async execute(_toolCallId, params) {
          const { dryRun } = params as { dryRun?: boolean };
          const args = [join(SCRIPTS_DIR, "run.mjs"), "reconcile"];
          if (dryRun) args.push("--dry-run");
          const result = await runScript(args, scriptEnv);
          return {
            content: [
              {
                type: "text",
                text:
                  result.code === 0
                    ? result.stdout
                    : `Reconcile failed (code=${result.code}): ${result.stderr.slice(0, 500)}`,
              },
            ],
          };
        },
      },
      { name: "pipeline_run_reconcile" },
    );

    // ========================================================================
    // CLI Commands
    // ========================================================================

    api.registerCli(
      ({ program }) => {
        const pipeline = program
          .command("pipeline")
          .description("Pipeline orchestrator commands");

        pipeline
          .command("status")
          .description("Show pipeline status")
          .action(async () => {
            const state = await readStateJson(STATE_PATH);
            console.log(JSON.stringify(state, null, 2));
          });

        pipeline
          .command("orchestrate")
          .description("Run one orchestration cycle")
          .action(async () => {
            await orchestrate();
            console.log("Orchestration cycle complete");
          });

        pipeline
          .command("reconcile")
          .description("Run reconciliation")
          .option("--dry-run", "Preview without making changes")
          .action(async (opts) => {
            const args = [join(SCRIPTS_DIR, "run.mjs"), "reconcile"];
            if (opts.dryRun) args.push("--dry-run");
            const result = await runScript(args, scriptEnv);
            console.log(result.stdout || result.stderr);
          });

        pipeline
          .command("pause")
          .description("Pause the pipeline")
          .argument("[reason]", "Reason for pausing")
          .action(async (reason) => {
            const state = await readStateJson(STATE_PATH);
            state.autoPickup = false;
            state.status = "paused";
            if (reason) state.pauseReason = reason;
            await writeFile(
              STATE_PATH,
              JSON.stringify(state, null, 2) + "\n",
            );
            console.log("Pipeline paused");
          });

        pipeline
          .command("resume")
          .description("Resume the pipeline")
          .action(async () => {
            const state = await readStateJson(STATE_PATH);
            state.autoPickup = true;
            state.status = "idle";
            delete state.pauseReason;
            await writeFile(
              STATE_PATH,
              JSON.stringify(state, null, 2) + "\n",
            );
            console.log("Pipeline resumed");
          });
      },
      { commands: ["pipeline"] },
    );

    // ========================================================================
    // Service
    // ========================================================================

    // Start orchestration loop directly (registerService.start doesn't fire on v2026.3.13)
    api.logger.info(
      `pipeline-orchestrator: starting loop (interval=${INTERVAL / 1000}s, repo=${REPO})`,
    );
    setTimeout(() => orchestrate(), 5000);
    orchestrateTimer = setInterval(() => orchestrate(), INTERVAL);
  },
};

export default pipelinePlugin;
