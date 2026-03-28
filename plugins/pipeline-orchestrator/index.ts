/**
 * OpenClaw Pipeline Orchestrator Plugin
 *
 * In-process pipeline management for GitHub issues, PRs, and CI.
 * Replaces the external systemd timer-based orchestrator.
 *
 * Key advantages over the systemd approach:
 * - Notifications appear in active session context
 * - No orphaned worker processes
 * - Direct access to the agent runtime for task dispatch
 * - Single process, better memory management
 */

import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import type { OpenClawPluginApi } from "openclaw/plugin-sdk/plugin-entry";
import { Type } from "@sinclair/typebox";

// ============================================================================
// Registration Guard
// ============================================================================

let _registered = false;

// ============================================================================
// Plugin Definition
// ============================================================================

export default definePluginEntry({
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
      intervalSec: { type: "number" },
      maxWip: { type: "number" },
      defaultWorker: { type: "string" },
      cooldownMinutes: { type: "number" },
      notifyEnabled: { type: "boolean" },
    },
    required: ["repo", "projectPath"] as const,
  },

  register(api: OpenClawPluginApi) {
    if (_registered) {
      api.logger.info("pipeline-orchestrator: skipping duplicate registration");
      return;
    }
    _registered = true;

    const cfg = api.pluginConfig as {
      repo?: string;
      projectPath?: string;
      intervalSec?: number;
      maxWip?: number;
      defaultWorker?: string;
      cooldownMinutes?: number;
      notifyEnabled?: boolean;
    };

    const REPO = cfg.repo || "noncelogic/signalflow";
    const PROJECT_PATH = cfg.projectPath || "";
    const INTERVAL = (cfg.intervalSec || 30) * 1000;
    const MAX_WIP = cfg.maxWip || 1;
    const NOTIFY = cfg.notifyEnabled !== false;

    let orchestrateTimer: ReturnType<typeof setInterval> | null = null;

    api.logger.info(
      `pipeline-orchestrator: registered (repo=${REPO}, interval=${INTERVAL / 1000}s)`,
    );

    // ========================================================================
    // Session-aware notifications via before_agent_start hook
    // ========================================================================

    const pendingNotifications: string[] = [];

    if (NOTIFY) {
      api.on("before_agent_start", async (_event) => {
        if (pendingNotifications.length === 0) return;
        const context = pendingNotifications.splice(0).join("\n\n");
        api.logger.info(
          `pipeline-orchestrator: injecting notifications into context`,
        );
        return {
          prependContext: `<pipeline-notifications>\n${context}\n</pipeline-notifications>`,
        };
      });
    }

    function queueNotification(message: string) {
      pendingNotifications.push(`[${new Date().toISOString()}] ${message}`);
      while (pendingNotifications.length > 10) pendingNotifications.shift();
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
          // TODO: Port from pipeline-orchestrator.mjs orchestrate() status logic
          return {
            content: [
              {
                type: "text",
                text: `Pipeline orchestrator active.\nRepo: ${REPO}\nProject: ${PROJECT_PATH}\nMax WIP: ${MAX_WIP}\nPending notifications: ${pendingNotifications.length}`,
              },
            ],
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
        description:
          "Resume the pipeline orchestrator after a pause.",
        parameters: Type.Object({}),
        async execute(_toolCallId, _params) {
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
        name: "pipeline_notify",
        label: "Pipeline Notify",
        description:
          "Queue a notification for the pipeline context. Used by external systems to inject status updates.",
        parameters: Type.Object({
          message: Type.String({ description: "Notification message" }),
        }),
        async execute(_toolCallId, params) {
          const { message } = params as { message: string };
          queueNotification(message);
          return {
            content: [{ type: "text", text: `Notification queued: ${message}` }],
          };
        },
      },
      { name: "pipeline_notify" },
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
            console.log("Pipeline orchestrator plugin active");
            console.log(`Repo: ${REPO}`);
            console.log(`Project: ${PROJECT_PATH}`);
            console.log(`Interval: ${INTERVAL / 1000}s`);
            console.log(`Max WIP: ${MAX_WIP}`);
            console.log(
              `Pending notifications: ${pendingNotifications.length}`,
            );
          });

        pipeline
          .command("notify")
          .description(
            "Queue a notification for the next agent interaction",
          )
          .argument("<message>", "Notification message")
          .action(async (message: string) => {
            queueNotification(message);
            console.log(`Queued: ${message}`);
          });
      },
      { commands: ["pipeline"] },
    );

    // ========================================================================
    // Service (orchestration loop)
    // ========================================================================

    api.registerService({
      id: "pipeline-orchestrator",
      start: () => {
        api.logger.info(
          `pipeline-orchestrator: started (interval=${INTERVAL / 1000}s, repo=${REPO})`,
        );
        // TODO: Port the full orchestrate() loop from pipeline-orchestrator.mjs
        // The orchestration cycle should:
        // 1. Fetch open issues from GitHub (prioritized, sized)
        // 2. Check open PRs and merge ready ones
        // 3. Dispatch workers for queued issues
        // 4. Monitor running workers and collect results
        // 5. Queue notifications for transitions
        orchestrateTimer = setInterval(() => {
          // TODO: orchestrate() cycle
        }, INTERVAL);
      },
      stop: () => {
        if (orchestrateTimer) {
          clearInterval(orchestrateTimer);
          orchestrateTimer = null;
        }
        api.logger.info("pipeline-orchestrator: stopped");
      },
    });
  },
});
