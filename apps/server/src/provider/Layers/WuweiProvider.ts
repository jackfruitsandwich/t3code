/**
 * wuwei provider snapshot. The model catalog comes from wuwei itself
 * (`main.ts --catalog`), so the picker shows exactly the models wuwei can
 * reach on the ChatGPT and Claude subscriptions it is logged into.
 *
 * @module provider/Layers/WuweiProvider
 */
import type { ModelCapabilities, ServerProviderModel, WuweiSettings } from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { resolveWuweiEntryPath } from "../acp/WuweiAcpSupport.ts";
import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  isCommandMissingCause,
  providerModelsFromSettings,
  type ServerProviderDraft,
  type ServerProviderPresentation,
  spawnAndCollect,
} from "../providerSnapshot.ts";

const CATALOG_TIMEOUT_MS = 30_000;

export const WUWEI_PRESENTATION: ServerProviderPresentation = {
  displayName: "wuwei",
  // wuwei has no plan mode; /plan and the Plan/Build toggle would do nothing.
  showInteractionModeToggle: false,
  reportsContextWindow: true,
  supportsConversationRollback: false,
};

const EFFORT_LABELS: Record<string, string> = {
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "Extra High",
  max: "Max",
  ultra: "Ultra",
};

const BACKEND_LABELS: Record<string, string> = {
  "codex-direct": "ChatGPT",
  "anthropic-direct": "Claude",
};

interface WuweiCatalogModel {
  readonly slug: string;
  readonly name: string;
  readonly backend: string;
  readonly reasoningEfforts: ReadonlyArray<string>;
}

interface WuweiCatalog {
  readonly models: ReadonlyArray<WuweiCatalogModel>;
  readonly warnings: ReadonlyArray<string>;
}

export function wuweiModelCapabilities(efforts: ReadonlyArray<string>): ModelCapabilities {
  const defaultEffort = efforts.includes("medium") ? "medium" : efforts[0];
  return createModelCapabilities({
    optionDescriptors: [
      buildSelectOptionDescriptor({
        id: "reasoningEffort",
        label: "Reasoning",
        options: efforts.map((effort) => ({
          value: effort,
          label: EFFORT_LABELS[effort] ?? effort,
          ...(effort === defaultEffort ? { isDefault: true } : {}),
        })),
      }),
    ],
  });
}

/** Shown until the first catalog probe answers. */
const FALLBACK_CATALOG: ReadonlyArray<WuweiCatalogModel> = [
  {
    slug: "gpt-6-astra",
    name: "GPT-6 Astra",
    backend: "codex-direct",
    reasoningEfforts: ["low", "medium", "high", "xhigh", "max", "ultra"],
  },
  {
    slug: "gpt-6-sol",
    name: "GPT-6 Sol",
    backend: "codex-direct",
    reasoningEfforts: ["low", "medium", "high", "xhigh", "max", "ultra"],
  },
  {
    slug: "gpt-6-luna",
    name: "GPT-6 Luna",
    backend: "codex-direct",
    reasoningEfforts: ["low", "medium", "high", "xhigh", "max"],
  },
  {
    slug: "claude-fable-5-1",
    name: "Claude Fable 5.1",
    backend: "anthropic-direct",
    reasoningEfforts: ["low", "medium", "high", "xhigh"],
  },
];

function modelsFromCatalog(
  catalog: ReadonlyArray<WuweiCatalogModel>,
  settings: WuweiSettings,
): ReadonlyArray<ServerProviderModel> {
  const builtIn = catalog.map((model, index): ServerProviderModel => ({
    slug: model.slug,
    name: model.name,
    ...(BACKEND_LABELS[model.backend] ? { subProvider: BACKEND_LABELS[model.backend] } : {}),
    isCustom: false,
    ...(index === 0 ? { isDefault: true } : {}),
    capabilities: wuweiModelCapabilities(model.reasoningEfforts),
  }));
  return providerModelsFromSettings(
    builtIn,
    settings.customModels,
    wuweiModelCapabilities(["low", "medium", "high"]),
  );
}

function parseCatalog(stdout: string): WuweiCatalog | undefined {
  try {
    const parsed = JSON.parse(stdout.trim()) as WuweiCatalog;
    return Array.isArray(parsed.models) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

export function buildInitialWuweiProviderSnapshot(
  settings: WuweiSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    return buildServerProvider({
      presentation: WUWEI_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: modelsFromCatalog(FALLBACK_CATALOG, settings),
      probe: {
        installed: settings.enabled,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: settings.enabled ? "Checking wuwei..." : "wuwei is disabled in T3 Code settings.",
      },
    });
  });
}

export const checkWuweiProviderStatus = Effect.fn("checkWuweiProviderStatus")(function* (
  settings: WuweiSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = modelsFromCatalog(FALLBACK_CATALOG, settings);
  if (!settings.enabled) {
    return buildServerProvider({
      presentation: WUWEI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "wuwei is disabled in T3 Code settings.",
      },
    });
  }

  const command = settings.binaryPath || "bun";
  const entryPath = resolveWuweiEntryPath(settings);
  const probe = yield* Effect.gen(function* () {
    const spawnCommand = yield* resolveSpawnCommand(command, [entryPath, "--catalog"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  }).pipe(Effect.timeoutOption(CATALOG_TIMEOUT_MS), Effect.result);

  const failed = (message: string, installed: boolean) =>
    buildServerProvider({
      presentation: WUWEI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: { installed, version: null, status: "error", auth: { status: "unknown" }, message },
    });

  if (Result.isFailure(probe)) {
    return isCommandMissingCause(probe.failure)
      ? failed(`Bun (\`${command}\`) is not installed or not on PATH.`, false)
      : failed("Failed to run wuwei's catalog check.", true);
  }
  if (Option.isNone(probe.success)) {
    return failed("wuwei's catalog check timed out.", true);
  }
  const output = probe.success.value;
  const catalog = output.code === 0 ? parseCatalog(output.stdout) : undefined;
  if (!catalog) {
    const detail = output.stderr.trim().split("\n").slice(-1)[0];
    return failed(
      `wuwei's catalog check failed${detail ? `: ${detail}` : ""} (${entryPath}).`,
      true,
    );
  }
  if (catalog.models.length === 0) {
    return buildServerProvider({
      presentation: WUWEI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unauthenticated" },
        message: catalog.warnings.join(" ") || "wuwei has no usable model.",
      },
    });
  }
  return buildServerProvider({
    presentation: WUWEI_PRESENTATION,
    enabled: true,
    checkedAt,
    models: modelsFromCatalog(catalog.models, settings),
    probe: {
      installed: true,
      version: null,
      // A missing login on one subscription leaves the other usable; the picker only lists ready providers.
      status: "ready",
      auth: { status: "authenticated", label: "ChatGPT + Claude subscriptions" },
      ...(catalog.warnings.length > 0 ? { message: catalog.warnings.join(" ") } : {}),
    },
  });
});
