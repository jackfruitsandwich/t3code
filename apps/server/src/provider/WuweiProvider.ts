/**
 * wuwei provider snapshot. Models, slash commands and login state come from
 * wuwei itself through its in-process host, so the picker shows exactly the
 * models wuwei can reach on the ChatGPT and Claude subscriptions it is logged
 * into, and `/` lists wuwei's own commands.
 *
 * @module provider/WuweiProvider
 */
import type {
  ModelCapabilities,
  ServerProviderModel,
  ServerProviderSlashCommand,
  WuweiSettings,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import {
  buildSelectOptionDescriptor,
  buildServerProvider,
  providerModelsFromSettings,
  type ServerProviderDraft,
  type ServerProviderPresentation,
} from "./providerSnapshot.ts";
import { loadWuweiHost } from "./wuwei/WuweiEmbed.ts";
import type { WuweiCommandInfo, WuweiModelInfo } from "./wuwei/wuweiEmbedApi.ts";

export const WUWEI_PRESENTATION: ServerProviderPresentation = {
  displayName: "wuwei",
  // wuwei has no plan mode; the Plan/Build toggle would do nothing.
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

/** Shown until wuwei's catalog answers. */
const FALLBACK_CATALOG: ReadonlyArray<
  Pick<WuweiModelInfo, "slug" | "name" | "backend" | "reasoningEfforts">
> = [
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
  catalog: ReadonlyArray<Pick<WuweiModelInfo, "slug" | "name" | "backend" | "reasoningEfforts">>,
  settings: WuweiSettings,
): ReadonlyArray<ServerProviderModel> {
  const builtIn = catalog.map((model, index): ServerProviderModel => {
    const subProvider = BACKEND_LABELS[model.backend];
    return {
      slug: model.slug,
      name: model.name,
      ...(subProvider === undefined ? {} : { subProvider }),
      isCustom: false,
      ...(index === 0 ? { isDefault: true } : {}),
      capabilities: wuweiModelCapabilities(model.reasoningEfforts),
    };
  });
  return providerModelsFromSettings(
    builtIn,
    settings.customModels,
    wuweiModelCapabilities(["low", "medium", "high"]),
  );
}

/** wuwei runs these itself when a message is exactly "/name args"; the model never sees them. */
function slashCommandsFromHost(
  commands: ReadonlyArray<WuweiCommandInfo>,
): ReadonlyArray<ServerProviderSlashCommand> {
  return commands.flatMap((command) => {
    const name = command.name.trim();
    if (!name) return [];
    const description = command.description.trim();
    const hint = command.hint?.trim();
    return [
      {
        name,
        ...(description ? { description } : {}),
        ...(hint ? { input: { hint } } : {}),
      },
    ];
  });
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
        message: settings.enabled ? "Loading wuwei..." : "wuwei is disabled in T3 Code settings.",
      },
    });
  });
}

export const checkWuweiProviderStatus = Effect.fn("checkWuweiProviderStatus")(function* (
  settings: WuweiSettings,
): Effect.fn.Return<ServerProviderDraft> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = modelsFromCatalog(FALLBACK_CATALOG, settings);
  const failed = (message: string, installed: boolean) =>
    buildServerProvider({
      presentation: WUWEI_PRESENTATION,
      enabled: settings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: { installed, version: null, status: "error", auth: { status: "unknown" }, message },
    });
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

  const loaded = yield* loadWuweiHost(settings.embedPath).pipe(Effect.result);
  if (loaded._tag === "Failure") {
    return failed(`${loaded.failure.message}. Build it with: bun run build:embed`, false);
  }
  const host = loaded.success;
  const [catalog, auth] = yield* Effect.promise(() =>
    Promise.all([host.catalog({ refresh: true }), host.authStatus()]),
  );
  const slashCommands = slashCommandsFromHost(host.commands());
  if (catalog.models.length === 0) {
    return buildServerProvider({
      presentation: WUWEI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      slashCommands,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unauthenticated" },
        message: catalog.warnings.join(" ") || "wuwei has no usable model; use /login.",
      },
    });
  }
  const loggedIn = [auth.codexReady ? "ChatGPT" : null, auth.anthropicReady ? "Claude" : null]
    .filter((label): label is string => label !== null)
    .join(" + ");
  return buildServerProvider({
    presentation: WUWEI_PRESENTATION,
    enabled: true,
    checkedAt,
    models: modelsFromCatalog(catalog.models, settings),
    slashCommands,
    probe: {
      installed: true,
      version: null,
      // A missing login on one subscription leaves the other usable.
      status: "ready",
      auth: { status: "authenticated", label: `${loggedIn || "wuwei"} subscription` },
      ...(catalog.warnings.length > 0 ? { message: catalog.warnings.join(" ") } : {}),
    },
  });
});
