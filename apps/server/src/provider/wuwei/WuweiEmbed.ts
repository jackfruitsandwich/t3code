// @effect-diagnostics nodeBuiltinImport:off -- Loads wuwei's bundle by file URL, once per process.
import * as NodeOs from "node:os";
import * as NodePath from "node:path";
import * as NodeUrl from "node:url";

import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

import { EMBED_API_VERSION, type WuweiEmbedModule, type WuweiHost } from "./wuweiEmbedApi.ts";

/**
 * wuwei runs inside this server: its bundle is imported once and every wuwei
 * chat (and the v1 migration of old wuwei chats) shares that one host.
 *
 * The bundle lives in wuwei's own repo, so wuwei changes ship by rebuilding
 * wuwei (`bun run build:embed`) and restarting the server, without a T3 build.
 */
export const DEFAULT_WUWEI_EMBED_PATH = NodePath.join(
  NodeOs.homedir(),
  "CODEEEEEEEE",
  "wuwei",
  "dist",
  "embed.mjs",
);

export class WuweiEmbedLoadError extends Schema.TaggedError<WuweiEmbedLoadError>()(
  "WuweiEmbedLoadError",
  { bundlePath: Schema.String, detail: Schema.String },
) {
  override get message() {
    return `Could not load wuwei from ${this.bundlePath}: ${this.detail}`;
  }
}

/** An explicit path wins, then T3CODE_WUWEI_EMBED, then wuwei's default checkout. */
export function resolveWuweiEmbedPath(configured?: string): string {
  const explicit = configured?.trim() || process.env.T3CODE_WUWEI_EMBED?.trim();
  if (!explicit) return DEFAULT_WUWEI_EMBED_PATH;
  return explicit.startsWith("~/") ? NodePath.join(NodeOs.homedir(), explicit.slice(2)) : explicit;
}

let loaded: { readonly bundlePath: string; readonly host: Promise<WuweiHost> } | undefined;

async function importHost(bundlePath: string): Promise<WuweiHost> {
  const module = (await import(
    NodeUrl.pathToFileURL(bundlePath).href
  )) as Partial<WuweiEmbedModule>;
  if (module.EMBED_API_VERSION !== EMBED_API_VERSION || typeof module.loadWuwei !== "function") {
    throw new Error(
      `embed API version ${String(module.EMBED_API_VERSION)} does not match ${EMBED_API_VERSION}; rebuild wuwei`,
    );
  }
  return module.loadWuwei();
}

/**
 * The process-wide wuwei host. The first successful load fixes the bundle;
 * a later call naming a different path keeps the loaded one (a restart picks
 * up a new path). A failed load is retried on the next call.
 */
export const loadWuweiHost = (configuredPath?: string) =>
  Effect.tryPromise({
    try: () => {
      if (loaded === undefined) {
        const bundlePath = resolveWuweiEmbedPath(configuredPath);
        const host = importHost(bundlePath);
        loaded = { bundlePath, host };
        host.catch(() => {
          if (loaded?.host === host) loaded = undefined;
        });
      }
      return loaded.host;
    },
    catch: (cause) =>
      new WuweiEmbedLoadError({
        bundlePath: loaded?.bundlePath ?? resolveWuweiEmbedPath(configuredPath),
        detail: cause instanceof Error ? cause.message : String(cause),
      }),
  });
