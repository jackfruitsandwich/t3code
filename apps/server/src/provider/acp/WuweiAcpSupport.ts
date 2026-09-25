/**
 * wuwei speaks ACP from `src/acp/main.ts` in its own repo, run by Bun. T3 spawns
 * one agent process per thread session; the model and reasoning effort travel
 * on each prompt's `_meta.wuwei`.
 *
 * @module provider/acp/WuweiAcpSupport
 */
import type { WuweiSettings } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Scope from "effect/Scope";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import type * as EffectAcpErrors from "effect-acp/errors";

import { expandHomePath } from "../../pathExpansion.ts";
import * as AcpSessionRuntime from "./AcpSessionRuntime.ts";

type WuweiRuntimeSettings = Pick<WuweiSettings, "binaryPath" | "entryPath">;

export const DEFAULT_WUWEI_ENTRY_PATH = "~/CODEEEEEEEE/wuwei/src/acp/main.ts";

export function resolveWuweiEntryPath(settings: WuweiRuntimeSettings | null | undefined): string {
  return expandHomePath(settings?.entryPath?.trim() || DEFAULT_WUWEI_ENTRY_PATH);
}

export function buildWuweiAcpSpawnInput(
  settings: WuweiRuntimeSettings | null | undefined,
  cwd: string,
  environment?: NodeJS.ProcessEnv,
): AcpSessionRuntime.AcpSpawnInput {
  return {
    command: settings?.binaryPath || "bun",
    args: [resolveWuweiEntryPath(settings)],
    cwd,
    ...(environment ? { env: environment } : {}),
  };
}

export interface WuweiAcpRuntimeInput extends Omit<
  AcpSessionRuntime.AcpSessionRuntimeOptions,
  "authMethodId" | "spawn"
> {
  readonly childProcessSpawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly wuweiSettings: WuweiRuntimeSettings | null | undefined;
  readonly environment?: NodeJS.ProcessEnv;
}

export const makeWuweiAcpRuntime = (
  input: WuweiAcpRuntimeInput,
): Effect.Effect<
  AcpSessionRuntime.AcpSessionRuntime["Service"],
  EffectAcpErrors.AcpError,
  Crypto.Crypto | Scope.Scope
> =>
  Effect.gen(function* () {
    const acpContext = yield* Layer.build(
      AcpSessionRuntime.layer({
        ...input,
        spawn: buildWuweiAcpSpawnInput(input.wuweiSettings, input.cwd, input.environment),
        authMethodId: "wuwei",
      }).pipe(
        Layer.provide(
          Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, input.childProcessSpawner),
        ),
      ),
    );
    return yield* Effect.service(AcpSessionRuntime.AcpSessionRuntime).pipe(
      Effect.provide(acpContext),
    );
  });
