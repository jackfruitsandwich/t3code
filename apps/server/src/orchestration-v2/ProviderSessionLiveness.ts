import type { ProviderDriverKind } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";

/**
 * A native session as this server finds it: `gone` with the server's
 * process, or still running on its own (a Claude Code session a terminal
 * holds) and then `running` a turn, which began at `turnStartedAt`, or `idle`.
 */
export type ProviderSessionLivenessState =
  | { readonly state: "gone" }
  | { readonly state: "idle" }
  | { readonly state: "running"; readonly turnStartedAt: number };

/**
 * Whether a provider's native session outlives this server. Runtime recovery
 * asks it about every provider thread it would close: a session that runs on
 * keeps its turn's run open for the next server to pick back up, and its
 * background work is not reported cancelled to its next turn. The default
 * knows of no such session: every provider process ends with the server.
 */
export class ProviderSessionLiveness extends Context.Reference<{
  readonly ofNativeThread: (input: {
    readonly driver: ProviderDriverKind;
    readonly nativeThreadId: string;
  }) => Effect.Effect<ProviderSessionLivenessState>;
}>("t3/orchestration-v2/ProviderSessionLiveness", {
  defaultValue: () => ({ ofNativeThread: () => Effect.succeed({ state: "gone" }) }),
}) {}
