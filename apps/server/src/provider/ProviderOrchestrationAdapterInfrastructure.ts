import * as Layer from "effect/Layer";

import * as ClaudeBridgeRegistry from "../claude-bridge/ClaudeBridgeRegistry.ts";
import * as ClaudeAdapterV2 from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as ClaudeBridgeQueryRunner from "../orchestration-v2/Adapters/ClaudeBridgeQueryRunner.ts";
import * as CodexAdapterV2 from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CursorAgentSdk from "../orchestration-v2/Adapters/CursorAgentSdk.ts";
import * as IdAllocator from "../orchestration-v2/IdAllocator.ts";
import * as ProviderContinuationRequests from "../orchestration-v2/ProviderContinuationRequests.ts";

export type ProviderOrchestrationAdapterInfrastructure =
  | ClaudeAdapterV2.ClaudeAgentSdkQueryRunner
  | CodexAdapterV2.CodexAppServerClientFactory
  | CursorAgentSdk.CursorAgentSdkRunner
  | IdAllocator.IdAllocatorV2;

/**
 * Infrastructure shared by the V2 adapters materialized inside provider
 * instances. `providerContinuationRequestsLayer` must be the same layer
 * reference the orchestration runtime provides to its continuation worker so
 * Effect layer memoization yields one shared queue.
 */
export const layer = Layer.mergeAll(
  // Claude runs over the t3bridge mod when a home has it installed, and over
  // the Agent SDK otherwise (ClaudeBridgeQueryRunner.ts). The registry layer
  // is the same reference the attach service provides, so memoization yields
  // one watcher of the bridge directory.
  ClaudeBridgeQueryRunner.layer.pipe(Layer.provide(ClaudeBridgeRegistry.layer)),
  CodexAdapterV2.layerAppServerClientFactory,
  CursorAgentSdk.layer,
  IdAllocator.layer,
  ProviderContinuationRequests.layer,
);
