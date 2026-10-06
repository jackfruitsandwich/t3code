/**
 * WuweiDriver — `ProviderDriver` for wuwei, Jack's own agent harness. wuwei
 * runs inside this server (orchestration-v2/Adapters/WuweiAdapterV2.ts); its
 * models, commands and login state come from its in-process host
 * (WuweiProvider.ts). Text generation (titles, commit messages) is not
 * offered.
 *
 * @module provider/Drivers/WuweiDriver
 */
import { ProviderDriverKind, TextGenerationError, WuweiSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import * as ServerConfig from "../../config.ts";
import * as IdAllocator from "../../orchestration-v2/IdAllocator.ts";
import { WuweiAdapterV2Driver } from "../../orchestration-v2/Adapters/WuweiAdapterV2.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import type { TextGeneration } from "../../textGeneration/TextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { makeManualOnlyProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";
import { buildInitialWuweiProviderSnapshot, checkWuweiProviderStatus } from "../WuweiProvider.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";

const decodeWuweiSettings = Schema.decodeSync(WuweiSettings);
const DRIVER_KIND = ProviderDriverKind.make("wuwei");

const unsupportedTextGeneration = (
  operation:
    | "generateCommitMessage"
    | "generatePrContent"
    | "generateBranchName"
    | "generateThreadTitle",
) =>
  Effect.fail(
    new TextGenerationError({ operation, detail: "wuwei does not generate text for T3 yet." }),
  );

const wuweiTextGeneration: TextGeneration["Service"] = {
  generateCommitMessage: () => unsupportedTextGeneration("generateCommitMessage"),
  generatePrContent: () => unsupportedTextGeneration("generatePrContent"),
  generateBranchName: () => unsupportedTextGeneration("generateBranchName"),
  generateThreadTitle: () => unsupportedTextGeneration("generateThreadTitle"),
};

export type WuweiDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | FileSystem.FileSystem
  | IdAllocator.IdAllocatorV2
  | ServerConfig.ServerConfig
  | ServerSettingsService;

export const WuweiDriver: ProviderDriver<WuweiSettings, WuweiDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "wuwei",
    supportsMultipleInstances: true,
  },
  configSchema: WuweiSettings,
  defaultConfig: (): WuweiSettings => decodeWuweiSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const serverSettings = yield* ServerSettingsService;
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies WuweiSettings;
      const maintenance = makeManualOnlyProviderMaintenanceCapabilities({
        provider: DRIVER_KIND,
        packageName: null,
      });

      const orchestrationAdapter = yield* WuweiAdapterV2Driver.create({
        instanceId,
        displayName,
        accentColor,
        environment,
        enabled,
        config,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build wuwei orchestration adapter.",
              cause,
            }),
        ),
      );

      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<WuweiSettings>>({
        resolveMaintenance: () => Effect.succeed(maintenance),
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialWuweiProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider: checkWuweiProviderStatus(effectiveConfig).pipe(Effect.map(stampIdentity)),
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build wuwei snapshot.",
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        orchestrationAdapter,
        textGeneration: wuweiTextGeneration,
      } satisfies ProviderInstance;
    }),
};
