// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { WuweiSettings } from "@t3tools/contracts";

import { checkWuweiProviderStatus } from "./WuweiProvider.ts";

const decodeWuweiSettings = Schema.decodeSync(WuweiSettings);

// Stands in for `bun src/acp/main.ts --catalog`.
const FAKE_CATALOG = `process.stdout.write(JSON.stringify({
  models: [{ slug: "gpt-6-astra", name: "GPT-6 Astra", backend: "codex-direct", backendLabel: "ChatGPT", reasoningEfforts: ["low", "medium"], contextWindow: 372000 }],
  commands: [
    { name: "memory", description: "toggle memory injection for this chat (on/off)", hint: "on | off" },
    { name: "help", description: "show wuwei's slash commands" },
    { name: "  " },
  ],
  warnings: [],
}) + "\\n");`;

it.layer(NodeServices.layer)("WuweiProvider", (it) => {
  it.effect("lists wuwei's own slash commands from its catalog", () =>
    Effect.gen(function* () {
      const entryPath = yield* Effect.promise(async () => {
        const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "wuwei-catalog-fake-"));
        const path = NodePath.join(dir, "fake-catalog.mjs");
        await NodeFSP.writeFile(path, FAKE_CATALOG, "utf8");
        return path;
      });
      const snapshot = yield* checkWuweiProviderStatus(
        decodeWuweiSettings({ binaryPath: process.execPath, entryPath }),
      );

      assert.equal(snapshot.status, "ready");
      assert.deepStrictEqual(snapshot.slashCommands, [
        {
          name: "memory",
          description: "toggle memory injection for this chat (on/off)",
          input: { hint: "on | off" },
        },
        { name: "help", description: "show wuwei's slash commands" },
      ]);
    }),
  );
});
