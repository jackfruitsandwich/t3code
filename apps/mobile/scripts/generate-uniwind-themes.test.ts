import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import { describe, expect, it } from "vite-plus/test";

import {
  customThemeNames,
  getGeneratedUniwindThemeOutputs,
  readDefaultThemeVariables,
  renderUniwindThemesCSS,
} from "./generate-uniwind-themes.mts";

describe("generate mobile Uniwind themes", () => {
  it("keeps the committed outputs current", () => {
    const staleOutputs = getGeneratedUniwindThemeOutputs()
      .filter(
        ([filename, contents]) =>
          !NodeFS.existsSync(filename) || NodeFS.readFileSync(filename, "utf8") !== contents,
      )
      .map(([filename]) => NodePath.relative(import.meta.dirname, filename));

    expect(
      staleOutputs,
      "Run `vp run --filter @t3tools/mobile generate` and commit the generated outputs.",
    ).toEqual([]);
  });

  it("registers every custom palette for both appearances", () => {
    expect(customThemeNames).toEqual([
      "t3-chat-light",
      "t3-chat-dark",
      "grove-light",
      "grove-dark",
      "ocean-light",
      "ocean-dark",
      "ember-light",
      "ember-dark",
      "iris-light",
      "iris-dark",
    ]);

    const stylesheet = renderUniwindThemesCSS();
    for (const themeName of customThemeNames) {
      expect(stylesheet.match(new RegExp(`@variant ${themeName} \\{`, "gu"))).toHaveLength(1);
    }
  });

  it("gives every custom theme the complete default token set required by Uniwind", () => {
    const authored = NodeFS.readFileSync(
      NodePath.resolve(import.meta.dirname, "../global.css"),
      "utf8",
    );
    const generated = renderUniwindThemesCSS();
    const variablesFor = (css: string, variant: string) =>
      [...css.matchAll(new RegExp(`@variant ${variant} \\{([^}]+)\\}`, "gu"))].flatMap((match) =>
        [...match[1]!.matchAll(/(--[\w-]+):/gu)].map((variable) => variable[1]!),
      );
    for (const name of customThemeNames) {
      const appearance = name.endsWith("-dark") ? "dark" : "light";
      expect(new Set(variablesFor(generated, name))).toEqual(
        new Set(variablesFor(authored + generated, appearance)),
      );
    }
  });

  it("generates the default runtime bridge from the authored CSS", () => {
    const css = NodeFS.readFileSync(NodePath.resolve(import.meta.dirname, "../global.css"), "utf8");
    const variables = readDefaultThemeVariables(css);

    expect(variables.light["--color-screen"]).toBe("#f2f2f7");
    expect(variables.dark["--color-screen"]).toBe("#0a0a0a");
    expect(Object.keys(variables.light)).toEqual(Object.keys(variables.dark));
  });
});
