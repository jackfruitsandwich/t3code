import { assert, describe, it } from "@effect/vitest";

import { proposedClaudeFileChange } from "./claudeToolChangePreview.ts";

describe("proposedClaudeFileChange", () => {
  it("shows what a write would put in the file", () => {
    assert.equal(
      proposedClaudeFileChange({ file_path: "/w/note.txt", content: "plum\npear\n" }),
      "+plum\n+pear",
    );
  });

  it("shows what an edit removes and adds", () => {
    assert.equal(
      proposedClaudeFileChange({
        file_path: "/w/a.ts",
        old_string: "let a = 1",
        new_string: "const a = 2",
      }),
      "-let a = 1\n+const a = 2",
    );
    assert.equal(
      proposedClaudeFileChange({
        file_path: "/w/a.ts",
        edits: [
          { old_string: "one", new_string: "1" },
          { old_string: "two", new_string: "" },
        ],
      }),
      "-one\n+1\n\n-two",
    );
  });

  it("says how much of a long change it left out", () => {
    const content = Array.from({ length: 30 }, (_, index) => `line ${index + 1}`).join("\n");
    const preview = proposedClaudeFileChange({ file_path: "/w/long.txt", content }) ?? "";
    const lines = preview.split("\n");
    assert.equal(lines.length, 25);
    assert.equal(lines[0], "+line 1");
    assert.equal(lines.at(-1), "… 6 more lines");
  });

  it("has nothing to show for a tool that changes no file", () => {
    assert.isUndefined(proposedClaudeFileChange({ command: "ls" }));
    assert.isUndefined(proposedClaudeFileChange({ file_path: "/w/read.txt" }));
    assert.isUndefined(proposedClaudeFileChange("nope"));
  });
});
