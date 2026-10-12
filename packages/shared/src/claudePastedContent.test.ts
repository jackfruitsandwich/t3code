import { describe, expect, it } from "vite-plus/test";

import { claudePromptAsDrawn } from "./claudePastedContent.ts";

const pasted = (body: string, id = "07d8") =>
  `<pasted_content id="${id}">\n${body}\n</pasted_content id="${id}">`;

describe("a prompt with pasted text, as the terminal draws it", () => {
  it("puts a paste of several lines on lines of its own, without the tags", () => {
    // The record and the terminal of a real session, as measured.
    const record = `before \n\n${pasted("line 1\nline 2")}\n\n Then reply with exactly: ok`;
    expect(claudePromptAsDrawn(record)).toBe("before\nline 1\nline 2\nThen reply with exactly: ok");
  });

  it("starts with the paste when the message does", () => {
    expect(claudePromptAsDrawn(`${pasted("Decisions\n1. first")}\n\n1. why`)).toBe(
      "Decisions\n1. first\n1. why",
    );
  });

  it("keeps a one-line paste in the sentence", () => {
    expect(claudePromptAsDrawn(`see \n\n${pasted("this word")}\n\n, then go on`)).toBe(
      "see this word, then go on",
    );
  });

  it("keeps two pastes apart", () => {
    expect(claudePromptAsDrawn(`${pasted("one")}\n\n${pasted("two")}`)).toBe("one\ntwo");
  });

  it("leaves tags inside a paste as the text they are", () => {
    const inner = pasted("from another session", "abcd");
    expect(claudePromptAsDrawn(pasted(`quoted:\n${inner}`))).toBe(`quoted:\n${inner}`);
  });

  it("leaves a prompt without a whole block alone", () => {
    expect(claudePromptAsDrawn("just words")).toBe("just words");
    expect(claudePromptAsDrawn('<pasted_content id="07d8"> is the tag')).toBe(
      '<pasted_content id="07d8"> is the tag',
    );
    expect(claudePromptAsDrawn('<pasted_content id="07d8">\nnever closed')).toBe(
      '<pasted_content id="07d8">\nnever closed',
    );
  });
});
