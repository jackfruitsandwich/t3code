import type { SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import { assert, describe, it } from "@effect/vitest";

import { claudeBridgeImageNote, claudeBridgePromptText } from "./ClaudeBridgeQueryRunner.ts";

const userMessage = (content: SDKUserMessage["message"]["content"]): SDKUserMessage => ({
  type: "user",
  message: { role: "user", content },
  parent_tool_use_id: null,
});

const image = {
  type: "image" as const,
  source: { type: "base64" as const, media_type: "image/png" as const, data: "iVBORw0KGgo=" },
};

describe("claudeBridgePromptText", () => {
  it("passes a string prompt through and joins text blocks", () => {
    assert.strictEqual(claudeBridgePromptText(userMessage("hello")), "hello");
    assert.strictEqual(
      claudeBridgePromptText(
        userMessage([
          { type: "text", text: "first" },
          { type: "text", text: "second" },
        ]),
      ),
      "first\n\nsecond",
    );
  });

  it("replaces image blocks with one note that points the model at the saved files", () => {
    const text = claudeBridgePromptText(
      userMessage([
        image,
        image,
        {
          type: "text",
          text: 'Compare these.\n\n[Attached image "a.png" is saved at: /tmp/a.png]',
        },
      ]),
    );
    assert.strictEqual(
      text,
      `Compare these.\n\n[Attached image "a.png" is saved at: /tmp/a.png]\n\n${claudeBridgeImageNote(2)}`,
    );
    assert.match(claudeBridgeImageNote(2), /2 images/);
    assert.match(claudeBridgeImageNote(1), /an image/);
    assert.match(claudeBridgeImageNote(1), /Read tool/);
  });

  it("sends the note alone for an image-only message, and nothing for tool results", () => {
    assert.strictEqual(claudeBridgePromptText(userMessage([image])), claudeBridgeImageNote(1));
    assert.strictEqual(
      claudeBridgePromptText(
        userMessage([{ type: "tool_result", tool_use_id: "toolu_1", content: "done" }]),
      ),
      null,
    );
  });
});
