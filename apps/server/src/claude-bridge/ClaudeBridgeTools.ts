/**
 * ClaudeBridgeTools - the T3 tools a bridged Claude Code session has.
 *
 * A session T3 starts through the SDK is given T3's tool server. A session
 * that lives in Claude Code itself (a terminal, a background job) is not
 * launched by T3, so the mod declares the tools in it instead and answers
 * each call from T3's tool endpoint, with the credential of the thread that
 * shows the session. The model calls them as `mcp__t3bridge__<name>`.
 *
 * @module claude-bridge/ClaudeBridgeTools
 */
import { HTML_RENDER_TOOL_NAME } from "@t3tools/shared/htmlRender";
import { Tool } from "effect/ai";

export interface ClaudeBridgeToolSpec {
  readonly name: string;
  readonly description: string;
  readonly inputSchema: unknown;
  /** Behind ToolSearch, as an MCP server's tools are; the one tool a reply is built around is not. */
  readonly isDeferred: boolean;
  /** Changes nothing in the workspace: allowed without asking, as T3 allows it elsewhere. */
  readonly isReadOnly: boolean;
}

/**
 * The tools that show something in the thread: they need the thread, not the
 * workspace. Read from T3's own toolkit when asked, not when this module
 * loads: the toolkit reaches back into orchestration, which loads the bridge.
 */
export const loadClaudeBridgeToolSpecs = (): Promise<ReadonlyArray<ClaudeBridgeToolSpec>> =>
  import("../mcp/toolkits/html/tools.ts").then(({ HtmlToolkit }) =>
    Object.values(HtmlToolkit.tools).map((tool) => ({
      name: tool.name,
      description: Tool.getDescription(tool) ?? tool.name,
      inputSchema: Tool.getJsonSchema(tool),
      isDeferred: tool.name !== HTML_RENDER_TOOL_NAME,
      isReadOnly: true,
    })),
  );
