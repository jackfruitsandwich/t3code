import type { OrchestrationV2TurnItem } from "@t3tools/contracts";

type ToolItemFields = Extract<
  OrchestrationV2TurnItem,
  { readonly type: "command_execution" | "file_change" | "web_search" | "dynamic_tool" }
>;

/** The item-specific fields of a wuwei tool call, shared by live turns and migrated history. */
export type WuweiToolItemFields =
  | Pick<
      Extract<ToolItemFields, { type: "command_execution" }>,
      "type" | "input" | "output" | "outputIndicatesFailure"
    >
  | Pick<
      Extract<ToolItemFields, { type: "file_change" }>,
      "type" | "fileName" | "oldStr" | "newStr"
    >
  | Pick<Extract<ToolItemFields, { type: "web_search" }>, "type" | "patterns">
  | Pick<
      Extract<ToolItemFields, { type: "dynamic_tool" }>,
      "type" | "toolName" | "input" | "output"
    >;

const stringArg = (input: Readonly<Record<string, unknown>>, key: string) => {
  const value = input[key];
  return typeof value === "string" && value.trim() !== "" ? value : undefined;
};

/** Maps wuwei's tools onto T3's tool items: bash, write/edit, exa; everything else is a dynamic tool. */
export function wuweiToolItemFields(
  name: string,
  input: Readonly<Record<string, unknown>>,
  output: string | null,
  isError: boolean,
): WuweiToolItemFields {
  const path = stringArg(input, "path");
  switch (name) {
    case "bash": {
      const command = stringArg(input, "command");
      if (command === undefined) break;
      return {
        type: "command_execution",
        input: command,
        ...(output === null ? {} : { output }),
        ...(isError ? { outputIndicatesFailure: true } : {}),
      };
    }
    case "write":
      if (path === undefined) break;
      return {
        type: "file_change",
        fileName: path,
        ...(typeof input.content === "string" ? { newStr: input.content } : {}),
      };
    case "edit":
      if (path === undefined) break;
      return {
        type: "file_change",
        fileName: path,
        ...(typeof input.oldText === "string" ? { oldStr: input.oldText } : {}),
        ...(typeof input.newText === "string" ? { newStr: input.newText } : {}),
      };
    case "exa": {
      const query = stringArg(input, "query");
      if (query === undefined) break;
      return { type: "web_search", patterns: [query] };
    }
  }
  return {
    type: "dynamic_tool",
    toolName: name.trim() === "" ? "tool" : name,
    input,
    ...(output === null ? {} : { output }),
  };
}
