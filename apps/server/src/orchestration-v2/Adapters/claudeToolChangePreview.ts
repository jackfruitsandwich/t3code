/**
 * What a Claude file tool is about to change, for the approval that asks
 * about it: the lines it would remove and add, read from the tool's own
 * input. Claude Code's terminal dialog shows the change before the person
 * answers; an approval that names only the path asks them to answer blind.
 */

const MAX_PREVIEW_LINES = 24;
const MAX_PREVIEW_CHARS = 2400;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const marked = (mark: "+" | "-", text: string): ReadonlyArray<string> =>
  text.length === 0
    ? []
    : text
        .replace(/\n$/u, "")
        .split("\n")
        .map((line) => `${mark}${line}`);

const editLines = (edit: unknown): ReadonlyArray<string> => {
  if (!isRecord(edit)) return [];
  const removed = typeof edit.old_string === "string" ? marked("-", edit.old_string) : [];
  const added = typeof edit.new_string === "string" ? marked("+", edit.new_string) : [];
  return [...removed, ...added];
};

/**
 * The proposed change as `-`/`+` lines, cut to what a card can hold with the
 * number of lines left out, or undefined when the input is not a file change.
 */
export function proposedClaudeFileChange(input: unknown): string | undefined {
  if (!isRecord(input)) return undefined;
  const lines: Array<string> =
    typeof input.content === "string"
      ? [...marked("+", input.content)]
      : typeof input.new_source === "string"
        ? [...marked("+", input.new_source)]
        : Array.isArray(input.edits)
          ? input.edits.flatMap((edit, index) => [...(index === 0 ? [] : [""]), ...editLines(edit)])
          : [...editLines(input)];
  if (lines.length === 0) return undefined;
  const shown: Array<string> = [];
  let chars = 0;
  for (const line of lines) {
    if (shown.length >= MAX_PREVIEW_LINES || chars + line.length > MAX_PREVIEW_CHARS) break;
    shown.push(line);
    chars += line.length + 1;
  }
  const left = lines.length - shown.length;
  if (shown.length === 0) return `… ${lines.length} line${lines.length === 1 ? "" : "s"}`;
  return left === 0
    ? shown.join("\n")
    : `${shown.join("\n")}\n… ${left} more line${left === 1 ? "" : "s"}`;
}
