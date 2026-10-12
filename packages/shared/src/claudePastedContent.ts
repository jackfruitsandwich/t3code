/**
 * Text pasted into Claude Code's prompt is kept in the session's record
 * between `<pasted_content id="…">` tags, and the terminal takes the tags off
 * again when it draws the message. This reads a prompt the way the terminal
 * draws it, rule for rule (Claude Code 2.1.296), so a thread shows what the
 * person sees there.
 *
 * The terminal unwraps only blocks under its session's own id. A reader here
 * has no session to ask, so any well-formed block counts.
 */
const OPEN = '<pasted_content id="';
const AFTER_ID = '">\n';
const OPENERS = /[([{"'`]/u;
const CLOSERS = /[)\]}"'`.,;:!?]/u;

type Part =
  | { readonly kind: "text"; readonly text: string }
  | { readonly kind: "block"; readonly body: string };

const partsOf = (text: string): ReadonlyArray<Part> => {
  const parts: Array<Part> = [];
  let done = 0;
  let from = 0;
  for (;;) {
    const open = text.indexOf(OPEN, from);
    if (open === -1) break;
    const idAt = open + OPEN.length;
    const id = text.slice(idAt, idAt + 4);
    if (!/^[0-9a-f]{4}$/u.test(id) || !text.startsWith(AFTER_ID, idAt + 4)) {
      from = idAt;
      continue;
    }
    const bodyAt = idAt + 4 + AFTER_ID.length;
    const close = `</pasted_content id="${id}">`;
    const closeAt = text.indexOf(`\n${close}`, bodyAt - 1) + 1;
    if (closeAt === 0) break;
    // Up to two line breaks either side belong to the wrapping, not the text.
    let start = open;
    for (let n = 0; n < 2 && start > done && text[start - 1] === "\n"; n++) start--;
    if (start > done) parts.push({ kind: "text", text: text.slice(done, start) });
    done = closeAt + close.length;
    for (let n = 0; n < 2 && text[done] === "\n"; n++) done++;
    parts.push({ kind: "block", body: text.slice(bodyAt, closeAt - 1) });
    from = done;
  }
  if (done < text.length) parts.push({ kind: "text", text: text.slice(done) });
  return parts;
};

const endsAWord = (text: string): boolean => {
  let at = 0;
  while (at < text.length && CLOSERS.test(text.charAt(at))) at++;
  return at >= text.length || /\s/u.test(text.charAt(at));
};

/**
 * A prompt from a Claude Code session's record as its terminal draws it:
 * pasted blocks without their tags, a paste of several lines on lines of its
 * own.
 */
export function claudePromptAsDrawn(text: string): string {
  if (!text.includes(OPEN)) return text;
  const parts = partsOf(text);
  if (!parts.some((part) => part.kind === "block")) return text;
  let out = "";
  let breakBefore = false;
  for (const [index, part] of parts.entries()) {
    const next = parts[index + 1];
    if (part.kind === "text") {
      let piece = part.text;
      if (breakBefore) piece = piece.replace(/^\s+/u, "");
      if (next?.kind === "block" && next.body.includes("\n")) piece = piece.replace(/\s+$/u, "");
      if (piece === "") continue;
      if (breakBefore && out !== "") out += "\n";
      out += piece;
      breakBefore = false;
      continue;
    }
    const severalLines = part.body.includes("\n");
    let at = out.length;
    while (at > 0 && OPENERS.test(out.charAt(at - 1))) at--;
    const afterSpace = at === 0 || /\s/u.test(out.charAt(at - 1));
    if ((severalLines || breakBefore || !afterSpace) && out !== "") out += "\n";
    out += part.body;
    const after = next?.kind === "text" ? next.text : "";
    breakBefore =
      severalLines || next?.kind === "block" || /^\s\s/u.test(after) || !endsAWord(after);
  }
  return out;
}
