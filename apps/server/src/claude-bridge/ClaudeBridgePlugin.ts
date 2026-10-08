/**
 * ClaudeBridgePlugin - the `t3bridge` Claude Code plugin (a "mod": a hooks
 * module the CLI loads into every session of a home it is installed in), kept
 * here as source text so the server can write it to disk and the user can
 * install it with `claude plugin marketplace add <dir>`.
 *
 * The mod is one side of the file protocol in ClaudeBridgeProtocol.ts: it
 * heartbeats, runs commands T3 drops in its inbox (prompt, approval,
 * interrupt), and reports turn boundaries and held approvals as events. It
 * also carries the stale-tip guard: a prompt typed into a process whose
 * transcript another process has since extended is dropped with a notice,
 * so two copies of one session can never fork it.
 *
 * Keep the module free of backticks and `${` so it can live in this template.
 */
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import {
  CLAUDE_BRIDGE_MARKETPLACE_NAME,
  CLAUDE_BRIDGE_MOD_VERSION,
  CLAUDE_BRIDGE_PLUGIN_NAME,
} from "./ClaudeBridgeProtocol.ts";

const MARKETPLACE_JSON = JSON.stringify(
  {
    name: CLAUDE_BRIDGE_MARKETPLACE_NAME,
    owner: { name: "T3 Code" },
    metadata: {
      description: "Live sync between Claude Code sessions and T3 Code.",
      version: CLAUDE_BRIDGE_MOD_VERSION,
    },
    plugins: [
      {
        name: CLAUDE_BRIDGE_PLUGIN_NAME,
        description:
          "Lets T3 Code attach to this session: prompts and approvals from T3, turns typed here shown in T3.",
        version: CLAUDE_BRIDGE_MOD_VERSION,
        author: { name: "T3 Code" },
        source: `./plugins/${CLAUDE_BRIDGE_PLUGIN_NAME}`,
      },
    ],
  },
  null,
  2,
);

const PLUGIN_JSON = JSON.stringify(
  {
    name: CLAUDE_BRIDGE_PLUGIN_NAME,
    version: CLAUDE_BRIDGE_MOD_VERSION,
    description:
      "Lets T3 Code attach to this session: prompts and approvals from T3, turns typed here shown in T3.",
    author: { name: "T3 Code" },
  },
  null,
  2,
);

const HOOKS_JSON = JSON.stringify(
  { description: "t3bridge hooks module", modules: ["./register.js"] },
  null,
  2,
);

const REGISTER_JS = `// t3bridge ${CLAUDE_BRIDGE_MOD_VERSION}: live sync between this Claude Code session and T3 Code.
// Files, under <bridge root>/claude/<session id>/<instance id>/:
//   session.json       this process, rewritten every few seconds (heartbeat)
//   inbox.json         commands from T3; inbox-cursor.json marks the last seq handled
//   events/<seq>.json  events for T3, one file each; T3 deletes what it has read
//   t3.json            T3's own heartbeat while it is attached
const PROTOCOL = 1
const MOD_VERSION = '${CLAUDE_BRIDGE_MOD_VERSION}'
const HEARTBEAT_MS = 3000
const INBOX_POLL_MS = 500
const T3_LIVE_MS = 12000
const PROMPT_MATCH_WINDOW_MS = 20000

const instanceId = Date.now().toString(36) + '-' + Math.random().toString(36).slice(2, 8)
let root = null
let sid = null
let dir = null
let cwd = ''
let configDir = ''
let transcriptPath = ''
let isInteractive = false
let surfaces = []
const startedAt = Date.now()
let activeTurn = null
let pendingCommand = null
let inboxSeq = 0
let eventSeq = 0
let ended = null
let timersStarted = false
// A session an SDK host drives (T3's own first turn, claude -p, the Agent
// SDK) draws on no surface and takes no typed prompts: nothing to bridge, and
// its turns must not look like terminal turns to T3.
let passive = false
const decisions = new Map()
// Stale-tip guard state: message rows present when this process started, and
// the rows it appended itself. Any other row means another process moved the
// conversation on.
let baseline = null
const own = new Set()

function messageRowIds(body) {
  const ids = []
  for (const line of body.split('\\n')) {
    if (!line.startsWith('{')) continue
    let row
    try { row = JSON.parse(line) } catch { continue }
    if ((row.type !== 'user' && row.type !== 'assistant') || typeof row.uuid !== 'string') continue
    // Bookkeeping rows the running process writes outside session.append.
    if (row.isCompactSummary === true || row.isMeta === true) continue
    const content = row.message && row.message.content
    if (typeof content === 'string' && /^\\s*<(command-name|command-message|local-command-stdout|local-command-caveat)>/.test(content)) continue
    ids.push(row.uuid)
  }
  return ids
}

async function takeBaseline($) {
  if (!transcriptPath) return
  try {
    baseline = new Set(messageRowIds(await $.fs.read(transcriptPath)))
  } catch {
    baseline = new Set()
  }
}

async function resolveRoot($) {
  if (root) return root
  const fromEnv = await $.env.get('T3CODE_BRIDGE_DIR')
  if (fromEnv) {
    root = fromEnv
  } else {
    const home = (await $.env.get('HOME')) || ''
    root = home + '/.t3/userdata/bridge'
  }
  return root
}

async function writeJson($, path, value) {
  await $.fs.write(path, JSON.stringify(value))
}

async function emit($, type, data) {
  if (!dir || passive) return
  eventSeq += 1
  const name = String(eventSeq).padStart(8, '0')
  await writeJson($, dir + '/events/' + name + '.json', Object.assign({ seq: eventSeq, type: type, at: Date.now() }, data))
}

async function transcriptOffset($) {
  if (!transcriptPath) return 0
  try { return (await $.fs.stat(transcriptPath)).size } catch { return 0 }
}

async function heartbeat($) {
  if (!dir || passive) return
  try { surfaces = await $.session.surfaces() } catch { surfaces = [] }
  let model = null
  try { model = await $.session.model() } catch {}
  await writeJson($, dir + '/session.json', {
    protocol: PROTOCOL,
    sid: sid,
    instanceId: instanceId,
    cwd: cwd,
    configDir: configDir,
    transcriptPath: transcriptPath,
    surfaces: surfaces,
    isInteractive: isInteractive,
    startedAt: startedAt,
    heartbeatAt: Date.now(),
    model: model,
    activeTurn: activeTurn,
    modVersion: MOD_VERSION,
    ended: ended,
  })
}

async function t3Attached($) {
  if (!dir) return false
  try {
    const t3 = JSON.parse(await $.fs.read(dir + '/t3.json'))
    return typeof t3.heartbeatAt === 'number' && Date.now() - t3.heartbeatAt < T3_LIVE_MS
  } catch {
    return false
  }
}

// Binds this process to its session id (which changes on /clear) and the
// directory the protocol files live in.
async function openSessionDir($) {
  if (passive) return
  let id = null
  try { id = await $.session.id() } catch { return }
  if (!id || id === sid) return
  const base = await resolveRoot($)
  sid = id
  dir = base + '/claude/' + sid + '/' + instanceId
  inboxSeq = 0
  eventSeq = 0
  ended = null
  activeTurn = null
  pendingCommand = null
  await heartbeat($)
}

async function pollInbox($) {
  if (!dir) return
  const path = dir + '/inbox.json'
  let inbox
  try {
    if (!(await $.fs.exists(path))) return
    inbox = JSON.parse(await $.fs.read(path))
  } catch {
    return
  }
  const commands = Array.isArray(inbox.commands) ? inbox.commands : []
  for (const command of commands) {
    if (typeof command.seq !== 'number' || command.seq <= inboxSeq) continue
    inboxSeq = command.seq
    await writeJson($, dir + '/inbox-cursor.json', { seq: inboxSeq })
    try {
      await handleCommand($, command)
    } catch (err) {
      await emit($, 'error', { message: String(err), commandId: command.id })
    }
  }
}

async function handleCommand($, command) {
  if (command.type === 'prompt') {
    pendingCommand = { id: command.id, text: command.text, promptUuid: command.promptUuid, at: Date.now() }
    // The person's half-typed draft survives a prompt T3 submits.
    let draft = { text: '', cursor: 0 }
    try { draft = await $.prompt.read() } catch {}
    $.prompt.submit({ text: command.text, asUser: true }).then(async (result) => {
      if (result && result.drop) {
        if (pendingCommand && pendingCommand.id === command.id) pendingCommand = null
        await emit($, 'prompt.dropped', { commandId: command.id, reason: String(result.drop) })
      }
      if (draft.text) {
        try { await $.prompt.fill({ text: draft.text, mode: 'replace' }) } catch {}
      }
    }).catch(async (err) => {
      if (pendingCommand && pendingCommand.id === command.id) pendingCommand = null
      await emit($, 'prompt.dropped', { commandId: command.id, reason: String(err) })
    })
    return
  }
  if (command.type === 'approval') {
    decisions.set(command.toolUseId, command)
    return
  }
  if (command.type === 'interrupt') {
    if (activeTurn) await $.turn.abort({ turnId: activeTurn.turnId })
    return
  }
}

function startTimers($) {
  if (timersStarted || passive) return
  timersStarted = true
  $.clock.every(HEARTBEAT_MS, () => {
    openSessionDir($).then(() => heartbeat($)).catch(() => {})
  })
  $.clock.every(INBOX_POLL_MS, () => {
    pollInbox($).catch(() => {})
  })
}

export function register(on) {
  on('session.start', async ($, e, next) => {
    cwd = String(e.cwd || '')
    isInteractive = e.isInteractive === true
    passive = e.surface === null && !isInteractive
    try {
      const fromEnv = await $.env.get('CLAUDE_CONFIG_DIR')
      configDir = fromEnv || (((await $.env.get('HOME')) || '') + '/.claude')
    } catch {}
    await openSessionDir($)
    startTimers($)
    return next(e)
  })

  on('classic.SessionStart', async ($, e, next) => {
    transcriptPath = String(e.transcript_path || transcriptPath)
    await takeBaseline($)
    await openSessionDir($)
    startTimers($)
    await heartbeat($)
    return next(e)
  })

  on('session.append', async ($, e, next) => {
    if (e && typeof e.uuid === 'string') own.add(e.uuid)
    return next(e)
  })

  // Stale-tip guard. Applies to typed prompts and to prompts T3 submits here.
  on('prompt.submit', async ($, e, next) => {
    if (baseline !== null && transcriptPath) {
      try {
        const body = await $.fs.read(transcriptPath)
        const foreign = messageRowIds(body).filter((id) => !baseline.has(id) && !own.has(id))
        if (foreign.length > 0) {
          return { drop: 'This conversation continued in another process (' + foreign.length + ' new messages). Reload before sending.' }
        }
      } catch {}
    }
    return next(e)
  })

  on('turn.start', async ($, e, next) => {
    const offset = await transcriptOffset($)
    let commandId
    let promptUuid
    if (pendingCommand && (pendingCommand.text === e.text || Date.now() - pendingCommand.at < PROMPT_MATCH_WINDOW_MS)) {
      commandId = pendingCommand.id
      promptUuid = pendingCommand.promptUuid
      pendingCommand = null
    }
    activeTurn = { turnId: e.turnId, startedAt: Date.now(), transcriptOffset: offset, commandId: commandId, promptUuid: promptUuid }
    await emit($, 'turn.start', { turnId: e.turnId, text: e.text, transcriptOffset: offset, commandId: commandId, promptUuid: promptUuid })
    await heartbeat($)
    return next(e)
  })

  on('turn.complete', async ($, e, next) => {
    if (e.agentId) return next(e)
    const offset = await transcriptOffset($)
    const turn = activeTurn
    activeTurn = null
    await emit($, 'turn.complete', {
      turnId: e.turnId,
      reason: e.reason,
      answer: e.answer,
      isAborted: e.isAborted === true,
      durationMs: e.durationMs,
      usage: e.usage,
      transcriptOffset: offset,
      commandId: turn ? turn.commandId : undefined,
      promptUuid: turn ? turn.promptUuid : undefined,
    })
    await heartbeat($)
    return next(e)
  })

  // While T3 is attached it decides approvals: a background session reports a
  // terminal surface whether or not anyone is attached to it, so there is no
  // way to tell an unseen dialog from a seen one. The wait runs through host
  // calls, which do not count against the hook's budget.
  on('tool.check', async ($, e, next) => {
    const verdict = await next(e)
    if (!verdict || verdict.decision !== 'ask' || !e.tool_use_id || e.agentId) return verdict
    if (!(await t3Attached($))) return verdict
    const toolUseId = e.tool_use_id
    await emit($, 'approval.request', { toolUseId: toolUseId, tool: e.tool, input: e.input, reason: verdict.reason, turnId: activeTurn ? activeTurn.turnId : undefined })
    for (;;) {
      const decision = decisions.get(toolUseId)
      if (decision) {
        decisions.delete(toolUseId)
        if (decision.decision === 'ask') return verdict
        await emit($, 'approval.resolved', { toolUseId: toolUseId, decision: decision.decision, by: 't3' })
        return decision.decision === 'allow'
          ? { decision: 'allow', reason: 'Approved in T3 Code' }
          : { decision: 'deny', reason: decision.reason || 'Denied in T3 Code' }
      }
      if (!(await t3Attached($))) {
        await emit($, 'approval.resolved', { toolUseId: toolUseId, decision: 'ask', by: 'detached' })
        return verdict
      }
      try { await $.process.run(['sleep', '1'], { timeoutMs: 5000 }) } catch {}
    }
  })

  on('session.end', async ($, e, next) => {
    ended = { reason: e.reason, sessionId: e.sessionId, at: Date.now() }
    await emit($, 'ended', { reason: e.reason, sessionId: e.sessionId })
    await heartbeat($)
    return next(e)
  })
}
`;

export interface ClaudeBridgePluginFiles {
  /** The marketplace root: what `claude plugin marketplace add` takes. */
  readonly marketplaceDir: string;
  /** The plugin directory inside the marketplace. */
  readonly pluginDir: string;
}

/**
 * Writes the marketplace (and the plugin inside it) under `dir`, replacing
 * whatever version was there. Idempotent and cheap, so it runs on every boot.
 */
export const materializeClaudeBridgePlugin = Effect.fn("materializeClaudeBridgePlugin")(function* (
  dir: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const pluginDir = path.join(dir, "plugins", CLAUDE_BRIDGE_PLUGIN_NAME);
  const files: ReadonlyArray<readonly [string, string]> = [
    [path.join(dir, ".claude-plugin", "marketplace.json"), MARKETPLACE_JSON + "\n"],
    [path.join(pluginDir, ".claude-plugin", "plugin.json"), PLUGIN_JSON + "\n"],
    [path.join(pluginDir, "hooks", "hooks.json"), HOOKS_JSON + "\n"],
    [path.join(pluginDir, "hooks", "register.js"), REGISTER_JS],
  ];
  for (const [file, content] of files) {
    yield* fs.makeDirectory(path.dirname(file), { recursive: true });
    const existing = yield* fs.readFileString(file).pipe(Effect.orElseSucceed(() => null));
    if (existing !== content) {
      yield* fs.writeFileString(file, content);
    }
  }
  return { marketplaceDir: dir, pluginDir } satisfies ClaudeBridgePluginFiles;
});

/** The mod source, for tests that load it into a real process. */
export const CLAUDE_BRIDGE_REGISTER_JS = REGISTER_JS;
