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

const REGISTER_JS = `// t3bridge ${CLAUDE_BRIDGE_MOD_VERSION}: makes T3 Code a window onto this Claude Code session.
// Files, under <bridge root>/claude/<session id>/<instance id>/:
//   session.json       this process, rewritten every few seconds (heartbeat)
//   commands.json      the slash commands this session has, rewritten when they change
//   inbox.json         requests from T3; inbox-cursor.json marks the last seq handled
//   events/<seq>.json  events for T3, one file each; T3 deletes what it has read
//   t3.json            T3's own heartbeat while it is attached
// <bridge root>/claude/mod-version.json names the mod version the server ships.
const PROTOCOL = 2
const MOD_VERSION = '${CLAUDE_BRIDGE_MOD_VERSION}'
const HEARTBEAT_MS = 3000
const INBOX_POLL_MS = 500
const COMMANDS_REFRESH_MS = 60000
const T3_LIVE_MS = 12000
const PROMPT_MATCH_WINDOW_MS = 20000
const COMMAND_TURN_WAIT_MS = 1500
const COMMAND_PANEL_WAIT_MS = 6000

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
let permissionMode = null
let effort = null
let commandsBody = ''
let commandsRev = 0
// Terminal clients attached to a background session, by client id.
const attached = new Set()
// A session an SDK host drives (T3's own first turn, claude -p, the Agent
// SDK) draws on no surface and takes no typed prompts: nothing to bridge, and
// its turns must not look like terminal turns to T3.
let passive = false
const decisions = new Map()
const answers = new Map()
// Tool calls the person allowed for the rest of this session from T3.
const grants = []
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

// What the classic hook payloads say about the session right now.
function noteClassic(e) {
  if (!e) return
  if (typeof e.permission_mode === 'string') permissionMode = e.permission_mode
  if (e.effort && typeof e.effort.level === 'string') effort = e.effort.level
}

function terminalPresent() {
  return isInteractive || attached.size > 0
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
    terminalPresent: terminalPresent(),
    startedAt: startedAt,
    heartbeatAt: Date.now(),
    model: model,
    permissionMode: permissionMode,
    effort: effort,
    commandsRev: commandsRev,
    activeTurn: activeTurn,
    modVersion: MOD_VERSION,
    ended: ended,
  })
}

// The slash commands this session has, as its own typeahead lists them.
async function refreshCommands($) {
  if (!dir || passive) return
  let list
  try { list = await $.command.list() } catch { return }
  const body = JSON.stringify(list.map((c) => ({ name: c.name, description: c.description, source: c.source })))
  if (body === commandsBody) return
  commandsBody = body
  commandsRev += 1
  try { await $.fs.write(dir + '/commands.json', body) } catch {}
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
  commandsBody = ''
  await heartbeat($)
  await refreshCommands($)
  await heartbeat($)
}

// When the server ships a newer mod, load it here without the person doing
// anything. Tried once per shipped version, so a reload that changes nothing
// cannot loop.
async function reloadIfOutdated($) {
  if (!root || !sid || activeTurn || pendingCommand) return
  let shipped
  try { shipped = JSON.parse(await $.fs.read(root + '/claude/mod-version.json')) } catch { return }
  if (!shipped || typeof shipped.version !== 'string' || shipped.version === MOD_VERSION) return
  const key = 'reload:' + sid + ':' + instanceId
  try {
    if ((await $.store.get(key)) === shipped.version) return
    await $.store.set(key, shipped.version)
  } catch { return }
  try { await $.command.run({ command: 'reload-plugins' }) } catch {}
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

async function sleepOutsideBudget($) {
  try { await $.process.run(['sleep', '1'], { timeoutMs: 5000 }) } catch {}
}

// Runs a slash command exactly as if the person typed it here. A command that
// starts a turn (a skill, /goal <condition>) reports through that turn; one
// that only prints reports its output; one that opened a panel in the
// terminal is reported as waiting there, because nothing but the person can
// close it.
function runSlashCommand($, command) {
  const label = '/' + command.name + (command.args ? ' ' + command.args : '')
  pendingCommand = { id: command.id, text: label, promptUuid: command.promptUuid, at: Date.now() }
  let settled = false
  const mine = () => pendingCommand !== null && pendingCommand.id === command.id
  $.clock.after(COMMAND_PANEL_WAIT_MS, () => {
    if (settled || !mine()) return
    settled = true
    pendingCommand = null
    emit($, 'command.result', {
      commandId: command.id,
      promptUuid: command.promptUuid,
      ok: false,
      waitingInTerminal: true,
      startedTurn: false,
      text: label + ' opened a panel in the Claude Code terminal and is waiting there. Close it in that terminal (Esc); nothing else runs in this session until then.',
    }).catch(() => {})
  })
  $.command.run({ command: command.name, args: command.args || '' }).then(async (result) => {
    if (settled) return
    const deadline = Date.now() + COMMAND_TURN_WAIT_MS
    while (Date.now() < deadline && mine()) {
      try { await $.clock.sleep(100) } catch { break }
    }
    if (settled) return
    settled = true
    const startedTurn = !mine()
    if (!startedTurn) pendingCommand = null
    await emit($, 'command.result', {
      commandId: command.id,
      promptUuid: command.promptUuid,
      ok: true,
      startedTurn: startedTurn,
      text: result && typeof result.text === 'string' ? result.text : '',
    })
    await refreshCommands($)
    await heartbeat($)
  }).catch(async (err) => {
    if (settled) return
    settled = true
    if (mine()) pendingCommand = null
    await emit($, 'command.result', { commandId: command.id, promptUuid: command.promptUuid, ok: false, startedTurn: false, text: String(err) })
  })
}

async function handleCommand($, command) {
  if (command.type === 'prompt') {
    pendingCommand = { id: command.id, text: command.text, promptUuid: command.promptUuid, at: Date.now() }
    // The person's half-typed draft stays where it is: a submit does not touch it.
    $.prompt.submit({ text: command.text, asUser: true }).then(async (result) => {
      if (result && result.drop) {
        if (pendingCommand && pendingCommand.id === command.id) pendingCommand = null
        await emit($, 'prompt.dropped', { commandId: command.id, reason: String(result.drop) })
      }
    }).catch(async (err) => {
      if (pendingCommand && pendingCommand.id === command.id) pendingCommand = null
      await emit($, 'prompt.dropped', { commandId: command.id, reason: String(err) })
    })
    return
  }
  if (command.type === 'command') {
    runSlashCommand($, command)
    return
  }
  if (command.type === 'approval') {
    decisions.set(command.toolUseId, command)
    return
  }
  if (command.type === 'answer') {
    answers.set(command.toolUseId, command)
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
    openSessionDir($).then(() => heartbeat($)).then(() => reloadIfOutdated($)).catch(() => {})
  })
  $.clock.every(INBOX_POLL_MS, () => {
    pollInbox($).catch(() => {})
  })
  $.clock.every(COMMANDS_REFRESH_MS, () => {
    refreshCommands($).then(() => heartbeat($)).catch(() => {})
  })
}

function grantKey(e) {
  let input = ''
  try { input = JSON.stringify(e.input) } catch {}
  return e.tool + ':' + input
}

function isGranted(e) {
  const key = grantKey(e)
  for (const grant of grants) {
    if (grant.tool !== e.tool) continue
    if (grant.wholeTool || grant.key === key) return true
  }
  return false
}

// While T3 is attached, the surface that started the turn answers it: a turn
// T3 started is held for T3, and a turn typed in the terminal keeps Claude
// Code's own dialog. A background session nobody has open is held for T3 too,
// since its dialog would be shown to no one.
async function heldForT3($) {
  if (!(await t3Attached($))) return false
  if (activeTurn && activeTurn.commandId) return true
  return !terminalPresent()
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

  // Only the guard baseline here: whether this process bridges at all is
  // known at session.start, which may come after this event.
  on('classic.SessionStart', async ($, e, next) => {
    transcriptPath = String(e.transcript_path || transcriptPath)
    noteClassic(e)
    await takeBaseline($)
    await heartbeat($)
    return next(e)
  })

  on('classic.UserPromptSubmit', async ($, e, next) => {
    noteClassic(e)
    return next(e)
  })

  on('classic.PreToolUse', async ($, e, next) => {
    noteClassic(e)
    return next(e)
  })

  on('classic.Stop', async ($, e, next) => {
    noteClassic(e)
    return next(e)
  })

  on('session.attach', async ($, e, next) => {
    if (e && typeof e.clientId === 'string') attached.add(e.clientId)
    await heartbeat($)
    return next(e)
  })

  on('session.detach', async ($, e, next) => {
    if (e && typeof e.clientId === 'string') attached.delete(e.clientId)
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

  // A question Claude asks the person. Held for T3 it is answered with the
  // answers themselves, as the terminal dialog would return them; a bare
  // approval cannot answer a question.
  on('tool.call', { tool: 'AskUserQuestion' }, async ($, e, next) => {
    if (!e.tool_use_id || e.agentId || !(await heldForT3($))) return next(e)
    const toolUseId = e.tool_use_id
    await emit($, 'question.request', { toolUseId: toolUseId, questions: e.questions, turnId: activeTurn ? activeTurn.turnId : undefined })
    for (;;) {
      const answer = answers.get(toolUseId)
      if (answer) {
        answers.delete(toolUseId)
        if (answer.decision === 'ask') return next(e)
        await emit($, 'question.resolved', { toolUseId: toolUseId, by: 't3' })
        if (answer.decision === 'deny') return { deny: answer.reason || 'The user declined to answer.' }
        return { result: { questions: e.questions, answers: answer.answers || {} } }
      }
      if (!(await t3Attached($))) {
        await emit($, 'question.resolved', { toolUseId: toolUseId, by: 'detached' })
        return next(e)
      }
      await sleepOutsideBudget($)
    }
  })

  // The wait runs through host calls, which do not count against the hook's
  // budget.
  on('tool.check', async ($, e, next) => {
    const verdict = await next(e)
    if (!verdict || verdict.decision !== 'ask' || !e.tool_use_id || e.agentId) return verdict
    if (isGranted(e)) return { decision: 'allow', reason: 'Allowed for this session in T3 Code' }
    if (!(await heldForT3($))) return verdict
    const toolUseId = e.tool_use_id
    await emit($, 'approval.request', { toolUseId: toolUseId, tool: e.tool, input: e.input, reason: verdict.reason, turnId: activeTurn ? activeTurn.turnId : undefined })
    for (;;) {
      const decision = decisions.get(toolUseId)
      if (decision) {
        decisions.delete(toolUseId)
        if (decision.decision === 'ask') return verdict
        await emit($, 'approval.resolved', { toolUseId: toolUseId, decision: decision.decision, by: 't3' })
        if (decision.decision !== 'allow') return { decision: 'deny', reason: decision.reason || 'Denied in T3 Code' }
        if (decision.scope === 'session') grants.push({ tool: e.tool, key: grantKey(e), wholeTool: decision.wholeTool === true })
        return { decision: 'allow', reason: 'Approved in T3 Code' }
      }
      if (!(await t3Attached($))) {
        await emit($, 'approval.resolved', { toolUseId: toolUseId, decision: 'ask', by: 'detached' })
        return verdict
      }
      await sleepOutsideBudget($)
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
