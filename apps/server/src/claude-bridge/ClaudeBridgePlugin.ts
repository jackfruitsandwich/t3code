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
  CLAUDE_BARE_PANEL_COMMANDS,
  CLAUDE_BRIDGE_MARKETPLACE_NAME,
  CLAUDE_BRIDGE_MOD_VERSION,
  CLAUDE_BRIDGE_PLUGIN_NAME,
} from "./ClaudeBridgeProtocol.ts";
import { loadClaudeBridgeToolSpecs, type ClaudeBridgeToolSpec } from "./ClaudeBridgeTools.ts";

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
//   t3.json            T3's own heartbeat while it is attached, with where it answers this session's tool calls
// <bridge root>/claude/mod-version.json names the mod version the server ships.
const PROTOCOL = 2
const MOD_VERSION = '${CLAUDE_BRIDGE_MOD_VERSION}'
const HEARTBEAT_MS = 3000
const INBOX_POLL_MS = 500
const COMMANDS_REFRESH_MS = 60000
// How old T3's heartbeat (one every 3 s) may be before T3 counts as gone. A
// server busy starting up can miss several beats; a prompt handed back or
// asked twice over a lapse is worse than one held a little longer.
const T3_LIVE_MS = 30000
const PROMPT_MATCH_WINDOW_MS = 20000
// T3's own tools, declared here so the session has them whatever started it;
// each call is answered from T3's tool endpoint (see t3.json).
const T3_TOOLS = __T3_TOOLS__
const T3_TOOL_PREFIX = 'mcp__${CLAUDE_BRIDGE_PLUGIN_NAME}__'
const MCP_PROTOCOL = '2025-06-18'
const T3_TOOL_ATTACH_WAIT = 20
const T3_ATTACH_GRACE_S = 15
// A session nobody else can answer for (no terminal open): how long a new
// request gives a T3 that has not been seen lately to attach, and how long a
// request already waiting outlasts a T3 that went away (a restart, an update).
const T3_ALONE_GRACE_S = 10
const T3_ALONE_PATIENCE_S = 120
const T3_FOLLOW_MS = 60000
const T3_RECENT_MS = 300000
// A request still waiting is told to T3 again this often. T3 shows a request
// inside a run of its thread; one that outlives that run (a background
// subagent's) is picked up by the next, and so is one whose card was put
// away because a new turn began. T3 ignores what it is already asking.
const T3_RETELL_MS = 5000
const SHELL_COMMAND_TIMEOUT_MS = 120000
// What Claude Code writes before a local command's rows, for the model.
const LOCAL_COMMAND_CAVEAT = "The command below was run directly in Claude Code, not sent to you as a request, and its output goes straight to the user. It's recorded here as context for later messages."
const SHELL_INPUT_ROW = /^\\s*<bash-input>([\\s\\S]*)<\\/bash-input>\\s*$/
const SHELL_OUTPUT_ROW = /^\\s*<bash-stdout>/
const COMMAND_TURN_WAIT_MS = 1500
const COMMAND_PANEL_WAIT_MS = 6000
// Built-in commands that, typed with no argument, open a panel in the terminal
// (observed on Claude Code 2.1.296 by running each one). A panel can only be
// closed by the person at that terminal and everything queued behind it waits,
// so these are not run from T3 without an argument. Commands found to behave
// this way later are remembered in the plugin store.
// Commands that ask in a dialog even when typed with their argument (observed).
const ARGUMENT_PANELS = new Set(['add-dir'])
const KNOWN_PANELS = new Set([${CLAUDE_BARE_PANEL_COMMANDS.map((name) => `'${name}'`).join(", ")}])

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
// The last "!" command the record took: the turn that follows it is started
// with what the command printed, and is shown as the command.
let lastShellInput = null
// Commands sent while a turn was running, run when it ends.
const afterTurn = []
// Panels commands sent from T3 have opened in the terminal and that are still open.
let panelsOpen = 0
// Whether the latest turn was sent from T3 (else typed here, or a wake).
let lastTurnFromT3 = false
let inboxSeq = 0
let eventSeq = 0
let ended = null
let timersStarted = false
let permissionMode = null
let toolsDeclared = false
const drawing = new Map()
// Tool calls Claude Code is about to ask about, until their dialog opens, and
// the calls whose result is already in the record (answered, here or anywhere).
const asks = []
const settledCalls = new Set()
let mcpSession = null
let mcpRequestId = 0
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
// What this session is holding for T3's answer right now, by tool call. It
// is written into session.json: a T3 that is running with nothing attached
// to this session (it was restarted, or let the session go) reads it there
// and attaches, and is then told the request itself.
const waiting = new Map()
// Tool calls the person allowed for the rest of this session from T3.
const grants = []
// Files T3 saved for a prompt it sent here (what the person attached).
const t3Files = new Set()
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

// What the classic hook payloads say about the session right now. Each names
// the session's record file, which a mod loaded mid-session (/reload-plugins)
// is told by nothing else; it is kept so the next load starts knowing it.
function noteClassic($, e) {
  if (!e) return
  if (typeof e.permission_mode === 'string') permissionMode = e.permission_mode
  if (e.effort && typeof e.effort.level === 'string') effort = e.effort.level
  if (typeof e.transcript_path === 'string' && e.transcript_path && e.transcript_path !== transcriptPath) {
    transcriptPath = e.transcript_path
    if (recordIsOf(sid)) $.store.set('transcript:' + sid, transcriptPath).catch(() => {})
  }
}

// A session's record is the file named after its id. While this process is
// moving on to another conversation (/clear, /resume, /branch) the path it
// has just learned is the next session's, not the one its files still speak
// for.
function recordIsOf(id) {
  return !!id && !!transcriptPath && transcriptPath.slice(-(id.length + 6)) === id + '.jsonl'
}

// Declares T3's tools in this session. The engine refuses until the session
// has bound, so it is tried again from the heartbeat until it holds.
async function declareT3Tools($) {
  if (toolsDeclared || passive) return
  try {
    for (const tool of T3_TOOLS) {
      await $.tool.register({ name: tool.name, description: tool.description, inputSchema: tool.inputSchema, isDeferred: tool.isDeferred })
    }
    toolsDeclared = true
  } catch {}
}

function isT3ReadOnlyTool(name) {
  for (const tool of T3_TOOLS) {
    if (tool.isReadOnly && name === T3_TOOL_PREFIX + tool.name) return true
  }
  return false
}

// Where T3 answers tool calls for this session and the credential of the
// thread that shows it: T3 writes both into t3.json while it is attached. A
// turn typed here is attached to a moment after it starts.
async function t3ToolEndpoint($) {
  for (let attempt = 0; attempt < T3_TOOL_ATTACH_WAIT; attempt += 1) {
    try {
      const t3 = JSON.parse(await $.fs.read(dir + '/t3.json'))
      const live = t3 && typeof t3.heartbeatAt === 'number' && Date.now() - t3.heartbeatAt < T3_LIVE_MS
      if (live && t3.mcp && typeof t3.mcp.endpoint === 'string' && typeof t3.mcp.authorization === 'string') return t3.mcp
    } catch {}
    await sleepOutsideBudget($)
  }
  return null
}

// One JSON-RPC message, or server-sent events whose data lines carry them.
function parseMcpBody(text) {
  const body = String(text || '').trim()
  if (!body) return null
  if (body[0] === '{') {
    try { return JSON.parse(body) } catch { return null }
  }
  let answer = null
  for (const line of body.split('\\n')) {
    if (line.indexOf('data:') !== 0) continue
    try {
      const message = JSON.parse(line.slice(5).trim())
      if (message && (message.result !== undefined || message.error !== undefined)) answer = message
    } catch {}
  }
  return answer
}

async function mcpRequest($, mcp, method, params, isNotification) {
  const headers = {
    accept: 'application/json, text/event-stream',
    authorization: mcp.authorization,
    'content-type': 'application/json',
    'mcp-protocol-version': MCP_PROTOCOL,
  }
  if (mcpSession && mcpSession.id) headers['mcp-session-id'] = mcpSession.id
  mcpRequestId += 1
  const message = isNotification
    ? { jsonrpc: '2.0', method: method, params: params }
    : { jsonrpc: '2.0', id: mcpRequestId, method: method, params: params }
  const response = await $.http.fetch(mcp.endpoint, { method: 'POST', headers: headers, body: JSON.stringify(message) })
  const responseHeaders = response.headers || {}
  const sessionId = responseHeaders['mcp-session-id'] || responseHeaders['Mcp-Session-Id']
  if (sessionId) mcpSession = { endpoint: mcp.endpoint, authorization: mcp.authorization, id: String(sessionId) }
  if (isNotification) return null
  if (!response.ok) throw new Error('T3 Code answered ' + response.status + ': ' + String(response.text || '').slice(0, 300))
  const answer = parseMcpBody(response.text)
  if (!answer) throw new Error('T3 Code sent no answer')
  if (answer.error) throw new Error(answer.error.message || 'T3 Code refused the call')
  return answer.result
}

// A tool's answer as the engine takes one from a mod: text, or blocks.
function t3ToolAnswer(result) {
  const blocks = []
  for (const block of (result && Array.isArray(result.content) ? result.content : [])) {
    if (block && block.type === 'text' && typeof block.text === 'string') {
      blocks.push({ type: 'text', text: block.text })
    } else if (block && block.type === 'image' && typeof block.data === 'string') {
      blocks.push({ type: 'image', source: { type: 'base64', media_type: block.mimeType || 'image/png', data: block.data } })
    }
  }
  if (result && result.isError) {
    const reason = blocks.filter((block) => block.type === 'text').map((block) => block.text).join('\\n')
    return { deny: reason || 'The tool reported an error.' }
  }
  if (blocks.length === 1 && blocks[0].type === 'text') return { result: blocks[0].text }
  return { result: blocks.length > 0 ? blocks : '' }
}

async function callT3Tool($, name, input) {
  const mcp = await t3ToolEndpoint($)
  if (!mcp) {
    return { deny: 'T3 Code is not showing this session right now, so its ' + name + ' tool cannot run. It works while T3 Code is running and has this session open as a thread.' }
  }
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const sameServer = mcpSession && mcpSession.endpoint === mcp.endpoint && mcpSession.authorization === mcp.authorization
      if (!sameServer || attempt > 0) {
        mcpSession = null
        await mcpRequest($, mcp, 'initialize', { protocolVersion: MCP_PROTOCOL, capabilities: {}, clientInfo: { name: 't3bridge', version: MOD_VERSION } }, false)
        await mcpRequest($, mcp, 'notifications/initialized', {}, true).catch(() => {})
      }
      return t3ToolAnswer(await mcpRequest($, mcp, 'tools/call', { name: name, arguments: input }, false))
    } catch (err) {
      if (attempt === 1) return { deny: String((err && err.message) || err) }
    }
  }
  return { deny: 'T3 Code did not answer.' }
}

function terminalPresent() {
  return isInteractive || attached.size > 0
}

// Whether a terminal is open on the session right now: its own, or one that
// attached. A session T3 started in the background is interactive and has
// none (measured: its surfaces are empty), so a dialog there is shown to
// nobody. Until Claude Code has said what the session's surfaces are, an
// interactive session is taken to have its terminal.
let surfacesKnown = false
async function terminalOpen($) {
  try {
    surfaces = await $.session.surfaces()
    surfacesKnown = true
  } catch {}
  if (attached.size > 0) return true
  if (await startedUnattended($)) return false
  return surfacesKnown ? Array.isArray(surfaces) && surfaces.indexOf('terminal') !== -1 : isInteractive
}

// A process T3 started itself runs in the background and draws on a terminal
// nobody is at. Claude Code reports a terminal surface for it all the same
// (measured), so T3 says so in the environment it starts the process with:
// until a client attaches, a dialog there is seen by no one.
let unattended = null
async function startedUnattended($) {
  if (unattended === null) {
    try { unattended = (await $.env.get('T3CODE_BRIDGE_LAUNCH')) === 'background' } catch { unattended = false }
  }
  return unattended
}

async function heartbeat($) {
  if (!dir || passive) return
  try {
    surfaces = await $.session.surfaces()
    surfacesKnown = true
  } catch { surfaces = [] }
  let model = null
  try { model = await $.session.model() } catch {}
  await writeJson($, dir + '/session.json', {
    protocol: PROTOCOL,
    sid: sid,
    instanceId: instanceId,
    cwd: cwd,
    configDir: configDir,
    transcriptPath: recordIsOf(sid) ? transcriptPath : '',
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
    waiting: Array.from(waiting.values()),
    modVersion: MOD_VERSION,
    ended: ended,
  })
}

// Lists a held request in session.json, or takes it off (entry null), at once.
async function noteWaiting($, toolUseId, entry) {
  if (entry) waiting.set(toolUseId, entry)
  else if (!waiting.delete(toolUseId)) return
  try { await heartbeat($) } catch {}
}

// Whether a dialog or panel is open in the terminal right now. Claude Code
// refuses to touch the prompt box under one and says so; appending nothing
// asks that without changing what the person has typed (measured).
async function dialogOpen($) {
  try {
    const box = await $.prompt.fill({ text: '', mode: 'append' })
    return !!box && box.isFilled === false && box.refusal === 'dialog'
  } catch {
    return false
  }
}

async function opensPanel($, name) {
  if (KNOWN_PANELS.has(name)) return true
  try { return (await $.store.get('panel:' + MOD_VERSION + ':' + name)) === true } catch { return false }
}

function settingLine(row) {
  const choices = Array.isArray(row.options) && row.options.length > 0 ? '   (' + row.options.join(', ') + ')' : ''
  return row.key + ' = ' + JSON.stringify(row.value) + choices + (row.isLocked ? '   [locked]' : '')
}

// The settings panel as text: every row, one row, or a row set to a value.
async function settingsByText($, args) {
  let rows = []
  try { rows = await $.config.list() } catch (err) { return { ok: false, text: String(err) } }
  const text = String(args).trim()
  if (!text) {
    return { ok: true, text: 'Settings of this session. Change one with /config <key> <value>.\\n\\n' + rows.map(settingLine).join('\\n') }
  }
  const space = text.indexOf(' ')
  const key = space === -1 ? text : text.slice(0, space)
  const row = rows.find((entry) => entry.key === key)
  if (!row) return { ok: false, text: 'No setting named ' + key + '. /config lists them.' }
  if (space === -1) {
    return { ok: true, text: settingLine(row) + (key === 'model' ? '\\n\\nChange it with /model <name>.' : '\\n\\nChange it with /config ' + key + ' <value>.') }
  }
  const raw = text.slice(space + 1).trim()
  let value = raw
  if (raw === 'true') value = true
  else if (raw === 'false') value = false
  else if (Array.isArray(row.options)) {
    const match = row.options.find((option) => String(option).toLowerCase() === raw.toLowerCase())
    if (match !== undefined) value = match
  }
  let outcome
  try { outcome = await $.config.set({ key: key, value: value }) } catch (err) { outcome = { deny: String(err) } }
  if (outcome && outcome.deny) return { ok: false, text: String(outcome.deny) }
  await heartbeat($)
  return { ok: true, text: 'Set ' + key + ' to ' + JSON.stringify(value) }
}

// The model row of /config takes these names; a full model id maps onto one.
async function modelChoice($, wanted) {
  let options = []
  try {
    const row = (await $.config.list()).find((entry) => entry.key === 'model')
    options = row && Array.isArray(row.options) ? row.options : []
  } catch { return null }
  const text = String(wanted).trim()
  if (options.includes(text)) return text
  const family = /(opus|sonnet|haiku|fable)/.exec(text.toLowerCase())
  if (!family) return null
  const wide = /\\[1m\\]\\s*$/.test(text) ? family[1] + '[1m]' : null
  if (wide && options.includes(wide)) return wide
  return options.includes(family[1]) ? family[1] : null
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

// T3's last heartbeat as last read whole. The file is read every second while
// a prompt waits: one read that fails or does not parse says nothing, and the
// file being gone says T3 let go.
let t3HeartbeatSeen = 0
let t3PidSeen = 0
let t3SeenIn = null
async function t3Attached($) {
  if (!dir) return false
  if (t3SeenIn !== dir) { t3SeenIn = dir; t3HeartbeatSeen = 0; t3PidSeen = 0 }
  try {
    const t3 = JSON.parse(await $.fs.read(dir + '/t3.json'))
    if (typeof t3.heartbeatAt === 'number') t3HeartbeatSeen = t3.heartbeatAt
    if (typeof t3.pid === 'number') t3PidSeen = t3.pid
  } catch {
    let exists = true
    try { exists = await $.fs.exists(dir + '/t3.json') } catch {}
    if (!exists) t3HeartbeatSeen = 0
  }
  return Date.now() - t3HeartbeatSeen < T3_LIVE_MS
}

// Whether T3 has been around lately: this mod was loaded a moment ago (T3
// follows a reloaded mod after a few seconds), or T3's heartbeat was seen in
// the last few minutes. Not so when T3 is simply not running.
const LOADED_AT = Date.now()
function t3Expected() {
  const now = Date.now()
  return now - LOADED_AT < T3_FOLLOW_MS || (t3HeartbeatSeen > 0 && now - t3HeartbeatSeen < T3_RECENT_MS)
}

// Whether T3 is in sight, waiting a few seconds for it where it has been
// around lately. isOver ends the wait when the thing asked about settled.
async function t3InSightSoon($, isOver, alone) {
  if (await t3Attached($)) return true
  // Where nobody else can answer, a T3 that is running but not attached gets
  // a moment too: it reads what waits from session.json and attaches.
  const grace = t3Expected() ? T3_ATTACH_GRACE_S : alone ? T3_ALONE_GRACE_S : 0
  for (let waited = 0; waited < grace; waited += 1) {
    if (isOver()) return false
    await sleepOutsideBudget($)
    if (await t3Attached($)) return true
  }
  return false
}

// Keeps T3 informed of a request that is waiting for an answer. Called once
// a second by the wait: it says so when T3 goes out of sight, and asks again
// when T3 is back or another T3 process has taken its place (a restart quick
// enough that the heartbeat never looked stale: the new process knows nothing
// of what the old one was asked). Resolves to whether T3 is in sight.
function t3Teller($, announce, retire) {
  let isTold = true
  let pid = t3PidSeen
  let toldAt = Date.now()
  return async () => {
    const attachedNow = await t3Attached($)
    if (!attachedNow && isTold) {
      isTold = false
      await retire()
    } else if (attachedNow && (!isTold || t3PidSeen !== pid || Date.now() - toldAt > T3_RETELL_MS)) {
      isTold = true
      pid = t3PidSeen
      toldAt = Date.now()
      await announce()
    }
    return attachedNow
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
  if (!recordIsOf(sid)) {
    try { transcriptPath = String((await $.store.get('transcript:' + sid)) || '') } catch {}
  }
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
async function runSlashCommand($, command) {
  const label = '/' + command.name + (command.args ? ' ' + command.args : '')
  const report = (data) => emit($, 'command.result', Object.assign({ commandId: command.id, promptUuid: command.promptUuid, startedTurn: false }, data))
  // /config is a panel in the terminal. Its rows are the session's own list,
  // so from T3 it is text: bare lists them, a key and a value sets that row,
  // as picking it in the panel does. /model bare lists the model row.
  if (command.name === 'config' || (command.name === 'model' && !command.args)) {
    await report(await settingsByText($, command.name === 'model' ? 'model' : (command.args || '')))
    return
  }
  if (ARGUMENT_PANELS.has(command.name)) {
    await report({ ok: true, text: 'Not run: ' + label + ' asks in a dialog in the Claude Code terminal even when given its argument, and only that terminal can show and answer it. Run it there.' })
    return
  }
  if (!command.args && (await opensPanel($, command.name))) {
    await report({ ok: true, text: 'Not run: ' + label + ' opens a panel in the Claude Code terminal, which only that terminal can show and close. Run it there, or give it an argument here (for example /model sonnet, /effort high, /goal <condition>).' })
    return
  }
  // Mid-conversation, /model <name> asks the terminal to confirm. The same
  // change through the settings row applies at once, as picking it in /config
  // does, and leaves nothing in the record; the terminal's /model leaves the
  // command and what it printed, so those rows are written here.
  if (command.name === 'model' && command.args) {
    const choice = await modelChoice($, command.args)
    if (choice !== null) {
      // Typed while a turn runs, a command waits for the turn's end.
      if (activeTurn) {
        afterTurn.push(command)
        return
      }
      let outcome
      try { outcome = await $.config.set({ key: 'model', value: choice }) } catch (err) { outcome = { deny: String(err) } }
      let model = choice
      try { model = await $.session.model() } catch {}
      await heartbeat($)
      if (outcome && outcome.deny) {
        await report({ ok: false, text: String(outcome.deny) })
        return
      }
      const text = 'Set model to ' + model
      // Before the report: T3 shows the record's own row for what was printed.
      await appendCommandRows($, 'model', command.args, text)
      await report({ ok: true, text: text })
      return
    }
  }
  // A command that has not come back after a few seconds is one of three
  // things. It waits behind what holds the terminal (a running turn, a dialog,
  // a panel an earlier command opened); or it opened a panel itself, which
  // only the person at that terminal can close; or it is simply still working
  // (a compaction takes minutes). Claude Code says whether a dialog is open,
  // so the three are told apart and only the second is remembered as a panel.
  const blockedAtSend = activeTurn !== null || panelsOpen > 0 || (await dialogOpen($))
  pendingCommand = { id: command.id, text: label, promptUuid: command.promptUuid, at: Date.now() }
  let settled = false
  let holdsPanel = false
  const panelClosed = () => {
    if (!holdsPanel) return
    holdsPanel = false
    panelsOpen = Math.max(0, panelsOpen - 1)
  }
  const mine = () => pendingCommand !== null && pendingCommand.id === command.id
  const lookAgain = () => $.clock.after(COMMAND_PANEL_WAIT_MS, () => { stillOut().catch(() => {}) })
  const stillOut = async () => {
    if (settled || !mine()) return
    if (blockedAtSend || activeTurn !== null) {
      settled = true
      pendingCommand = null
      await report({
        ok: true,
        waitingInTerminal: true,
        text: label + ' is waiting in the Claude Code terminal behind what is running or open there, and runs when that is done.',
      })
      return
    }
    if (!(await dialogOpen($))) {
      lookAgain()
      return
    }
    settled = true
    pendingCommand = null
    holdsPanel = true
    panelsOpen += 1
    $.store.set('panel:' + MOD_VERSION + ':' + command.name, true).catch(() => {})
    await report({
      ok: true,
      waitingInTerminal: true,
      text: label + ' opened a panel in the Claude Code terminal and is waiting there. Close it in that terminal (Esc); nothing else runs in this session until then.',
    })
  }
  lookAgain()
  $.command.run({ command: command.name, args: command.args || '' }).then(async (result) => {
    panelClosed()
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
    panelClosed()
    if (settled) return
    settled = true
    if (mine()) pendingCommand = null
    await emit($, 'command.result', { commandId: command.id, promptUuid: command.promptUuid, ok: false, startedTurn: false, text: String(err) })
  })
}

// The rows Claude Code writes when a slash command that only acts locally is
// typed: a note for the model, the command, what it printed.
async function appendCommandRows($, name, args, printed) {
  const row = (text) => $.session.append({ message: { type: 'user', content: [{ type: 'text', text: text }] } })
  try {
    await row('<local-command-caveat>' + LOCAL_COMMAND_CAVEAT + '</local-command-caveat>')
    await row('<command-name>/' + name + '</command-name>\\n            <command-message>' + name + '</command-message>\\n            <command-args>' + args + '</command-args>')
    await row('<local-command-stdout>' + printed + '</local-command-stdout>')
  } catch {}
}

// "!" in the terminal runs a shell command with no permission prompt, writes
// the command and what it printed into the record, and hands that output to
// the model, which replies. The same from T3: run here, recorded the same
// way, and what it printed submitted as the turn's prompt.
async function runShellCommand($, command) {
  const report = (data) => emit($, 'command.result', Object.assign({ commandId: command.id, promptUuid: command.promptUuid, startedTurn: false }, data))
  let shell = '/bin/sh'
  try { shell = (await $.env.get('SHELL')) || shell } catch {}
  let outcome
  const startedAt = Date.now()
  try {
    outcome = await $.process.run([shell, '-c', command.command], { timeoutMs: SHELL_COMMAND_TIMEOUT_MS })
  } catch (err) {
    // Cut at the limit, as the terminal cuts it: the record says so in the
    // terminal's words, and the model is handed that like any other output.
    if (Date.now() - startedAt < SHELL_COMMAND_TIMEOUT_MS - 1000) {
      await report({ ok: false, text: String((err && err.message) || err) })
      return
    }
    outcome = { stdout: '', stderr: 'Command timed out after ' + Math.floor(SHELL_COMMAND_TIMEOUT_MS / 60000) + 'm ' + Math.floor((SHELL_COMMAND_TIMEOUT_MS % 60000) / 1000) + 's' }
  }
  const printed = '<bash-stdout>' + String(outcome.stdout || '').trimEnd() + '</bash-stdout><bash-stderr>' + String(outcome.stderr || '').trimEnd() + '</bash-stderr>'
  try {
    await $.session.append({ message: { type: 'user', content: [{ type: 'text', text: '<bash-input>' + command.command + '</bash-input>' }] } })
  } catch {}
  pendingCommand = { id: command.id, text: printed, promptUuid: command.promptUuid, at: Date.now() }
  const dropped = async (reason) => {
    if (pendingCommand && pendingCommand.id === command.id) pendingCommand = null
    await emit($, 'prompt.dropped', { commandId: command.id, reason: String(reason) })
  }
  $.prompt.submit({ text: printed, asUser: true }).then((result) => (result && result.drop ? dropped(result.drop) : undefined)).catch(dropped)
}

async function handleCommand($, command) {
  if (command.type === 'prompt') {
    // Files T3 saved for this prompt, under its own attachments folder.
    const bridge = await resolveRoot($)
    const folder = (bridge.slice(-7) === '/bridge' ? bridge.slice(0, -7) : bridge) + '/attachments/'
    for (const file of (Array.isArray(command.attachments) ? command.attachments : [])) {
      if (typeof file === 'string' && file.indexOf(folder) === 0 && file.indexOf('/../') === -1) t3Files.add(file)
      if (t3Files.size > 64) t3Files.delete(t3Files.values().next().value)
    }
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
    await runSlashCommand($, command)
    return
  }
  if (command.type === 'bash') {
    await runShellCommand($, command)
    return
  }
  if (command.type === 'config') {
    let outcome
    try { outcome = await $.config.set({ key: command.key, value: command.value }) } catch (err) { outcome = { deny: String(err) } }
    await heartbeat($)
    await emit($, 'config.result', { commandId: command.id, key: command.key, ok: !(outcome && outcome.deny), text: outcome && outcome.deny ? String(outcome.deny) : '' })
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
    if (!activeTurn) return
    const stoppedAt = Date.now()
    await $.turn.abort({ turnId: activeTurn.turnId })
    await stopCommandsLeftRunning($, stoppedAt)
    return
  }
}

// Ending a turn from a mod moves a command that is still running to the
// background, where it would finish and then wake the session to carry on.
// Esc in the terminal kills the command. Stop from T3 means the same: the
// commands the abort left running are stopped, as /tasks would stop them.
async function stopCommandsLeftRunning($, stoppedAt) {
  if (!transcriptPath) return
  const stopped = new Set()
  for (let attempt = 0; attempt < 6; attempt += 1) {
    try { await $.clock.sleep(400) } catch { return }
    let tail = ''
    try { tail = String((await $.process.run(['tail', '-c', '262144', transcriptPath], { timeoutMs: 5000 })).stdout || '') } catch { continue }
    for (const line of tail.split('\\n')) {
      if (line.indexOf('backgroundedByTurnAbort') === -1) continue
      let row = null
      try { row = JSON.parse(line) } catch { continue }
      const result = row && row.toolUseResult
      if (!result || result.backgroundedByTurnAbort !== true || typeof result.backgroundTaskId !== 'string') continue
      if (Date.parse(row.timestamp || '') < stoppedAt - 2000 || stopped.has(result.backgroundTaskId)) continue
      stopped.add(result.backgroundTaskId)
      try { await $.tool.call({ tool: 'TaskStop', task_id: result.backgroundTaskId }) } catch {}
    }
    if (stopped.size > 0 && attempt >= 2) return
  }
}

function startTimers($) {
  if (timersStarted || passive) return
  timersStarted = true
  $.clock.every(HEARTBEAT_MS, () => {
    openSessionDir($).then(() => heartbeat($)).then(() => declareT3Tools($)).then(() => reloadIfOutdated($)).catch(() => {})
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

// Claude Code's dialog for a call names the tool and its input, not the call.
// The calls left to ask are matched to it: the one with that input, else the
// oldest call of that tool still waiting (dialogs open in the order the calls
// were made). A call that already has its result is no longer waiting.
function takeAsk(tool, input) {
  for (let index = asks.length - 1; index >= 0; index -= 1) {
    if (settledCalls.has(asks[index].toolUseId)) asks.splice(index, 1)
  }
  let key = null
  try { key = tool + ':' + JSON.stringify(input) } catch {}
  let index = asks.findIndex((ask) => ask.key === key)
  if (index === -1) index = asks.findIndex((ask) => ask.tool === tool)
  return index === -1 ? null : asks.splice(index, 1)[0]
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
    await declareT3Tools($)
    return next(e)
  })

  for (const tool of T3_TOOLS) {
    on('tool.call', { tool: T3_TOOL_PREFIX + tool.name }, async ($, e) => {
      const input = {}
      for (const key of Object.keys(e || {})) {
        if (key !== 'tool' && key !== 'tool_use_id' && key !== 'consent') input[key] = e[key]
      }
      return callT3Tool($, tool.name, input)
    })
  }

  // Only the guard baseline here: whether this process bridges at all is
  // known at session.start, which may come after this event.
  on('classic.SessionStart', async ($, e, next) => {
    transcriptPath = String(e.transcript_path || transcriptPath)
    noteClassic($, e)
    await takeBaseline($)
    await heartbeat($)
    return next(e)
  })

  on('classic.UserPromptSubmit', async ($, e, next) => {
    noteClassic($, e)
    return next(e)
  })

  on('classic.PreToolUse', async ($, e, next) => {
    noteClassic($, e)
    return next(e)
  })

  on('classic.Stop', async ($, e, next) => {
    noteClassic($, e)
    return next(e)
  })

  // Each message as the terminal draws it, whole once it is finished. The
  // session's record does not keep every one, and T3 shows what it lacks.
  on('classic.MessageDisplay', async ($, e, next) => {
    try {
      if (e && typeof e.message_id === 'string' && !e.agent_id) {
        const drawn = (drawing.get(e.message_id) || '') + (typeof e.delta === 'string' ? e.delta : '')
        if (e.final === true) {
          drawing.delete(e.message_id)
          if (drawn.trim()) await emit($, 'message.display', { turnId: e.turn_id, messageId: e.message_id, text: drawn })
        } else {
          drawing.set(e.message_id, drawn)
          if (drawing.size > 32) drawing.delete(drawing.keys().next().value)
        }
      }
    } catch {}
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
    try {
      const raw = e && e.message ? e.message.content : null
      const text = typeof raw === 'string' ? raw : Array.isArray(raw) ? raw.map((block) => (block && block.type === 'text' ? String(block.text || '') : '')).join('') : ''
      const shell = SHELL_INPUT_ROW.exec(text)
      if (shell) lastShellInput = shell[1].trim()
    } catch {}
    try {
      const content = e && e.message && Array.isArray(e.message.content) ? e.message.content : []
      for (const block of content) {
        if (block && block.type === 'tool_result' && typeof block.tool_use_id === 'string') {
          settledCalls.add(block.tool_use_id)
          if (settledCalls.size > 64) settledCalls.delete(settledCalls.values().next().value)
        }
      }
    } catch {}
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
    lastTurnFromT3 = commandId !== undefined
    // A turn that follows a "!" command starts with what the command printed.
    // What the person did was type the command: the turn is named by that.
    let text = e.text
    if (typeof text === 'string' && SHELL_OUTPUT_ROW.test(text) && lastShellInput !== null) {
      text = '! ' + lastShellInput
      lastShellInput = null
    }
    await emit($, 'turn.start', { turnId: e.turnId, text: text, transcriptOffset: offset, commandId: commandId, promptUuid: promptUuid })
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
    for (const command of afterTurn.splice(0)) {
      runSlashCommand($, command).catch(() => {})
    }
    return next(e)
  })

  // A question Claude asks the person. T3 answers it with the answers
  // themselves, as the terminal dialog would return them; a bare approval
  // cannot answer a question.
  on('tool.call', { tool: 'AskUserQuestion' }, async ($, e, next) => {
    // A session a host drives answers its own questions; T3 cannot see it.
    if (passive || !e.tool_use_id || e.agentId) return next(e)
    const withTerminal = await terminalOpen($)
    // A terminal is open and T3 is nowhere near: the dialog is all there is.
    if (withTerminal && !(await t3Attached($)) && !t3Expected()) return next(e)
    const toolUseId = e.tool_use_id
    // Held for T3 from here on, and listed as waiting for as long.
    await noteWaiting($, toolUseId, { toolUseId: toolUseId, kind: 'question' })
    try {
      const request = { toolUseId: toolUseId, questions: e.questions, turnId: activeTurn ? activeTurn.turnId : undefined }
      const fromT3 = (answer) => (answer.decision === 'deny'
        ? { deny: answer.reason || 'The user declined to answer.' }
        : { result: { questions: e.questions, answers: answer.answers || {} } })
      if (withTerminal) {
        // With a terminal on the session the question is asked in both places:
        // Claude Code's own dialog runs beneath this hook, and T3 shows the same
        // question. An answer from T3 returns here while the dialog is still
        // open, which closes it; an answer in the terminal ends the wait.
        let beneath = null
        const running = next(e).then((value) => { beneath = { value: value } }, (error) => { beneath = { error: error } })
        const settled = async () => {
          await running
          if (beneath.error) throw beneath.error
          return beneath.value
        }
        const ask = () => emit($, 'question.request', Object.assign({ alsoInTerminal: true }, request))
        // The dialog is open meanwhile; T3 can be a moment behind.
        if (!(await t3InSightSoon($, () => beneath !== null))) return settled()
        await ask()
        const keepT3Told = t3Teller($, ask, () => emit($, 'question.resolved', { toolUseId: toolUseId, by: 'detached' }))
        for (;;) {
          if (beneath) {
            await emit($, 'question.resolved', { toolUseId: toolUseId, by: 'terminal' })
            return settled()
          }
          const answer = answers.get(toolUseId)
          if (answer) {
            answers.delete(toolUseId)
            // Nobody in T3 took it: it stays with the terminal.
            if (answer.decision === 'ask') return settled()
            await emit($, 'question.resolved', { toolUseId: toolUseId, by: 't3' })
            return fromT3(answer)
          }
          await keepT3Told()
          await sleepOutsideBudget($)
        }
      }
      // A background session nobody has open: asked in T3 alone. While T3 is
      // out of sight the question waits for it (the dialog would be shown to no
      // one), unless a terminal has been opened on the session meanwhile.
      // A T3 that is running but not attached reads the question's waiting from
      // session.json and attaches; it is given a moment for that.
      if (!(await t3InSightSoon($, () => false, true)) && !t3Expected()) return next(e)
      const askT3 = () => emit($, 'question.request', request)
      await askT3()
      const keepT3Told = t3Teller($, askT3, () => emit($, 'question.resolved', { toolUseId: toolUseId, by: 'detached' }))
      let unseenFor = 0
      for (;;) {
        const answer = answers.get(toolUseId)
        if (answer) {
          answers.delete(toolUseId)
          if (answer.decision === 'ask') return next(e)
          await emit($, 'question.resolved', { toolUseId: toolUseId, by: 't3' })
          return fromT3(answer)
        }
        unseenFor = (await keepT3Told()) ? 0 : unseenFor + 1
        if (unseenFor > 0 && ((await terminalOpen($)) || (!t3Expected() && unseenFor > T3_ALONE_PATIENCE_S))) return next(e)
        await sleepOutsideBudget($)
      }
    } finally {
      await noteWaiting($, toolUseId, null)
    }
  })

  // The wait runs through host calls, which do not count against the hook's
  // budget.
  on('tool.check', async ($, e, next) => {
    const verdict = await next(e)
    if (!verdict || verdict.decision !== 'ask' || !e.tool_use_id) return verdict
    // T3's display tools change nothing in the workspace; T3 allows them wherever it attaches them.
    if (isT3ReadOnlyTool(e.tool)) return { decision: 'allow', reason: 'T3 Code display tool' }
    if (isGranted(e)) return { decision: 'allow', reason: 'Allowed for this session in T3 Code' }
    // An image the person attached in T3 reaches the session as a file T3
    // saved and a line saying where: reading that file is what they asked.
    if (e.tool === 'Read' && e.input && typeof e.input.file_path === 'string' && t3Files.has(e.input.file_path)) {
      return { decision: 'allow', reason: 'Attached in T3 Code' }
    }
    // A question is asked and answered through its own hook (tool.call).
    if (e.tool === 'AskUserQuestion') return verdict
    // A subagent's call can be asked in one place only: Claude Code draws its
    // dialog for it once this mod is done with the request, not beside it
    // (measured). It is asked where the person last spoke from: in T3 when
    // the turn was sent from there, else in the terminal's own dialog.
    if (e.agentId && !lastTurnFromT3 && (await terminalOpen($))) return verdict
    // What is left to ask comes back as Claude Code's own permission request
    // (classic.PermissionRequest), after the session's mode has had its say:
    // auto mode settles most asks itself, and this check runs before it
    // (measured, with and without a terminal). The call is remembered until
    // then, whether or not T3 is in sight at this instant. Not in a session
    // a host drives (claude -p, an SDK): it has no protocol files, so T3 can
    // never see the request, and holding it only delays the host's own answer
    // (measured: a claude -p run sat a minute before its refusal).
    if (passive) return verdict
    asks.push({ toolUseId: e.tool_use_id, tool: e.tool, key: grantKey(e), at: Date.now() })
    if (asks.length > 16) asks.shift()
    return verdict
  })

  // Claude Code asks for permission. With a terminal on the session its
  // dialog is open there: T3 shows the same request, its answer closes the
  // dialog, and an answer in the terminal comes first as often (T3 then puts
  // its card away). With no terminal nobody else can answer: the request is
  // T3's alone and waits for it. If T3 goes away and comes back while the
  // request is still open, it is told again.
  on('classic.PermissionRequest', async ($, e, next) => {
    noteClassic($, e)
    if (passive) return next(e)
    const ask = takeAsk(e.tool_name, e.tool_input)
    if (!ask) return next(e)
    const toolUseId = ask.toolUseId
    const withTerminal = await terminalOpen($)
    // A terminal is open and T3 is nowhere near: the dialog is all there is.
    if (withTerminal && !(await t3Attached($)) && !t3Expected()) return next(e)
    // Held for T3 from here on, and listed as waiting for as long: a T3 that
    // is running with nothing attached to this session attaches for it.
    await noteWaiting($, toolUseId, { toolUseId: toolUseId, kind: 'approval', agentType: e.agent_id ? String(e.agent_type || 'subagent') : undefined })
    try {
      // T3 is told once it is in sight, which can take a few seconds after
      // this mod was reloaded. A dialog in the terminal takes the request if
      // T3 does not show; with no terminal it waits for T3 as long as T3 has
      // been around at all.
      if (!(await t3InSightSoon($, () => settledCalls.has(toolUseId), !withTerminal)) && (withTerminal || !t3Expected())) return next(e)
      const request = { toolUseId: toolUseId, tool: e.tool_name, input: e.tool_input, turnId: activeTurn ? activeTurn.turnId : undefined, alsoInTerminal: true, suggestions: Array.isArray(e.permission_suggestions) ? e.permission_suggestions : undefined, agentType: e.agent_id ? String(e.agent_type || 'subagent') : undefined }
      const askT3 = () => emit($, 'approval.request', request)
      await askT3()
      const keepT3Told = t3Teller($, askT3, () => emit($, 'approval.resolved', { toolUseId: toolUseId, decision: 'ask', by: 'detached' }))
      let unseenFor = 0
      for (;;) {
        const decision = decisions.get(toolUseId)
        if (decision) {
          decisions.delete(toolUseId)
          if (decision.decision === 'ask') return next(e)
          await emit($, 'approval.resolved', { toolUseId: toolUseId, decision: decision.decision, by: 't3' })
          // A refusal from T3 is the dialog's own "No": the call is refused and
          // the turn ends, with the rows the terminal's answer leaves.
          if (decision.decision !== 'allow' && decision.stopTurn === true) return { decision: { behavior: 'deny', interrupt: true } }
          if (decision.decision !== 'allow') return { decision: { behavior: 'deny', message: decision.reason || 'Denied in T3 Code' } }
          // The input goes back with the answer, as an SDK host's does: a
          // dialog that asks more than yes or no (a plan's "Ready to code?")
          // takes an allow without it as no answer at all.
          // "For this session" with Claude Code's own suggested rules is the
          // dialog's "don't ask again" option: Claude Code adds the rules.
          if (Array.isArray(decision.updatedPermissions) && decision.updatedPermissions.length > 0) {
            return { decision: { behavior: 'allow', updatedInput: e.tool_input, updatedPermissions: decision.updatedPermissions } }
          }
          if (decision.scope === 'session') grants.push({ tool: ask.tool, key: ask.key, wholeTool: decision.wholeTool === true })
          return { decision: { behavior: 'allow', updatedInput: e.tool_input } }
        }
        // Answered in the terminal. Nothing says so while the tool it allowed
        // is still running (measured: neither the chain beneath this hook nor
        // its signal moves before the tool is over); the call's result does.
        if (settledCalls.has(toolUseId)) {
          await emit($, 'approval.resolved', { toolUseId: toolUseId, decision: 'ask', by: 'terminal' })
          return next(e)
        }
        // A subagent's request has no dialog anywhere while it is held here.
        // With T3 out of sight for good it goes to the terminal's own.
        unseenFor = (await keepT3Told()) ? 0 : unseenFor + 1
        if (e.agent_id && unseenFor > T3_ATTACH_GRACE_S && (await terminalOpen($))) {
          await emit($, 'approval.resolved', { toolUseId: toolUseId, decision: 'ask', by: 'terminal' })
          return next(e)
        }
        // No terminal when it was asked, T3 out of sight: it waits for T3,
        // unless a terminal has been opened on the session meanwhile (its
        // dialog can take it), or T3 has not been around for minutes and has
        // stayed away longer than a restart or an update of it takes.
        if (!withTerminal && unseenFor > 0 && ((await terminalOpen($)) || (!t3Expected() && unseenFor > T3_ALONE_PATIENCE_S))) return next(e)
        await sleepOutsideBudget($)
      }
    } finally {
      await noteWaiting($, toolUseId, null)
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
  // Without the tool definitions the mod still bridges; it declares none.
  const tools = yield* Effect.promise(loadClaudeBridgeToolSpecs).pipe(
    Effect.catchDefect((defect) =>
      Effect.logWarning("claude-bridge.tool-specs-unavailable", { defect }).pipe(
        Effect.as([] as ReadonlyArray<ClaudeBridgeToolSpec>),
      ),
    ),
  );
  const files: ReadonlyArray<readonly [string, string]> = [
    [path.join(dir, ".claude-plugin", "marketplace.json"), MARKETPLACE_JSON + "\n"],
    [path.join(pluginDir, ".claude-plugin", "plugin.json"), PLUGIN_JSON + "\n"],
    [path.join(pluginDir, "hooks", "hooks.json"), HOOKS_JSON + "\n"],
    [path.join(pluginDir, "hooks", "register.js"), claudeBridgeRegisterJs(tools)],
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

/** The mod's source, with the tools it declares filled in. */
export const claudeBridgeRegisterJs = (tools: ReadonlyArray<ClaudeBridgeToolSpec>) =>
  REGISTER_JS.replace("__T3_TOOLS__", () => JSON.stringify(tools));
