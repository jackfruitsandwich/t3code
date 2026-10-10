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
const T3_LIVE_MS = 12000
const PROMPT_MATCH_WINDOW_MS = 20000
// T3's own tools, declared here so the session has them whatever started it;
// each call is answered from T3's tool endpoint (see t3.json).
const T3_TOOLS = __T3_TOOLS__
const T3_TOOL_PREFIX = 'mcp__${CLAUDE_BRIDGE_PLUGIN_NAME}__'
const MCP_PROTOCOL = '2025-06-18'
const T3_TOOL_ATTACH_WAIT = 20
const SHELL_COMMAND_TIMEOUT_MS = 120000
const SHELL_INPUT_ROW = /^\\s*<bash-input>([\\s\\S]*)<\\/bash-input>\\s*$/
const SHELL_OUTPUT_ROW = /^\\s*<bash-stdout>/
const COMMAND_TURN_WAIT_MS = 1500
const COMMAND_PANEL_WAIT_MS = 6000
// Built-in commands that, typed with no argument, open a panel in the terminal
// (observed on Claude Code 2.1.296 by running each one). A panel can only be
// closed by the person at that terminal and everything queued behind it waits,
// so these are not run from T3 without an argument. Commands found to behave
// this way later are remembered in the plugin store.
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

// What the classic hook payloads say about the session right now. Each names
// the session's record file, which a mod loaded mid-session (/reload-plugins)
// is told by nothing else; it is kept so the next load starts knowing it.
function noteClassic($, e) {
  if (!e) return
  if (typeof e.permission_mode === 'string') permissionMode = e.permission_mode
  if (e.effort && typeof e.effort.level === 'string') effort = e.effort.level
  if (typeof e.transcript_path === 'string' && e.transcript_path && e.transcript_path !== transcriptPath) {
    transcriptPath = e.transcript_path
    if (sid) $.store.set('transcript:' + sid, transcriptPath).catch(() => {})
  }
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
  if (!transcriptPath) {
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
  if (!command.args && (await opensPanel($, command.name))) {
    await report({ ok: true, text: 'Not run: ' + label + ' opens a panel in the Claude Code terminal, which only that terminal can show and close. Run it there, or give it an argument here (for example /model sonnet, /effort high, /goal <condition>).' })
    return
  }
  // Mid-conversation, /model <name> asks the terminal to confirm. The same
  // change through the settings row applies at once, as picking it in /config does.
  if (command.name === 'model' && command.args) {
    const choice = await modelChoice($, command.args)
    if (choice !== null) {
      let outcome
      try { outcome = await $.config.set({ key: 'model', value: choice }) } catch (err) { outcome = { deny: String(err) } }
      let model = choice
      try { model = await $.session.model() } catch {}
      await heartbeat($)
      await report(outcome && outcome.deny ? { ok: false, text: String(outcome.deny) } : { ok: true, text: 'Set model to ' + model })
      return
    }
  }
  pendingCommand = { id: command.id, text: label, promptUuid: command.promptUuid, at: Date.now() }
  let settled = false
  const mine = () => pendingCommand !== null && pendingCommand.id === command.id
  $.clock.after(COMMAND_PANEL_WAIT_MS, () => {
    if (settled || !mine()) return
    settled = true
    pendingCommand = null
    $.store.set('panel:' + MOD_VERSION + ':' + command.name, true).catch(() => {})
    report({
      ok: false,
      waitingInTerminal: true,
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

// "!" in the terminal runs a shell command with no permission prompt, writes
// the command and what it printed into the record, and hands that output to
// the model, which replies. The same from T3: run here, recorded the same
// way, and what it printed submitted as the turn's prompt.
async function runShellCommand($, command) {
  const report = (data) => emit($, 'command.result', Object.assign({ commandId: command.id, promptUuid: command.promptUuid, startedTurn: false }, data))
  let shell = '/bin/sh'
  try { shell = (await $.env.get('SHELL')) || shell } catch {}
  let outcome
  try {
    outcome = await $.process.run([shell, '-c', command.command], { timeoutMs: SHELL_COMMAND_TIMEOUT_MS })
  } catch (err) {
    await report({ ok: false, text: String((err && err.message) || err) })
    return
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
    return next(e)
  })

  // A question Claude asks the person. T3 answers it with the answers
  // themselves, as the terminal dialog would return them; a bare approval
  // cannot answer a question.
  on('tool.call', { tool: 'AskUserQuestion' }, async ($, e, next) => {
    if (!e.tool_use_id || e.agentId || !(await t3Attached($))) return next(e)
    const toolUseId = e.tool_use_id
    const request = { toolUseId: toolUseId, questions: e.questions, turnId: activeTurn ? activeTurn.turnId : undefined }
    const fromT3 = (answer) => (answer.decision === 'deny'
      ? { deny: answer.reason || 'The user declined to answer.' }
      : { result: { questions: e.questions, answers: answer.answers || {} } })
    if (terminalPresent()) {
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
      await emit($, 'question.request', Object.assign({ alsoInTerminal: true }, request))
      let told = true
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
        const attachedNow = await t3Attached($)
        if (!attachedNow && told) {
          told = false
          await emit($, 'question.resolved', { toolUseId: toolUseId, by: 'detached' })
        } else if (attachedNow && !told) {
          told = true
          await emit($, 'question.request', Object.assign({ alsoInTerminal: true }, request))
        }
        await sleepOutsideBudget($)
      }
    }
    // A background session nobody has open: asked in T3 alone.
    await emit($, 'question.request', request)
    for (;;) {
      const answer = answers.get(toolUseId)
      if (answer) {
        answers.delete(toolUseId)
        if (answer.decision === 'ask') return next(e)
        await emit($, 'question.resolved', { toolUseId: toolUseId, by: 't3' })
        return fromT3(answer)
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
    // T3's display tools change nothing in the workspace; T3 allows them wherever it attaches them.
    if (isT3ReadOnlyTool(e.tool)) return { decision: 'allow', reason: 'T3 Code display tool' }
    if (isGranted(e)) return { decision: 'allow', reason: 'Allowed for this session in T3 Code' }
    if (!(await t3Attached($))) return verdict
    // A question is asked and answered through its own hook (tool.call).
    if (e.tool === 'AskUserQuestion') return verdict
    // With a terminal on the session, Claude Code's own dialog opens there
    // and T3 shows the same request; whichever is answered first settles it
    // (see classic.PermissionRequest). The call is remembered until then.
    if (terminalPresent()) {
      asks.push({ toolUseId: e.tool_use_id, tool: e.tool, key: grantKey(e), at: Date.now() })
      if (asks.length > 16) asks.shift()
      return verdict
    }
    // A background session nobody has open: its dialog would be shown to no
    // one, so the request is held for T3 alone.
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

  // Claude Code is opening its permission dialog in the terminal. T3 shows
  // the same request, and its answer closes the dialog; an answer in the
  // terminal comes first as often, and T3 then puts its card away. If T3
  // goes away and comes back while the dialog is still open, it is told again.
  on('classic.PermissionRequest', async ($, e, next) => {
    noteClassic($, e)
    const ask = takeAsk(e.tool_name, e.tool_input)
    if (!ask || !(await t3Attached($))) return next(e)
    const toolUseId = ask.toolUseId
    const request = { toolUseId: toolUseId, tool: e.tool_name, input: e.tool_input, turnId: activeTurn ? activeTurn.turnId : undefined, alsoInTerminal: true }
    await emit($, 'approval.request', request)
    let told = true
    for (;;) {
      const decision = decisions.get(toolUseId)
      if (decision) {
        decisions.delete(toolUseId)
        if (decision.decision === 'ask') return next(e)
        await emit($, 'approval.resolved', { toolUseId: toolUseId, decision: decision.decision, by: 't3' })
        if (decision.decision !== 'allow') return { decision: { behavior: 'deny', message: decision.reason || 'Denied in T3 Code' } }
        if (decision.scope === 'session') grants.push({ tool: ask.tool, key: ask.key, wholeTool: decision.wholeTool === true })
        // The input goes back with the answer, as an SDK host's does: a
        // dialog that asks more than yes or no (a plan's "Ready to code?")
        // takes an allow without it as no answer at all.
        return { decision: { behavior: 'allow', updatedInput: e.tool_input } }
      }
      // Answered in the terminal. Nothing says so while the tool it allowed
      // is still running (measured: neither the chain beneath this hook nor
      // its signal moves before the tool is over); the call's result does.
      if (settledCalls.has(toolUseId)) {
        await emit($, 'approval.resolved', { toolUseId: toolUseId, decision: 'ask', by: 'terminal' })
        return next(e)
      }
      const attachedNow = await t3Attached($)
      if (!attachedNow && told) {
        told = false
        await emit($, 'approval.resolved', { toolUseId: toolUseId, decision: 'ask', by: 'detached' })
      } else if (attachedNow && !told) {
        told = true
        await emit($, 'approval.request', request)
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
