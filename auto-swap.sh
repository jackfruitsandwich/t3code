#!/bin/bash
# Waits for T3 Code (Alpha) to quit, then swaps in the freshly built app and relaunches.
# Lives outside release/ so rebuilds don't delete it. Gives up after 6 hours.
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)" || exit 1
LOG="$ROOT/auto-swap.log"
# An explicit artifact wins; otherwise use this checkout's build version.
VERSION="$(node -e 'process.stdout.write(require(process.argv[1]).version)' "$ROOT/apps/desktop/package.json")" || exit 1
ZIP="${1:-$ROOT/release/T3-Code-$VERSION-arm64.zip}"
[ -f "$ZIP" ] || { echo "Artifact missing: $ZIP" >&2; exit 1; }
APP="/Applications/T3 Code (Alpha).app"
echo "$(date) armed, waiting for app to quit" >>"$LOG"
for i in $(seq 1 4320); do
  if ! pgrep -f "T3 Code .Alpha..app/Contents/MacOS" >/dev/null 2>&1; then
    sleep 3
    if pgrep -f "T3 Code .Alpha..app/Contents/MacOS" >/dev/null 2>&1; then continue; fi
    [ -f "$ZIP" ] || { echo "$(date) zip missing, aborting" >>"$LOG"; exit 1; }
    mv "$APP" "$HOME/.Trash/T3 Code (Alpha)-pre-forks-ui-$(date +%H%M%S).app" 2>>"$LOG"
    ditto -x -k "$ZIP" /Applications/ >>"$LOG" 2>&1
    sleep 2
    open -a "T3 Code (Alpha)" >>"$LOG" 2>&1 || { sleep 3; open -a "T3 Code (Alpha)" >>"$LOG" 2>&1; }
    echo "$(date) swap complete" >>"$LOG"
    exit 0
  fi
  sleep 5
done
echo "$(date) timed out after 6h without a quit, disarming" >>"$LOG"
