#!/bin/bash
# Waits for T3 Code (Alpha) to quit, then swaps in the freshly built app and relaunches.
# Lives outside release/ so rebuilds don't delete it. Gives up after 6 hours.
LOG="$HOME/hack/t3code/auto-swap.log"
ZIP="$HOME/hack/t3code/release/T3-Code-0.0.30-arm64.zip"
APP="/Applications/T3 Code (Alpha).app"
echo "$(date) armed, waiting for app to quit" >>"$LOG"
for i in $(seq 1 4320); do
  if ! pgrep -f "T3 Code .Alpha..app/Contents/MacOS" >/dev/null 2>&1; then
    sleep 3
    if pgrep -f "T3 Code .Alpha..app/Contents/MacOS" >/dev/null 2>&1; then continue; fi
    [ -f "$ZIP" ] || { echo "$(date) zip missing, aborting" >>"$LOG"; exit 1; }
    mv "$APP" "$HOME/.Trash/T3 Code (Alpha)-pre-chats-fix-$(date +%H%M%S).app" 2>>"$LOG"
    ditto -x -k "$ZIP" /Applications/ >>"$LOG" 2>&1
    sleep 2
    open -a "T3 Code (Alpha)" >>"$LOG" 2>&1 || { sleep 3; open -a "T3 Code (Alpha)" >>"$LOG" 2>&1; }
    echo "$(date) swap complete" >>"$LOG"
    exit 0
  fi
  sleep 5
done
echo "$(date) timed out after 6h without a quit, disarming" >>"$LOG"
