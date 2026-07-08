#!/bin/bash
# Runs the live-menu pipeline and appends to the log ONLY on a real failure.
# update_menu_inventory_library.py exits 1 on "no changes" by design (see
# CLAUDE.md) — that is not an error and must stay silent, so it's handled
# as a special case rather than treated as a generic non-zero exit.
set -uo pipefail

LOG=/home/ceres/logs/update_menu.log
PY=/home/ceres/ceresenv/bin/python
cd /home/ceres/live-menu || exit 1

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

{
    echo "+ git pull --ff-only origin main"
    git pull --ff-only origin main
} >>"$tmp" 2>&1
pull_status=$?

if [ $pull_status -ne 0 ]; then
    { echo "=== $(date '+%Y-%m-%d %H:%M:%S %Z') — FAILURE (git pull) ==="; cat "$tmp"; echo; } >>"$LOG"
    exit $pull_status
fi

{
    echo "+ update_menu_inventory_library.py"
    "$PY" update_menu_inventory_library.py
} >>"$tmp" 2>&1
update_status=$?

if [ $update_status -eq 1 ] && grep -q "NO CHANGES" "$tmp"; then
    exit 0
fi

if [ $update_status -ne 0 ]; then
    { echo "=== $(date '+%Y-%m-%d %H:%M:%S %Z') — FAILURE (update_menu_inventory_library.py) ==="; cat "$tmp"; echo; } >>"$LOG"
    exit $update_status
fi

{
    echo "+ build_menu_v2.py"
    "$PY" build_menu_v2.py
} >>"$tmp" 2>&1
build_status=$?

if [ $build_status -ne 0 ]; then
    { echo "=== $(date '+%Y-%m-%d %H:%M:%S %Z') — FAILURE (build_menu_v2.py) ==="; cat "$tmp"; echo; } >>"$LOG"
    exit $build_status
fi

exit 0
