#!/usr/bin/env bash
# Run on 185 after copying this patch and script into /home/s30083118/jdredis/.
# Builds a separate server binary; baseline Redis source/binary stay untouched.
set -euo pipefail
ROOT="${ROOT:-/home/s30083118/jdredis}"
SRC="${SRC:-$ROOT/redis-7.2.5}"
BUILD="$ROOT/redis-7.2.5-opt4"
PATCH="$ROOT/redis_7_2_5_guard.patch"
OUT="$ROOT/redis-server-opt4"
test -f "$SRC/src/t_string.c" || { echo "[!!] source missing: $SRC"; exit 1; }
test -f "$PATCH" || { echo "[!!] patch missing: $PATCH"; exit 1; }
if [ ! -d "$BUILD" ]; then
    cp -a "$SRC" "$BUILD"
fi
if patch --dry-run -d "$BUILD" -p1 < "$PATCH" >/dev/null; then
    patch -d "$BUILD" -p1 < "$PATCH"
elif patch -R --dry-run -d "$BUILD" -p1 < "$PATCH" >/dev/null; then
    echo "[i] patch already applied"
else
    echo "[!!] source differs from Redis 7.2.5 patch context; stop"; exit 1
fi
make -C "$BUILD" -j "$(nproc)" BUILD_TLS=no
install -m 0755 "$BUILD/src/redis-server" "$OUT"
echo "[ok] separate patched Redis: $OUT"
