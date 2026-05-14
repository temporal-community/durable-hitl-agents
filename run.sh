#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Load .env if present
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "Error: uv is required. Install with 'brew install uv' or see https://docs.astral.sh/uv/" >&2
    exit 1
fi

echo "Syncing dependencies with uv..."
uv sync --all-extras

cleanup() {
    echo ""
    echo "Shutting down..."
    kill "$TEMPORAL_PID" "$WORKER_PID" "$SERVER_PID" 2>/dev/null
    wait "$TEMPORAL_PID" "$WORKER_PID" "$SERVER_PID" 2>/dev/null
    echo "Done."
}
trap cleanup SIGINT SIGTERM

echo "Cleaning up state..."
rm -f fleet_state.db fleet_state.db-wal fleet_state.db-shm

echo "Cleaning up any existing processes..."
pkill -f "temporal server" 2>/dev/null || true
pkill -f "agent_fleet.server" 2>/dev/null || true
pkill -f "agent_fleet.worker" 2>/dev/null || true
pkill -f "uvicorn" 2>/dev/null || true
lsof -ti:8080 | xargs kill -9 2>/dev/null || true
sleep 1

echo "Starting Temporal dev server..."
temporal server start-dev &
TEMPORAL_PID=$!

echo "Waiting for Temporal to be ready..."
until temporal operator cluster health 2>/dev/null | grep -q "SERVING"; do
    sleep 0.5
done

echo "Starting workers..."
uv run python -m agent_fleet.worker &
WORKER_PID=$!
sleep 2

echo "Starting server..."
uv run python -m agent_fleet.server &
SERVER_PID=$!

echo ""
echo "  App:      http://localhost:8080"
echo "  Temporal: http://localhost:8233"
echo ""
echo "Press Ctrl+C to stop."

wait "$WORKER_PID" "$SERVER_PID"
