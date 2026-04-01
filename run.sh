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

# Create venv if it doesn't exist
if [ ! -d venv ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    echo "Installing dependencies..."
    pip install -e ".[dev]"
else
    source venv/bin/activate
fi

cleanup() {
    echo ""
    echo "Shutting down..."
    kill "$TEMPORAL_PID" "$SERVER_PID" 2>/dev/null
    wait "$TEMPORAL_PID" "$SERVER_PID" 2>/dev/null
    echo "Done."
}
trap cleanup SIGINT SIGTERM

echo "Cleaning up any existing processes..."
pkill -f "temporal server" 2>/dev/null || true
pkill -f "agent_fleet.server" 2>/dev/null || true
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

echo "Starting agent fleet server..."
python3 -m agent_fleet.server &
SERVER_PID=$!

echo ""
echo "  App:      http://localhost:8080"
echo "  Temporal: http://localhost:8233"
echo ""
echo "Press Ctrl+C to stop."

wait "$SERVER_PID"
