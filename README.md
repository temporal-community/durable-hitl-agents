# Drone Fleet Demo — Google ADK + Temporal

A 2-3 minute demo showing AI agent orchestration (Google ADK) with durable
execution (Temporal). Two delivery drones fly packages across San Francisco.
Kill the worker mid-flight — Temporal recovers and the delivery completes.

## Architecture

```
┌──────────────────────────────────┐
│         Temporal Server          │
│   (workflow state + replay)      │
└──────────┬───────────────────────┘
           │
┌──────────▼───────────────────────┐
│       Temporal Worker            │
│  ┌─────────────────────────────┐ │
│  │  FleetDispatchWorkflow      │ │
│  │    └─ DeliveryMissionWF x2  │ │
│  │        ├─ assign_drone()    │ │
│  │        ├─ navigate_to()  ←──┼─┼── heartbeats every step
│  │        ├─ pickup_package()  │ │
│  │        ├─ navigate_to()  ←──┼─┼── THIS is where we kill it
│  │        └─ deliver_package() │ │
│  └─────────────────────────────┘ │
│  ┌─────────────────────────────┐ │
│  │  ADK Agents                 │ │
│  │  ├─ DispatchAgent (LLM)     │ │
│  │  └─ DroneAgent (LLM)        │ │
│  │      model=TemporalModel()  │ │
│  └─────────────────────────────┘ │
└──────────────────────────────────┘
           │
┌──────────▼───────────────────────┐
│     FastAPI + WebSocket          │
│     └─ Frontend (Leaflet map)    │
└──────────────────────────────────┘
```

## Prerequisites

- Python 3.11+
- Temporal CLI (`brew install temporal` or https://docs.temporal.io/cli)
- Google Gemini API key (for ADK agents)

## Quick Start

### 1. Start Temporal dev server

```bash
temporal server start-dev
```

### 2. Install dependencies

```bash
cd drone-fleet-demo
pip install -r requirements.txt
```

### 3. Set your Gemini API key

```bash
export GOOGLE_API_KEY="your-key-here"
```

### 4. Run the demo server

```bash
python server.py
```

### 5. Open the dashboard

Navigate to http://localhost:8080

## Demo Script (2-3 minutes)

| Time | Action | What Audience Sees |
|------|--------|--------------------|
| 0:00 | Click "Launch Deliveries" | Two drones leave warehouse, moving across SF map |
| 0:30 | Drone 1 completes | First delivery card turns green. Baseline. |
| 0:50 | Click "Kill Worker" mid-flight on Drone 2 | RED OVERLAY. Drone 2 freezes. Worker status: OFFLINE |
| 1:00 | Show Temporal UI (localhost:8233) | Workflow running, activity timed out, pending retry |
| 1:30 | Click "Restart Worker" | Drone 2 resumes from exact position, completes delivery |
| 2:00 | Wrap up | "Now imagine 10,000 drones. Every crash self-heals." |

## Key Files

| File | What it does |
|------|-------------|
| `models.py` | Data models for drones, missions, coordinates |
| `simulation.py` | In-memory fleet state (positions, statuses) |
| `activities.py` | Temporal activities — the retryable units of work |
| `workflows.py` | Temporal workflows — orchestrate the delivery sequence |
| `agents.py` | ADK agent definitions (dispatch + drone) |
| `worker.py` | Temporal worker setup |
| `server.py` | FastAPI server — APIs, WebSocket, serves frontend |
| `frontend/index.html` | Leaflet map dashboard with dark theme |

## How Failure/Recovery Works

1. `navigate_to` activity heartbeats every 0.8s as the drone moves
2. Heartbeat timeout is 5s — if no heartbeat for 5s, Temporal marks it failed
3. Retry policy: up to 10 attempts with 2s initial backoff
4. When worker restarts, Temporal replays the workflow event history
5. Completed activities are skipped (instant), failed activity is retried
6. Drone picks up from its last known position, not from the warehouse


NEXT
Framing: courier app 
Two agents: Dispatch (assignment reasoning) + Exception (disruption handling)
Visual: Couriers moving on SF map, still the core demo
Kill/recovery: Fleet ops brain goes down, couriers lose their intelligence layer, Temporal recovers everything
