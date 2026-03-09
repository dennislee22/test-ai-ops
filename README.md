<h1><img src="web/static/chatbot-icon.svg" width="30" height="30"> <img src="web/static/k8s-logo.svg" width="30" height="30"> Cloudera ECS AI Ops Chatbot</h1>

An air-gapped Kubernetes operations chatbot for Cloudera ECS, powered by a local LLM (Qwen3-8B), LangGraph agentic loop, and ChromaDB RAG.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Browser (UI)                             │
│                   Single-file index.html                        │
│         SSE streaming · AGENT STATUS loop timeline              │
└──────────────────────────┬──────────────────────────────────────┘
                           │ HTTP / SSE
┌──────────────────────────▼──────────────────────────────────────┐
│                    FastAPI  (app.py)                             │
│   POST /chat/stream   ·   GET /api/*   ·   POST /api/tool       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│              LangGraph  —  Agentic ReAct Loop                   │
│                                                                 │
│   ┌─────────────┐   tool_calls?  ┌──────────────────────────┐  │
│   │  llm_node   │ ─────yes──────▶│       tool_node          │  │
│   │             │                │  executes K8s API calls  │  │
│   │  Qwen3-8B   │ ◀──results─────│  enforces decode toggle  │  │
│   │             │                │  bypass for list queries │  │
│   │ reads Tool  │                └──────────────────────────┘  │
│   │ Selection   │                                               │
│   │ Guide from  │   no tool_calls → END (final answer)         │
│   │ sys prompt  │                                               │
│   └─────────────┘                                               │
│                                                                 │
│   itr cap: 6   ·   fallback routing only on complete failure    │
└──────────────┬───────────────────────────┬──────────────────────┘
               │                           │
┌──────────────▼──────────┐   ┌────────────▼────────────────────┐
│   K8s Tools (22 total)  │   │   RAG  (ChromaDB + nomic-embed) │
│   kubernetes Python SDK │   │   runbooks · known-issues · SOPs│
│   read-only by default  │   │   rag_search tool               │
│                         │   └─────────────────────────────────┘
│  get_pod_status         │
│  describe_pod           │   ┌─────────────────────────────────┐
│  get_node_health        │   │   config/system_prompt.txt      │
│  get_events             │   │   · Tool Selection Guide        │
│  get_deployment_status  │   │   · Multi-hop Reasoning rules   │
│  get_pvc_status         │   │   · Never Narrate — Always Act  │
│  get_secrets            │   │   · Site-specific CUSTOM_RULES  │
│  describe_pod  …        │   └─────────────────────────────────┘
└─────────────────────────┘

Multi-hop example — "why is my vault pod crashing?":
  Loop 1 → get_pod_status          sees: OOMKilled, 28 restarts
  Loop 2 → describe_pod            sees: memory limit 256Mi
         + get_resource_quotas     sees: namespace quota 512Mi total
  Loop 3 → get_events              sees: repeated OOMKilled warnings
  Loop 4 → synthesise final answer (no tool calls → END)
```

### How it works

Each user query runs through an **agentic ReAct loop** driven entirely by the LLM:

```
User query
    ↓
Loop itr=1: LLM reads Tool Selection Guide → decides which tools to call
    ↓
Tool execution (K8s API calls)
    ↓
Loop itr=2: LLM sees results → decides: call more tools OR synthesise answer
    ↓  (multi-hop: repeats if more data needed, e.g. OOMKilled → check quotas → check events)
Final answer streamed to UI
```

The LLM controls how many loop iterations to take. For a broad health check it calls 5 tools at once. For a crash diagnosis it chains calls — pod status → describe pod → resource quotas → events — stopping when it has enough to answer. The iteration count is visible live in the AGENT STATUS panel.

---

## Project Structure

```
k8s-ai-ops-agentic/
├── app.py                    # FastAPI server + LangGraph agent
│
├── config/
│   └── system_prompt.txt     # LLM persona, Tool Selection Guide, multi-hop rules
│
├── agent/
│   ├── routing.py            # Namespace resolution + emergency fallback routing
│   └── bypass.py             # LLM synthesis bypass for simple list queries
│
├── tools/
│   └── tools_k8s.py          # All K8s tool implementations + TOOL_REGISTRY
│
├── web/
│   ├── index.html            # Single-file UI (served by FastAPI)
│   └── static/
│       ├── k8s-logo.svg
│       ├── rancher-logo.svg
│       ├── chatbot-icon.svg
│       └── ecs-ai-ops.gif
│
├── docs/                     # Ingested into ChromaDB for RAG
├── requirements.txt
├── chromadb/                 # Auto-created on first ingest
└── logs/
    └── app.log
```

---

## Stack

| Layer | Technology | Notes |
|---|---|---|
| LLM | HuggingFace Transformers | `Qwen/Qwen3-8B` — excellent tool-calling, works on CPU and GPU |
| Agent | LangGraph | ReAct loop: LLM selects tools → executes → observes → repeats or answers |
| Embeddings | SentenceTransformers | `nomic-ai/nomic-embed-text-v1.5` (local) |
| Vector DB | ChromaDB (embedded) | Zero external dependencies |
| K8s tools | kubernetes Python client | Typed, read-only tools — no kubectl binary needed |
| API | FastAPI | REST + SSE streaming |
| Frontend | Single-file HTML/JS | Served by FastAPI at `/` |

---

## K8s Tools

| Category | Tools |
|---|---|
| Pods | `get_pod_status`, `get_pod_logs`, `describe_pod` |
| Nodes | `get_node_health` |
| Events | `get_events` |
| Workloads | `get_deployment_status`, `get_daemonset_status`, `get_statefulset_status`, `get_job_status`, `get_hpa_status` |
| Storage | `get_pvc_status`, `get_persistent_volumes` |
| Networking | `get_service_status`, `get_ingress_status` |
| Config | `get_configmap_list`, `get_secrets`, `get_resource_quotas`, `get_limit_ranges` |
| RBAC | `get_service_accounts`, `get_cluster_role_bindings` |
| Namespaces | `get_namespace_status` |
| kubectl exec | `kubectl_exec` — parses kubectl-style commands via K8s Python API (no binary needed) |
| RAG | `rag_search` — searches ingested runbooks and known-issues docs |

---

## Example Queries

- Is the Vault doing ok?
- Is my cluster having any issues?
- Which pod is OOMKilled recently and what could be the cause?
- Which node has GPUs available and in use?
- What is the resource limit for cdp/dp-cadence-worker pod?
- List all PVCs not bound across all namespaces
- Show warning events in cdp namespace
- Which secret in cdp has user credentials?
- How many nodes are in the cluster?

---

## Quick Start

### Prerequisites

- **Python 3.12** is required.

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

For NVIDIA GPU:
```bash
pip install torch --index-url https://download.pytorch.org/whl/cu121
```

### 2. Configure environment

Create an `env` file next to `app.py`:

```ini
KUBECONFIG_PATH=~/kubeconfig

LLM_MODEL=Qwen/Qwen3-8B
EMBED_MODEL=nomic-ai/nomic-embed-text-v1.5

NUM_GPU=1           # 0 = CPU only
LOG_LEVEL=DEBUG     # DEBUG shows full agentic loop: tool selection, raw LLM output, multi-hop decisions
CHROMA_DIR=./chromadb

KUBECTL_ALLOW_WRITES=false
KUBECTL_MAX_CHARS=20000
MAX_NEW_TOKENS=4096
LLM_TIMEOUT=300
```

### 3. Ingest documents

```bash
python3 app.py --ingest ./docs
python3 app.py --ingest ./docs --force   # re-ingest all
```

### 4. Start the server

```bash
python3 app.py                                          # http://0.0.0.0:9000
python3 app.py --port 9000 --host 0.0.0.0
python3 app.py --model-dir /models/Qwen3-8B             # local model path
```

Open `http://localhost:9000` in your browser.

---

## Customising the System Prompt

The LLM's tool selection behaviour and persona all live in **`config/system_prompt.txt`**. This is the primary place to tune the assistant without touching Python code.

Key sections in the prompt:
- **Tool Selection Guide** — tells the LLM which tools to call for each query type
- **CRITICAL: Never Narrate** — prevents the LLM from describing what it *would* do instead of doing it

Hot-reload after editing (no restart needed):
```bash
curl -s -X POST http://localhost:9000/api/reload-prompt
```

---

## REST API

```bash
# Ask the AI
curl -s -X POST http://localhost:9000/api/ask \
     -H 'Content-Type: application/json' \
     -d '{"q":"are there any failing pods?"}'

# Call a tool directly
curl -s -X POST http://localhost:9000/api/tool \
     -H 'Content-Type: application/json' \
     -d '{"name":"get_node_health","args":{}}'

# List all tools
curl -s http://localhost:9000/api/tools

# System metrics
curl -s http://localhost:9000/api/system
```

Interactive docs: **[/docs](http://localhost:9000/docs)** (Swagger) · **[/redoc](http://localhost:9000/redoc)**

---

## Runtime Tuning

| Setting | Controls | Default |
|---|---|---|
| `KUBECTL_MAX_CHARS` | Characters of cluster data the LLM reads per tool call | 20000 |
| `MAX_NEW_TOKENS` | Tokens the LLM writes per response (~4 chars/token) | 4096 |
| `LLM_TIMEOUT` | Hard timeout per request (seconds) | 300 |

Adjust at runtime via ⚙ Settings → LLM Input/Output, or via API:
```bash
curl -s -X POST http://localhost:9000/api/config \
     -H 'Content-Type: application/json' \
     -d '{"kubectl_max_chars": 50000, "max_new_tokens": 6144}'
```

---

## Hardware Sizing

| Setup | RAM | VRAM | Speed |
|---|---|---|---|
| CPU only, 8-core | 32 GB | — | ~6–10 tok/s |
| GPU (recommended) — **Qwen3-8B** (tested and recommended for its tool invocation capability) | 32 GB | ~20 GB | ~30–60 tok/s |

Qwen3-8B in bfloat16 uses ~16–20 GB VRAM. In CPU-only mode the model loads into system RAM.

---

## Security Notes

- All typed K8s tools are **read-only** by design.
- `kubectl_exec` is **read-only by default**. Set `KUBECTL_ALLOW_WRITES=true` to enable write ops.
- Secret values are hidden by default. Toggle in ⚙ Settings → Security. The toggle state persists in browser localStorage per user.
- Never expose this service publicly — it has direct cluster read access.
- Restrict the env file: `chmod 600 env`
