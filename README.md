<h1><img src="web/static/chatbot-icon.svg" width="30" height="30"> <img src="web/static/k8s-logo.svg" width="30" height="30"> Cloudera ECS AI Ops Chatbot</h1>

An air-gapped Kubernetes operations chatbot for Cloudera ECS, powered by a local LLM (Qwen3-8B), LangGraph agentic loop, and ChromaDB RAG.

---

## Table of Contents

- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Stack](#stack)
- [K8s Tools](#k8s-tools)
- [Example Queries](#example-queries)
- [Quick Start](#quick-start)
- [Customising the System Prompt](#customising-the-system-prompt)
- [REST API](#rest-api)
- [Runtime Tuning](#runtime-tuning)
- [Hardware Sizing](#hardware-sizing)
- [Security Notes](#security-notes)

---

## Architecture

![ECS AI Ops Architecture](web/static/ecs-ai-arch.gif)

---

## Project Structure

```
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
│       └── ecs-ai-arch.gif
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
| K8s tools | kubernetes Python client | 24 typed, read-only tools — no kubectl binary needed |
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
| Namespaces | `get_namespace_status`, `get_namespace_resource_summary` |
| Database | `exec_db_query` — read-only SQL inside DB pods (MySQL/MariaDB/PostgreSQL) |
| RAG | `rag_search` — searches ingested runbooks and known-issues docs |

---

## Example Queries

- is the vault pod doing ok?
- which namespace has the least pods?
- list all namespaces with total pods
- calculate CPU requests for all pods in longhorn-system namespace
- which node has a GPU available and in use?
- get tables in db-0 of cmlwb1 namespace
- explain how you access the database of a pod to get the table names, don't run it, just explain

---

## Quick Start

### Prerequisites

- **Python 3.12** is required.
- Download the LLM and embedding models before starting (air-gapped environment):

```bash
# Qwen3-8B
git clone https://huggingface.co/Qwen/Qwen3-8B /models/Qwen3-8B

# SentenceTransformers embedding model
git clone https://huggingface.co/nomic-ai/nomic-embed-text-v1.5 /models/nomic-embed-text-v1.5
```

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

LLM_MODEL=/models/Qwen3-8B
EMBED_MODEL=/models/nomic-embed-text-v1.5

NUM_GPU=1           # 0 = CPU only
LOG_LEVEL=DEBUG     # DEBUG shows full agentic loop: tool selection, raw LLM output, multi-hop decisions
CHROMA_DIR=./chromadb

KUBECTL_ALLOW_WRITES=false
ALLOW_DB_EXEC=true
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

Interactive docs: **[/docs](http://localhost:9000/docs)** (Swagger) · **[/redoc](http://localhost:9000/redoc)**

| Method | Path | Description |
|---|---|---|
| GET | `/api/info` | Model, GPU, cluster status |
| POST | `/api/ask` | Ask the AI (blocking) |
| POST | `/chat/stream` | Ask the AI (SSE streaming) |
| POST | `/api/tool` | Call a K8s tool directly |
| GET | `/api/tools` | List all registered tools and their signatures |
| GET | `/api/pods` | Pod health summary (optional: `?ns=cdp`) |
| GET | `/api/pods/raw` | kubectl-style pod table (optional: `?ns=cdp`) |
| GET | `/api/nodes` | Node health and GPU summary |
| GET | `/api/events` | Cluster events (optional: `?ns=X&warn=1`) |
| GET | `/api/deployments` | Deployment status (optional: `?ns=X`) |
| GET | `/api/pvcs` | PVC / storage status (optional: `?ns=X`) |
| GET | `/api/namespaces` | All namespaces and their status |
| GET | `/api/rag/stats` | ChromaDB document chunk statistics |
| GET | `/api/system` | Live CPU / RAM / GPU metrics |
| POST | `/api/kubeconfig` | Apply a new kubeconfig |
| POST | `/api/ingest/upload` | Upload docs to ChromaDB |
| POST | `/api/reload-prompt` | Hot-reload system_prompt.txt |
| GET | `/api/prompt` | Read current system prompt |
| PUT | `/api/prompt` | Update system prompt |
| GET | `/api/config` | Read runtime config |
| POST | `/api/config` | Update runtime config |

```bash
# Ask the AI (SSE streaming — recommended, avoids proxy timeout)
curl -s --no-buffer -X POST http://localhost:9000/chat/stream \
     -H 'Content-Type: application/json' \
     -H 'Accept: text/event-stream' \
     -d '{"message":"is the vault pod doing ok?","history":[],"decode_secrets":false}'

# Ask the AI (blocking)
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

> ⚠️ `/api/ask` is a blocking request. For long-running CPU queries, use `/chat/stream` instead to avoid nginx proxy read timeout.

---

## Runtime Tuning

| Setting | Controls | Default |
|---|---|---|
| `KUBECTL_MAX_CHARS` | Characters of cluster data the LLM reads per tool call | 20000 |
| `MAX_NEW_TOKENS` | Tokens the LLM writes per response (~4 chars/token) | 4096 |
| `LLM_TIMEOUT` | Hard timeout per request (seconds) | 300s GPU / 900s CPU |

Adjust at runtime via ⚙ Settings → LLM Input/Output, or via API:
```bash
curl -s -X POST http://localhost:9000/api/config \
     -H 'Content-Type: application/json' \
     -d '{"kubectl_max_chars": 50000, "max_new_tokens": 6144}'
```

---

## Hardware Sizing

| Inference | RAM | VRAM |
|---|---|---|
| CPU only, min. 28 cores | min. 64 GB | — |
| GPU, **Qwen3-8B** (recommended) | 32 GB | ~20 GB |

> ⚠️ CPU inference takes **several minutes** to generate a response per query. A GPU is strongly recommended for practical use.

Qwen3-8B in bfloat16 uses ~16–20 GB VRAM. In CPU-only mode the model loads into system RAM.

---

## Security Notes

- All typed K8s tools are **read-only** by design.
- `kubectl_exec` is **read-only by default**. Set `KUBECTL_ALLOW_WRITES=true` to enable write ops.
- `exec_db_query` runs read-only SQL (SELECT/SHOW/DESCRIBE) inside DB pods. Write SQL (INSERT/UPDATE/DELETE/DROP) is always blocked. Disable entirely with `ALLOW_DB_EXEC=false`.
- Secret values are hidden by default. Toggle in ⚙ Settings → Security. The toggle state persists in browser localStorage per user.
- Never expose this service publicly — it has direct cluster read access.
- Restrict the env file: `chmod 600 env`
