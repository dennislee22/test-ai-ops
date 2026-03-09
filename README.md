<h1><img src="web/static/chatbot-icon.svg" width="30" height="30"><img src="web/static/k8s-logo.svg" width="30" height="30"> Cloudera ECS AI Ops Chatbot</h1>

An air-gapped Kubernetes operations chatbot for ECS, powered by a local LLM and ChromaDB.

---

## Architecture

<img src="web/static/ecs-ai-ops.gif" width="700" alt="ECS AI Ops Architecture"/>

---

## Project Structure

```
ecs-ai-ops/
├── app.py
│
├── config/
│   └── system_prompt.txt
│
├── tools/
│   ├── __init__.py
│   └── tools_k8s.py
│
├── web/
│   ├── index.html
│   └── static/
│       ├── k8s-logo.svg
│       └── rancher-logo.svg
│
├── docs/
│   ├── API.md
│   ├── known-issues.md
│   ├── dos-and-donts.md
│   └── longhorn.md
│
├── requirements.txt
├── chromadb/
└── logs/
    └── app.log
```

---

## Stack

| Layer | Technology | Notes |
|---|---|---|
| LLM | HuggingFace Transformers | `Qwen/Qwen3-8B` recommended — it excels in tool invocation, which is important for this agentic AI solution |
| Embeddings | SentenceTransformers | `nomic-ai/nomic-embed-text-v1.5` (local, GPU-accelerated if available) |
| Vector DB | ChromaDB (embedded) | Zero external dependencies |
| Agent | LangGraph | Fully local |
| K8s typed tools | kubernetes Python client | Structured, read-only |
| K8s exec tool | kubernetes Python client | `kubectl_exec` — parses kubectl-style commands via Python API, no binary needed |
| Doc parsing | pypdf + markdown-it-py | |
| API | FastAPI | REST + SSE streaming + CORS |
| Frontend | Single-file HTML/JS | Served directly by FastAPI (`FileResponse` at `/`) |

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
| Config | `get_configmap_list`, `get_resource_quotas`, `get_limit_ranges` |
| RBAC | `get_service_accounts`, `get_cluster_role_bindings` |
| Namespaces | `get_namespace_status` |
| **kubectl exec** | **`kubectl_exec`** — accepts kubectl-style command strings, routed via the Kubernetes Python API (no binary needed) |

`kubectl_exec` parses kubectl command strings and calls the appropriate Kubernetes Python API
client methods directly. No `kubectl` binary or PATH configuration is required — everything
goes over HTTPS to the cluster API server using your kubeconfig credentials.

Supported verbs: `get`, `describe`, `logs`, `top`, `rollout`, `auth can-i`, `api-resources`, `version`.

| Check | Behaviour |
|---|---|
| Shell operators | `\|`, `&&`, `awk`, `grep` etc. — always rejected; use a typed tool instead |
| Interactive commands | `exec -it`, `port-forward`, `attach` — always blocked |
| Write operations | Blocked by default; set `KUBECTL_ALLOW_WRITES=true` to enable |
| Output limit | 20 000 chars (override with `KUBECTL_MAX_CHARS`) |

---

## Example Queries

The ECS AI Ops Chatbot understands natural language questions about your cluster:

- Is the Vault doing ok?
- Show me all the nodes' status
- Any pod is currently not Running?
- What storage class is Vault using?
- List all storage types (RWO/RWX) used by pods in cdp namespace
- Check all namespaces — which PVC is not bound?
- Which node has GPUs available and in use?
- List all Persistent Volume Claims in the longhorn-system namespace
- Show warning events in cdp-drs namespace
- How many nodes are in the cluster?
- List all namespaces

---

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

For NVIDIA GPU:
```bash
pip install torch --index-url https://download.pytorch.org/whl/cu121
```

### 2. Configure environment

Create an `env` file next to `app.py` (not included in the repo — create it manually):

```ini
KUBECONFIG_PATH=~/kubeconfig

LLM_MODEL=Qwen/Qwen3-8B
EMBED_MODEL=nomic-ai/nomic-embed-text-v1.5

NUM_GPU=1          # 0 = CPU only

LOG_LEVEL=INFO
# Set LOG_LEVEL=DEBUG to trace LLM tool selection, raw responses, and tool schemas.
# Key log lines to watch at INFO level:
#   [llm_node itr=1] tool_calls resolved: [...]   <- what tools LLM picked
#   [llm_node itr=1] LLM returned no tool_calls   <- why fallback fired
#   [tool_node] calling tool=... args=...          <- what args were passed
CHROMA_DIR=./chromadb

KUBECTL_ALLOW_WRITES=false
KUBECTL_MAX_CHARS=20000  # max chars of K8s tool output fed into the LLM (input)
MAX_NEW_TOKENS=4096      # max tokens the LLM can generate per response (output)
LLM_TIMEOUT=300          # hard timeout (seconds) per request
```

### 3. Ingest documents

```bash
python3 app.py --ingest ./docs
python3 app.py --ingest ./docs --force   # re-ingest all
```

### 4. Start the server

```bash
python3 app.py                                              # default  http://0.0.0.0:9000
python3 app.py --port 9000 --host 0.0.0.0
python3 app.py --model-dir /models/Qwen2.5-7B-Instruct     # local model path
python3 app.py --reload                                     # dev auto-reload
```

Open `http://localhost:9000` in your browser.

---

## Customising the System Prompt

The LLM system prompt lives in **`config/system_prompt.txt`** — a plain text file
you can edit without touching any Python code.

Two placeholders are required and must remain in the file:

| Placeholder | Replaced with |
|---|---|
| `{custom_rules}` | Value of `CUSTOM_RULES` env variable |

After editing the file, hot-reload without restarting:

```bash
curl -s -X POST http://localhost:9000/api/reload-prompt
```

Or push a new prompt entirely via the API — see [docs/API.md](docs/API.md).

---

## REST API

A full curl-friendly REST API is available at `/api`.  See **[docs/API.md](docs/API.md)**
for the complete reference.

Quick examples:

```bash
curl -s -X POST http://localhost:9000/api/ask \
     -H 'Content-Type: application/json' \
     -d '{"q":"are there any pods with problems?"}'

# List all available tools
curl -s http://localhost:9000/api/tools

# Live pod table  (kubectl get pods style)
curl -s 'http://localhost:9000/api/pods/raw?ns=longhorn-system'

# System metrics
curl -s http://localhost:9000/api/system
```

FastAPI also auto-generates interactive docs:
- **Swagger UI** — http://localhost:9000/docs
- **ReDoc** — http://localhost:9000/redoc

---

## Adding Your Own Documents

Drop `.md`, `.pdf`, or `.txt` files into `docs/` and run:

```bash
python3 app.py --ingest ./docs
```

Document type is auto-detected from filename keywords:

| Keyword in filename | Tagged as |
|---|---|
| `known`, `issue`, `bug`, `error` | `known_issue` |
| `runbook`, `playbook`, `procedure` | `runbook` |
| `dos`, `donts`, `guidelines` | `dos_donts` |
| anything else | `general` |

---

## Runtime Tuning — Truncation

If responses appear cut off, two independent settings control output length:

| Setting | Controls | Where to adjust |
|---|---|---|
| `KUBECTL_MAX_CHARS` | How much raw cluster data the LLM **reads** (characters) | ⚙ Settings → LLM Input/Output → MAX INPUT CHARS slider |
| `MAX_NEW_TOKENS` | How many tokens the LLM **writes** per response (~4 chars/token) | ⚙ Settings → LLM Input/Output → MAX RESPONSE TOKENS slider |

Both can independently cause truncation. Typical fix for a large cluster:

```
KUBECTL_MAX_CHARS=50000
MAX_NEW_TOKENS=6144
```

Changes apply immediately without restarting — use the Settings UI or the API:

```bash
curl -s -X POST http://localhost:9000/api/config \
     -H 'Content-Type: application/json' \
     -d '{"kubectl_max_chars": 50000, "max_new_tokens": 6144}'
```

---

## Hardware Sizing

| Setup | RAM | VRAM | Model | Speed |
|---|---|---|---|---|
| CPU only, 8-core | 32 GB | — ¹ | Qwen/Qwen3-8B | ~6–10 tok/s |
| GPU (recommended) | 32 GB | ~20 GB ² | Qwen/Qwen3-8B | ~30–60 tok/s |

> ¹ **CPU RAM shows `—`** in the GPU MONITOR panel because VRAM is a GPU-specific metric. In CPU-only mode the model is loaded into system RAM — monitor usage via the RAM MONITOR in the CPU panel instead.
>
> ² VRAM usage depends on model quantisation and batch size. Qwen3-8B in bfloat16 uses approximately 16–20 GB VRAM.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `LLM_MODEL` | `Qwen/Qwen3-8B` | HuggingFace model ID or local path |
| `EMBED_MODEL` | `nomic-ai/nomic-embed-text-v1.5` | SentenceTransformers model ID or local path |
| `NUM_GPU` | auto-detected | GPU count (0 = CPU only) |
| `KUBECONFIG_PATH` | `~/kubeconfig` | Path to kubeconfig (blank = in-cluster) |
| `LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `CHROMA_DIR` | `./chromadb` | ChromaDB persistent storage path |
| `CUSTOM_RULES` | see env | Site-specific rules injected into the system prompt |
| `KUBECTL_ALLOW_WRITES` | `false` | Allow write operations via `kubectl_exec` |
| `KUBECTL_MAX_CHARS` | `20000` | Max characters of K8s tool output fed into the LLM (input). Adjustable at runtime via ⚙ Settings → LLM Input/Output. |
| `MAX_NEW_TOKENS` | `4096` | Max tokens the LLM can generate per response (output). Adjustable at runtime via ⚙ Settings → LLM Input/Output. |
| `LLM_TIMEOUT` | `300` | Hard timeout (seconds) per request. Increase for large clusters where tool calls take >60 s. |

---

## Security Notes

- All typed K8s SDK tools are **read-only** by design.
- `kubectl_exec` is **read-only by default**. Write operations require `KUBECTL_ALLOW_WRITES=true`.
- Never expose this service publicly — it has direct cluster access.
- Restrict the env file: `chmod 600 env`
