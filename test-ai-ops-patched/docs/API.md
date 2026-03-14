# ECS AI Ops — API Reference

All endpoints return JSON.  Replace `http://localhost:8000` with your server address.

---

## Quick-start

```bash
# Check everything is up
curl -s http://localhost:8000/api/info | python3 -m json.tool

# Ask the AI a question
curl -s -X POST http://localhost:8000/api/ask \
     -H 'Content-Type: application/json' \
     -d '{"q":"are there any pods with problems?"}' | python3 -m json.tool
```

---

## Endpoint Reference

### GET /api
Index of all endpoints with curl examples.
```bash
curl -s http://localhost:8000/api
```

---

### GET /api/info
Model, GPU, cluster connectivity status.
```bash
curl -s http://localhost:8000/api/info
```
```json
{
  "model": "/home/cdsw/Qwen2.5-7B-Instruct",
  "inference": "GPU",
  "num_gpu": 1,
  "cluster_ok": true,
  "rag_chunks": 142,
  "tools_count": 22
}
```

---

### POST /api/ask
Ask the AI chatbot a question (blocking — waits for full response).
```bash
# General cluster health
curl -s -X POST http://localhost:8000/api/ask \
     -H 'Content-Type: application/json' \
     -d '{"q":"check overall cluster health"}'

# Pod problems
curl -s -X POST http://localhost:8000/api/ask \
     -H 'Content-Type: application/json' \
     -d '{"q":"list all pods that have problems and tell me why"}'

# Namespace-specific
curl -s -X POST http://localhost:8000/api/ask \
     -H 'Content-Type: application/json' \
     -d '{"q":"how many pods are in longhorn-system?"}'

# Storage
curl -s -X POST http://localhost:8000/api/ask \
     -H 'Content-Type: application/json' \
     -d '{"q":"check Longhorn volume health"}'

# Off-topic (tests clarification response)
curl -s -X POST http://localhost:8000/api/ask \
     -H 'Content-Type: application/json' \
     -d '{"q":"what is the weather today?"}'
```
Response fields: `question`, `answer`, `tools_used`, `iterations`, `elapsed_seconds`

---

### POST /api/tool
Call a specific K8s tool directly — bypasses the AI, returns raw tool output.
```bash
# Pod status for all namespaces
curl -s -X POST http://localhost:8000/api/tool \
     -H 'Content-Type: application/json' \
     -d '{"name":"get_pod_status","args":{"namespace":"all","show_all":true}}'

# Node health
curl -s -X POST http://localhost:8000/api/tool \
     -H 'Content-Type: application/json' \
     -d '{"name":"get_node_health","args":{}}'

# Warning events in a namespace
curl -s -X POST http://localhost:8000/api/tool \
     -H 'Content-Type: application/json' \
     -d '{"name":"get_events","args":{"namespace":"cdp-drs","warning_only":true}}'

# PVC status
curl -s -X POST http://localhost:8000/api/tool \
     -H 'Content-Type: application/json' \
     -d '{"name":"get_pvc_status","args":{"namespace":"all"}}'

# Pod logs
curl -s -X POST http://localhost:8000/api/tool \
     -H 'Content-Type: application/json' \
     -d '{"name":"get_pod_logs","args":{"pod_name":"longhorn-manager-6wvlk","namespace":"longhorn-system","tail_lines":30}}'

# Raw kubectl command
curl -s -X POST http://localhost:8000/api/tool \
     -H 'Content-Type: application/json' \
     -d '{"name":"kubectl_exec","args":{"command":"kubectl get nodes -o wide"}}'
```

---

### GET /api/tools
List every registered tool with its parameters and a ready-to-run curl example.
```bash
curl -s http://localhost:8000/api/tools | python3 -m json.tool
```

---

### GET /api/pods
Pod health summary (non-healthy pods highlighted).
```bash
curl -s 'http://localhost:8000/api/pods'              # all namespaces
curl -s 'http://localhost:8000/api/pods?ns=cdp-drs'  # specific namespace
```

### GET /api/pods/raw
kubectl-style tabular output (`NAME  READY  STATUS  RESTARTS  AGE`).
```bash
curl -s 'http://localhost:8000/api/pods/raw'
curl -s 'http://localhost:8000/api/pods/raw?ns=longhorn-system'
```

---

### GET /api/nodes
Node health, resource pressure, and GPU detection.
```bash
curl -s http://localhost:8000/api/nodes
```

---

### GET /api/events
Cluster events — optionally filter by namespace and warning severity.
```bash
curl -s 'http://localhost:8000/api/events'                        # all events
curl -s 'http://localhost:8000/api/events?warn=1'                 # warnings only
curl -s 'http://localhost:8000/api/events?ns=cdp-drs&warn=1'     # namespace + warnings
curl -s 'http://localhost:8000/api/events?ns=longhorn-system'     # namespace, all severity
```

---

### GET /api/deployments
Deployment replica health.
```bash
curl -s 'http://localhost:8000/api/deployments'
curl -s 'http://localhost:8000/api/deployments?ns=cattle-system'
```

---

### GET /api/pvcs
PVC / Longhorn storage status.
```bash
curl -s 'http://localhost:8000/api/pvcs'
curl -s 'http://localhost:8000/api/pvcs?ns=longhorn-system'
```

---

### GET /api/namespaces
All namespaces and their Active/Terminating phase.
```bash
curl -s http://localhost:8000/api/namespaces
```

---

### GET /api/rag/stats
ChromaDB ingestion statistics.
```bash
curl -s http://localhost:8000/api/rag/stats
```

---

### GET /api/system
Live CPU / RAM / GPU metrics.
```bash
curl -s http://localhost:8000/api/system
```

---

### POST /api/kubeconfig
Load a new kubeconfig to switch target cluster at runtime.
```bash
curl -s -X POST http://localhost:8000/api/kubeconfig \
     -H 'Content-Type: application/json' \
     -d "{\"kubeconfig\": \"$(cat ~/.kube/config | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')\"}"
```

---

## Interactive API docs

FastAPI auto-generates interactive Swagger UI and ReDoc at:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**:      http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json
