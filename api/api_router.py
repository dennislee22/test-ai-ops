import json, psutil
from typing import Optional
from pathlib import Path
from fastapi import APIRouter, HTTPException, UploadFile, File, Form as FastAPIForm
from fastapi.responses import StreamingResponse, JSONResponse as _JSONResponse
from pydantic import BaseModel

import config
from agent.engine import run_agent, run_agent_streaming, _llm_synthesise, _is_kb_topic
from rag import rag_retrieve, get_doc_stats, ingest_directory, ingest_file, ingest_excel, _MSG_NO_INGEST
from tools.tools_k8s import K8S_TOOLS, reload_kubeconfig, _core as _k8s_core

api_router = APIRouter()

class HistoryMessage(BaseModel): role: str; content: str
class ChatRequest(BaseModel): message: str; decode_secrets: bool = False; history: list[HistoryMessage] = []; max_new_tokens: int = 0
class ChatResponse(BaseModel): response: str; tools_used: list; iterations: int; status_updates: list; elapsed_seconds: float
class AskRequest(BaseModel): q: str
class KbAskRequest(BaseModel): q: str; top_k: int = 50; max_tokens: int = 1312; sheet: Optional[str] = None
class KubeconfigRequest(BaseModel): kubeconfig: str
class PromptUpdateRequest(BaseModel): content: str
class ToolCallRequest(BaseModel): name: str; args: dict = {}
class IngestRequest(BaseModel): docs_dir: str; force: bool = False

@api_router.get("/health")
async def health():
    stats = get_doc_stats()
    return {"status": "ok", "model": config.LLM_MODEL, "num_gpu": config.NUM_GPU, "lancedb_docs": stats["docs_chunks"], "lancedb_excel_rows": stats["excel_rows"], "k8s_tools": len(K8S_TOOLS), "cluster_server": config.CLUSTER_SERVER}

@api_router.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    if not req.message.strip(): raise HTTPException(400, "Empty message")
    try: return ChatResponse(**await run_agent(req.message))
    except Exception as e: raise HTTPException(500, f"Agent failed: {e}")

@api_router.post("/chat/stream")
async def chat_stream(req: ChatRequest):
    if not req.message.strip(): raise HTTPException(400, "Empty message")
    config._decode_secrets_ctx.set(req.decode_secrets)
    import asyncio
    async def _keepalive_stream():
        queue, _SENTINEL = asyncio.Queue(), object()
        async def _producer():
            try:
                async for chunk in run_agent_streaming(req.message, req.history, req.max_new_tokens): await queue.put(chunk)
            finally: await queue.put(_SENTINEL)
        task = asyncio.ensure_future(_producer())
        try:
            while True:
                try: item = await asyncio.wait_for(queue.get(), timeout=10)
                except asyncio.TimeoutError: yield ": keep-alive\n\n"; continue
                if item is _SENTINEL: break
                yield item
        finally: task.cancel()
    return StreamingResponse(_keepalive_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@api_router.get("/metrics")
async def metrics():
    cpu_per, mem, freq = psutil.cpu_percent(interval=0.2, percpu=True), psutil.virtual_memory(), psutil.cpu_freq()
    return {"cpu_total": round(psutil.cpu_percent(interval=None), 1), "cpu_per_core": [round(p, 1) for p in cpu_per], "cpu_count": psutil.cpu_count(logical=True), "freq_mhz": round(freq.current) if freq else 0, "load_avg": [round(x, 2) for x in psutil.getloadavg()], "mem_total_gb": round(mem.total / 1e9, 1), "mem_used_gb": round(mem.used / 1e9, 1), "mem_pct": mem.percent, "gpus": config.gpu_metrics(), "num_gpu": config.NUM_GPU}

@api_router.post("/ingest")
async def ingest_api(req: IngestRequest):
    results = ingest_directory(req.docs_dir, force=req.force)
    return {"results": results, "total_files": len(results), "total_chunks": sum(r.get("chunks", 0) for r in results)}

@api_router.get("/api/pods")
async def api_pods(ns: str = "all"):
    from tools.tools_k8s import get_pod_status
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(None, lambda: get_pod_status(namespace=ns, show_all=True, raw_output=False))
    return {"namespace": ns, "output": raw}

@api_router.post("/api/kb/ask")
async def api_kb_ask(req: KbAskRequest):
    if not req.q.strip(): return _JSONResponse(status_code=400, content={"error": "q must not be empty"})
    top_k, sheet = max(10, min(req.top_k, 500)), req.sheet
    if not sheet:
        ql = req.q.lower()
        if any(k in ql for k in ["past learning", "incident"]): sheet = "Past Learnings"
        elif any(k in ql for k in ["known issue"]): sheet = "Known Issues"
        elif any(k in ql for k in ["dos and don", "best practice"]): sheet = "Dos and Donts"
        elif any(k in ql for k in ["prerequisite"]): sheet = "Prerequisites"

    import asyncio
    context = await asyncio.get_event_loop().run_in_executor(None, lambda: rag_retrieve(query=req.q, top_k=top_k, sheet=sheet))
    no_rag = not context.strip() or context == "No relevant documentation found." or (isinstance(context, str) and context.startswith("KB_EMPTY:"))
    
    if no_rag and _is_kb_topic(req.q): return {"answer": _MSG_NO_INGEST, "query": req.q, "top_k": top_k}
    answer = await asyncio.get_event_loop().run_in_executor(None, lambda: _llm_synthesise(None if no_rag else context, req.q, top_k, req.max_tokens))
    return {"answer": answer or "I'm sorry, I was unable to generate a response.", "query": req.q, "top_k": top_k}

# (Add all other @api_router.xxx endpoints corresponding to @api.xxx here directly).
