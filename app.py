import os, json, re, time
from contextlib import asynccontextmanager
from typing import Annotated, TypedDict, Literal, Optional
from pathlib import Path
from contextvars import ContextVar

from fastapi import FastAPI, HTTPException, UploadFile, File, Form as FastAPIForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse as _JSONResponse
from pydantic import BaseModel
import uvicorn

from langchain_core.messages import HumanMessage, ToolMessage, SystemMessage, AIMessage
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages

import config.config as config
from rag import init_db, get_doc_stats, RAG_TOOLS, rag_retrieve, ingest_directory, ingest_file, ingest_excel
from rag.retrieve import _MSG_NO_INGEST, _is_kb_topic
from tools.tools_k8s import K8S_TOOLS, reload_kubeconfig, _core as _k8s_core
from agent.bypass import should_bypass_llm, build_direct_answer
from agent.routing import default_tools_for, resolve_namespace

_decode_secrets_ctx: ContextVar[bool] = ContextVar("decode_secrets", default=False)

# ── 1. SCHEMAS ───────────────────────────────────────────────────────────────

class HistoryMessage(BaseModel): role: str; content: str
class ChatRequest(BaseModel): message: str; decode_secrets: bool = False; history: list[HistoryMessage] = []; max_new_tokens: int = 0
class ChatResponse(BaseModel): response: str; tools_used: list; iterations: int; status_updates: list; elapsed_seconds: float
class AskRequest(BaseModel): q: str
class KbAskRequest(BaseModel): q: str; top_k: int = 50; max_tokens: int = 1312; sheet: Optional[str] = None
class KubeconfigRequest(BaseModel): kubeconfig: str
class PromptUpdateRequest(BaseModel): content: str
class ToolCallRequest(BaseModel): name: str; args: dict = {}
class IngestRequest(BaseModel): docs_dir: str; force: bool = False

# ── 2. AGENT & LLM LOGIC ─────────────────────────────────────────────────────

_PROMPT_FILE = config._HERE / "config" / "system_prompt.txt"

def _load_system_prompt() -> str:
    if _PROMPT_FILE.exists():
        text = _PROMPT_FILE.read_text(encoding="utf-8")
        config.logger.info(f"[Prompt] Loaded config/system_prompt.txt ({len(text)} chars)")
        return text
    config.logger.warning("[Prompt] system_prompt.txt not found — using built-in fallback prompt")
    return (
        "You are an expert Kubernetes operations assistant.\n"
        "ALWAYS call tools first. NEVER fabricate data.\n"
        "ALWAYS search documentation before finalising a diagnosis.\n"
        "SITE-SPECIFIC RULES:\n{custom_rules}\n"
    )

SYSTEM_PROMPT = _load_system_prompt()

def _registry_to_openai_schema(name: str, cfg: dict) -> dict:
    params = cfg.get("parameters", {})
    properties, required = {}, []
    for k, v in params.items():
        prop = {"type": v.get("type", "string")}
        if "description" in v: prop["description"] = v["description"]
        if "enum" in v: prop["enum"] = v["enum"]
        properties[k] = prop
        if "default" not in v: required.append(k)

    schema = {
        "type": "function",
        "function": {"name": name, "description": cfg["description"], "parameters": {"type": "object", "properties": properties}},
    }
    if required: schema["function"]["parameters"]["required"] = required
    return schema

def _call_tool(name: str, args: dict, all_tools: dict) -> str:
    cfg = all_tools.get(name)
    if not cfg: return f"Tool '{name}' not found."
    fn, params = cfg["fn"], cfg.get("parameters", {})

    for k, v in params.items():
        if k not in args and "default" in v: args[k] = v["default"]
    try:
        return str(fn(**args))
    except Exception as e:
        config.logger.error(f"[_call_tool] {name} raised: {e}", exc_info=True)
        return f"Tool '{name}' failed: {e}"

class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    tool_calls_made: list
    iteration: int
    status_updates: list
    direct_answer: Optional[str]
    req_id: str

def _build_llm():
    config.logger.info(f"[LLM] Loading model: {config.LLM_MODEL}")
    is_gguf = config.LLM_MODEL.lower().endswith(".gguf") or "gguf" in config.LLM_MODEL.lower()

    if is_gguf:
        return _build_llm_gguf()

    try:
        import transformers, torch
        is_qwen3 = "qwen3" in config.LLM_MODEL.lower()
        if is_qwen3: config.logger.info("[LLM] Qwen3 detected — native tool-calling via apply_chat_template")

        device_map = "auto" if config.NUM_GPU > 0 else "cpu"
        dtype = torch.bfloat16 if config.NUM_GPU > 0 else torch.float32

        tokenizer = transformers.AutoTokenizer.from_pretrained(config.LLM_MODEL, trust_remote_code=True)
        model = transformers.AutoModelForCausalLM.from_pretrained(
            config.LLM_MODEL, torch_dtype=dtype, device_map=device_map, trust_remote_code=True, use_cache=True
        )
        model.eval()
        config.logger.info("[LLM] Model loaded")
        return tokenizer, model, is_qwen3
    except Exception as e:
        config.logger.error(f"[LLM] Load failed: {e}")
        raise

def _build_llm_gguf():
    try: from llama_cpp import Llama
    except ImportError: raise ImportError("llama-cpp-python is required for GGUF models.")

    model_path = config.LLM_MODEL
    n_ctx      = int(os.environ.get("GGUF_N_CTX", "8192"))
    n_threads  = int(os.environ.get("GGUF_N_THREADS", str(os.cpu_count() or 4)))

    config.logger.info(f"[LLM/GGUF] Loading {model_path} | ctx={n_ctx} threads={n_threads}")
    if not os.path.isfile(model_path):
        try:
            from huggingface_hub import hf_hub_download
            for quant in ["Q4_K_M.gguf", "Q4_0.gguf", "Q5_K_M.gguf", "Q8_0.gguf"]:
                repo_id, filename = model_path, quant
                parts = model_path.split("/")
                if len(parts) == 3 and parts[-1].endswith(".gguf"):
                    repo_id, filename, quant = "/".join(parts[:2]), parts[-1], parts[-1]
                try: 
                    model_path = hf_hub_download(repo_id=repo_id, filename=filename)
                    config.logger.info(f"[LLM/GGUF] Downloaded {filename} from {repo_id}")
                    break
                except Exception: continue
        except ImportError: pass

    if not os.path.isfile(model_path): raise FileNotFoundError(f"GGUF model file not found: {model_path}")
    model = Llama(model_path=model_path, n_ctx=n_ctx, n_threads=n_threads, n_gpu_layers=0, verbose=False)
    is_qwen3 = "qwen" in model_path.lower()
    config.logger.info(f"[LLM/GGUF] Model loaded (CPU, {n_threads} threads, ctx={n_ctx})")
    return None, model, is_qwen3

def build_agent():
    all_tools = {**K8S_TOOLS, **RAG_TOOLS}
    tool_schemas = [_registry_to_openai_schema(n, c) for n, c in all_tools.items()]
    tool_names   = [s["function"]["name"] for s in tool_schemas]
    config.logger.info(f"[build_agent] {len(tool_schemas)} tools: {tool_names}")

    tokenizer, model, _is_qwen3 = _build_llm()
    globals()["_kb_tokenizer"], globals()["_kb_model"], globals()["_kb_is_qwen3"] = tokenizer, model, _is_qwen3

    _sys_prompt = _load_system_prompt().format(custom_rules="")
    prompt = (_sys_prompt + "\n/no_think") if _is_qwen3 else _sys_prompt

    def _prepare_messages_for_hf(msgs: list, req_id: str = "") -> list:
        if not msgs: return msgs
        has_tool_results = any(isinstance(m, ToolMessage) for m in msgs)
        if not has_tool_results: return [m for m in msgs if isinstance(m, (HumanMessage, SystemMessage))]

        original_question = next((m.content for m in msgs if isinstance(m, HumanMessage)), "")
        tool_results = [m for m in msgs if isinstance(m, ToolMessage)]
        
        parts = []
        for i, tr in enumerate(tool_results, 1):
            body = tr.content if len(tr.content) <= 40000 else tr.content[:40000] + "\n...[truncated]"
            parts.append(f"--- TOOL RESULT {i} ---\n{body}\n")
        combined = "".join(parts)

        synthesis_prompt = f"Question: {original_question}\n\nTool Results:\n{combined}\nAnswer the question using only the tool results above."
        return [HumanMessage(content=synthesis_prompt)]

    def _msgs_to_qwen3(msgs: list, include_tools: bool) -> list:
        result = []
        for m in msgs:
            if isinstance(m, SystemMessage): result.append({"role": "system", "content": m.content})
            elif isinstance(m, HumanMessage): result.append({"role": "user", "content": m.content})
            elif isinstance(m, ToolMessage): result.append({"role": "tool", "name": "tool", "content": m.content})
            else:
                tcs = getattr(m, "tool_calls", None) or []
                if tcs: 
                    result.append({"role": "assistant", "content": "", "tool_calls": [{"id": tc.get("id", ""), "type": "function", "function": {"name": tc["name"], "arguments": json.dumps(tc.get("args", {}))}} for tc in tcs]})
                else: 
                    result.append({"role": "assistant", "content": getattr(m, "content", "")})
        return result

    def _parse_tool_calls(text: str) -> list:
        import uuid
        tcs = []
        for m in re.finditer(r'<tool_call>\s*(.*?)\s*</tool_call>', text, re.DOTALL):
            raw = m.group(1).strip()
            try:
                obj = json.loads(raw)
                args_parsed = json.loads(obj.get("arguments", {})) if isinstance(obj.get("arguments", {}), str) else obj.get("arguments", {})
                tcs.append({"id": f"tc_{uuid.uuid4().hex[:8]}", "name": obj["name"], "args": args_parsed, "type": "tool_call"})
            except Exception: pass
        return tcs

    def llm_node(state: AgentState):
        itr, msgs, updates = state.get("iteration", 0) + 1, state["messages"], list(state.get("status_updates", []))
        if state.get("direct_answer"): 
            return {"messages": [AIMessage(content=state["direct_answer"])], "tool_calls_made": state.get("tool_calls_made", []), "iteration": itr, "status_updates": updates, "direct_answer": None}
        
        has_tool_results = any(isinstance(m, ToolMessage) for m in msgs)
        invoke_msgs = _prepare_messages_for_hf(msgs, req_id=state.get("req_id", ""))
        chat_msgs = [{"role": "system", "content": prompt}] + _msgs_to_qwen3(invoke_msgs, True)
        
        _max_new = max(512, config.MAX_NEW_TOKENS) if has_tool_results else max(1024, config.MAX_NEW_TOKENS // 2)

        if tokenizer is None:
            tools_json = json.dumps(tool_schemas, indent=2)
            tool_system = f"{prompt}\n\nAvailable tools:\n{tools_json}"
            gguf_msgs = [{"role": "system", "content": tool_system}] + chat_msgs[1:]
            resp = model.create_chat_completion(messages=gguf_msgs, max_tokens=_max_new, temperature=0.7, top_p=0.8, top_k=20, repeat_penalty=1.05)
            raw_text = resp["choices"][0]["message"].get("content", "") or ""
        else:
            import torch
            kw = {"add_generation_prompt": True, "tools": tool_schemas}
            if _is_qwen3: kw["enable_thinking"] = False
            encoded = tokenizer.apply_chat_template(chat_msgs, tokenize=True, return_tensors="pt", **kw)
            input_ids = (encoded["input_ids"] if hasattr(encoded, "__getitem__") and not hasattr(encoded, "shape") else encoded).to(model.device)
            with torch.no_grad(): 
                output_ids = model.generate(input_ids, max_new_tokens=_max_new, do_sample=True, temperature=0.7, top_p=0.8, top_k=20, repetition_penalty=1.05, pad_token_id=tokenizer.eos_token_id)
            raw_text = tokenizer.decode(output_ids[0][input_ids.shape[-1]:], skip_special_tokens=True)

        tcs = _parse_tool_calls(raw_text)
        content = re.sub(r'<tool_call>[\s\S]*?</tool_call>', '', raw_text).strip()
        response = AIMessage(content=content, tool_calls=tcs)
        if tcs: updates.append(f"🔧 {', '.join(tc['name'] for tc in tcs)}")
        return {"messages": [response], "tool_calls_made": state.get("tool_calls_made", []), "iteration": itr, "status_updates": updates}

    def tool_node(state: AgentState):
        last, results, tools_called, updates = state["messages"][-1], [], list(state.get("tool_calls_made", [])), list(state.get("status_updates", []))
        user_q = next((m.content for m in state["messages"] if isinstance(m, HumanMessage)), "")
        tcs, direct_answer = getattr(last, "tool_calls", []) or [], None

        for tc in tcs:
            name, args = tc["name"], dict(tc.get("args", {}) or {})
            if name == "get_secrets": args["decode"] = _decode_secrets_ctx.get()
            tools_called.append(name)
            updates.append(f"$ {args['command']}" if name == "kubectl_exec" and "command" in args else f"⚙️ {name}")
            out = _call_tool(name, args, all_tools)
            results.append(ToolMessage(content=out, tool_call_id=tc["id"], name=name))

            if name == "rag_search" and isinstance(out, str) and out.startswith("KB_EMPTY:"):
                updates.append("⚠️ Knowledge base is empty")
                direct_answer = "⚠️ " + _MSG_NO_INGEST + "\n\nUse the ⚙ Settings → RAG Documents panel to upload."
            elif len(tcs) == 1 and should_bypass_llm(name, args, out, user_q, req_id=state.get("req_id", "")):
                updates.append("⚡ Direct output (LLM synthesis skipped)")
                direct_answer = build_direct_answer(name, out, user_q, req_id=state.get("req_id", ""))
        return {"messages": results, "tool_calls_made": tools_called, "iteration": state.get("iteration", 0), "status_updates": updates, "direct_answer": direct_answer}

    def router(state: AgentState) -> Literal["tools", "end"]:
        if state.get("iteration", 0) >= 6: return "end"
        tcs = getattr(state["messages"][-1], "tool_calls", None)
        if not tcs: return "end"
        already, pending = state.get("tool_calls_made", []), [tc["name"] for tc in tcs]
        if already and all(name in already for name in pending): return "end"
        return "tools"

    g = StateGraph(AgentState)
    g.add_node("llm", llm_node)
    g.add_node("tools", tool_node)
    g.set_entry_point("llm")
    g.add_conditional_edges("llm", router, {"tools": "tools", "end": END})
    g.add_edge("tools", "llm")
    return g.compile()

_agent = None
def get_agent():
    global _agent
    if _agent is None: _agent = build_agent()
    return _agent

def _clean_response(text: str, user_question: str = "") -> str:
    text = re.sub(r'<think>[\s\S]*?</think>\s*', '', text)
    text = re.sub(r'<\|im_start\|>\w+\s*\n?[\s\S]*?<\|im_end\|>\n?', '', text)
    for tok in ['<|im_end|>', '<s>', '</s>', '[INST]', '[/INST]', '<<SYS>>', '<</SYS>>']: text = text.replace(tok, '')
    if user_question:
        q_stripped, escaped = user_question.strip(), re.escape(user_question.strip())
        text = re.sub(r'(?i)(\s*' + escaped + r'[?!.]?\s*){2,}', ' ', text)
        text = re.sub(r'(?i)^\s*' + escaped + r'[?!.]?\s*\n', '', text)
    text = re.sub(r'Summarise the above tool results.*', '', text, flags=re.IGNORECASE)
    return re.sub(r'\n{3,}', '\n\n', text).strip()

async def run_agent(user_message: str) -> dict:
    import uuid
    req_id = uuid.uuid4().hex[:8]
    agent, t0 = get_agent(), time.time()
    final = await agent.ainvoke({"messages": [HumanMessage(content=user_message)], "tool_calls_made": [], "iteration": 0, "status_updates": [f"🤖 Model: {config.LLM_MODEL}"], "req_id": req_id})
    elapsed, last = time.time() - t0, final["messages"][-1]
    raw = last.content if hasattr(last, "content") else str(last)
    updates = final.get("status_updates", [])
    updates.append(f"✅ Done in {elapsed:.0f}s")
    return {"response": _clean_response(raw, user_message), "tools_used": final.get("tool_calls_made", []), "iterations": final.get("iteration", 0), "status_updates": updates, "elapsed_seconds": round(elapsed, 1), "clarification_needed": False}

async def run_agent_streaming(user_message: str, history: list = None, max_new_tokens: int = 0):
    def _sse(payload: dict) -> str: return f"data: {json.dumps(payload)}\n\n"
    import uuid, asyncio
    req_id, agent, t0 = uuid.uuid4().hex[:8], get_agent(), time.time()
    yield _sse({"type": "status", "text": f"🤖 Model: {config.LLM_MODEL}"})
    
    _saved_max = config.MAX_NEW_TOKENS
    if max_new_tokens > 0: config.MAX_NEW_TOKENS = max_new_tokens
    all_updates, tools_called, final_answer, iteration_count = [f"🤖 Model: {config.LLM_MODEL}"], [], "", 0
    _hb_queue, _hb_stop = asyncio.Queue(), asyncio.Event()

    async def _heartbeat_task():
        tick = 0
        while not _hb_stop.is_set():
            try: await asyncio.wait_for(asyncio.shield(asyncio.sleep(15)), timeout=15)
            except Exception: pass
            tick += 15
            if not _hb_stop.is_set(): await _hb_queue.put(tick)
    _hb_task = asyncio.ensure_future(_heartbeat_task())

    try:
        from langchain_core.messages import AIMessage as _AIMessage
        history_msgs = [HumanMessage(content=t.content) if t.role == "user" else _AIMessage(content=t.content) for t in (history or [])]
        all_messages = history_msgs + [HumanMessage(content=user_message)]

        async for event in agent.astream_events({"messages": all_messages, "tool_calls_made": [], "iteration": 0, "status_updates": [], "req_id": req_id}, version="v2", config={"recursion_limit": 12}):
            while not _hb_queue.empty(): yield _sse({"type": "heartbeat", "text": f"⏳ Still processing… ({_hb_queue.get_nowait()}s elapsed)", "timeout": config.LLM_TIMEOUT})
            kind, name = event.get("event", ""), event.get("name", "")
            
            if kind == "on_tool_start":
                tool_name = event.get("name", "unknown_tool")
                cmd = event.get("data", {}).get("input", {}).get("command") if tool_name == "kubectl_exec" else None
                txt = f"$ {cmd}" if cmd else f"⚙️ {tool_name}"
                all_updates.append(txt); tools_called.append(tool_name)
                yield _sse({"type": "tool", "name": tool_name, "text": txt, "cmd": cmd})
            elif kind == "on_tool_end":
                tool_name, output = event.get("name", ""), event.get("data", {}).get("output", "")
                txt = f"✓ {tool_name}: {str(output)[:80].replace(chr(10), ' ')}…"
                all_updates.append(txt)
                yield _sse({"type": "status", "text": txt})
            elif kind == "on_chain_end" and name == "llm":
                output = event.get("data", {}).get("output", {})
                iteration_count = output.get("iteration", iteration_count)
                has_tool_calls = any(getattr(m, "tool_calls", None) for m in output.get("messages", []))
                for m in output.get("messages", []):
                    if getattr(m, "content", "") and not getattr(m, "tool_calls", None): final_answer = m.content
                itr_txt = f"🔄 Loop {iteration_count} — LLM called tools, waiting for results…" if has_tool_calls else f"✍️ Loop {iteration_count} — LLM synthesising final answer…"
                yield _sse({"type": "iteration", "iteration": iteration_count, "text": itr_txt, "has_tool_calls": has_tool_calls})

        elapsed = round(time.time() - t0, 1)
        _hb_stop.set(); _hb_task.cancel()
        yield _sse({"type": "status", "text": f"✅ Done in {elapsed}s"})
        yield _sse({"type": "result", "response": _clean_response(final_answer, user_message), "tools_used": list(dict.fromkeys(tools_called)), "iterations": iteration_count, "status_updates": all_updates, "elapsed_seconds": elapsed, "clarification_needed": False})
    except Exception as exc:
        _hb_stop.set(); _hb_task.cancel()
        yield _sse({"type": "error", "text": str(exc)})
    finally: config.MAX_NEW_TOKENS = _saved_max

def _llm_synthesise(context: str, question: str, top_k: int = 50, max_tokens: int = 0) -> str:
    _kb_is_empty = not context or not context.strip() or (isinstance(context, str) and context.startswith("KB_EMPTY:"))
    _no_match    = (isinstance(context, str) and context.strip() == "No relevant documentation found.")
    _no_context  = _kb_is_empty or _no_match
    
    if _no_context and _is_kb_topic(question): return _MSG_NO_INGEST
    
    try: tok, mdl, is_q3 = globals()["_kb_tokenizer"], globals()["_kb_model"], globals()["_kb_is_qwen3"]
    except KeyError: return context or ""
    
    sys_prompt = "You are the ECS Knowledge Bot for Cloudera ECS... Answer questions strictly from the knowledge base context provided."
    user_msg = f"[KNOWLEDGE BASE CONTEXT]\n{context}\n[END CONTEXT]\n\nQuestion: {question}\n\nAnswer using only the context above." if context else f"Question: {question}"
    msgs = [{"role": "system", "content": sys_prompt}, {"role": "user", "content": user_msg}]
    _max_out = max_tokens if max_tokens > 0 else min(512 + top_k * 16, 4096)
    
    try:
        if tok is None:
            resp = mdl.create_chat_completion(messages=msgs, max_tokens=_max_out, temperature=0.3, top_p=0.9, repeat_penalty=1.05)
            raw = resp["choices"][0]["message"].get("content", "") or ""
        else:
            import torch
            kw = {"add_generation_prompt": True}
            if is_q3: kw["enable_thinking"] = False
            encoded = tok.apply_chat_template(msgs, tokenize=True, return_tensors="pt", **kw)
            ids = (encoded["input_ids"] if hasattr(encoded, "__getitem__") and not hasattr(encoded, "shape") else encoded).to(mdl.device)
            with torch.no_grad(): 
                out = mdl.generate(ids, max_new_tokens=_max_out, do_sample=False, temperature=1.0, repetition_penalty=1.05, pad_token_id=tok.eos_token_id)
            raw = tok.decode(out[0][ids.shape[-1]:], skip_special_tokens=True)
        return re.sub(r'<think>[\s\S]*?</think>\s*', '', raw).strip() or context or ""
    except Exception as exc: 
        return context or ""

# ── 3. API ENDPOINTS ─────────────────────────────────────────────────────────

app = FastAPI(title="Cloudera ECS AI Ops")

@app.get("/health")
async def health():
    stats = get_doc_stats()
    return {"status": "ok", "model": config.LLM_MODEL, "num_gpu": config.NUM_GPU, "lancedb_docs": stats["docs_chunks"], "lancedb_excel_rows": stats["excel_rows"], "k8s_tools": len(K8S_TOOLS), "cluster_server": config.CLUSTER_SERVER}

@app.post("/api/kubeconfig")
async def apply_kubeconfig(req: KubeconfigRequest):
    try:
        result = reload_kubeconfig(req.kubeconfig)
        if result.get("server") and result["server"] != "unknown":
            config.CLUSTER_SERVER = re.sub(r'^https?://', '', result["server"]).strip()
        return result
    except ValueError as e: return _JSONResponse(status_code=400, content={"ok": False, "error": str(e)})

@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    if not req.message.strip(): raise HTTPException(400, "Empty message")
    try: return ChatResponse(**await run_agent(req.message))
    except Exception as e: raise HTTPException(500, f"Agent failed: {e}")

@app.post("/chat/stream")
async def chat_stream(req: ChatRequest):
    if not req.message.strip(): raise HTTPException(400, "Empty message")
    _decode_secrets_ctx.set(req.decode_secrets)
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

@app.get("/metrics")
async def metrics():
    cpu_per, mem, freq = psutil.cpu_percent(interval=0.2, percpu=True), psutil.virtual_memory(), psutil.cpu_freq()
    return {
        "cpu_total": round(psutil.cpu_percent(interval=None), 1), 
        "cpu_per_core": [round(p, 1) for p in cpu_per], 
        "cpu_count": psutil.cpu_count(logical=True), 
        "freq_mhz": round(freq.current) if freq else 0, 
        "load_avg": [round(x, 2) for x in psutil.getloadavg()], 
        "mem_total_gb": round(mem.total / 1e9, 1), 
        "mem_used_gb": round(mem.used / 1e9, 1), 
        "mem_pct": mem.percent, 
        "num_gpu": config.NUM_GPU
    }

@app.post("/ingest")
async def ingest_api(req: IngestRequest):
    results = ingest_directory(req.docs_dir, force=req.force)
    return {"results": results, "total_files": len(results), "total_chunks": sum(r.get("chunks", 0) for r in results)}

@app.post("/api/ingest/upload")
async def ingest_upload_real(files: list[UploadFile] = File(...), force: str = FastAPIForm(default="false")):
    do_force = force.lower() in ("true", "1", "yes")
    docs_dir = config._HERE / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for upload in files:
        suffix = Path(upload.filename).suffix.lower()
        if suffix not in (".md", ".pdf", ".txt", ".xlsx", ".xls"):
            results.append({"file": upload.filename, "status": "rejected", "chunks": 0, "error": f"Unsupported type '{suffix}'."})
            continue
        dest = docs_dir / upload.filename
        content = await upload.read()
        dest.write_bytes(content)
        if suffix in (".xlsx", ".xls"): result = ingest_excel(str(dest), force=do_force)
        else: result = ingest_file(str(dest), force=do_force)
        results.append(result)
    return {"results": results, "total_files": len(results), "total_chunks": sum(r.get("chunks", 0) for r in results)}

@app.post("/api/ask")
async def api_ask(req: AskRequest):
    if not req.q.strip(): return _JSONResponse(status_code=400, content={"error": "q must not be empty"})
    try:
        result = await run_agent(req.q)
        return {"question": req.q, "answer": result["response"], "tools_used": result["tools_used"], "iterations": result["iterations"], "elapsed_seconds": result["elapsed_seconds"]}
    except Exception as e: return _JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/tool")
async def api_tool(req: ToolCallRequest):
    import asyncio, inspect
    entry = K8S_TOOLS.get(req.name)
    if not entry: return _JSONResponse(status_code=404, content={"error": f"Tool '{req.name}' not found.", "available": list(K8S_TOOLS.keys())})
    fn = entry.get("fn")
    if fn is None: return _JSONResponse(status_code=501, content={"error": f"Tool '{req.name}' has no callable fn."})
    try:
        if inspect.iscoroutinefunction(fn): raw = await fn(**req.args)
        else: raw = await asyncio.get_event_loop().run_in_executor(None, lambda: fn(**req.args))
        return {"tool": req.name, "args": req.args, "output": raw}
    except TypeError as e: return _JSONResponse(status_code=400, content={"error": f"Bad args for '{req.name}': {e}"})
    except Exception as e: return _JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/tools")
async def api_tools():
    import inspect as _inspect
    out = {}
    for name, entry in K8S_TOOLS.items():
        fn = entry.get("fn")
        params = {}
        if fn:
            for pname, p in _inspect.signature(fn).parameters.items():
                params[pname] = {"default": None if p.default is _inspect.Parameter.empty else p.default, "required": p.default is _inspect.Parameter.empty}
        out[name] = {"description": entry.get("description", ""), "parameters": params}
    return {"count": len(out), "tools": out}

@app.get("/api/pods")
async def api_pods(ns: str = "all"):
    from tools.tools_k8s import get_pod_status
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(None, lambda: get_pod_status(namespace=ns, show_all=True, raw_output=False))
    return {"namespace": ns, "output": raw}

@app.get("/api/pods/raw")
async def api_pods_raw(ns: str = "all"):
    from tools.tools_k8s import get_pod_status
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(None, lambda: get_pod_status(namespace=ns, show_all=True, raw_output=True))
    return {"namespace": ns, "output": raw}

@app.get("/api/nodes")
async def api_nodes():
    from tools.tools_k8s import get_node_health
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(None, get_node_health)
    return {"output": raw}

@app.get("/api/events")
async def api_events(ns: str = "all", warn: int = 0):
    from tools.tools_k8s import get_events
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(None, lambda: get_events(namespace=ns, warning_only=bool(warn)))
    return {"namespace": ns, "warnings_only": bool(warn), "output": raw}

@app.get("/api/deployments")
async def api_deployments(ns: str = "all"):
    from tools.tools_k8s import get_deployment_status
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(None, lambda: get_deployment_status(namespace=ns))
    return {"namespace": ns, "output": raw}

@app.get("/api/pvcs")
async def api_pvcs(ns: str = "all"):
    from tools.tools_k8s import get_pvc_status
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(None, lambda: get_pvc_status(namespace=ns))
    return {"namespace": ns, "output": raw}

@app.get("/api/namespaces")
async def api_namespaces():
    from tools.tools_k8s import get_namespace_status
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(None, get_namespace_status)
    return {"output": raw}

@app.get("/api/rag/stats")
async def api_rag_stats():
    return get_doc_stats()

@app.get("/api/rag/query")
async def api_rag_query(query: str, top_k: int = 50, sheet: Optional[str] = None):
    if not query.strip(): return {"answer": "", "context": ""}
    top_k = max(10, min(top_k, 500))
    try:
        context = rag_retrieve(query=query, top_k=top_k, sheet=sheet)
        if not context.strip(): context = "No matching entries found in the knowledge base for this query."
        return {"answer": context, "query": query, "sheet": sheet or "all"}
    except Exception as e: return _JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/kb/ask")
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

@app.post("/api/kb/stream")
async def api_kb_stream(req: KbAskRequest):
    import asyncio as _asyncio, time as _time
    async def _generate():
        def _sse(obj): return f"data: {json.dumps(obj)}\n\n"
        start, q = _time.time(), req.q.strip()
        if not q: yield _sse({"type": "error", "text": "Empty query"}); return
        top_k, sheet = max(10, min(req.top_k, 500)), req.sheet
        if not sheet:
            ql = q.lower()
            if any(k in ql for k in ["past learning", "incident"]): sheet = "Past Learnings"
            elif any(k in ql for k in ["known issue"]): sheet = "Known Issues"
            elif any(k in ql for k in ["dos and don"]): sheet = "Dos and Donts"
            elif any(k in ql for k in ["prerequisite"]): sheet = "Prerequisites"

        yield _sse({"type": "question", "text": q})
        yield _sse({"type": "status", "text": f"Searching knowledge base{(' · sheet: ' + sheet) if sheet else ''}…"})

        try:
            context = await _asyncio.get_event_loop().run_in_executor(None, lambda: rag_retrieve(query=q, top_k=top_k, sheet=sheet))
            no_rag = not context.strip() or context == "No relevant documentation found." or (isinstance(context, str) and context.startswith("KB_EMPTY:"))
            if no_rag and _is_kb_topic(q):
                yield _sse({"type": "result", "answer": _MSG_NO_INGEST, "query": q, "elapsed": round(_time.time() - start, 1), "top_k": top_k})
                return
            yield _sse({"type": "status", "text": f"Found match(es) — synthesising answer…" if not no_rag else "No matches found — generating response…"})
            answer = await _asyncio.get_event_loop().run_in_executor(None, lambda: _llm_synthesise(None if no_rag else context, q, top_k, req.max_tokens))
            yield _sse({"type": "result", "answer": answer or "I'm sorry, I was unable to generate a response.", "query": q, "elapsed": round(_time.time() - start, 1), "top_k": top_k})
        except Exception as exc: yield _sse({"type": "error", "text": str(exc)})
    return StreamingResponse(_generate(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.get("/api/prompt")
async def api_get_prompt():
    if not _PROMPT_FILE.exists(): return _JSONResponse(status_code=404, content={"error": "config/system_prompt.txt not found"})
    return {"file": str(_PROMPT_FILE), "content": _PROMPT_FILE.read_text(encoding="utf-8")}

@app.put("/api/prompt")
async def api_put_prompt(req: PromptUpdateRequest):
    if not req.content.strip(): return _JSONResponse(status_code=400, content={"error": "content must not be empty"})
    _PROMPT_FILE.write_text(req.content, encoding="utf-8")
    global _agent
    _agent = None
    return {"ok": True, "chars": len(req.content), "message": "Prompt saved. Agent will rebuild on next request."}

@app.post("/api/reload-prompt")
async def api_reload_prompt():
    if not _PROMPT_FILE.exists(): return _JSONResponse(status_code=404, content={"error": "config/system_prompt.txt not found"})
    global _agent
    _agent = None
    return {"ok": True, "chars": len(_PROMPT_FILE.read_text(encoding="utf-8")), "message": "Agent cache cleared. New prompt active on next request."}

@app.get("/api/config")
async def api_get_config():
    import tools.tools_k8s as _tk
    return {"kubectl_max_chars": _tk._KUBECTL_MAX_OUT, "max_new_tokens": config.MAX_NEW_TOKENS, "llm_timeout": config.LLM_TIMEOUT}

@app.post("/api/config")
async def api_set_config(body: dict):
    import tools.tools_k8s as _tk
    updated = {}
    if "kubectl_max_chars" in body:
        val = max(1000, min(int(body["kubectl_max_chars"]), 200000))
        _tk._KUBECTL_MAX_OUT = val
        updated["kubectl_max_chars"] = val
    if "max_new_tokens" in body:
        val = max(256, min(int(body["max_new_tokens"]), 16384))
        config.MAX_NEW_TOKENS = val
        updated["max_new_tokens"] = val
    if "llm_timeout" in body:
        val = max(30, min(int(body["llm_timeout"]), 1800))
        config.LLM_TIMEOUT = val
        updated["llm_timeout"] = val
    if not updated: return _JSONResponse(status_code=400, content={"error": "No recognised config keys in body"})
    return {"ok": True, "updated": updated}

# ── 4. LIFESPAN & STARTUP ────────────────────────────────────────────────────

def _run_startup_checks():
    SMOKE_TESTS = [("get_node_health", {}), ("get_namespace_status", {}), ("get_pod_status", {"namespace": "all"})]
    config.logger.info("[Self-test] Running kubectl tool smoke-tests…")
    for name, kwargs in SMOKE_TESTS:
        cfg = K8S_TOOLS.get(name)
        if cfg is None: continue
        try:
            result = cfg["fn"](**kwargs)
            if "error" in result.lower(): config.logger.warning(f"[Self-test] ⚠ {name}: {result[:120]}")
            else: config.logger.info(f"[Self-test] ✓ {name}: {result.replace(chr(10), ' ')[:80]}…")
        except Exception as e: config.logger.warning(f"[Self-test] ⚠ {name} raised: {e}")

@app.on_event("startup")
async def _startup_event():
    config.logger.info("=" * 60)
    config.logger.info(f"Cloudera ECS AI Ops")
    config.logger.info(f"  LLM      : {config.LLM_MODEL}")
    config.logger.info(f"  Embed    : {config.EMBED_MODEL}")
    config.logger.info(f"  GPU      : {config.NUM_GPU} GPU(s)")
    config.logger.info(f"  Tools    : {len(K8S_TOOLS)} kubectl tools registered")
    config.logger.info("=" * 60)

    _run_startup_checks()
    try:
        init_db()
        stats = get_doc_stats()
        config.logger.info(f"[LanceDB] Ready — {stats['docs_chunks']} doc chunks, {stats['excel_rows']} Excel rows")
    except Exception as e: config.logger.error(f"[LanceDB] Init failed: {e}")

    config.logger.info("[Agent] Pre-warming LLM…")
    get_agent()
    config.logger.info("Startup complete ✓")

@app.on_event("shutdown")
async def _shutdown_event():
    config.logger.info("Shutting down")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

if os.path.exists("web/static"): app.mount("/static", StaticFiles(directory="web/static"), name="static")

@app.get("/")
async def serve_ui():
    if os.path.exists("web/index.html"): 
        return FileResponse("web/index.html", media_type="text/html")
    return _JSONResponse(
        status_code=404, 
        content={"error": "web/index.html not found"}
    )

if __name__ == "__main__":
    if config.ARGS.ingest:
        from rag.ingest import ingest_directory
        print(f"\n📂 Ingesting documents from: {config.ARGS.ingest}  (force={config.ARGS.force})")
        init_db()
        results = ingest_directory(config.ARGS.ingest, force=config.ARGS.force)
        total   = sum(r.get("chunks", 0) for r in results)
        print(f"\n✅  {len(results)} file(s)  |  {total} total chunks stored in LanceDB\n")
        for r in results:
            icon = ("✓" if r["status"] == "ingested" else "—" if r["status"] == "skipped" else "✗")
            print(f"  {icon}  {r['file']:<42} {r['status']:<10} ({r['chunks']} chunks)")
        print()

    gpu_str    = (f"{config.NUM_GPU} GPU(s) — GPU inference" if config.NUM_GPU > 0 else "None — CPU inference")
    tool_count = len(K8S_TOOLS)

    print(f"""
╔════════════════════════════════════════════════════════════╗
║            Cloudera ECS AI Ops  v2.0 (Transformers)        ║
╠════════════════════════════════════════════════════════════╣
║  LLM      : {config.LLM_MODEL:<46} ║
║  Embed    : {config.EMBED_MODEL:<46} ║
║  GPU      : {gpu_str:<46} ║
║  Tools    : {tool_count} kubectl tools registered{'':<26} ║
║  LanceDB  : {config.LANCEDB_DIR:<46} ║
║  Server   : http://{config.ARGS.host}:{config.ARGS.port:<38} ║
╚════════════════════════════════════════════════════════════╝
""")

    uvicorn.run("app:app", host=config.ARGS.host, port=config.ARGS.port, reload=config.ARGS.reload, log_level="warning")
