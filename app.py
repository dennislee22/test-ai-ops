import os, json, re, time, psutil
from contextlib import asynccontextmanager
from typing import Annotated, TypedDict, Literal, Optional
from pathlib import Path
from contextvars import ContextVar

from fastapi import FastAPI, HTTPException, UploadFile, File, Form as FastAPIForm, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse as _JSONResponse
from pydantic import BaseModel
import uvicorn

from langchain_core.messages import HumanMessage, ToolMessage, SystemMessage, AIMessage
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages

import config.config as config
import tools.tools_k8s as _tk
from rag import init_db, get_doc_stats, RAG_TOOLS, rag_retrieve, ingest_directory, ingest_file, ingest_excel
from rag.retrieve import _MSG_NO_INGEST
from tools.tools_k8s import reload_kubeconfig, _core as _k8s_core
from tools.tools_metadata import K8S_TOOL_METADATA
from agent.bypass import should_bypass_llm

if not hasattr(config, "DISABLE_LOOP_PROTECTION"):
    config.DISABLE_LOOP_PROTECTION = False

_HERE = Path(__file__).resolve().parent
SETTINGS_FILE = _HERE / "config" / "settings.json"

_SETTINGS_MAP = {
    "kubectl_max_chars":       lambda v: setattr(_tk, "_KUBECTL_MAX_OUT", v),
    "max_new_tokens":          lambda v: setattr(config, "MAX_NEW_TOKENS", v),
    "llm_timeout":             lambda v: setattr(config, "LLM_TIMEOUT", v),
    "disable_loop_protection": lambda v: setattr(config, "DISABLE_LOOP_PROTECTION", v),
    "show_secret_values":      lambda v: setattr(config, "SHOW_SECRET_VALUES", v),
}

def load_settings():
    if not SETTINGS_FILE.exists():
        return
    try:
        s = json.loads(SETTINGS_FILE.read_text())
        for key, apply in _SETTINGS_MAP.items():
            if key in s:
                apply(s[key])
    except Exception as e:
        config.logger.error(f"Failed to load settings.json: {e}")

def save_settings():
    s = {
        "kubectl_max_chars":       _tk._KUBECTL_MAX_OUT,
        "max_new_tokens":          getattr(config, "MAX_NEW_TOKENS", 4096),
        "llm_timeout":             getattr(config, "LLM_TIMEOUT", 300),
        "disable_loop_protection": getattr(config, "DISABLE_LOOP_PROTECTION", False),
        "show_secret_values":      getattr(config, "SHOW_SECRET_VALUES", False),
    }
    try:
        SETTINGS_FILE.write_text(json.dumps(s, indent=2))
    except Exception as e:
        config.logger.error(f"Failed to save settings.json: {e}")

load_settings()

_decode_secrets_ctx: ContextVar[bool] = ContextVar("decode_secrets", default=False)
_timezone_ctx:       ContextVar[str]  = ContextVar("user_timezone",  default="UTC")

_log_ag = config._log_ag
_log_rag = config._log_rag
logger = config.logger

LOCAL_NS_MAP = {
    "vault": "vault-system",
    "longhorn": "longhorn-system",
    "cdp": "cdp",
    "cdp-drs": "cdp-drs",
    "cdp-keda": "cdp-keda",
    "cdp-obs": "cdp-obs",
    "cdp-services": "cdp-services",
}

IGNORE_NS = {
    "all", "the", "any", "which", "what", "my", "this", "that", "a", "some", "in",
    "for", "of", "to", "is", "not", "are", "and", "or", "pvc", "pvcs", "pod", "pods",
    "node", "nodes", "deployment", "deployments", "status", "health", "check", "get",
    "show", "has", "have", "had", "with", "without", "using", "uses", "does", "do"
}

def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"

_SHEET_KEYWORDS = [
    (["past learning", "incident"],     "Past Learnings"),
    (["known issue"],                   "Known Issues"),
    (["dos and don", "best practice"],  "Dos and Donts"),
    (["prerequisite"],                  "Prerequisites"),
]

def _detect_sheet(q: str) -> str | None:
    ql = q.lower()
    for keywords, sheet in _SHEET_KEYWORDS:
        if any(k in ql for k in keywords):
            return sheet
    return None

def _ingest_response(results: list) -> dict:
    return {
        "results":      results,
        "total_files":  len(results),
        "total_chunks": sum(r.get("chunks", 0) for r in results),
    }

# ── 1. SCHEMAS ───────────────────────────────────────────────────────────────

class HistoryMessage(BaseModel): role: str; content: str
class ChatRequest(BaseModel): message: str; decode_secrets: bool = False; history: list[HistoryMessage] = []; max_new_tokens: int = 0; skip_synthesise: bool = False; timezone: str = "UTC"
class ChatResponse(BaseModel): response: str; tools_used: list; iterations: int; status_updates: list; elapsed_seconds: float
class AskRequest(BaseModel): q: str; skip_synthesise: bool = False
class KbAskRequest(BaseModel): q: str; top_k: int = 50; max_tokens: int = 1312; sheet: Optional[str] = None
class KubeconfigRequest(BaseModel): kubeconfig: str
class PromptUpdateRequest(BaseModel): content: str
class ToolCallRequest(BaseModel): name: str; args: dict = {}
class IngestRequest(BaseModel): docs_dir: str; force: bool = False

# ── 2. FULL AGENT & LLM LOGIC ────────────────────────────────────────────────

_PROMPT_FILE = config._HERE / "config" / "system_prompt.txt"

def _load_system_prompt() -> str:
    if _PROMPT_FILE.exists():
        text = _PROMPT_FILE.read_text(encoding="utf-8")
        logger.info(f"[Prompt] Loaded config/system_prompt.txt ({len(text)} chars)")
        return text
    logger.warning("[Prompt] system_prompt.txt not found — using built-in fallback prompt")
    return (
        "You are an ECS Operations Assistant curated by dennislee for a Cloudera ECS cluster running in an air-gapped environment.\n"
        "You have access to tools that query the live cluster.\n"
        "ALWAYS call tools first. NEVER fabricate data.\n"
        "ALWAYS search documentation before finalising a diagnosis.\n"
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

    args = {k: v for k, v in args.items() if k in params or k == "decode"}

    try:
        return str(fn(**args))
    except Exception as e:
        _log_ag.error(f"[_call_tool] {name} raised: {e}", exc_info=True)
        return f"Tool '{name}' failed: {e}"

class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    tool_calls_made: list
    iteration: int
    status_updates: list
    direct_answer: Optional[str]
    req_id: str
    skip_synthesise: bool

def _build_llm():
    _log_ag.info(f"[LLM] Loading model: {config.LLM_MODEL}")
    is_gguf = config.LLM_MODEL.lower().endswith(".gguf") or "gguf" in config.LLM_MODEL.lower()

    if is_gguf:
        return _build_llm_gguf()

    try:
        import transformers, torch
        is_qwen3 = "qwen3" in config.LLM_MODEL.lower()
        if is_qwen3: _log_ag.info("[LLM] Qwen3 detected — native tool-calling via apply_chat_template")

        device_map = "auto" if config.NUM_GPU > 0 else "cpu"
        dtype = torch.bfloat16 if config.NUM_GPU > 0 else torch.float32

        tokenizer = transformers.AutoTokenizer.from_pretrained(config.LLM_MODEL, trust_remote_code=True)
        model = transformers.AutoModelForCausalLM.from_pretrained(
            config.LLM_MODEL, torch_dtype=dtype, device_map=device_map, trust_remote_code=True, use_cache=True
        )
        model.eval()
        _log_ag.info("[LLM] Model loaded")
        return tokenizer, model, is_qwen3
    except Exception as e:
        _log_ag.error(f"[LLM] Load failed: {e}")
        raise

def _build_llm_gguf():
    try: from llama_cpp import Llama
    except ImportError: raise ImportError("llama-cpp-python is required for GGUF models.")

    model_path = config.LLM_MODEL
    n_ctx      = int(os.environ.get("GGUF_N_CTX", "40960"))
    n_threads  = int(os.environ.get("GGUF_N_THREADS", str(os.cpu_count() or 4)))

    _log_ag.info(f"[LLM/GGUF] Loading {model_path} | ctx={n_ctx} threads={n_threads}")
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
                    _log_ag.info(f"[LLM/GGUF] Downloaded {filename} from {repo_id}")
                    break
                except Exception: continue
        except ImportError: pass

    if not os.path.isfile(model_path): raise FileNotFoundError(f"GGUF model file not found: {model_path}")
    model = Llama(model_path=model_path, n_ctx=n_ctx, n_threads=n_threads, n_gpu_layers=0, verbose=False)
    is_qwen3 = "qwen" in model_path.lower()
    _log_ag.info(f"[LLM/GGUF] Model loaded (CPU, {n_threads} threads, ctx={n_ctx})")
    return None, model, is_qwen3

def _extract_namespace(text: str) -> str:
    text = text.lower()

    # 1. Interrogative Short-Circuit: "which namespace", "what ns", "how many namespaces"
    if re.search(r'\b(which|what|how many|list all|show all)\b\s+(?:namespaces|namespace|ns)\b', text):
        return "all"

    # 2. Map known hardcoded keywords (Sort by length descending: cdp-services, cdp-keda, cdp...)
    sorted_keywords = sorted(LOCAL_NS_MAP.keys(), key=len, reverse=True)
    for keyword in sorted_keywords:
        if re.search(rf'\b{keyword}\b', text):
            return LOCAL_NS_MAP[keyword]

    # 3. Explicit "all namespaces", "all ns", or "-a" flag
    if re.search(r'\ball\b[^a-z0-9-]+(?:namespace|namespaces|namespac|namespcs|ns)\b', text) or "-a" in text.split():
        return "all"

    # 4. Explicit flag "-n xyz" or "--namespace=xyz"
    match_flag = re.search(r'\b(?:-n|--namespace)[\s=]+([a-z0-9-]+)\b', text)
    if match_flag:
        return match_flag.group(1)

    # 5. Forward shorthand "namespace xyz" (ignores punctuation & typos)
    for match in re.finditer(r'\b(?:namespace|namespaces|namespac|namespcs|ns)\b[^a-z0-9-]+([a-z0-9-]+)\b', text):
        extracted = match.group(1)
        if extracted not in IGNORE_NS and len(extracted) > 1:
            return extracted

    # 6. Reverse shorthand "xyz namespace"
    for match in re.finditer(r'\b([a-z0-9-]+)[^a-z0-9-]+\b(?:namespace|namespaces|namespac|namespcs|ns)\b', text):
        extracted = match.group(1)
        if extracted not in IGNORE_NS and len(extracted) > 1:
            return extracted

    return "all"

def build_agent():
    all_tools = {**K8S_TOOL_METADATA, **RAG_TOOLS}
    tool_schemas = [_registry_to_openai_schema(n, c) for n, c in all_tools.items()]
    tool_names = [s["function"]["name"] for s in tool_schemas]
    _log_ag.info(f"[build_agent] {len(tool_schemas)} tools: {tool_names}")

    tokenizer, model, _is_qwen3 = _build_llm()
    globals()["_kb_tokenizer"], globals()["_kb_model"], globals()["_kb_is_qwen3"] = tokenizer, model, _is_qwen3

    _sys_prompt = _load_system_prompt().format(custom_rules="")
    prompt = (_sys_prompt + "\n/no_think") if _is_qwen3 else _sys_prompt

    def _prepare_messages_for_hf(msgs: list, req_id: str = "") -> list:
        if not msgs:
            return msgs

        has_tool_results = any(isinstance(m, ToolMessage) for m in msgs)
        if not has_tool_results:
            filtered = [m for m in msgs if isinstance(m, (HumanMessage, SystemMessage))]
            _log_ag.debug(f"[REQ:{req_id}] [prepare_msgs] tool selection — passing {len(filtered)} msg(s)")
            return filtered

        original_question = next((m.content for m in msgs if isinstance(m, HumanMessage)), "")
        tool_results = [m for m in msgs if isinstance(m, ToolMessage)]

        _tools_used = {getattr(tr, "name", "") for tr in tool_results}

        _EXEMPT_TOOLS = {
            "get_coredns_health", "get_node_info", "get_gpu_info", "get_node_capacity", "run_cluster_health",
            "get_pv_usage", "get_persistent_volumes", "query_prometheus_metrics", "find_resource",
            "get_node_resource_requests", "rag_search", "exec_db_query", "kubectl_exec",
            "get_node_labels", "get_node_taints", "get_storage_classes", "get_cluster_version"
        }

        _needs_ns = bool(_tools_used - _EXEMPT_TOOLS - {""})

        # Guardrail
        _ns_prefix = ""
        if _needs_ns:
            detected_ns = _extract_namespace(original_question)
            if detected_ns == "all":
                _ns_prefix = "§NS_PREFIX§As no namespace was mentioned, I checked across all namespaces.§END_NS§\n\n"
            else:
                _ns_prefix = f"§NS_PREFIX§I mapped the keyword in your question and scoped this check to the `{detected_ns}` namespace.§END_NS§\n\n"

        _tool_char_limit = 40000
        parts = []
        for i, tr in enumerate(tool_results, 1):
            body = tr.content if len(tr.content) <= _tool_char_limit else tr.content[:_tool_char_limit] + "\n...[truncated]"
            parts.append(f"--- TOOL RESULT {i} ---\n{body}\n")
        combined = "".join(parts)

        _log_ag.info(f"[REQ:{req_id}] [prepare_msgs] combining {len(tool_results)} tool result(s) ({len(combined)} chars) for LLM synthesis")

        _TOOL_FORMATS = { # Unique prompt for individual tool
            "query_prometheus_metrics": (
                "Present the metrics exactly as returned. "
                "List each series with its last value. Do not round or omit any series."
            ),
            "get_gpu_info": (
                "Available is not equivalent to being used or in use."
                "If the table does not specify which pod is attached to the GPU, it means the GPU is available to be used, it is not currently in use"
            ),
            "find_resource": (
                "Reproduce the command output VERBATIM. "
                "Do NOT reformat, summarise, or omit any rows."
            ),
            "get_namespace_resource_summary": (
                "ALWAYS calculate and lead with the total figures at the very top of your answer: "
                "TOTAL CPU REQUESTED, TOTAL CPU LIMIT, TOTAL MEMORY REQUESTED, TOTAL MEMORY LIMIT. "
                "Only after providing the totals should you list the per-pod breakdown. "
                "Do NOT just list the pods—the total is the answer to a calculate question."
            ),
            "get_node_capacity": (
                "EVALUATE the tool results. IF the user is asking to fit a specific pod with requested CPU/Memory,"
                "You MUST evaluate node capacity using STRICT BOOLEAN logic and use this EXACT output format:"
                "Node: [Node Name]"
                "--------------------------------------------------"
                "CPU: [Available] >= [Requested] -> [TRUE/FALSE],"
                "RAM: [Available] >= [Requested] -> [TRUE/FALSE],"
                "Available on node?: [TRUE/FALSE]"
                "(Repeat the block above for each node evaluated)"
                "Explanation: [Briefly explain the math results in plain English.]"
                "Conclusion: [Write your final statement on whether the pod can be scheduled and where.]"
                "For example if user asks if a pod with 100CPU and 500GB"
                "CPU: [50] >= [100] -> FALSE"
                "RAM: [50] >= [500] -> FALSE"
                "Fits on node: FALSE"
                "IF the user is just asking a general question about capacity (e.g., how much capacity is available):"
                "Do NOT use the boolean format. Just provide a helpful summary of the current capacity based on the table."
            ),
            "kubectl_exec": (
                "Reproduce the command output VERBATIM. "
                "Do NOT reformat, summarise, or omit any rows."
            ),
        }

        _ENUMERATION_TOOLS = { # Unique common prompt
             "get_pod_logs", "describe_pod",
        }

        _single_tool = next(iter(_tools_used - {""}), None)
        if len(_tools_used - {""}) == 1 and _single_tool in _TOOL_FORMATS:
            synthesis_prompt = (
                f"Question: {original_question}\n\n"
                f"Tool Results:\n{combined}\n"
                + _TOOL_FORMATS[_single_tool]
            )

        elif _single_tool in _ENUMERATION_TOOLS:
            synthesis_prompt = (
                f"Question: {original_question}\n\n"
                f"Tool Results:\n{combined}\n"
                "List EVERY item from the tool results."
                "One bullet per item. Do NOT skip or summarise any item."
            )

        else:
            synthesis_prompt = (
                f"Question: {original_question}\n\n"
                f"Tool Results:\n{combined}\n"
                "EVALUATE the tool results above. Do they contain the correct data to answer the user's question?\n"
                "- If the data is correct/sufficient: Write the final plain-text answer right now. DO NOT start your response with 'YES' or any preamble. Answer the user directly.\n"
                "- If the data is missing/incorrect: Output a new <tool_call> to try a different tool.\n"
                "CRITICAL GUARDRAILS:\n"
                "1. If a tool result field is empty, 'None', or '-', it means NO data exists for that specific attribute. Do NOT assume the tool failed or that more data exists elsewhere.\n"
                "2. NEVER call a tool that you have already used in this conversation. If you have already used the necessary tools, you MUST synthesize the final answer immediately using the data you have, even if some fields are empty."
            )

        return [HumanMessage(content=_ns_prefix + synthesis_prompt)]

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

    def tool_node(state: AgentState):
        last, results, tools_called, updates = state["messages"][-1], [], list(state.get("tool_calls_made", [])), list(state.get("status_updates", []))
        user_q = next((m.content for m in state["messages"] if isinstance(m, HumanMessage)), "")
        tcs, direct_answer = getattr(last, "tool_calls", []) or [], None

        forced_ns = _extract_namespace(user_q)
        forced_skip = state.get("skip_synthesise", False)

        for tc in tcs:
            name, args = tc["name"], dict(tc.get("args", {}) or {})

            bad_ns = ["cluster", "across the cluster", "entire cluster", "any", "none"]
            safe_ns = str(args.get("namespace") or "").lower()

            if safe_ns in bad_ns:
                args["namespace"] = "all"

            if "namespace" in args and forced_ns != "all":
                args["namespace"] = forced_ns
                config.logger.info(f"[REQ:{state.get('req_id', '')}] Overriding LLM namespace -> forced to '{forced_ns}'")

            if name == "get_secret_list":
                args["decode"] = _decode_secrets_ctx.get()

            if name in ("query_prometheus_metrics", "get_top_pods", "get_top_nodes"):
                args["user_timezone"] = _timezone_ctx.get()

            tools_called.append(name)

            updates.append(
                f"$ {args['command']}"
                if name == "kubectl_exec" and "command" in args
                else f"⚙️ {name}"
            )

            out = _call_tool(name, args, all_tools)
            _out_str = str(out)

            _log_ag.info(f"[REQ:{state.get('req_id', '')}] [tool_node] {name} returned {len(_out_str)} chars:\n{_out_str}")

            results.append(ToolMessage(content=out, tool_call_id=tc["id"], name=name))

            if name == "rag_search" and isinstance(out, str) and out.startswith("KB_EMPTY:"):
                updates.append("⚠️ Knowledge base is empty")
                direct_answer = "⚠️ " + _MSG_NO_INGEST + "\n\nUse the ⚙ Settings → RAG Documents panel to upload."

            elif forced_skip:
                pass

            elif len(tcs) == 1 and should_bypass_llm(name, args, out, user_q, req_id=state.get("req_id", "")):
                updates.append("⚡ Direct output (LLM synthesis skipped)")
                direct_answer = out

        return {
            "messages": results,
            "tool_calls_made": tools_called,
            "iteration": state.get("iteration", 0),
            "status_updates": updates,
            "direct_answer": direct_answer,
        }

    def llm_node(state: AgentState):
        itr, msgs, updates = state.get("iteration", 0) + 1, state["messages"], list(state.get("status_updates", []))
        req_id = state.get("req_id", "")

        if state.get("direct_answer"):
            return {
                "messages": [AIMessage(content=state["direct_answer"])],
                "tool_calls_made": state.get("tool_calls_made", []),
                "iteration": itr,
                "status_updates": updates,
                "direct_answer": None,
            }

        if state.get("skip_synthesise", False):
            tool_msgs = [m for m in msgs if isinstance(m, ToolMessage)]
            if tool_msgs:
                parts = []
                for r in tool_msgs:
                    content_str = str(r.content).strip()

                    if "\n|" in content_str or content_str.startswith("|") or "|---" in content_str or "```" in content_str:
                        parts.append(f"**Raw output from tool `{r.name}`**:\n\n{content_str}")
                    else:
                        parts.append(f"**Raw output from tool `{r.name}`**:\n\n```text\n{content_str}\n```")

                fallback_answer = "\n\n".join(parts)
                updates.append("⚡ Skip Synthesise toggled: LLM synthesis bypassed")

                config.logger.info(f"[REQ:{req_id}] [llm_node] Skip Synthesise is ON. Bypassing LLM...")

                return {
                    "messages": [AIMessage(content=fallback_answer)],
                    "tool_calls_made": state.get("tool_calls_made", []),
                    "iteration": itr,
                    "status_updates": updates,
                    "direct_answer": None,
                }

        has_tool_results = any(isinstance(m, ToolMessage) for m in msgs)
        invoke_msgs = _prepare_messages_for_hf(msgs, req_id=req_id)
        chat_msgs = [{"role": "system", "content": prompt}] + _msgs_to_qwen3(invoke_msgs, True)

        _max_new = max(512, config.MAX_NEW_TOKENS) if has_tool_results else max(1024, config.MAX_NEW_TOKENS // 2)

        if tokenizer is None:
            format_rules = (
                "\n\nTo call a tool, you MUST use exactly this JSON format. "
                "Do not write Python code. Use this syntax:\n"
                "<tool_call>\n"
                "{\"name\": \"tool_name\", \"arguments\": {\"arg_name\": \"value\"}}\n"
                "</tool_call>"
            )

            tools_json = json.dumps(tool_schemas, indent=2)
            tool_system = f"{prompt}\n\nAvailable tools:\n{tools_json}{format_rules}"
            gguf_msgs = [{"role": "system", "content": tool_system}] + chat_msgs[1:]

            resp = model.create_chat_completion(
                messages=gguf_msgs,
                max_tokens=_max_new,
                temperature=0.1,
                top_p=0.8,
                top_k=20,
                repeat_penalty=1.05
            )

            raw_text = resp["choices"][0]["message"].get("content", "") or ""

        else:
            import torch

            kw = {"add_generation_prompt": True, "tools": tool_schemas}
            if _is_qwen3:
                kw["enable_thinking"] = False

            encoded = tokenizer.apply_chat_template(chat_msgs, tokenize=True, return_tensors="pt", **kw)

            input_ids = (
                encoded["input_ids"]
                if hasattr(encoded, "__getitem__") and not hasattr(encoded, "shape")
                else encoded
            ).to(model.device)

            with torch.no_grad():
                output_ids = model.generate(
                    input_ids,
                    max_new_tokens=_max_new,
                    do_sample=True,
                    temperature=0.1,
                    top_p=0.8,
                    top_k=20,
                    repetition_penalty=1.05,
                    pad_token_id=tokenizer.eos_token_id
                )

            raw_text = tokenizer.decode(output_ids[0][input_ids.shape[-1]:], skip_special_tokens=True)

        _log_ag.info(f"[REQ:{req_id}] [llm_node] RAW LLM OUTPUT ({len(raw_text)} chars):\n{raw_text!r}")

        tcs = _parse_tool_calls(raw_text)
        content = re.sub(r'<tool_call>[\s\S]*?</tool_call>', '', raw_text).strip()

        _ns_prepend = ""
        for m in invoke_msgs:
            if isinstance(m, HumanMessage):
                _m = re.match(r'^§NS_PREFIX§(.*?)§END_NS§\n\n', m.content, re.DOTALL)
                if _m:
                    _ns_prepend = _m.group(1).strip() + "\n\n"
                break

        if _ns_prepend:
            content = _ns_prepend + content

        response = AIMessage(content=content, tool_calls=tcs)

        if tcs:
            updates.append(f"🔧 {', '.join(tc['name'] for tc in tcs)}")

        return {
            "messages": [response],
            "tool_calls_made": state.get("tool_calls_made", []),
            "iteration": itr,
            "status_updates": updates,
        }

    def router(state: AgentState) -> Literal["tools", "end"]:
        if state.get("iteration", 0) >= 6:
            return "end"

        tcs = getattr(state["messages"][-1], "tool_calls", None)
        if not tcs:
            return "end"

        already = state.get("tool_calls_made", [])
        pending = [tc["name"] for tc in tcs]

        disable_loop_protection = getattr(config, "DISABLE_LOOP_PROTECTION", False)

        if not disable_loop_protection and already and all(name in already for name in pending):
            return "end"

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
    if _agent is None:
        _agent = build_agent()
    return _agent

def _clean_response(text: str, user_question: str = "") -> str:
    text = re.sub(r'<think>[\s\S]*?</think>\s*', '', text)

    # Safely strip formatting tokens without matching random content in between
    for tok in ['<|im_start|>', '<|im_end|>', '<s>', '</s>', '[INST]', '[/INST]', '<<SYS>>', '<</SYS>>']:
        text = text.replace(tok, '')

    text = text.strip()
    if text.startswith("assistant\n"):
        text = text[10:]

    if user_question:
        q_stripped, escaped = user_question.strip(), re.escape(user_question.strip())
        text = re.sub(r'(?i)(\s*' + escaped + r'[?!.]?\s*){2,}', ' ', text)
        text = re.sub(r'(?i)^\s*' + escaped + r'[?!.]?\s*\n', '', text)

    text = re.sub(r'Summarise the above tool results.*', '', text, flags=re.IGNORECASE)
    return re.sub(r'\n{3,}', '\n\n', text).strip()

async def run_agent(user_message: str, skip_synthesise: bool = False) -> dict:
    import uuid
    req_id = uuid.uuid4().hex[:8]
    config.logger.info(f"[REQ:{req_id}] /api/ask  q={user_message[:120]!r}")
    _runnable_agent = get_agent()
    t0 = time.time()

    final = await _runnable_agent.ainvoke({"messages": [HumanMessage(content=user_message)], "tool_calls_made": [], "iteration": 0, "status_updates": [f"🤖 Model: {config.LLM_MODEL}"], "req_id": req_id, "skip_synthesise": skip_synthesise})
    elapsed, last = time.time() - t0, final["messages"][-1]
    raw = last.content if hasattr(last, "content") else str(last)
    updates = final.get("status_updates", [])
    updates.append(f"✅ Done in {elapsed:.0f}s")

    _final_cleaned = _clean_response(raw, user_message)
    config.logger.info(f"[REQ:{req_id}] done elapsed={elapsed:.1f}s tools={final.get('tool_calls_made', [])}\n[QUESTION] {user_message}\n[FINAL ANSWER]\n{_final_cleaned}\n[END FINAL ANSWER]")
    return {"response": _final_cleaned, "tools_used": final.get("tool_calls_made", []), "iterations": final.get("iteration", 0), "status_updates": updates, "elapsed_seconds": round(elapsed, 1), "clarification_needed": False}

async def run_agent_streaming(user_message: str, history: list = None, max_new_tokens: int = 0, skip_synthesise: bool = False):
    import uuid, asyncio
    req_id, t0 = uuid.uuid4().hex[:8], time.time()
    _runnable_agent = get_agent()

    yield _sse({"type": "status", "text": f"🤖 Model: {config.LLM_MODEL}"})
    config.logger.info(f"[REQ:{req_id}] /chat/stream  q={user_message[:120]!r}")

    _saved_max = config.MAX_NEW_TOKENS
    if max_new_tokens > 0: config.MAX_NEW_TOKENS = max_new_tokens

    all_updates, tools_called, final_answer, iteration_count = [f"🤖 Model: {config.LLM_MODEL}"], [], "", 0
    last_tool_content = ""  # <-- Added fallback tracking variable
    _hb_queue, _hb_stop = asyncio.Queue(), asyncio.Event()

    async def _heartbeat_task():
        tick = 0
        while not _hb_stop.is_set():
            try: await asyncio.wait_for(_hb_stop.wait(), timeout=15)
            except asyncio.TimeoutError: pass
            else: break  # stop event fired — exit cleanly without putting a tick
            tick += 15
            if not _hb_stop.is_set(): await _hb_queue.put(tick)
    _hb_task = asyncio.ensure_future(_heartbeat_task())

    try:
        from langchain_core.messages import AIMessage as _AIMessage
        history_msgs = [HumanMessage(content=t.content) if t.role == "user" else _AIMessage(content=t.content) for t in (history or [])]
        all_messages = history_msgs + [HumanMessage(content=user_message)]

        async for event in _runnable_agent.astream_events(
            {"messages": all_messages, "tool_calls_made": [], "iteration": 0, "status_updates": [], "req_id": req_id, "skip_synthesise": skip_synthesise},
            version="v2",
            config={"recursion_limit": 12}
        ):
            while not _hb_queue.empty():
                tick = _hb_queue.get_nowait()
                yield _sse({"type": "heartbeat", "text": f"⏳ Still processing… ({tick}s elapsed)", "timeout": config.LLM_TIMEOUT})

            kind, name = event.get("event", ""), event.get("name", "")

            if kind == "on_chain_start" and name == "llm":
                iteration_count += 1
                txt = f"🧠 Loop {iteration_count} — LLM thinking…"
                config.logger.info(f"[REQ:{req_id}] {txt}")
                yield _sse({"type": "iteration", "iteration": iteration_count, "text": txt, "has_tool_calls": None})

            elif kind == "on_chain_end" and name in ["llm", "tools"]:
                output = event.get("data", {}).get("output", {})
                if not isinstance(output, dict):
                    continue

                node_updates = output.get("status_updates", [])
                for u in node_updates:
                    if u not in all_updates:
                        all_updates.append(u)
                        yield _sse({"type": "status", "text": u})

                node_tools = output.get("tool_calls_made", [])
                for t in node_tools:
                    if t not in tools_called:
                        tools_called.append(t)

                if name == "tools":
                    tool_texts = [m.content for m in output.get("messages", []) if hasattr(m, "content") and m.content]
                    if tool_texts:
                        last_tool_content = "\n\n---\n\n".join(tool_texts)
                        yield _sse({"type": "tool_chars", "chars": len(last_tool_content)})

                if name == "llm":
                    has_tool_calls = any(getattr(m, "tool_calls", None) for m in output.get("messages", []))

                    for m in output.get("messages", []):
                        if getattr(m, "content", "") and not getattr(m, "tool_calls", None):
                            final_answer = m.content

                    if has_tool_calls:
                        itr_txt = f"🔄 Loop {iteration_count} — LLM called tools, waiting for results…"
                    elif final_answer:
                        itr_txt = f"✍️ Loop {iteration_count} — LLM synthesising final answer…"
                    else:
                        itr_txt = f"🔄 Loop {iteration_count} — LLM processing…"

                    yield _sse({"type": "iteration", "iteration": iteration_count, "text": itr_txt, "has_tool_calls": has_tool_calls})

        elapsed = round(time.time() - t0, 1)
        _hb_stop.set(); _hb_task.cancel()

        _final_cleaned = _clean_response(final_answer, user_message)

        if not _final_cleaned.strip():
            config.logger.warning(f"[REQ:{req_id}] LLM returned blank response. Falling back to raw tool output.")

            if last_tool_content:
                _final_cleaned = f"⚠️ The AI could not synthesize a summary, but here is the raw data (generated by the tools):\n\n{last_tool_content}"
            else:
                _final_cleaned = "⚠️ The AI encountered an error and returned a blank response without retrieving any data."

        config.logger.info(f"[REQ:{req_id}] stream_done elapsed={elapsed}s tools={list(set(tools_called))}\n[QUESTION] {user_message}\n[FINAL ANSWER]\n{_final_cleaned}\n[END FINAL ANSWER]")
        yield _sse({"type": "status", "text": f"✅ Done in {elapsed}s"})
        yield _sse({
            "type": "result",
            "response": _final_cleaned,
            "tools_used": list(dict.fromkeys(tools_called)),
            "iterations": iteration_count,
            "status_updates": all_updates,
            "elapsed_seconds": elapsed,
            "clarification_needed": False
        })

    except Exception as exc:
        _hb_stop.set(); _hb_task.cancel()
        config.logger.error(f"[Stream Error] {exc}", exc_info=True)
        yield _sse({"type": "error", "text": str(exc)})
    finally:
        config.MAX_NEW_TOKENS = _saved_max

def _llm_synthesise(context: str, question: str, top_k: int = 50, max_tokens: int = 0) -> str:
    try:
        tok, mdl, is_q3 = globals()["_kb_tokenizer"], globals()["_kb_model"], globals()["_kb_is_qwen3"]
    except KeyError:
        return context or ""

    kb_prompt_path = config._HERE / "config" / "kb_prompt.txt"
    if kb_prompt_path.exists():
        sys_prompt = kb_prompt_path.read_text(encoding="utf-8")
    else:
        sys_prompt = (
            "You are the ECS Knowledge Bot for Cloudera ECS. Your job is to answer questions using ONLY the provided Knowledge Base Context.\n"
            "1. If the context says 'KB_EMPTY' or 'No relevant documentation', tell the user you don't have the answer in your database. Do not make things up.\n"
            "2. If the user greets you or asks what you can do, politely introduce yourself. Tell them you can search for 'Known Issues', 'Past Learnings', 'Dos and Donts', and 'Prerequisites'.\n"
            "3. For technical questions, answer STRICTLY using the context provided."
        )

    user_msg = f"[KNOWLEDGE BASE CONTEXT]\n{context}\n[END CONTEXT]\n\nQuestion: {question}"

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
            input_ids = (encoded["input_ids"] if hasattr(encoded, "__getitem__") and not hasattr(encoded, "shape") else encoded).to(mdl.device)
            with torch.no_grad():
                out = mdl.generate(input_ids, max_new_tokens=_max_out, do_sample=False, temperature=1.0, repetition_penalty=1.05, pad_token_id=tok.eos_token_id)
            raw = tok.decode(out[0][input_ids.shape[-1]:], skip_special_tokens=True)

        return re.sub(r'<think>[\s\S]*?</think>\s*', '', raw).strip() or context or ""
    except Exception as exc:
        return context or ""

# ── 3. FASTAPI SETUP & LIFESPAN ──────────────────────────────────────────────

def _run_startup_checks():
    SMOKE_TESTS = [("get_node_info", {}), ("get_namespace_status", {}), ("get_pod_status", {"namespace": "all"})]
    config.logger.info("[Self-test] Running kubectl tool smoke-tests…")
    for name, kwargs in SMOKE_TESTS:
        cfg = K8S_TOOL_METADATA.get(name)
        if cfg is None: continue
        try:
            result = cfg["fn"](**kwargs)
            if "error" in result.lower(): config.logger.warning(f"[Self-test] ⚠ {name}: {result[:120]}")
            else: config.logger.info(f"[Self-test] ✓ {name}: {result.replace(chr(10), ' ')[:80]}…")
        except Exception as e: config.logger.warning(f"[Self-test] ⚠ {name} raised: {e}")

@asynccontextmanager
async def _lifespan(app: FastAPI):
    config.logger.info("=" * 60)
    config.logger.info(f"Cloudera ECS AI Ops")
    config.logger.info(f"  Creator  : dennislee@cloudera.com")
    config.logger.info(f"  LLM      : {config.LLM_MODEL}")
    config.logger.info(f"  Embed    : {config.EMBED_MODEL}")
    config.logger.info(f"  GPU      : {config.NUM_GPU} GPU(s)")
    config.logger.info(f"  Tools    : {len(K8S_TOOL_METADATA) + len(RAG_TOOLS)} total tools registered")
    config.logger.info(f"  LanceDB  : {config.LANCEDB_DIR}")
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

    yield

    config.logger.info("Shutting down")

app = FastAPI(title="Cloudera ECS AI Ops", lifespan=_lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

if _HERE.joinpath("web", "static").exists():
    app.mount("/static", StaticFiles(directory=str(_HERE / "web" / "static")), name="static")

# ── 4. API ENDPOINTS ─────────────────────────────────────────────────────────

def _gpu_metrics() -> list:
    gpus = []
    try:
        import pynvml
        pynvml.nvmlInit()
        for i in range(pynvml.nvmlDeviceGetCount()):
            h    = pynvml.nvmlDeviceGetHandleByIndex(i)
            name = pynvml.nvmlDeviceGetName(h)
            if isinstance(name, bytes):
                name = name.decode()
            util = pynvml.nvmlDeviceGetUtilizationRates(h)
            mem  = pynvml.nvmlDeviceGetMemoryInfo(h)
            try:    temp = pynvml.nvmlDeviceGetTemperature(h, pynvml.NVML_TEMPERATURE_GPU)
            except: temp = 0
            try:    pw   = round(pynvml.nvmlDeviceGetPowerUsage(h) / 1000.0, 1)
            except: pw   = None
            gpus.append({
                "index":        i,
                "name":         name,
                "util_pct":     util.gpu,
                "mem_used_gb":  round(mem.used  / 1e9, 1),
                "mem_total_gb": round(mem.total / 1e9, 1),
                "mem_pct":      round(mem.used  / mem.total * 100, 1),
                "temp_c":       temp,
                "power_w":      pw,
            })
        pynvml.nvmlShutdown()
    except Exception:
        pass
    return gpus

@app.get("/api", summary="API index — lists every endpoint with curl examples")
@app.get("/api/", include_in_schema=False)
async def api_index(request: Request):
    base = str(request.base_url).rstrip("/")
    return {
        "description": "Cloudera ECS AI Ops — curl-friendly REST API",
        "endpoints": [
            {"method": "POST", "path": "/api/ask",            "description": "Ask the AI chatbot (blocking)",
             "curl": f'curl -s -X POST {base}/api/ask -H "Content-Type: application/json" -d \'{{"q":"list all pods with problems", "skip_synthesise": true}}\''},
            {"method": "POST", "path": "/api/tool",           "description": "Call a specific K8s tool directly",
             "curl": f'curl -s -X POST {base}/api/tool -H "Content-Type: application/json" -d \'{{"name":"get_pod_status","args":{{"namespace":"all","show_all":true}}}}\''},
            {"method": "GET",  "path": "/api/tools",          "description": "List all registered tools and their signatures"},
            {"method": "GET",  "path": "/api/rag/stats",      "description": "LanceDB document and Excel row statistics"},
            {"method": "GET",  "path": "/api/rag/files",      "description": "List all previously ingested filenames"},
            {"method": "GET",  "path": "/api/rag/query",      "description": "RAG-only query for Knowledge Bot (no LLM, no truncation)"},
            {"method": "GET",  "path": "/metrics",            "description": "Live CPU / RAM / GPU metrics"},
        ],
    }

@app.get("/health")
async def health():
    stats = get_doc_stats()
    return {"status": "ok", "model": config.LLM_MODEL, "num_gpu": config.NUM_GPU, "lancedb_docs": stats["docs_chunks"], "lancedb_excel_rows": stats["excel_rows"], "K8S_TOOL_METADATA": len(K8S_TOOL_METADATA), "cluster_server": config.CLUSTER_SERVER}

@app.post("/api/kubeconfig")
async def apply_kubeconfig(req: KubeconfigRequest):
    try:
        result = reload_kubeconfig(req.kubeconfig)
        if result.get("server") and result["server"] != "unknown":
            import re
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
    _timezone_ctx.set(req.timezone or "UTC")

    import asyncio
    async def _keepalive_stream():
        queue, _SENTINEL = asyncio.Queue(), object()
        async def _producer():
            try:
                async for chunk in run_agent_streaming(req.message, req.history, req.max_new_tokens, req.skip_synthesise): await queue.put(chunk)
            finally: await queue.put(_SENTINEL)
        task = asyncio.ensure_future(_producer())
        try:
            while True:
                try: item = await asyncio.wait_for(queue.get(), timeout=10)
                except asyncio.TimeoutError: yield ": keep-alive\n\n"; continue
                if item is _SENTINEL:
                    await asyncio.sleep(0)  # yield to event loop so uvicorn flushes the last SSE chunk
                    break
                yield item
        finally: task.cancel()
    return StreamingResponse(_keepalive_stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

def _collect_metrics() -> dict:
    cpu_per = psutil.cpu_percent(interval=0.2, percpu=True)
    mem     = psutil.virtual_memory()
    freq    = psutil.cpu_freq()
    cpu_total = round(psutil.cpu_percent(interval=None), 1)
    return {
        "cpu_total":       cpu_total,
        "cpu_total_pct":   cpu_total,
        "cpu_per_core":    [round(p, 1) for p in cpu_per],
        "cpu_count":       psutil.cpu_count(logical=True),
        "freq_mhz":        round(freq.current) if freq else 0,
        "load_avg":        [round(x, 2) for x in psutil.getloadavg()],
        "mem_total_gb":    round(mem.total / 1e9, 1),
        "mem_used_gb":     round(mem.used  / 1e9, 1),
        "mem_pct":         mem.percent,
        "gpus":            _gpu_metrics(),
        "num_gpu":         config.NUM_GPU,
    }

@app.get("/metrics")
async def metrics():
    return _collect_metrics()

@app.get("/api/system", summary="Live CPU / RAM / GPU utilisation metrics")
async def api_system():
    return _collect_metrics()

@app.post("/ingest")
async def ingest_api(req: IngestRequest):
    results = ingest_directory(req.docs_dir, force=req.force)
    return _ingest_response(results)

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
    return _ingest_response(results)

@app.post("/api/ask")
async def api_ask(req: AskRequest):
    if not req.q.strip(): return _JSONResponse(status_code=400, content={"error": "q must not be empty"})
    try:
        result = await run_agent(req.q, skip_synthesise=req.skip_synthesise)
        return {"question": req.q, "answer": result["response"], "tools_used": result["tools_used"], "iterations": result["iterations"], "elapsed_seconds": result["elapsed_seconds"]}
    except Exception as e: return _JSONResponse(status_code=500, content={"error": str(e)})

_REPORT_DIR = _HERE / "report"

@app.get("/api/healthcheck-report", summary="Generate health report, save to /report/, return filename")
async def api_healthcheck_report():
    import asyncio, re as _re, uuid, time as _time
    req_id  = uuid.uuid4().hex[:8]
    t_start = _time.monotonic()
    config.logger.info(f"[REQ:{req_id}] /api/healthcheck-report  generating full cluster health report")
    try:
        _REPORT_DIR.mkdir(parents=True, exist_ok=True)

        report = await asyncio.get_event_loop().run_in_executor(
            None, _tk.generate_healthcheck_report)
        report = _re.sub(r'```[^\n]*\n?', '', report)
        report = _re.sub(r'\n{3,}', '\n\n', report).strip()

        charts = {}
        try:
            charts = await asyncio.get_event_loop().run_in_executor(
                None, _fetch_report_charts)
        except Exception as e:
            config.logger.warning(f"[REQ:{req_id}] /api/healthcheck-report  charts failed: {e}")

        elapsed = _time.monotonic() - t_start
        config.logger.info(
            f"[REQ:{req_id}] /api/healthcheck-report  done elapsed={elapsed:.1f}s "
            f"report={len(report)}chars charts={list(charts.keys())}")

        return {"report": report, "charts": charts}
    except Exception as e:
        config.logger.error(f"[REQ:{req_id}] /api/healthcheck-report  error: {e}")
        return _JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/reports/save", summary="Save compiled report to /report/ folder — PDF if weasyprint available, else HTML")
async def api_reports_save(request: Request):
    import uuid, time as _time, asyncio
    req_id = uuid.uuid4().hex[:8]
    try:
        body = await request.json()
        html = body.get("html", "")
        if not html:
            return _JSONResponse(status_code=400, content={"error": "html field required"})
        _REPORT_DIR.mkdir(parents=True, exist_ok=True)
        ts = _time.strftime("%Y%m%d-%H%M%S")

        # Try weasyprint PDF first
        try:
            import weasyprint as _wp
            filename = f"ecs-health-report-{ts}.pdf"
            filepath = _REPORT_DIR / filename
            def _write_pdf():
                _wp.HTML(string=html).write_pdf(str(filepath))
            await asyncio.get_event_loop().run_in_executor(None, _write_pdf)
            size_kb  = filepath.stat().st_size // 1024
            config.logger.info(f"[REQ:{req_id}] /api/reports/save  saved PDF {filename} ({size_kb} KB)")
            return {"filename": filename, "size_kb": size_kb, "format": "pdf"}
        except ImportError:
            pass
        except Exception as pdf_err:
            config.logger.warning(f"[REQ:{req_id}] /api/reports/save  weasyprint failed ({pdf_err}), falling back to HTML")

        # Fallback: save as HTML
        filename = f"ecs-health-report-{ts}.html"
        filepath = _REPORT_DIR / filename
        filepath.write_text(html, encoding="utf-8")
        size_kb  = filepath.stat().st_size // 1024
        config.logger.info(f"[REQ:{req_id}] /api/reports/save  saved HTML {filename} ({size_kb} KB)")
        return {"filename": filename, "size_kb": size_kb, "format": "html"}

    except Exception as e:
        config.logger.error(f"[REQ:{req_id}] /api/reports/save  error: {e}")
        return _JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/reports", summary="List report files present in /report/ folder")
async def api_reports_list():
    try:
        if not _REPORT_DIR.exists():
            return {"reports": []}
        files = []
        for f in sorted(
            list(_REPORT_DIR.glob("*.pdf")) + list(_REPORT_DIR.glob("*.html")),
            key=lambda x: x.stat().st_mtime, reverse=True
        ):
            stat = f.stat()
            files.append({
                "filename": f.name,
                "size_kb":  stat.st_size // 1024,
                "created":  stat.st_mtime,
                "format":   f.suffix.lstrip("."),
            })
        return {"reports": files}
    except Exception as e:
        return _JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/reports/{filename}", summary="Download a report file from /report/ folder")
async def api_reports_download(filename: str):
    import re
    if not re.match(r'^[\w\-\.]+\.(html|pdf)$', filename):
        raise HTTPException(status_code=400, detail="Invalid filename")
    filepath = _REPORT_DIR / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail=f"{filename} not found")
    media_type = "application/pdf" if filename.endswith(".pdf") else "text/html"
    return FileResponse(
        path=str(filepath),
        media_type=media_type,
        filename=filename,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'})


def _fetch_report_charts() -> dict:
    """
    Query Prometheus for top-10 pod CPU and memory over 1d, 1w, 1m.
    Returns a dict: { "cpu_1d": [...series...], "mem_1d": [...], ... }
    Each series: { "label": "ns/pod", "values": [[ts, val], ...] }
    """
    import time as _time
    from kubernetes.stream import stream as _k8s_stream

    prom_pod = prom_ns = prom_container = None
    try:
        try:
            all_pods = _tk._core.list_pod_for_all_namespaces(
                    field_selector="status.phase=Running").items
        except Exception:
            # Some clusters (e.g. RKE2) raise WebSocket errors on field_selector
            all_pods = _tk._core.list_pod_for_all_namespaces().items
        for p in all_pods:
            if p.status.phase != "Running":
                continue
            n = p.metadata.name.lower()
            if "prometheus-server" in n and "operator" not in n:
                cnames         = [c.name for c in (p.spec.containers or [])]
                prom_pod       = p.metadata.name
                prom_ns        = p.metadata.namespace
                prom_container = ("prometheus-server" if "prometheus-server" in cnames
                                  else (cnames[0] if cnames else "prometheus-server"))
                break
    except Exception:
        pass
    if not prom_pod:
        return {}

    def _exec(cmd):
        try:
            ws = _k8s_stream(
                _tk._core.connect_get_namespaced_pod_exec,
                prom_pod, prom_ns,
                command=["/bin/sh", "-c", cmd],
                container=prom_container,
                stderr=False, stdin=False, stdout=True, tty=False,
                _preload_content=False)
            chunks = []
            while ws.is_open():
                ws.update(timeout=120)
                if ws.peek_stdout():
                    chunks.append(ws.read_stdout())
            ws.close()
            return "".join(chunks).strip()
        except Exception as exc:
            return f"[exec error: {exc}]"

    base_url = "http://localhost:9090"
    probe    = _exec(f"curl -s -o /dev/null -w '%{{http_code}}' '{base_url}/prometheus/api/v1/query?query=up'")
    api_base = (f"{base_url}/prometheus/api/v1" if probe.strip() == "200"
                else f"{base_url}/api/v1")

    now_ts = int(_time.time())
    windows = {
        "1d": (now_ts - 86400,      now_ts, "5m"),
        "1w": (now_ts - 86400 * 7,  now_ts, "30m"),
        "1m": (now_ts - 86400 * 30, now_ts, "2h"),
    }

    cpu_pql = ('sum by (pod, namespace) '
               '(rate(container_cpu_usage_seconds_total'
               '{container!="",container!="POD"}[5m])) * 1000')
    mem_pql = ('sum by (pod, namespace) '
               '(container_memory_working_set_bytes'
               '{container!="",container!="POD"}) / 1048576')

    import json as _json, urllib.parse

    def _query_range(pql, start, end, step):
        enc = urllib.parse.quote(pql, safe="")
        url = f"{api_base}/query_range?query={enc}&start={start}&end={end}&step={step}"
        raw = _exec(f"curl -s --max-time 120 '{url}'")
        if not raw or raw.startswith("[exec error"):
            return []
        try:
            data = _json.loads(raw)
        except Exception:
            return []
        if data.get("status") != "success":
            return []
        results = data.get("data", {}).get("result", [])

        def _mean(r):
            vals = [float(v[1]) for v in r.get("values", []) if v[1] != "NaN"]
            return sum(vals) / len(vals) if vals else 0.0

        results.sort(key=_mean, reverse=True)
        series = []
        for r in results[:10]:
            ml    = r.get("metric", {})
            ns_v  = ml.get("namespace", "")
            pod_v = ml.get("pod", ml.get("pod_name", "?"))
            label = f"{ns_v}/{pod_v}" if ns_v else pod_v
            series.append({
                "label":  label,
                "values": [[float(ts), float(v)]
                           for ts, v in r.get("values", [])
                           if v != "NaN"],
            })
        return series

    result = {}
    for win, (start, end, step) in windows.items():
        result[f"cpu_{win}"] = _query_range(cpu_pql, start, end, step)
        result[f"mem_{win}"] = _query_range(mem_pql, start, end, step)
    return result

@app.post("/api/tool")
async def api_tool(req: ToolCallRequest):
    import asyncio, inspect
    entry = K8S_TOOL_METADATA.get(req.name)
    if not entry: return _JSONResponse(status_code=404, content={"error": f"Tool '{req.name}' not found.", "available": list(K8S_TOOL_METADATA.keys())})
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
    for name, entry in K8S_TOOL_METADATA.items():
        fn = entry.get("fn")
        params = {}
        if fn:
            for pname, p in _inspect.signature(fn).parameters.items():
                params[pname] = {"default": None if p.default is _inspect.Parameter.empty else p.default, "required": p.default is _inspect.Parameter.empty}
        out[name] = {"description": entry.get("description", ""), "parameters": params}
    return {"count": len(out), "tools": out}

@app.get("/api/rag/stats")
async def api_rag_stats():
    return get_doc_stats()

@app.get("/api/rag/files")
async def api_rag_files():
    docs_dir = config._HERE / "docs"
    if not docs_dir.exists():
        return {"count": 0, "files": []}

    # List all supported files currently in the docs directory
    files = [f.name for f in docs_dir.iterdir() if f.is_file() and f.suffix.lower() in (".md", ".pdf", ".txt", ".xlsx", ".xls")]
    return {"count": len(files), "files": sorted(files)}

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
    top_k, sheet = max(10, min(req.top_k, 500)), req.sheet or _detect_sheet(req.q)

    import asyncio
    context = await asyncio.get_event_loop().run_in_executor(None, lambda: rag_retrieve(query=req.q, top_k=top_k, sheet=sheet))
    no_rag = not context.strip() or context == "No relevant documentation found." or (isinstance(context, str) and context.startswith("KB_EMPTY:"))

    # Guardrail
    if no_rag:
        ans = _MSG_NO_INGEST if "KB_EMPTY" in str(context) else "No relevant documentation found in the Knowledge Base."
        return {"answer": ans, "query": req.q, "top_k": top_k}

    answer = await asyncio.get_event_loop().run_in_executor(None, lambda: _llm_synthesise(context, req.q, top_k, req.max_tokens))
    return {"answer": answer or "I'm sorry, I was unable to generate a response.", "query": req.q, "top_k": top_k}

@app.post("/api/kb/stream")
async def api_kb_stream(req: KbAskRequest):
    import asyncio as _asyncio, time as _time

    async def _generate():
        start, q = _time.time(), req.q.strip()

        if not q:
            yield _sse({"type": "error", "text": "Empty query"})
            return

        # --- SANITY CHECK: ONLY BLOCK PURE GIBBERISH (e.g. "a a a") ---
        words = q.split()
        max_word_len = max((len(w) for w in words), default=0)

        if len(words) < 3 or max_word_len < 3:
            yield _sse({
                "type": "result",
                "answer": "Please ask a complete question or provide a more detailed statement (e.g., 'Why my vault pod is not running?' or 'List all known issues with Longhorn in 1.5.5 SP1').",
                "query": q,
                "elapsed": 0.0,
                "top_k": req.top_k
            })
            return
        # --------------------------------------------------------------

        top_k, sheet = max(10, min(req.top_k, 500)), req.sheet or _detect_sheet(q)

        yield _sse({"type": "question", "text": q})
        yield _sse({"type": "status", "text": f"Searching knowledge base{(' · sheet: ' + sheet) if sheet else ''}…"})

        try:
            # 1. Search the DB
            context = await _asyncio.get_event_loop().run_in_executor(None, lambda: rag_retrieve(query=q, top_k=top_k, sheet=sheet))
            no_rag = not context.strip() or context == "No relevant documentation found." or (isinstance(context, str) and context.startswith("KB_EMPTY:"))

            # 2. DO NOT ABORT IF DB IS EMPTY. Format it for the LLM instead.
            if no_rag:
                context = "KB_EMPTY"

            yield _sse({"type": "status", "text": f"Synthesising answer…"})

            # 3. ALWAYS INVOKE THE LLM
            answer = await _asyncio.get_event_loop().run_in_executor(None, lambda: _llm_synthesise(context, q, top_k, req.max_tokens))

            yield _sse({"type": "result", "answer": answer or "I'm sorry, I was unable to generate a response.", "query": q, "elapsed": round(_time.time() - start, 1), "top_k": top_k})

        except Exception as exc:
            yield _sse({"type": "error", "text": str(exc)})

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
    return {
        "kubectl_max_chars":       _tk._KUBECTL_MAX_OUT,
        "max_new_tokens":          config.MAX_NEW_TOKENS,
        "llm_timeout":             config.LLM_TIMEOUT,
        "disable_loop_protection": getattr(config, "DISABLE_LOOP_PROTECTION", False),
        "show_secret_values":      getattr(config, "SHOW_SECRET_VALUES", False),
    }

@app.post("/api/config")
async def api_set_config(body: dict):
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
    if "disable_loop_protection" in body:
        val = bool(body["disable_loop_protection"])
        config.DISABLE_LOOP_PROTECTION = val
        updated["disable_loop_protection"] = val
    if "show_secret_values" in body:
        val = bool(body["show_secret_values"])
        config.SHOW_SECRET_VALUES = val
        updated["show_secret_values"] = val

    if updated:
        save_settings() # Persist to settings.json

    return {"ok": True, "updated": updated}

@app.get("/")
async def serve_ui():
    if _HERE.joinpath("web", "index.html").exists():
        return FileResponse(str(_HERE / "web" / "index.html"), media_type="text/html")
    return _JSONResponse(status_code=404, content={"error": "web/index.html not found"})

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

    uvicorn.run("app:app", host=config.ARGS.host, port=config.ARGS.port, reload=config.ARGS.reload, log_level="warning")
