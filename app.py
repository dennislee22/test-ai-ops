#!/usr/bin/env python3
"""
Cloudera ECS AI Ops
"""

import os, sys, argparse, re, hashlib, time, json, logging, logging.handlers
from pathlib import Path
from typing import Annotated, TypedDict, Literal, Optional
from contextvars import ContextVar

import psutil
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, APIRouter, UploadFile, File, Form as FastAPIForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse as _JSONResponse
from pydantic import BaseModel

from langchain_core.messages import HumanMessage, ToolMessage, SystemMessage, AIMessage
from langchain_core.tools import tool
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages

from agent.bypass import should_bypass_llm, build_direct_answer
from agent.routing import default_tools_for, resolve_namespace, NS_ALIASES


_HERE = Path(__file__).resolve().parent

sys.path.insert(0, str(_HERE / "tools"))

_pre = argparse.ArgumentParser(add_help=False)
_pre.add_argument("--model-dir", default=None)
_pre.add_argument("--embed-dir", default=None)
_pre.add_argument("--ingest",    default=None)
_pre.add_argument("--force",     action="store_true")
_pre.add_argument("--port",      type=int, default=8000)
_pre.add_argument("--host",      default="0.0.0.0")
_pre.add_argument("--reload",    action="store_true")
_ARGS, _ = _pre.parse_known_args()

_env_file = _HERE / "env"
if _env_file.exists():
    from dotenv import load_dotenv
    load_dotenv(_env_file)

os.environ.setdefault("LLM_MODEL",       "Qwen/Qwen3-8B")
os.environ.setdefault("EMBED_MODEL",     "nomic-ai/nomic-embed-text-v1.5")
os.environ.setdefault("KUBECONFIG_PATH", "~/kubeconfig")
os.environ.setdefault("LANCEDB_DIR",     str(Path(__file__).resolve().parent / "lancedb"))

def _read_cluster_server() -> str:
    try:
        import yaml as _y
        kc = os.path.expanduser(os.getenv("KUBECONFIG_PATH", "~/kubeconfig"))
        if not Path(kc).exists():
            return "unknown"
        with open(kc) as f:
            cfg = _y.safe_load(f)
        clusters = cfg.get("clusters", [])
        if clusters:
            server = clusters[0].get("cluster", {}).get("server", "")
            if server:
                return re.sub(r'^https?://', '', server).strip()
    except Exception:
        pass
    return "unknown"

CLUSTER_SERVER = _read_cluster_server()

os.environ.setdefault("LOG_LEVEL",       "DEBUG")
os.environ.setdefault("LANCEDB_DIR",     str(_HERE / "lancedb"))
os.environ.setdefault("CUSTOM_RULES",    "- Do NOT recommend migrating to cgroupv2. This environment uses cgroupv1.")

if _ARGS.model_dir:
    os.environ["LLM_MODEL"] = _ARGS.model_dir
if _ARGS.embed_dir:
    os.environ["EMBED_MODEL"] = _ARGS.embed_dir

LLM_MODEL       = os.getenv("LLM_MODEL",           "Qwen/Qwen3-8B").strip()
EMBED_MODEL     = os.getenv("EMBED_MODEL",         "nomic-ai/nomic-embed-text-v1.5").strip()
LANCEDB_DIR     = os.getenv("LANCEDB_DIR",         str(_HERE / "lancedb"))
CUSTOM_RULES    = os.getenv("CUSTOM_RULES",        "").strip()

# tool-call JSON (e.g. small local models like Qwen3-8B).
ENABLE_FALLBACK_ROUTING: bool = True

# Runtime-adjustable generation cap.  Exposed via GET/POST /api/config and
# the Settings → Max Output tab.  Synthesis calls use this value; tool-
# selection calls always use 256 (just a small <tool_call> JSON block).
_MAX_NEW_TOKENS: int = int(os.getenv("MAX_NEW_TOKENS", "4096"))

# Request-scoped flag — True when the user has 'Show Secret Values' enabled in Settings.
# Set per-request in chat_stream; read by agent/routing.py via get_decode_secrets().
_decode_secrets_ctx: ContextVar[bool] = ContextVar("decode_secrets", default=False)

def get_decode_secrets() -> bool:
    return _decode_secrets_ctx.get()

def _detect_gpu_count() -> int:
    explicit = os.getenv("NUM_GPU")
    if explicit is not None:
        return int(explicit)
    try:
        import pynvml
        pynvml.nvmlInit()
        n = pynvml.nvmlDeviceGetCount()
        pynvml.nvmlShutdown()
        return n
    except Exception:
        pass
    try:
        import subprocess
        out = subprocess.check_output(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            timeout=5, stderr=subprocess.DEVNULL)
        return len([l for l in out.decode().strip().splitlines() if l.strip()])
    except Exception:
        pass
    return 0

NUM_GPU = _detect_gpu_count()

# Runtime-adjustable request timeout (seconds).  Exposed via GET/POST /api/config
# and the Settings → LLM Input/Output tab.  Defaults to 900s on CPU, 300s on GPU.
_LLM_TIMEOUT: int = int(os.getenv("LLM_TIMEOUT", "0")) or (900 if NUM_GPU == 0 else 300)


_LOG_DIR = _HERE / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_LEVEL   = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
_FMT_CON = "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s"
_FMT_FIL = "%(asctime)s  %(levelname)-8s  [%(name)s]  %(filename)s:%(lineno)d  %(message)s"
_DATE    = "%Y-%m-%d %H:%M:%S"
_cfg_set: set = set()

def get_logger(name: str) -> logging.Logger:
    if name in _cfg_set:
        return logging.getLogger(name)
    log = logging.getLogger(name)
    log.setLevel(_LEVEL)
    if not log.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(_LEVEL)
        ch.setFormatter(logging.Formatter(_FMT_CON, datefmt=_DATE))
        log.addHandler(ch)
        fh = logging.handlers.RotatingFileHandler(
            _LOG_DIR / "app.log", maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
        fh.setLevel(_LEVEL)
        fh.setFormatter(logging.Formatter(_FMT_FIL, datefmt=_DATE))
        log.addHandler(fh)
        log.propagate = False
    _cfg_set.add(name)
    return log

for _noisy in ["httpx", "httpcore", "urllib3", "kubernetes.client", "langchain", "langsmith", "watchfiles", "lancedb"]:
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger   = get_logger("app")
_log_rag = get_logger("rag")
_log_ag  = get_logger("agent")


from tools_k8s import K8S_TOOLS, _core, reload_kubeconfig


CHUNK_SIZE    = 512
CHUNK_OVERLAP = 64
TOP_K         = 10

_embedder_fn  = None

# ── LanceDB globals ───────────────────────────────────────────────────────────
_lancedb_conn  = None   # lancedb.LanceDBConnection
_docs_table    = None   # LanceTable — prose chunks (.md/.pdf/.txt)
_excel_table   = None   # LanceTable — Excel structured rows

# Vector dimension — must match EMBED_MODEL output (nomic-embed-text-v1.5 = 768)
_EMBED_DIM = 768


def _get_embedder():
    global _embedder_fn
    if _embedder_fn is not None:
        return _embedder_fn

    _log_rag.info(f"[Embed] Loading SentenceTransformer: {EMBED_MODEL}")
    from sentence_transformers import SentenceTransformer
    import transformers as _tf
    _tf.logging.set_verbosity_error()

    if NUM_GPU > 0:
        device = "cuda"
        try:
            import torch
            if not torch.cuda.is_available():
                _log_rag.warning(
                    "[Embed] NUM_GPU=%d but torch.cuda.is_available()=False "
                    "(CUDA runtime issue?) — falling back to CPU", NUM_GPU
                )
                device = "cpu"
        except ImportError:
            pass
    else:
        device = "cpu"

    _log_rag.info(f"[Embed] device={device} (NUM_GPU={NUM_GPU})")
    _st = SentenceTransformer(EMBED_MODEL, device=device, trust_remote_code=True)

    def _local(text: str) -> list:
        return _st.encode(text, normalize_embeddings=True).tolist()

    _embedder_fn = _local
    return _embedder_fn


def embed_text(text: str) -> list:
    return _get_embedder()(text)


def _get_lancedb():
    """Return (connection, docs_table, excel_table), opening/creating as needed."""
    global _lancedb_conn, _docs_table, _excel_table
    if _lancedb_conn is not None:
        return _lancedb_conn, _docs_table, _excel_table

    import lancedb
    import pyarrow as pa

    Path(LANCEDB_DIR).mkdir(parents=True, exist_ok=True)
    _log_rag.info(f"[LanceDB] Opening store: {LANCEDB_DIR}")
    _lancedb_conn = lancedb.connect(LANCEDB_DIR)

    # ── Prose docs table ──────────────────────────────────────────────────────
    docs_schema = pa.schema([
        pa.field("id",          pa.utf8()),
        pa.field("vector",      pa.list_(pa.float32(), _EMBED_DIM)),
        pa.field("text",        pa.utf8()),
        pa.field("source",      pa.utf8()),
        pa.field("doc_type",    pa.utf8()),
        pa.field("chunk_index", pa.int32()),
        pa.field("file_hash",   pa.utf8()),
    ])
    if "docs" in _lancedb_conn.table_names():
        _docs_table = _lancedb_conn.open_table("docs")
    else:
        _docs_table = _lancedb_conn.create_table("docs", schema=docs_schema)
        _log_rag.info("[LanceDB] Created table: docs")

    # ── Excel structured-issues table ─────────────────────────────────────────
    # Primary search column: "symptom" — the vector is the embedding of the
    # symptom/search text only.  All other columns are stored as typed fields
    # and returned in full on retrieval, preserving column relationships.
    excel_schema = pa.schema([
        pa.field("id",            pa.utf8()),
        pa.field("vector",        pa.list_(pa.float32(), _EMBED_DIM)),
        pa.field("source_file",   pa.utf8()),   # original filename
        pa.field("file_hash",     pa.utf8()),   # for dedup / listing
        pa.field("sheet",         pa.utf8()),
        pa.field("symptom",       pa.utf8()),   # PRIMARY search column (embedded)
        pa.field("issue_id",      pa.utf8()),
        pa.field("category",      pa.utf8()),
        pa.field("problem",       pa.utf8()),
        pa.field("root_cause",    pa.utf8()),
        pa.field("fix",           pa.utf8()),
        pa.field("severity",      pa.utf8()),
        pa.field("present",       pa.utf8()),   # "Yes" = unresolved
        pa.field("jira",          pa.utf8()),
        pa.field("discovered",    pa.utf8()),
        pa.field("resolved",      pa.utf8()),
        pa.field("notes",         pa.utf8()),
        pa.field("do_text",       pa.utf8()),
        pa.field("dont_text",     pa.utf8()),
        pa.field("rationale",     pa.utf8()),
        pa.field("prerequisite",  pa.utf8()),
        pa.field("how_to_verify", pa.utf8()),
        pa.field("learning",      pa.utf8()),
        pa.field("action_taken",  pa.utf8()),
    ])
    if "excel_issues" in _lancedb_conn.table_names():
        _excel_table = _lancedb_conn.open_table("excel_issues")
    else:
        _excel_table = _lancedb_conn.create_table("excel_issues", schema=excel_schema)
        _log_rag.info("[LanceDB] Created table: excel_issues")

    return _lancedb_conn, _docs_table, _excel_table


def init_db():
    _get_lancedb()
    _get_embedder()


def chunk_text(text: str) -> list:
    chunks, start = [], 0
    text = text.strip()
    while start < len(text):
        end = start + CHUNK_SIZE
        if end < len(text):
            pb = text.rfind("\n\n", start, end)
            if pb > start + CHUNK_SIZE // 2:
                end = pb
            else:
                sb = max(text.rfind(". ", start, end), text.rfind(".\n", start, end))
                if sb > start + CHUNK_SIZE // 2:
                    end = sb + 1
        chunk = text[start:end].strip()
        if chunk: chunks.append(chunk)
        start = end - CHUNK_OVERLAP
    return chunks


def _doc_type(filename: str) -> str:
    n = filename.lower()
    if any(k in n for k in ["known", "issue", "bug", "error"]):   return "known_issue"
    if any(k in n for k in ["runbook", "playbook", "procedure"]): return "runbook"
    if any(k in n for k in ["dos", "donts", "guidelines"]):       return "dos_donts"
    return "general"


def ingest_file(file_path: str, force: bool = False) -> dict:
    """Ingest a prose document (.md, .pdf, .txt) into the LanceDB docs table."""
    path  = Path(file_path)
    fhash = hashlib.md5(path.read_bytes()).hexdigest()
    _, docs_tbl, _ = _get_lancedb()

    if not force:
        try:
            existing = docs_tbl.search().where(
                f"source = '{str(path)}' AND file_hash = '{fhash}'"
            ).limit(1).to_list()
            if existing:
                _log_rag.info(f"[RAG] Skip (unchanged): {path.name}")
                return {"file": path.name, "status": "skipped", "chunks": 0}
        except Exception:
            pass

    try:
        suffix = path.suffix.lower()
        if suffix == ".pdf":
            from pypdf import PdfReader
            text = "\n\n".join(p.extract_text() or "" for p in PdfReader(str(path)).pages)
        elif suffix == ".md":
            from markdown_it import MarkdownIt
            html = MarkdownIt().render(path.read_text(encoding="utf-8"))
            text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).strip()
        else:
            text = path.read_text(encoding="utf-8")
    except Exception as e:
        return {"file": path.name, "status": "error", "chunks": 0, "error": str(e)}

    if not text.strip(): return {"file": path.name, "status": "empty", "chunks": 0}

    chunks   = chunk_text(text)
    doc_type = _doc_type(path.name)
    _log_rag.info(f"[RAG] {path.name}: {len(chunks)} chunks  type={doc_type}")

    try:
        docs_tbl.delete(f"source = '{str(path)}'")
    except Exception:
        pass

    rows = []
    for i, ch in enumerate(chunks):
        rows.append({
            "id":          f"{fhash}_{i}",
            "vector":      embed_text(ch),
            "text":        ch,
            "source":      str(path),
            "doc_type":    doc_type,
            "chunk_index": i,
            "file_hash":   fhash,
        })
    docs_tbl.add(rows)
    return {"file": path.name, "status": "ingested", "chunks": len(chunks), "doc_type": doc_type}


def ingest_excel(file_path: str, force: bool = False) -> dict:
    """
    Ingest a structured Excel knowledge base into the LanceDB excel_issues table.

    Each sheet is processed with column-aware embedding:
      - Known Issues   → vector = embed(symptom)
      - Dos and Don'ts → vector = embed(do_text + " / " + dont_text)
      - Prerequisites  → vector = embed(prerequisite)
      - Past Learnings → vector = embed(what_went_wrong + " / " + key_learning)

    The full row is stored alongside the vector so retrieval returns
    complete structured context to the LLM — not just the matched text.
    """
    try:
        import pandas as pd
    except ImportError:
        return {"file": Path(file_path).name, "status": "error", "chunks": 0,
                "error": "pandas not installed — pip install pandas openpyxl"}

    path  = Path(file_path)
    fhash = hashlib.md5(path.read_bytes()).hexdigest()
    _, _, excel_tbl = _get_lancedb()

    def _s(val) -> str:
        if val is None: return ""
        try:
            import math
            if isinstance(val, float) and math.isnan(val): return ""
        except Exception:
            pass
        return str(val).strip()

    rows  = []
    total = 0

    try:
        xl = pd.read_excel(str(path), sheet_name=None, dtype=str)
    except Exception as e:
        return {"file": path.name, "status": "error", "chunks": 0, "error": str(e)}

    for sheet_name, df in xl.items():
        sn   = sheet_name.strip()
        df.columns = [c.strip() for c in df.columns]

        if "Known Issues" in sn or "known" in sn.lower():
            _log_rag.info(f"[RAG/Excel] Sheet '{sn}' → Known Issues ({len(df)} rows)")
            for _, row in df.iterrows():
                symptom = _s(row.get("Symptom (Observable)", row.get("Symptom", "")))
                if not symptom:
                    continue
                rows.append({
                    "id": f"ki-{fhash}-{total}", "vector": embed_text(symptom),
                    "source_file": path.name, "file_hash": fhash,
                    "sheet": "Known Issues", "symptom": symptom,
                    "issue_id":   _s(row.get("Issue ID", "")),
                    "category":   _s(row.get("Category", "")),
                    "problem":    _s(row.get("Problem Summary", row.get("Problem", ""))),
                    "root_cause": _s(row.get("Root Cause", "")),
                    "fix":        _s(row.get("Remediation Steps", row.get("Fix", ""))),
                    "severity":   _s(row.get("Severity", "")),
                    "present":    _s(row.get("Present / Unresolved?", row.get("Present", ""))),
                    "jira":       _s(row.get("Jira Ticket", row.get("Jira", ""))),
                    "discovered": _s(row.get("Discovered Date", "")),
                    "resolved":   _s(row.get("Resolved Date", "")),
                    "notes":      _s(row.get("Notes / Lessons Learned", row.get("Notes", ""))),
                    "do_text": "", "dont_text": "", "rationale": "",
                    "prerequisite": "", "how_to_verify": "", "learning": "", "action_taken": "",
                })
                total += 1

        elif "Dos" in sn or "Don" in sn or "donts" in sn.lower():
            _log_rag.info(f"[RAG/Excel] Sheet '{sn}' → Dos and Don'ts ({len(df)} rows)")
            for _, row in df.iterrows():
                do_t   = _s(row.get("✅  DO", row.get("DO", row.get("Do", ""))))
                dont_t = _s(row.get("❌  DON'T", row.get("DONT", row.get("Don't", ""))))
                search_text = f"{do_t} / {dont_t}".strip(" /")
                if not search_text:
                    continue
                rows.append({
                    "id": f"dd-{fhash}-{total}", "vector": embed_text(search_text),
                    "source_file": path.name, "file_hash": fhash,
                    "sheet": "Dos and Donts", "symptom": search_text,
                    "issue_id": "", "category": _s(row.get("Category", "")),
                    "problem": "", "root_cause": "", "fix": "", "severity": "", "present": "",
                    "jira": _s(row.get("Related Issue", "")),
                    "discovered": "", "resolved": "",
                    "notes": _s(row.get("Rationale", "")),
                    "do_text": do_t, "dont_text": dont_t,
                    "rationale": _s(row.get("Rationale", "")),
                    "prerequisite": "", "how_to_verify": "", "learning": "", "action_taken": "",
                })
                total += 1

        elif "Prerequisite" in sn or "prereq" in sn.lower():
            _log_rag.info(f"[RAG/Excel] Sheet '{sn}' → Prerequisites ({len(df)} rows)")
            for _, row in df.iterrows():
                prereq = _s(row.get("Prerequisite", ""))
                if not prereq:
                    continue
                rows.append({
                    "id": f"pr-{fhash}-{total}", "vector": embed_text(prereq),
                    "source_file": path.name, "file_hash": fhash,
                    "sheet": "Prerequisites", "symptom": prereq,
                    "issue_id": "", "category": _s(row.get("Category", "")),
                    "problem": _s(row.get("Why It Matters", "")), "root_cause": "",
                    "fix": "", "severity": "", "present": "", "jira": "",
                    "discovered": "", "resolved": "", "notes": "",
                    "do_text": "", "dont_text": "",
                    "rationale": _s(row.get("Why It Matters", "")),
                    "prerequisite": prereq,
                    "how_to_verify": _s(row.get("How to Verify", "")),
                    "learning": "", "action_taken": "",
                })
                total += 1

        elif "Learning" in sn or "learning" in sn.lower() or "Past" in sn:
            _log_rag.info(f"[RAG/Excel] Sheet '{sn}' → Past Learnings ({len(df)} rows)")
            for _, row in df.iterrows():
                went_wrong = _s(row.get("What Went Wrong", ""))
                learning   = _s(row.get("Key Learning", ""))
                search_text = f"{went_wrong} / {learning}".strip(" /")
                if not search_text:
                    continue
                rows.append({
                    "id": f"pl-{fhash}-{total}", "vector": embed_text(search_text),
                    "source_file": path.name, "file_hash": fhash,
                    "sheet": "Past Learnings", "symptom": search_text,
                    "issue_id": "", "category": "",
                    "problem": _s(row.get("Incident Summary", "")),
                    "root_cause": went_wrong, "fix": _s(row.get("What Worked Well", "")),
                    "severity": "",
                    "present": _s(row.get("Prevented Recurrence?", "")),
                    "jira": _s(row.get("Jira / Postmortem", "")),
                    "discovered": _s(row.get("Incident Date", "")), "resolved": "", "notes": "",
                    "do_text": "", "dont_text": "", "rationale": "",
                    "prerequisite": "", "how_to_verify": "",
                    "learning": learning, "action_taken": _s(row.get("Action Taken", "")),
                })
                total += 1
        else:
            _log_rag.info(f"[RAG/Excel] Skipping unrecognised sheet '{sn}'")

    if not rows:
        return {"file": path.name, "status": "empty", "chunks": 0}

    try:
        excel_tbl.delete(f"id LIKE 'ki-{fhash}%' OR id LIKE 'dd-{fhash}%' OR id LIKE 'pr-{fhash}%' OR id LIKE 'pl-{fhash}%'")
    except Exception:
        pass

    excel_tbl.add(rows)
    _log_rag.info(f"[RAG/Excel] {path.name}: {total} rows ingested")
    return {"file": path.name, "status": "ingested", "chunks": total, "doc_type": "excel"}


def ingest_directory(docs_dir: str, force: bool = False) -> list:
    p = Path(docs_dir)
    results = []
    for f in sorted(p.glob("**/*.md")) + sorted(p.glob("**/*.pdf")) + sorted(p.glob("**/*.txt")):
        results.append(ingest_file(str(f), force=force))
    for f in sorted(p.glob("**/*.xlsx")) + sorted(p.glob("**/*.xls")):
        results.append(ingest_excel(str(f), force=force))
    return results


def rag_retrieve(query: str, top_k: int = TOP_K, doc_type: Optional[str] = None,
                 sheet: Optional[str] = None) -> str:
    """
    Unified semantic retrieval across both LanceDB tables.

    Searches excel_issues first (column-aware, unresolved issues surfaced first),
    then the docs table (prose chunks).  Returns a single formatted context
    string combining both, ready for the LLM synthesis step.

    sheet: optional filter to restrict Excel results to a specific sheet.
           Valid values: "Known Issues", "Dos and Donts", "Prerequisites", "Past Learnings"
    """
    _, docs_tbl, excel_tbl = _get_lancedb()
    sections = []

    # Normalise sheet name — accept common shorthands
    _SHEET_ALIASES = {
        "dos":          "Dos and Donts",
        "donts":        "Dos and Donts",
        "dos and donts": "Dos and Donts",
        "dos & donts":  "Dos and Donts",
        "known issues": "Known Issues",
        "known":        "Known Issues",
        "issues":       "Known Issues",
        "prerequisites": "Prerequisites",
        "prereq":       "Prerequisites",
        "past learnings": "Past Learnings",
        "learnings":    "Past Learnings",
        "past":         "Past Learnings",
    }
    if sheet:
        sheet = _SHEET_ALIASES.get(sheet.lower().strip(), sheet)

    # ── 1. Excel structured knowledge base ───────────────────────────────────
    try:
        excel_count = excel_tbl.count_rows()
    except Exception:
        excel_count = 0

    if excel_count > 0:
        try:
            qvec = embed_text(query)
            sheet_filter = f"sheet = '{sheet}'" if sheet else None

            # Try unresolved-first, fall back to all results
            try:
                _uq = excel_tbl.search(qvec, vector_column_name="vector").where("present = 'Yes'")
                if sheet_filter:
                    _uq = _uq.where(sheet_filter)
                unresolved = _uq.limit(top_k).to_list()
            except Exception:
                unresolved = []

            _aq = excel_tbl.search(qvec, vector_column_name="vector")
            if sheet_filter:
                _aq = _aq.where(sheet_filter)
            all_hits = _aq.limit(top_k).to_list()

            seen, merged = set(), []
            for r in (unresolved + all_hits):
                if r["id"] not in seen:
                    seen.add(r["id"])
                    merged.append(r)
            # No re-cap here — top_k already applied per search query above.
            # Unresolved results are prepended so they surface first.

            if merged:
                lines = [f"📋 Knowledge Base ({len(merged)} match(es)):\n"]
                for hit in merged:
                    sheet = hit.get("sheet", "")
                    sim   = round(1 - hit.get("_distance", 1.0), 3)
                    if sheet == "Known Issues":
                        status = "⚠️ UNRESOLVED" if hit.get("present") == "Yes" else "✅ Resolved"
                        jira   = hit.get("jira", "")
                        lines.append(
                            f"[{sheet}] {hit.get('issue_id','')} | {hit.get('severity','')} | {status}"
                            + (f" | {jira}" if jira else "") + f" | relevance:{sim}\n"
                            f"  Problem  : {hit.get('problem','')}\n"
                            f"  Symptom  : {hit.get('symptom','')}\n"
                            f"  RootCause: {hit.get('root_cause','')}\n"
                            f"  Fix      : {hit.get('fix','')}\n"
                            + (f"  Notes    : {hit.get('notes','')}\n" if hit.get("notes") else "")
                        )
                    elif sheet == "Dos and Donts":
                        lines.append(
                            f"[{sheet}] {hit.get('category','')} | relevance:{sim}\n"
                            f"  ✅ DO   : {hit.get('do_text','')}\n"
                            f"  ❌ DON'T: {hit.get('dont_text','')}\n"
                            f"  Why     : {hit.get('rationale','')}\n"
                        )
                    elif sheet == "Prerequisites":
                        lines.append(
                            f"[{sheet}] {hit.get('category','')} | relevance:{sim}\n"
                            f"  Prerequisite : {hit.get('prerequisite','')}\n"
                            f"  Why it matters: {hit.get('rationale','')}\n"
                            f"  How to verify : {hit.get('how_to_verify','')}\n"
                        )
                    elif sheet == "Past Learnings":
                        lines.append(
                            f"[{sheet}] {hit.get('discovered','')} | relevance:{sim}\n"
                            f"  Incident  : {hit.get('problem','')}\n"
                            f"  Went wrong: {hit.get('root_cause','')}\n"
                            f"  Learning  : {hit.get('learning','')}\n"
                            f"  Action    : {hit.get('action_taken','')}\n"
                        )
                    else:
                        lines.append(f"[{sheet}] relevance:{sim}\n  {hit.get('symptom','')}\n")
                sections.append("\n".join(lines))
        except Exception as e:
            _log_rag.warning(f"[RAG/Excel] Search failed: {e}")

    # ── 2. Prose docs table ───────────────────────────────────────────────────
    try:
        docs_count = docs_tbl.count_rows()
    except Exception:
        docs_count = 0

    if docs_count > 0:
        try:
            qvec  = embed_text(query)
            srch  = docs_tbl.search(qvec, vector_column_name="vector")
            if doc_type:
                srch = srch.where(f"doc_type = '{doc_type}'")
            hits = srch.limit(top_k).to_list()
            if hits:
                lines = [f"📄 Documentation ({len(hits)} chunk(s)):\n"]
                for hit in hits:
                    sim = round(1 - hit.get("_distance", 1.0), 3)
                    src = Path(hit.get("source", "?")).name
                    lines.append(f"[{src}] relevance:{sim}\n{hit.get('text','')}\n")
                sections.append("\n".join(lines))
        except Exception as e:
            _log_rag.warning(f"[RAG/Docs] Search failed: {e}")

    return "\n\n---\n\n".join(sections) if sections else "No relevant documentation found."


def get_doc_stats() -> dict:
    """Row counts and breakdown for both LanceDB tables."""
    try:
        _, docs_tbl, excel_tbl = _get_lancedb()
        docs_count  = docs_tbl.count_rows()
        excel_count = excel_tbl.count_rows()
        excel_by_sheet: dict = {}
        if excel_count > 0:
            try:
                from collections import Counter
                rows = excel_tbl.search().limit(excel_count + 1).to_list()
                excel_by_sheet = dict(Counter(r.get("sheet", "unknown") for r in rows))
            except Exception:
                pass
        docs_by_type: dict = {}
        if docs_count > 0:
            try:
                from collections import Counter
                rows = docs_tbl.search().limit(docs_count + 1).to_list()
                docs_by_type = dict(Counter(r.get("doc_type", "general") for r in rows))
            except Exception:
                pass
        return {
            "total_chunks":   docs_count + excel_count,
            "docs_chunks":    docs_count,
            "excel_rows":     excel_count,
            "docs_by_type":   docs_by_type,
            "excel_by_sheet": excel_by_sheet,
        }
    except Exception as e:
        return {"total_chunks": 0, "docs_chunks": 0, "excel_rows": 0,
                "docs_by_type": {}, "excel_by_sheet": {}, "error": str(e)}

RAG_TOOLS = {
    "rag_search": {
        "fn": rag_retrieve,
        "description": (
            "Search the internal knowledge base for known issues, runbooks, troubleshooting guides, "
            "and operational best practices. "
            "ALWAYS call this after describe_pod when a pod is unhealthy, crashing, OOMKilled, "
            "CrashLoopBackOff, Pending, or not Ready — to check whether a known fix is documented. "
            "Use the specific error or component name as the query. "
            "When the user explicitly asks for a specific sheet (dos and donts, known issues, "
            "prerequisites, past learnings), pass the sheet parameter to filter results to that sheet only. "
            "Examples: "
            "rag_search(query='CrashLoopBackOff cdp-cadence') "
            "rag_search(query='OOMKilled sense-db memory limit') "
            "rag_search(query='dos and donts ecs cluster', sheet='Dos and Donts') "
            "rag_search(query='longhorn storage issues', sheet='Known Issues') "
            "rag_search(query='prerequisites before deploy', sheet='Prerequisites') "
        ),
        "parameters": {
            "query":    {"type": "string",
                         "description": "Search query — use specific error names, component names, or symptoms."},
            "top_k":    {"type": "integer", "default": 10},
            "doc_type": {"type": "string",  "default": None},
            "sheet":    {"type": "string",  "default": None,
                         "description": (
                             "Optional sheet filter. Valid values: "
                             "'Known Issues', 'Dos and Donts', 'Prerequisites', 'Past Learnings'. "
                             "Use when the user explicitly asks for a specific category."
                         )},
        },
    },
}


_PROMPT_FILE = _HERE / "config" / "system_prompt.txt"

def _load_system_prompt() -> str:
    """
    Load the system prompt from system_prompt.txt (next to app.py).
    The file is re-read every time the agent is rebuilt, so you can edit it
    and trigger a reload via  POST /api/reload-prompt  without restarting.
    Falls back to a minimal hard-coded prompt if the file is missing.
    """
    if _PROMPT_FILE.exists():
        text = _PROMPT_FILE.read_text(encoding="utf-8")
        logger.info(f"[Prompt] Loaded config/system_prompt.txt ({len(text)} chars)")
        return text
    logger.warning("[Prompt] system_prompt.txt not found — using built-in fallback prompt")
    return (
        "You are an expert Kubernetes operations assistant.\n"
        "ALWAYS call tools first. NEVER fabricate data.\n"
        "ALWAYS search documentation before finalising a diagnosis.\n"
        "SITE-SPECIFIC RULES:\n{custom_rules}\n"
    )

SYSTEM_PROMPT = _load_system_prompt()

def _registry_to_openai_schema(name: str, cfg: dict) -> dict:
    """
    Convert a tool registry entry into the OpenAI function-calling JSON schema
    that Qwen3's Jinja chat template expects when passed as tools=[...] to
    tokenizer.apply_chat_template().

    Qwen3 is trained on this exact format — the template serialises it into
    the prompt as:
        # Tools
        ## get_pod_status
        ...JSON schema...
    and the model emits tool calls as:
        <tool_call>
        {"name": "get_pod_status", "arguments": {"namespace": "all"}}
        </tool_call>
    """
    params = cfg.get("parameters", {})
    properties = {}
    required = []
    for k, v in params.items():
        prop = {"type": v.get("type", "string")}
        if "description" in v:
            prop["description"] = v["description"]
        if "enum" in v:
            prop["enum"] = v["enum"]
        properties[k] = prop
        if "default" not in v:
            required.append(k)

    schema = {
        "type": "function",
        "function": {
            "name": name,
            "description": cfg["description"],
            "parameters": {
                "type": "object",
                "properties": properties,
            },
        },
    }
    if required:
        schema["function"]["parameters"]["required"] = required
    return schema


def _call_tool(name: str, args: dict, all_tools: dict) -> str:
    """Invoke a tool by name from the registry with the given args, filling defaults."""
    cfg = all_tools.get(name)
    if not cfg:
        return f"Tool '{name}' not found."
    fn     = cfg["fn"]
    params = cfg.get("parameters", {})
    # Fill missing defaults
    for k, v in params.items():
        if k not in args and "default" in v:
            args[k] = v["default"]
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
    direct_answer: Optional[str]   # set by tool_node to bypass LLM synthesis

def _build_llm():
    """
    Load tokenizer + model. Supports two backends:

    1. GGUF (CPU-optimised quantised models):
       Detected when LLM_MODEL ends with .gguf or contains 'gguf' in the name.
       Uses llama-cpp-python for inference — no GPU required, runs efficiently on CPU.
       Tool calls are parsed from raw text output (same <tool_call> block format).

    2. HuggingFace Transformers (default):
       Loads via AutoModelForCausalLM. GPU used automatically if available.
       Qwen3 native tool-calling via tokenizer.apply_chat_template().
    """
    _log_ag.info(f"[LLM] Loading model: {LLM_MODEL}")

    is_gguf = LLM_MODEL.lower().endswith(".gguf") or "gguf" in LLM_MODEL.lower()

    if is_gguf:
        return _build_llm_gguf()

    try:
        import transformers, torch

        is_qwen3 = "qwen3" in LLM_MODEL.lower()
        if is_qwen3:
            _log_ag.info("[LLM] Qwen3 detected — native tool-calling via apply_chat_template")

        device_map = "auto" if NUM_GPU > 0 else "cpu"
        dtype = torch.bfloat16 if NUM_GPU > 0 else torch.float32

        tokenizer = transformers.AutoTokenizer.from_pretrained(
            LLM_MODEL,
            trust_remote_code=True,
        )

        model = transformers.AutoModelForCausalLM.from_pretrained(
            LLM_MODEL,
            torch_dtype=dtype,
            device_map=device_map,
            trust_remote_code=True,
            use_cache=True,
        )
        model.eval()
        _log_ag.info("[LLM] Model loaded")
        return tokenizer, model, is_qwen3
    except Exception as e:
        _log_ag.error(f"[LLM] Load failed: {e}")
        raise


def _build_llm_gguf():
    """
    Load a GGUF model via llama-cpp-python for CPU inference.

    Returns a (tokenizer=None, model=<Llama>, is_qwen3) tuple.
    tokenizer is None — GGUF models use Llama's built-in tokeniser.
    The llm_node detects tokenizer=None and uses the GGUF inference path.

    Install: pip install llama-cpp-python
    Environment variables:
      LLM_MODEL     — path to .gguf file or HF repo/filename containing 'gguf'
      GGUF_N_CTX    — context window size (default: 8192)
      GGUF_N_THREADS — CPU threads (default: all available)
    """
    try:
        from llama_cpp import Llama
    except ImportError:
        raise ImportError(
            "llama-cpp-python is required for GGUF models.\n"
            "Install: pip install llama-cpp-python"
        )

    import os

    model_path = LLM_MODEL
    n_ctx      = int(os.environ.get("GGUF_N_CTX", "8192"))
    n_threads  = int(os.environ.get("GGUF_N_THREADS", str(os.cpu_count() or 4)))
    n_gpu_layers = 0  # CPU-only — set to -1 to offload all layers to GPU if available

    _log_ag.info(f"[LLM/GGUF] Loading {model_path} | ctx={n_ctx} threads={n_threads}")

    # If given a HF repo string like "org/model-name-GGUF" without a file path,
    # attempt to pull the largest Q4_K_M or Q4_0 file via huggingface_hub.
    if not os.path.isfile(model_path):
        try:
            from huggingface_hub import hf_hub_download
            # Try common quantisation filenames in preference order
            for quant in ["Q4_K_M.gguf", "Q4_0.gguf", "Q5_K_M.gguf", "Q8_0.gguf"]:
                # Repo id may be "org/repo" — filename is the last path component
                repo_id  = model_path
                filename = quant
                # If model_path looks like "org/repo/filename.gguf" split it
                parts = model_path.split("/")
                if len(parts) == 3 and parts[-1].endswith(".gguf"):
                    repo_id  = "/".join(parts[:2])
                    filename = parts[-1]
                    quant    = filename  # skip loop after first match
                try:
                    model_path = hf_hub_download(repo_id=repo_id, filename=filename)
                    _log_ag.info(f"[LLM/GGUF] Downloaded {filename} from {repo_id}")
                    break
                except Exception:
                    continue
        except ImportError:
            pass

    if not os.path.isfile(model_path):
        raise FileNotFoundError(
            f"GGUF model file not found: {model_path}\n"
            "Provide a full path to a .gguf file, or a HuggingFace repo/filename."
        )

    model = Llama(
        model_path=model_path,
        n_ctx=n_ctx,
        n_threads=n_threads,
        n_gpu_layers=n_gpu_layers,
        verbose=False,
    )
    is_qwen3 = "qwen" in model_path.lower()
    _log_ag.info(f"[LLM/GGUF] Model loaded (CPU, {n_threads} threads, ctx={n_ctx})")
    return None, model, is_qwen3

def build_agent():
    all_tools = {**K8S_TOOLS, **RAG_TOOLS}

    # Build the OpenAI-schema list that Qwen3's Jinja chat template expects.
    # Passed as tools= to tokenizer.apply_chat_template() in llm_node.
    tool_schemas = [_registry_to_openai_schema(n, c) for n, c in all_tools.items()]
    tool_names   = [s["function"]["name"] for s in tool_schemas]
    _log_ag.info(f"[build_agent] {len(tool_schemas)} tools: {tool_names}")
    if tool_schemas:
        _log_ag.debug(f"[build_agent] sample schema: {json.dumps(tool_schemas[0], indent=2)}")

    tokenizer, model, _is_qwen3 = _build_llm()

    # System prompt — /no_think suffix is the Qwen3 soft switch for thinking mode.
    # enable_thinking=False in apply_chat_template is the hard switch (Jinja-level).
    _sys_prompt = _load_system_prompt().format(custom_rules=CUSTOM_RULES or "None.")
    prompt = (_sys_prompt + "\n/no_think") if _is_qwen3 else _sys_prompt

    # ── Tool routing — see agent/routing.py ──────────────────────────────────
    # Namespace aliases and routing logic live in agent/routing.py.
    # _default_tools_for and _resolve_namespace are imported at module level.
    def _default_tools_for(user_msg: str):
        return default_tools_for(user_msg)

    def _resolve_namespace(lm: str) -> str:
        return resolve_namespace(lm)

    def _prepare_messages_for_hf(msgs: list) -> list:
        """
        Prepare messages for the LLM call.

        AGENTIC MODE: Pass the FULL conversation history at every call.
        This is what enables multi-hop reasoning — Qwen3 sees its previous
        tool calls and results and decides whether to call more tools or answer.

        The system prompt's Tool Selection Guide replaces the old routing.py
        keyword matching. Qwen3 reads the guide and selects tools accordingly.

        For synthesis (after tools have run), we replace the raw tool messages
        with a structured synthesis prompt to guide response format.
        """
        if not msgs:
            return msgs

        has_tool_results = any(isinstance(m, ToolMessage) for m in msgs)

        # ── Tool selection phase: pass full history so LLM sees conversation ──
        # No filtering, no injection — the system prompt Tool Selection Guide
        # tells Qwen3 what to call. Trust the LLM.
        if not has_tool_results:
            filtered = [m for m in msgs if isinstance(m, (HumanMessage, SystemMessage))]
            _log_ag.debug(f"[prepare_msgs] tool selection — passing {len(filtered)} msg(s)")
            return filtered

        # ── Synthesis phase: build structured prompt from accumulated results ──
        original_question = next((m.content for m in msgs if isinstance(m, HumanMessage)), "")
        tool_results = [m for m in msgs if isinstance(m, ToolMessage)]

        # Detect namespace assumption needed
        _oq_lower = original_question.lower()
        _NS_WORDS = ("namespace", " ns=", " ns ", "in namespace", "for namespace",
                     "in the namespace", "-n ", "cdp", "cmlwb", "longhorn", "vault",
                     "cattle", "rancher", "cert", "default namespace")
        _ns_specified = any(k in _oq_lower for k in _NS_WORDS)
        _needs_ns = any(
            tc_name in (getattr(m, "name", "") or "")
            for m in msgs if isinstance(m, ToolMessage)
            for tc_name in ("get_pod_status", "get_deployment_status", "get_daemonset_status",
                            "get_statefulset_status", "get_job_status", "get_hpa_status",
                            "get_pvc_status", "get_service_status", "get_ingress_status",
                            "get_resource_quotas", "get_configmap_list", "get_service_accounts")
        )
        _ns_prefix = (
            f"§NS_PREFIX§As no namespace was specified, I am assuming you are requesting for all namespaces.§END_NS§\n\n"
            if (_needs_ns and not _ns_specified) else ""
        )

        _ANALYSIS_KEYWORDS = (
            "ok", "okay", "healthy", "health", "doing", "status", "issue",
            "problem", "error", "fail", "warning", "trouble", "concern",
            "pressure", "crashing", "restart", "stuck", "degraded",
            "why", "what", "how", "is there", "are there", "should i",
            "diagnos", "analys", "check if", "verify", "confirm",
        )
        _LIST_KEYWORDS = (
            "list all", "list every", "show all", "show me all", "display all",
            "all pods", "all namespaces", "all nodes", "all pv", "all ingress",
            "all pvcs", "all services", "all deployments", "all daemonsets",
            "all jobs", "all events",
            "pods in", "output of", "show pods", "show namespaces",
            "get pods", "get namespaces", "get nodes",
            "list pods", "list nodes", "list pvc", "list services",
        )
        _COMPARISON_KEYWORDS = (
            "most", "least", "fewest", "highest", "lowest", "which namespace",
            "which node", "which pod", "rank", "top", "bottom", "compare",
            "more than", "less than", "most pods", "least pods",
        )
        _oq = original_question.lower()
        is_list_query = (
            any(k in _oq for k in _LIST_KEYWORDS)
            and not any(k in _oq for k in _ANALYSIS_KEYWORDS)
        )
        is_comparison_query = any(k in _oq for k in _COMPARISON_KEYWORDS)

        parts = []
        _tool_char_limit = 40000
        for i, tr in enumerate(tool_results, 1):
            body = tr.content if len(tr.content) <= _tool_char_limit else tr.content[:_tool_char_limit] + "\n...[truncated]"
            parts.append(f"--- TOOL RESULT {i} ---\n{body}\n")
        combined = "".join(parts)

        is_health_summary = len(tool_results) >= 3 and not is_list_query

        if is_health_summary:
            synthesis_prompt = (
                f"Question: {original_question}\n\n"
                f"Tool Results:\n{combined}\n"
                "Write a concise cluster health summary:\n"
                "1. Overall status in one sentence (healthy / issues found).\n"
                "2. If any problems exist, list them specifically: name the exact pod, node, deployment, PVC, or event with the issue and its state.\n"
                "3. If everything is healthy, say so briefly — do not list healthy items.\n"
                "Use plain sentences. No markdown headers. No closing remarks."
            )
        elif is_list_query and not is_comparison_query:
            synthesis_prompt = (
                f"Question: {original_question}\n\n"
                f"Tool Results:\n{combined}\n"
                "Reproduce the tool results VERBATIM. "
                "Do NOT summarise, count, or omit any items. "
                "If the output ends mid-list due to truncation, state the total count from the header line and note the list was truncated."
            )
        elif is_comparison_query:
            synthesis_prompt = (
                f"Question: {original_question}\n\n"
                f"Tool Results:\n{combined}\n"
                "Write a natural, conversational answer to the question. "
                "Use complete sentences. Reference the specific resource name and relevant numbers. "
                "Example: 'Node ecs-w-03 has 2 GPUs allocatable with 0 currently in use, so both are available.' "
                "Do NOT dump a list. Two sentences maximum."
            )
        else:
            synthesis_prompt = (
                f"Question: {original_question}\n\n"
                f"Tool Results:\n{combined}\n"
                "Write a natural, conversational answer using only the tool results above. "
                "Use complete sentences. Be specific — name exact pods, nodes, or resources. "
                "No preamble. No closing remarks. "
                "If the results contain a list of items, reproduce it in full. "
                "If the question asks for a count and the tool output contains a total (e.g. '43 total'), state that number directly."
            )

        return [HumanMessage(content=_ns_prefix + synthesis_prompt)]

    def _msgs_to_qwen3(msgs: list, include_tools: bool) -> list:
        """
        Convert LangChain message objects to the plain dicts that
        tokenizer.apply_chat_template() expects.

        Qwen3 tool turn format:
          assistant: {"role": "assistant", "content": "", "tool_calls": [...]}
          tool:      {"role": "tool", "name": "...", "content": "..."}
        """
        result = []
        for m in msgs:
            if isinstance(m, SystemMessage):
                result.append({"role": "system", "content": m.content})
            elif isinstance(m, HumanMessage):
                result.append({"role": "user", "content": m.content})
            elif isinstance(m, ToolMessage):
                # Find the tool name from the preceding AIMessage tool_calls
                tname = "tool"
                for prev in reversed(result):
                    if prev.get("role") == "assistant":
                        for tc in (prev.get("tool_calls") or []):
                            if tc.get("id") == m.tool_call_id:
                                tname = tc["function"]["name"]
                                break
                        break
                result.append({"role": "tool", "name": tname, "content": m.content})
            else:
                # AIMessage — may carry tool_calls
                tcs = getattr(m, "tool_calls", None) or []
                if tcs:
                    # Format tool_calls as Qwen3 expects in the assistant turn
                    formatted_tcs = [
                        {
                            "id":       tc.get("id", ""),
                            "type":     "function",
                            "function": {
                                "name":      tc["name"],
                                "arguments": json.dumps(tc.get("args", {})),
                            },
                        }
                        for tc in tcs
                    ]
                    result.append({
                        "role":       "assistant",
                        "content":    "",
                        "tool_calls": formatted_tcs,
                    })
                else:
                    result.append({"role": "assistant", "content": getattr(m, "content", "")})
        return result

    def _parse_tool_calls(text: str) -> list:
        """
        Parse Qwen3 tool call output.

        Qwen3 emits tool calls as:
            <tool_call>
            {"name": "get_node_health", "arguments": {}}
            </tool_call>

        Returns a list of {"id": str, "name": str, "args": dict} dicts,
        ready to be stored in AIMessage.tool_calls.
        """
        import uuid
        tcs = []
        for m in re.finditer(r'<tool_call>\s*(.*?)\s*</tool_call>', text, re.DOTALL):
            raw = m.group(1).strip()
            try:
                obj = json.loads(raw)
                # arguments may be a pre-serialized JSON string (Qwen3 sometimes
                # emits them that way) — parse it to avoid double-escaping later.
                args_raw = obj.get("arguments", {})
                args_parsed = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
                tcs.append({
                    "id":   f"tc_{uuid.uuid4().hex[:8]}",
                    "name": obj["name"],
                    "args": args_parsed,
                    "type": "tool_call",
                })
            except Exception as e:
                _log_ag.warning(f"[parse_tool_calls] failed to parse: {raw!r} — {e}")
        return tcs

    def llm_node(state: AgentState):
        """
        Invoke Qwen3 natively via tokenizer.apply_chat_template().

        AGENTIC MODE:
        - Tools are available at EVERY iteration, not just itr=1.
        - Qwen3 sees the full conversation: user question + all previous tool
          calls + all tool results. It decides autonomously whether to call
          another tool or produce a final answer.
        - This enables genuine multi-hop reasoning:
            itr=1: call get_pod_status → sees OOMKilled
            itr=2: call get_resource_quotas → sees 256Mi limit
            itr=3: call get_events → sees repeated OOM events
            itr=4: synthesise final answer
        - Token budget raised to 512 for tool selection iters so multi-tool
          calls fit comfortably.
        - Fallback routing only fires when Qwen3 produces NOTHING at itr=1
          (complete failure), not when it picks fewer tools than we'd like.
        """
        import torch
        itr    = state.get("iteration", 0) + 1
        msgs   = state["messages"]
        updates = list(state.get("status_updates", []))

        # ── LLM bypass — tool_node already set a direct answer ───────────────
        if state.get("direct_answer"):
            _log_ag.info(f"[llm_node itr={itr}] direct_answer bypass — skipping inference")
            return {
                "messages":       [AIMessage(content=state["direct_answer"])],
                "tool_calls_made": state.get("tool_calls_made", []),
                "iteration":      itr,
                "status_updates": updates,
                "direct_answer":  None,
            }

        has_tool_results = any(isinstance(m, ToolMessage) for m in msgs)

        # ── AGENTIC: include tools on every iteration ─────────────────────────
        # After tool results exist, _prepare_messages_for_hf switches to the
        # synthesis prompt format — we still pass tools= so the LLM CAN call
        # another tool if it judges that necessary (multi-hop), but the
        # synthesis prompt nudges it to answer if results are sufficient.
        include_tools = True

        # ── Build message list ────────────────────────────────────────────────
        invoke_msgs = _prepare_messages_for_hf(msgs)
        chat_msgs = [{"role": "system", "content": prompt}] + _msgs_to_qwen3(invoke_msgs, include_tools)
        _log_ag.debug(f"[llm_node itr={itr}] chat_msgs count={len(chat_msgs)} has_tool_results={has_tool_results}")

        # ── Token budget ──────────────────────────────────────────────────────
        if not has_tool_results:
            _max_new = 512
        else:
            _max_new = max(512, _MAX_NEW_TOKENS)
        _log_ag.debug(f"[llm_node itr={itr}] max_new_tokens={_max_new}")

        # ── GGUF path (tokenizer is None) ─────────────────────────────────────
        if tokenizer is None:
            # llama-cpp-python: use create_chat_completion for chat format.
            # Tools are injected into the system prompt as JSON since llama-cpp
            # doesn't support the apply_chat_template tools= kwarg universally.
            tools_json = json.dumps(tool_schemas, indent=2)
            tool_system = (
                f"{prompt}\n\n"
                f"Available tools (call using <tool_call>{{\"name\": ..., \"arguments\": {{...}}}}</tool_call>):\n"
                f"{tools_json}"
            )
            gguf_msgs = [{"role": "system", "content": tool_system}]
            for m in chat_msgs[1:]:   # skip the original system msg — already merged
                gguf_msgs.append(m)

            resp = model.create_chat_completion(
                messages=gguf_msgs,
                max_tokens=_max_new,
                temperature=0.7,
                top_p=0.8,
                top_k=20,
                repeat_penalty=1.05,
            )
            raw_text = resp["choices"][0]["message"].get("content", "") or ""
            _log_ag.info(f"[llm_node/GGUF itr={itr}] raw output: {raw_text[:400]!r}")

        # ── HuggingFace Transformers path ─────────────────────────────────────
        else:
            import torch
            template_kwargs = {"add_generation_prompt": True}
            if _is_qwen3:
                template_kwargs["enable_thinking"] = False
            template_kwargs["tools"] = tool_schemas

            encoded = tokenizer.apply_chat_template(
                chat_msgs,
                tokenize=True,
                return_tensors="pt",
                **template_kwargs,
            )
            input_ids = (encoded["input_ids"] if hasattr(encoded, "__getitem__") and not hasattr(encoded, "shape")
                         else encoded).to(model.device)
            input_len = input_ids.shape[-1]
            _log_ag.debug(f"[llm_node itr={itr}] input tokens={input_len}")

            # ── Context window guard ──────────────────────────────────────────
            # If input + output budget would exceed the model's context window,
            # trim the input by dropping tokens from the middle of the tool
            # results (preserving the system prompt head and synthesis tail).
            model_max = getattr(tokenizer, "model_max_length", None) or 32768
            # model_max_length is sometimes set to absurd values (e.g. 1e30) by
            # HF configs — cap to a sane value.
            if model_max > 131072:
                model_max = 32768
            budget = model_max - _max_new - 64   # 64 token safety margin
            if input_len > budget:
                overflow = input_len - budget
                keep_head = budget // 2
                keep_tail = budget - keep_head
                trimmed = torch.cat([
                    input_ids[:, :keep_head],
                    input_ids[:, -keep_tail:]
                ], dim=1)
                _log_ag.warning(
                    f"[llm_node itr={itr}] input {input_len} tokens exceeds ctx budget {budget} "
                    f"— trimmed {overflow} tokens from middle"
                )
                input_ids = trimmed
                input_len  = input_ids.shape[-1]

            with torch.no_grad():
                output_ids = model.generate(
                    input_ids,
                    max_new_tokens=_max_new,
                    do_sample=True,
                    temperature=0.7,
                    top_p=0.8,
                    top_k=20,
                    repetition_penalty=1.05,
                    pad_token_id=tokenizer.eos_token_id,
                )

            new_tokens = output_ids[0][input_len:]
            raw_text   = tokenizer.decode(new_tokens, skip_special_tokens=True)
            _log_ag.info(f"[llm_node itr={itr}] raw output: {raw_text[:400]!r}")

        # ── Parse tool calls ─────────────────────────────────────────────────
        tcs = _parse_tool_calls(raw_text)
        _log_ag.info(f"[llm_node itr={itr}] tool_calls parsed: {[tc['name'] for tc in tcs]}")

        content = re.sub(r'<tool_call>[\s\S]*?</tool_call>', '', raw_text).strip()
        content = re.sub(r'<think>[\s\S]*?</think>\s*', '', content).strip()

        # Namespace prefix extraction
        _ns_prepend = ""
        for m in invoke_msgs:
            if isinstance(m, HumanMessage):
                _m = re.match(r'^§NS_PREFIX§(.*?)§END_NS§\n\n', m.content, re.DOTALL)
                if _m:
                    _ns_prepend = _m.group(1).strip() + "\n\n"
                break
        if _ns_prepend:
            content = _ns_prepend + content

        # ── Fallback routing — only when itr=1 produces nothing at all ───────
        # This handles complete LLM failures (empty output, malformed JSON).
        # It does NOT fire just because Qwen3 picked fewer tools than expected —
        # that's now the LLM's prerogative in agentic mode.
        if not tcs and not content.strip() and itr == 1 and ENABLE_FALLBACK_ROUTING:
            user_msg = next((m.content for m in reversed(msgs) if isinstance(m, HumanMessage)), "")
            _log_ag.warning(f"[llm_node itr={itr}] complete failure — fallback routing for: {user_msg!r}")
            import uuid
            fallback = _default_tools_for(user_msg)

            # ── Conversational / how-to sentinel — no tool needed ─────────────
            if fallback and fallback[0][0] == "__conversational__":
                _log_ag.info("[llm_node] how-to query — injecting conversational prompt")
                # Re-run LLM with a focused conversational prompt instead of tool calls
                conv_msgs = msgs + [HumanMessage(content=(
                    f"The user asked: {user_msg!r}\n\n"
                    "Answer this as the ECS Operations Assistant — explain step by step "
                    "how you would handle this using your available tools and capabilities. "
                    "Be specific: name the exact tool you call, the parameters you pass, "
                    "and what the output looks like. Do NOT call any tool — just explain."
                ))]
                conv_resp = llm_pipeline.invoke(conv_msgs)
                conv_content = (conv_resp.content if hasattr(conv_resp, "content")
                                else str(conv_resp)).strip()
                if conv_content:
                    return {
                        "messages":        [AIMessage(content=conv_content, tool_calls=[])],
                        "tool_calls_made": state.get("tool_calls_made", []),
                        "iteration":       itr,
                        "status_updates":  updates,
                        "direct_answer":   conv_content,
                    }

            tcs = [
                {"name": tname, "args": targs,
                 "id": f"fallback_{uuid.uuid4().hex[:8]}", "type": "tool_call"}
                for tname, targs in fallback
                if tname in tool_names
            ]
            if tcs:
                updates.append("⚙️ Fallback routing — LLM produced no output")

        response = AIMessage(content=content, tool_calls=tcs)
        if tcs:
            updates.append(f"🔧 {', '.join(tc['name'] for tc in tcs)}")

        return {
            "messages":       [response],
            "tool_calls_made": state.get("tool_calls_made", []),
            "iteration":      itr,
            "status_updates": updates,
        }

    # ── LLM bypass — see agent/bypass.py ────────────────────────────────────
    # should_bypass_llm() and build_direct_answer() are imported at module level.
    # The bypass rules (BYPASS_TOOLS, BYPASS_TOOL_ARGS, ANALYSIS_INTENTS) live
    # in agent/bypass.py and are unit-testable without loading the LLM.

    def tool_node(state: AgentState):
        last         = state["messages"][-1]
        results      = []
        tools_called = list(state.get("tool_calls_made", []))
        updates      = list(state.get("status_updates", []))

        # Original user question — needed for bypass intent check
        user_q = next((m.content for m in state["messages"]
                       if isinstance(m, HumanMessage)), "")

        tcs = getattr(last, "tool_calls", []) or []
        _log_ag.debug(f"[tool_node] executing {len(tcs)} tool call(s)")

        direct_answer = None

        for tc in tcs:
            name = tc["name"]
            args = dict(tc.get("args", {}) or {})

            # ── Security: enforce decode from the UI toggle, never from LLM ──
            # The LLM must not control whether secret values are decoded —
            # it has no knowledge of the user's security preference and will
            # always set decode=True when the query mentions passwords/credentials.
            # Strip any LLM-supplied decode and replace with the ContextVar value.
            if name == "get_secrets":
                llm_decode = args.pop("decode", None)
                server_decode = get_decode_secrets()
                args["decode"] = server_decode
                _log_ag.debug(
                    f"[tool_node] get_secrets decode override: "
                    f"llm_supplied={llm_decode} → server_toggle={server_decode}")

            _log_ag.info(f"[tool_node] calling tool={name!r} args={args!r}")
            tools_called.append(name)
            if name == "kubectl_exec" and "command" in args:
                updates.append(f"$ {args['command']}")
            else:
                updates.append(f"⚙️ {name}")

            out = _call_tool(name, args, all_tools)
            _log_ag.debug(f"[tool_node] {name} result preview: {str(out)[:200]!r}")
            results.append(ToolMessage(content=out, tool_call_id=tc["id"]))

            # Check if this single-tool result can bypass LLM synthesis
            if len(tcs) == 1 and should_bypass_llm(name, args, out, user_q):
                _log_ag.info(f"[tool_node] LLM bypass — returning tool output directly for {name!r}")
                updates.append("⚡ Direct output (LLM synthesis skipped)")
                direct_answer = build_direct_answer(name, out, user_q)

        return {
            "messages":        results,
            "tool_calls_made": tools_called,
            "iteration":       state.get("iteration", 0),
            "status_updates":  updates,
            "direct_answer":   direct_answer,
        }

    def router(state: AgentState) -> Literal["tools", "end"]:
        # Cap at 6 iterations — enough for 4-hop tool chains + synthesis.
        # In agentic mode each tool call is one hop: pod → quotas → events → answer.
        if state.get("iteration", 0) >= 6: return "end"
        return "tools" if getattr(state["messages"][-1], "tool_calls", None) else "end"

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
    # Strip Qwen3 <think>...</think> blocks (non-thinking mode may still emit empty ones)
    text = re.sub(r'<think>[\s\S]*?</think>\s*', '', text)
    # Strip Qwen2.5 / legacy special tokens
    text = re.sub(r'<\|im_start\|>\w+\s*\n?[\s\S]*?<\|im_end\|>\n?', '', text)
    if '<|im_start|>' in text: text = re.sub(r'^\w+\s*\n', '', text.split('<|im_start|>')[-1], count=1)
    for tok in ['<|im_end|>', '<s>', '</s>', '[INST]', '[/INST]', '<<SYS>>', '<</SYS>>']: text = text.replace(tok, '')

    if user_question:
        q_stripped = user_question.strip()
        escaped    = re.escape(q_stripped)
        text = re.sub(r'(?i)(\s*' + escaped + r'[?!.]?\s*){2,}', ' ', text)
        text = re.sub(r'(?i)^\s*' + escaped + r'[?!.]?\s*\n', '', text)

    text = re.sub(r'Summarise the above tool results.*', '', text, flags=re.IGNORECASE)

    _OPENER_PATTERNS = [
        r'^(sure[,!]?\s*)',
        r'^(certainly[,!]?\s*)',
        r'^(of course[,!]?\s*)',
        r'^(great[,!]?\s*)',
        r'^(absolutely[,!]?\s*)',
        r'^(here\s+is\s+(a\s+)?(the\s+)?[^.]{0,60}[:.]\s*)',
        r'^(here\s+are\s+(the\s+)?[^.]{0,60}[:.]\s*)',
        r'^(based\s+on\s+(the\s+)?(tool\s+)?(results?|data|output)[^.]{0,80}[:.]\s*)',
        r'^(according\s+to\s+(the\s+)?(tool\s+)?(results?|data|output)[^.]{0,80}[:.]\s*)',
        r'^(the\s+(tool\s+)?(results?|data|output)\s+(show|indicate|reveal)[^.]{0,80}[:.]\s*)',
        r'^(i\s+(can\s+see|found|have\s+checked|checked|will\s+now|am\s+now)[^.]{0,80}[.!]\s*)',
        r'^(let\s+me\s+[^.]{0,60}[.!]\s*)',
    ]
    for pat in _OPENER_PATTERNS:
        text = re.sub(pat, '', text, flags=re.IGNORECASE)

    _CLOSER_PATTERNS = [
        r'(\s*let\s+me\s+know\s+if\s+you\s+(need|want|have)[^.]*\.\s*)$',
        r'(\s*feel\s+free\s+to\s+ask[^.]*\.\s*)$',
        r'(\s*i\s+hope\s+this\s+helps?[^.]*\.\s*)$',
        r'(\s*if\s+you\s+(need|want|have)\s+(any\s+)?(more|further|additional)[^.]*\.\s*)$',
        r'(\s*please\s+(let\s+me\s+know|don\'t\s+hesitate)[^.]*\.\s*)$',
        r'(\n\s*#+\s*(Next Steps|Summary|Conclusion|Recommendations?)[^\n]*(\n[^\n]+)*)',
    ]
    for pat in _CLOSER_PATTERNS:
        text = re.sub(pat, '', text, flags=re.IGNORECASE)

    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


async def run_agent(user_message: str) -> dict:
    import uuid as _uuid
    req_id = _uuid.uuid4().hex[:8]
    logger.info(f"[REQ:{req_id}] /chat  q={user_message!r:.120}")

    agent = get_agent()
    t0 = time.time()
    logger.info(f"[REQ:{req_id}] agent.ainvoke starting")
    final = await agent.ainvoke({
        "messages": [HumanMessage(content=user_message)],
        "tool_calls_made": [],
        "iteration": 0,
        "status_updates": [f"🤖 Model: {LLM_MODEL}"],
    })
    elapsed = time.time() - t0
    last    = final["messages"][-1]
    raw     = last.content if hasattr(last, "content") else str(last)
    updates = final.get("status_updates", [])
    updates.append(f"✅ Done in {elapsed:.0f}s")
    tools   = final.get("tool_calls_made", [])
    itr     = final.get("iteration", 0)
    logger.info(
        f"[REQ:{req_id}] done  elapsed={elapsed:.1f}s  itr={itr}"
        f"  tools={tools}  ans_chars={len(raw)}"
    )
    return {
        "response":             _clean_response(raw, user_message),
        "tools_used":           tools,
        "iterations":           itr,
        "status_updates":       updates,
        "elapsed_seconds":      round(elapsed, 1),
        "clarification_needed": False,
    }

async def run_agent_streaming(user_message: str, history: list = None):
    """
    Async generator that yields Server-Sent Events (SSE) strings.

    Event types emitted:
      data: {"type": "status",  "text": "..."}          — agent status update
      data: {"type": "tool",    "name": "...", "cmd": "..."}  — tool being called
      data: {"type": "result",  ...full response payload...}  — final answer
      data: {"type": "error",   "text": "..."}          — exception
    """
    def _sse(payload: dict) -> str:
        return f"data: {json.dumps(payload)}\n\n"

    import uuid as _uuid
    req_id = _uuid.uuid4().hex[:8]
    logger.info(f"[REQ:{req_id}] /chat/stream  q={user_message!r:.120}")

    yield _sse({"type": "status", "text": f"🤖 Model: {LLM_MODEL}"})

    agent          = get_agent()
    t0             = time.time()
    all_updates: list      = [f"🤖 Model: {LLM_MODEL}"]
    tools_called: list     = []
    raw_tool_outputs: list = []
    final_answer: str      = ""
    iteration_count: int   = 0

    # Hard timeout — uses the runtime-adjustable _LLM_TIMEOUT (default 900s CPU, 300s GPU).
    # Adjustable via GET/POST /api/config or Settings → LLM Input/Output.
    _STREAM_TIMEOUT = _LLM_TIMEOUT

    try:
        import asyncio as _asyncio

        # ── Heartbeat: emit a "Still processing…" SSE every 15s so the user
        # knows the server is alive during long LLM generation passes.
        # The event queue bridges the heartbeat task and the main generator.
        _hb_queue: _asyncio.Queue = _asyncio.Queue()
        _hb_stop = _asyncio.Event()

        async def _heartbeat_task():
            """Emit elapsed-time pings every 15s while the agent is running."""
            tick = 0
            while not _hb_stop.is_set():
                try:
                    await _asyncio.wait_for(_asyncio.shield(_asyncio.sleep(15)), timeout=15)
                except Exception:
                    pass
                tick += 15
                if not _hb_stop.is_set():
                    await _hb_queue.put(tick)

        _hb_task = _asyncio.ensure_future(_heartbeat_task())

        async def _run_stream():
            # Build message list: prior conversation turns + current message
            from langchain_core.messages import AIMessage as _AIMessage
            history_msgs = []
            for turn in (history or []):
                if turn.role == "user":
                    history_msgs.append(HumanMessage(content=turn.content))
                elif turn.role == "assistant":
                    history_msgs.append(_AIMessage(content=turn.content))
            all_messages = history_msgs + [HumanMessage(content=user_message)]
            logger.debug(f"[REQ:{req_id}] history_turns={len(history or [])}  total_messages={len(all_messages)}")

            async for event in agent.astream_events(
                {
                    "messages":        all_messages,
                    "tool_calls_made": [],
                    "iteration":       0,
                    "status_updates":  [],
                },
                version="v2",
            ):
                # Drain any pending heartbeat pings before each agent event
                while not _hb_queue.empty():
                    tick = _hb_queue.get_nowait()
                    yield {"_heartbeat": tick}
                yield event
            # Drain final heartbeats
            while not _hb_queue.empty():
                tick = _hb_queue.get_nowait()
                yield {"_heartbeat": tick}

        async for event in _run_stream():
            # ── Heartbeat event — tell the user we're still working ───────────
            if "_heartbeat" in event:
                tick = event["_heartbeat"]
                hb_txt = f"⏳ Still processing… ({tick}s elapsed)"
                all_updates.append(hb_txt)
                yield _sse({"type": "heartbeat", "text": hb_txt, "elapsed": tick,
                             "timeout": _STREAM_TIMEOUT})
                continue
            kind = event.get("event", "")
            name = event.get("name", "")

            if kind == "on_chat_model_start":
                pass

            elif kind == "on_chain_start" and name == "llm":
                itr_hint = iteration_count + 1
                txt = f"🧠 Loop {itr_hint} — LLM thinking…"
                yield _sse({"type": "iteration", "iteration": itr_hint,
                            "text": txt, "has_tool_calls": None})

            elif kind == "on_tool_start":
                tool_name  = event.get("name", "unknown_tool")
                tool_input = event.get("data", {}).get("input", {})
                cmd = None
                if tool_name == "kubectl_exec" and isinstance(tool_input, dict):
                    cmd = tool_input.get("command")
                    txt = f"$ {cmd}"
                else:
                    txt = f"⚙️ {tool_name}"
                logger.info(f"[REQ:{req_id}] tool_start  name={tool_name!r}  input={str(tool_input)[:120]!r}")
                all_updates.append(txt)
                tools_called.append(tool_name)
                yield _sse({"type": "tool", "name": tool_name,
                            "text": txt, "cmd": cmd})

            elif kind == "on_tool_end":
                tool_name = event.get("name", "")
                output    = event.get("data", {}).get("output", "")
                raw_tool_outputs.append(str(output))
                preview   = str(output)[:120].replace("\n", " ")
                logger.info(f"[REQ:{req_id}] tool_end  name={tool_name!r}  out_chars={len(str(output))}  preview={preview!r}")
                txt = f"✓ {tool_name}: {str(output)[:80].replace(chr(10), ' ')}…"
                all_updates.append(txt)
                yield _sse({"type": "status", "text": txt})

            elif kind == "on_chain_end" and name == "llm":
                output       = event.get("data", {}).get("output", {})
                node_updates = output.get("status_updates", [])
                iteration_count = output.get("iteration", iteration_count)
                has_tool_calls = False
                for m in output.get("messages", []):
                    tc  = getattr(m, "tool_calls", None)
                    txt = getattr(m, "content", "") or ""
                    if tc:
                        has_tool_calls = True
                    if txt and not tc:
                        final_answer = txt
                logger.info(
                    f"[REQ:{req_id}] llm_chain_end  itr={iteration_count}"
                    f"  has_answer={bool(final_answer)}  updates={node_updates}"
                )

                # ── Emit iteration stage event ────────────────────────────────
                # This is what lets the UI show the loop timeline in real time.
                if has_tool_calls:
                    itr_txt = f"🔄 Loop {iteration_count} — LLM called tools, waiting for results…"
                elif final_answer:
                    itr_txt = f"✍️ Loop {iteration_count} — LLM synthesising final answer…"
                else:
                    itr_txt = f"🔄 Loop {iteration_count} — LLM processing…"
                yield _sse({"type": "iteration", "iteration": iteration_count,
                            "text": itr_txt, "has_tool_calls": has_tool_calls})

                for u in node_updates:
                    if u not in all_updates:
                        all_updates.append(u)
                        yield _sse({"type": "status", "text": u})

        elapsed  = round(time.time() - t0, 1)
        _hb_stop.set()
        _hb_task.cancel()
        done_txt = f"✅ Done in {elapsed}s"
        all_updates.append(done_txt)
        logger.info(
            f"[REQ:{req_id}] stream_done  elapsed={elapsed}s"
            f"  tools={list(dict.fromkeys(tools_called))}"
            f"  itr={iteration_count}  answer_chars={len(final_answer)}"
        )
        yield _sse({"type": "status", "text": done_txt})

        # The graph already ran the full cycle (tool selection → tools → synthesis).
        # The final answer is in final_answer captured from on_chain_end of the graph.
        # If not captured, fall back to a single ainvoke.
        raw = final_answer
        if not raw:
            try:
                final = await _asyncio.wait_for(
                    agent.ainvoke({
                        "messages":        [HumanMessage(content=user_message)],
                        "tool_calls_made": [],
                        "iteration":       0,
                        "status_updates":  [],
                    }),
                    timeout=_STREAM_TIMEOUT,
                )
                last = final["messages"][-1]
                raw  = last.content if hasattr(last, "content") else str(last)
            except _asyncio.TimeoutError:
                yield _sse({"type": "error",
                            "text": f"LLM timed out after {_STREAM_TIMEOUT}s. "
                                    "Try a simpler query or increase LLM_TIMEOUT."})
                return

        yield _sse({
            "type":                 "result",
            "response":             _clean_response(raw, user_message),
            "tools_used":           list(dict.fromkeys(tools_called)),
            "iterations":           iteration_count,
            "status_updates":       all_updates,
            "elapsed_seconds":      elapsed,
            "clarification_needed": False,
        })

    except _asyncio.TimeoutError:
        _hb_stop.set()
        _hb_task.cancel()
        yield _sse({"type": "error",
                    "text": f"LLM timed out after {_STREAM_TIMEOUT}s. "
                            "Try a simpler query or increase LLM_TIMEOUT."})
    except Exception as exc:
        _hb_stop.set()
        _hb_task.cancel()
        logger.error(f"[Stream] {exc}", exc_info=True)
        yield _sse({"type": "error", "text": str(exc)})


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

def _run_startup_checks():
    """Run kubectl tool smoke-tests and log results."""
    from tools_k8s import K8S_TOOLS as _tools

    SMOKE_TESTS = [
        ("get_node_health",    {}),
        ("get_namespace_status", {}),
        ("get_pod_status",     {"namespace": "all"}),
        ("get_events",         {"namespace": "all", "warning_only": True}),
    ]

    logger.info("[Self-test] Running kubectl tool smoke-tests…")
    all_ok = True
    for name, kwargs in SMOKE_TESTS:
        cfg = _tools.get(name)
        if cfg is None:
            logger.warning(f"[Self-test] ⚠ Tool not found: {name}")
            all_ok = False
            continue
        try:
            result = cfg["fn"](**kwargs)
            if result.startswith("K8s API error") or result.startswith("K8s error") or result.startswith("[ERROR]"):
                logger.warning(f"[Self-test] ⚠ {name}: {result[:120]}")
                all_ok = False
            else:
                preview = result.replace("\n", " ")[:80]
                logger.info(f"[Self-test] ✓ {name}: {preview}…")
        except Exception as e:
            logger.warning(f"[Self-test] ⚠ {name} raised: {e}")
            all_ok = False

    if all_ok:
        logger.info("[Self-test] All kubectl tools OK ✓")
    else:
        logger.warning(
            "[Self-test] Some kubectl tools failed — check KUBECONFIG_PATH "
            "and cluster connectivity.")

@asynccontextmanager
async def _lifespan(app: FastAPI):
    logger.info("=" * 60)
    logger.info(f"Cloudera ECS AI Ops")
    gpu_info = f"{NUM_GPU} GPU(s) — GPU inference" if NUM_GPU > 0 else "No GPU — CPU inference"
    logger.info(f"  LLM      : {LLM_MODEL}")
    logger.info(f"  Embed    : {EMBED_MODEL}")
    logger.info(f"  GPU      : {gpu_info}")
    logger.info(f"  LanceDB  : {LANCEDB_DIR}")
    logger.info(f"  Tools    : {len(K8S_TOOLS)} kubectl tools registered")
    logger.info("=" * 60)

    _run_startup_checks()

    try:
        _log_rag.info("[LanceDB] Initialising store…")
        init_db()
        stats = get_doc_stats()
        _log_rag.info(
            f"[LanceDB] Ready — {stats['docs_chunks']} doc chunks, "
            f"{stats['excel_rows']} Excel rows  |  "
            f"sheets: {stats.get('excel_by_sheet', {})}  "
            f"doc types: {stats.get('docs_by_type', {})}")
    except Exception as e:
        _log_rag.error(f"[LanceDB] Init failed — RAG unavailable: {e}")

    logger.info("[Agent] Pre-warming LLM…")
    t0 = time.time()
    get_agent()
    logger.info(f"[Agent] Ready in {time.time()-t0:.1f}s")
    logger.info("Startup complete ✓")
    yield
    logger.info("Shutting down")

app = FastAPI(title="Cloudera ECS AI Ops", lifespan=_lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class HistoryMessage(BaseModel):
    role: str   # "user" or "assistant"
    content: str

class ChatRequest(BaseModel):
    message: str
    decode_secrets: bool = False
    history: list[HistoryMessage] = []
class ChatResponse(BaseModel): response: str; tools_used: list; iterations: int; status_updates: list; elapsed_seconds: float
class IngestRequest(BaseModel): docs_dir: str; force: bool = False
class IngestResponse(BaseModel): results: list; total_files: int; total_chunks: int

@app.get("/health")
async def health():
    stats = get_doc_stats()
    return {"status": "ok", "model": LLM_MODEL, "model_source": "huggingface", "embed_source": "huggingface", "num_gpu": NUM_GPU, "lancedb_docs": stats["docs_chunks"], "lancedb_excel_rows": stats["excel_rows"], "k8s_tools": len(K8S_TOOLS), "cluster_server": CLUSTER_SERVER}

class KubeconfigRequest(BaseModel):
    kubeconfig: str

@app.post("/api/kubeconfig")
async def apply_kubeconfig(req: KubeconfigRequest):
    global CLUSTER_SERVER
    try:
        result = reload_kubeconfig(req.kubeconfig)
        if result.get("server") and result["server"] != "unknown":
            CLUSTER_SERVER = re.sub(r'^https?://', '', result["server"]).strip()
        return result
    except ValueError as e:
        return _JSONResponse(status_code=400, content={"ok": False, "error": str(e)})

@app.post("/chat", response_model=ChatResponse)
async def chat(req: ChatRequest):
    if not req.message.strip(): raise HTTPException(400, "Empty message")
    logger.info(f"[API] POST /chat  message={req.message!r:.120}")
    try: return ChatResponse(**await run_agent(req.message))
    except Exception as e:
        logger.error(f"[API] POST /chat FAILED: {e}", exc_info=True)
        raise HTTPException(500, f"Agent failed: {e}")

@app.post("/chat/stream")
async def chat_stream(req: ChatRequest):
    """
    SSE endpoint — emits agent events in real-time as the agentic loop runs.
    Each line is: data: <json>\\n\\n
    Event types: status | tool | result | error
    """
    if not req.message.strip():
        raise HTTPException(400, "Empty message")
    logger.info(f"[API] POST /chat/stream  message={req.message!r:.120}  decode_secrets={req.decode_secrets}  history_turns={len(req.history)}")
    _decode_secrets_ctx.set(req.decode_secrets)
    logger.debug(f"[API] ContextVar set: decode_secrets={_decode_secrets_ctx.get()}")

    import asyncio as _asyncio

    async def _keepalive_stream():
        """
        Pumps run_agent_streaming through a queue so keep-alive SSE comment
        bytes can be sent every 10s during long CPU inference gaps.
        The agent generator runs in a background task; the main loop polls the
        queue with a timeout and emits ': keep-alive' when nothing arrives,
        resetting the browser/proxy TCP idle timer without touching the agent.
        """
        queue: _asyncio.Queue = _asyncio.Queue()
        _SENTINEL = object()

        async def _producer():
            try:
                async for chunk in run_agent_streaming(req.message, history=req.history):
                    await queue.put(chunk)
            finally:
                await queue.put(_SENTINEL)

        task = _asyncio.ensure_future(_producer())
        try:
            while True:
                try:
                    item = await _asyncio.wait_for(queue.get(), timeout=10)
                except _asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                if item is _SENTINEL:
                    break
                yield item
        finally:
            task.cancel()

    return StreamingResponse(
        _keepalive_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":   "no-cache",
            "X-Accel-Buffering": "no",
        },
    )

@app.get("/metrics")
async def metrics():
    cpu_per = psutil.cpu_percent(interval=0.2, percpu=True)
    mem     = psutil.virtual_memory()
    freq    = psutil.cpu_freq()
    return {
        "cpu_total":    round(psutil.cpu_percent(interval=None), 1),
        "cpu_per_core": [round(p, 1) for p in cpu_per],
        "cpu_count":    psutil.cpu_count(logical=True),
        "freq_mhz":     round(freq.current) if freq else 0,
        "load_avg":     [round(x, 2) for x in psutil.getloadavg()],
        "mem_total_gb": round(mem.total / 1e9, 1),
        "mem_used_gb":  round(mem.used  / 1e9, 1),
        "mem_pct":      mem.percent,
        "gpus":         _gpu_metrics(),
        "num_gpu":      NUM_GPU,
    }

@app.post("/ingest", response_model=IngestResponse)
async def ingest_api(req: IngestRequest):
    results = ingest_directory(req.docs_dir, force=req.force)
    return IngestResponse(results=results, total_files=len(results), total_chunks=sum(r.get("chunks", 0) for r in results))

@app.post("/api/ingest/upload")
async def ingest_upload_real(
    files: list[UploadFile] = File(...),
    force: str = FastAPIForm(default="false"),
):
    """
    Upload documents directly into LanceDB via multipart form upload.
    Accepts .md, .pdf, .txt files (prose → docs table) and
    .xlsx / .xls files (structured Excel → excel_issues table).

    curl -s -X POST http://localhost:8000/api/ingest/upload \\
         -F 'files=@runbook.md' -F 'files=@knowledge_base.xlsx' -F 'force=false'
    """
    do_force = force.lower() in ("true", "1", "yes")
    docs_dir = _HERE / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for upload in files:
        suffix = Path(upload.filename).suffix.lower()
        if suffix not in (".md", ".pdf", ".txt", ".xlsx", ".xls"):
            results.append({
                "file": upload.filename, "status": "rejected",
                "chunks": 0, "error": f"Unsupported type '{suffix}'. Use .md, .pdf, .txt, .xlsx, or .xls."
            })
            continue
        dest = docs_dir / upload.filename
        content = await upload.read()
        dest.write_bytes(content)
        if suffix in (".xlsx", ".xls"):
            result = ingest_excel(str(dest), force=do_force)
        else:
            result = ingest_file(str(dest), force=do_force)
        results.append(result)
        logger.info(f"[Ingest/Upload] {upload.filename} → {result['status']} ({result['chunks']} chunks)")

    total_chunks = sum(r.get("chunks", 0) for r in results)
    return {"results": results, "total_files": len(results), "total_chunks": total_chunks}

api = APIRouter(prefix="/api", tags=["API"])

class AskRequest(BaseModel):
    q: str

class ToolCallRequest(BaseModel):
    name: str
    args: dict = {}

@api.get("", summary="API index — lists every endpoint with curl examples")
async def api_index():
    base = "http://localhost:8000"
    return {
        "description": "Cloudera ECS AI Ops — curl-friendly REST API",
        "endpoints": [
            {"method": "GET",  "path": "/api/info",           "description": "Model, GPU, cluster info"},
            {"method": "POST", "path": "/api/ask",            "description": "Ask the AI chatbot (blocking)",
             "curl": f'curl -s -X POST {base}/api/ask -H "Content-Type: application/json" -d \'{{"q":"list all pods with problems"}}\''},
            {"method": "POST", "path": "/api/tool",           "description": "Call a specific K8s tool directly",
             "curl": f'curl -s -X POST {base}/api/tool -H "Content-Type: application/json" -d \'{{"name":"get_pod_status","args":{{"namespace":"all","show_all":true}}}}\''},
            {"method": "GET",  "path": "/api/tools",          "description": "List all registered tools and their signatures"},
            {"method": "GET",  "path": "/api/pods",           "description": "Pod health summary  (optional: ?ns=cdp-drs)"},
            {"method": "GET",  "path": "/api/pods/raw",       "description": "kubectl-style pod table  (optional: ?ns=cdp-drs)",
             "curl": f"curl -s '{base}/api/pods/raw?ns=longhorn-system'"},
            {"method": "GET",  "path": "/api/nodes",          "description": "Node health and GPU summary"},
            {"method": "GET",  "path": "/api/events",         "description": "Cluster events  (optional: ?ns=X&warn=1 for warnings only)",
             "curl": f"curl -s '{base}/api/events?warn=1'"},
            {"method": "GET",  "path": "/api/deployments",    "description": "Deployment status  (optional: ?ns=X)"},
            {"method": "GET",  "path": "/api/pvcs",           "description": "PVC / storage status  (optional: ?ns=X)"},
            {"method": "GET",  "path": "/api/namespaces",     "description": "All namespaces and their status"},
            {"method": "GET",  "path": "/api/rag/stats",      "description": "LanceDB document and Excel row statistics"},
            {"method": "GET",  "path": "/api/rag/files",      "description": "List all previously ingested filenames"},
            {"method": "GET",  "path": "/api/rag/query",      "description": "RAG-only query for Knowledge Bot (no LLM, no truncation)"},
            {"method": "GET",  "path": "/api/system",         "description": "Live CPU / RAM / GPU metrics"},
        ],
    }

@api.get("/info", summary="Model, GPU, cluster connectivity info")
async def api_info():
    from tools_k8s import _core as _k8s_core
    cluster_ok, cluster_err = True, None
    try:
        _k8s_core.list_namespace(_request_timeout=3)
    except Exception as e:
        cluster_ok, cluster_err = False, str(e)
    return {
        "model":        LLM_MODEL,
        "embed_model":  EMBED_MODEL,
        "inference":    "GPU" if NUM_GPU > 0 else "CPU",
        "num_gpu":      NUM_GPU,
        "cluster_ok":   cluster_ok,
        "cluster_error": cluster_err,
        "rag_docs":     get_doc_stats()["docs_chunks"],
        "rag_excel":    get_doc_stats()["excel_rows"],
        "tools_count":  len(K8S_TOOLS),
    }

@api.post("/ask", summary="Ask the AI a question — returns the full agent response")
async def api_ask(req: AskRequest):
    """
    curl -s -X POST http://localhost:8000/api/ask \\
         -H 'Content-Type: application/json' \\
         -d '{"q":"are there any failing pods?"}'
    """
    if not req.q.strip():
        return _JSONResponse(status_code=400, content={"error": "q must not be empty"})
    logger.info(f"[API] POST /api/ask  q={req.q!r:.120}")
    try:
        result = await run_agent(req.q)
        return {
            "question":       req.q,
            "answer":         result["response"],
            "tools_used":     result["tools_used"],
            "iterations":     result["iterations"],
            "elapsed_seconds": result["elapsed_seconds"],
        }
    except Exception as e:
        logger.error(f"[API/ask] {e}", exc_info=True)
        return _JSONResponse(status_code=500, content={"error": str(e)})

@api.post("/tool", summary="Call a specific K8s tool directly and get raw output")
async def api_tool(req: ToolCallRequest):
    """
    curl -s -X POST http://localhost:8000/api/tool \\
         -H 'Content-Type: application/json' \\
         -d '{"name":"get_node_health","args":{}}'
    """
    from tools_k8s import K8S_TOOLS
    import asyncio, inspect

    logger.info(f"[API] POST /api/tool  name={req.name!r}  args={req.args!r}")
    entry = K8S_TOOLS.get(req.name)
    if not entry:
        return _JSONResponse(status_code=404, content={
            "error": f"Tool '{req.name}' not found.",
            "available": list(K8S_TOOLS.keys())
        })
    fn = entry.get("fn")
    if fn is None:
        return _JSONResponse(status_code=501, content={"error": f"Tool '{req.name}' has no callable fn."})
    try:
        if inspect.iscoroutinefunction(fn):
            raw = await fn(**req.args)
        else:
            raw = await asyncio.get_event_loop().run_in_executor(None, lambda: fn(**req.args))
        return {"tool": req.name, "args": req.args, "output": raw}
    except TypeError as e:
        return _JSONResponse(status_code=400, content={"error": f"Bad args for '{req.name}': {e}"})
    except Exception as e:
        return _JSONResponse(status_code=500, content={"error": str(e)})

@api.get("/tools", summary="List all registered K8s tools with their parameter signatures")
async def api_tools():
    import inspect as _inspect
    out = {}
    for name, entry in K8S_TOOLS.items():
        fn = entry.get("fn")
        params = {}
        if fn:
            for pname, p in _inspect.signature(fn).parameters.items():
                params[pname] = {
                    "default": None if p.default is _inspect.Parameter.empty else p.default,
                    "required": p.default is _inspect.Parameter.empty,
                }
        out[name] = {
            "description": entry.get("description", ""),
            "parameters":  params,
            "curl_example": (
                f'curl -s -X POST http://localhost:8000/api/tool '
                f'-H "Content-Type: application/json" '
                f'-d \'{{"name":"{name}","args":{{}}}}\''
            ),
        }
    return {"count": len(out), "tools": out}

@api.get("/pods", summary="Pod health summary  — ?ns=<namespace>  (default: all)")
async def api_pods(ns: str = "all"):
    """
    curl -s 'http://localhost:8000/api/pods?ns=cdp-drs'
    curl -s 'http://localhost:8000/api/pods'
    """
    from tools_k8s import get_pod_status
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(
        None, lambda: get_pod_status(namespace=ns, show_all=True, raw_output=False))
    return {"namespace": ns, "output": raw}

@api.get("/pods/raw", summary="kubectl-style pod table — ?ns=<namespace>  (default: all)")
async def api_pods_raw(ns: str = "all"):
    """
    curl -s 'http://localhost:8000/api/pods/raw?ns=longhorn-system'
    """
    from tools_k8s import get_pod_status
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(
        None, lambda: get_pod_status(namespace=ns, show_all=True, raw_output=True))
    return {"namespace": ns, "output": raw}

@api.get("/nodes", summary="Node health including GPU detection")
async def api_nodes():
    """
    curl -s http://localhost:8000/api/nodes
    """
    from tools_k8s import get_node_health
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(None, get_node_health)
    return {"output": raw}

@api.get("/events", summary="Cluster events — ?ns=<namespace>&warn=1 for warnings only")
async def api_events(ns: str = "all", warn: int = 0):
    """
    curl -s 'http://localhost:8000/api/events?warn=1'
    curl -s 'http://localhost:8000/api/events?ns=cdp-drs&warn=0'
    """
    from tools_k8s import get_events
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(
        None, lambda: get_events(namespace=ns, warning_only=bool(warn)))
    return {"namespace": ns, "warnings_only": bool(warn), "output": raw}

@api.get("/deployments", summary="Deployment replica status — ?ns=<namespace>")
async def api_deployments(ns: str = "all"):
    """
    curl -s 'http://localhost:8000/api/deployments?ns=cattle-system'
    """
    from tools_k8s import get_deployment_status
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(
        None, lambda: get_deployment_status(namespace=ns))
    return {"namespace": ns, "output": raw}

@api.get("/pvcs", summary="PVC / persistent storage status — ?ns=<namespace>")
async def api_pvcs(ns: str = "all"):
    """
    curl -s 'http://localhost:8000/api/pvcs?ns=longhorn-system'
    curl -s 'http://localhost:8000/api/pvcs'
    """
    from tools_k8s import get_pvc_status
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(
        None, lambda: get_pvc_status(namespace=ns))
    return {"namespace": ns, "output": raw}

@api.get("/namespaces", summary="All namespaces and their phase/status")
async def api_namespaces():
    """
    curl -s http://localhost:8000/api/namespaces
    """
    from tools_k8s import get_namespace_status
    import asyncio
    raw = await asyncio.get_event_loop().run_in_executor(None, get_namespace_status)
    return {"output": raw}

@api.get("/rag/stats", summary="LanceDB document and Excel row statistics")
async def api_rag_stats():
    """
    curl -s http://localhost:8000/api/rag/stats
    """
    return get_doc_stats()

@api.get("/rag/files", summary="List all ingested files in LanceDB")
async def api_rag_files():
    """
    Returns distinct filenames ingested into both LanceDB tables.
    curl -s http://localhost:8000/api/rag/files
    """
    try:
        _, docs_tbl, excel_tbl = _get_lancedb()
        files = []

        # ── Prose docs table ──────────────────────────────────────────────────
        try:
            docs_count = docs_tbl.count_rows()
            if docs_count > 0:
                rows = docs_tbl.search().limit(docs_count + 1).to_list()
                seen = {}
                for r in rows:
                    src = r.get("source", "")
                    if src and src not in seen:
                        seen[src] = {
                            "filename": Path(src).name,
                            "type":     r.get("doc_type", "general"),
                            "table":    "docs",
                        }
                files.extend(seen.values())
        except Exception:
            pass

        # ── Excel table ───────────────────────────────────────────────────────
        try:
            excel_count = excel_tbl.count_rows()
            if excel_count > 0:
                rows = excel_tbl.search().limit(excel_count + 1).to_list()
                seen = {}
                for r in rows:
                    fhash = r.get("file_hash", "")
                    fname = r.get("source_file", f"excel-{fhash[:8]}")
                    if fhash and fhash not in seen:
                        seen[fhash] = {
                            "filename": fname,
                            "type":     "excel",
                            "table":    "excel_issues",
                            "sheets":   set(),
                            "rows":     0,
                        }
                    if fhash in seen:
                        seen[fhash]["sheets"].add(r.get("sheet", ""))
                        seen[fhash]["rows"] += 1
                for v in seen.values():
                    v["sheets"] = sorted(v["sheets"])
                files.extend(seen.values())
        except Exception:
            pass

        return {"total": len(files), "files": files}
    except Exception as e:
        return _JSONResponse(status_code=500, content={"error": str(e)})

@api.get("/rag/query", summary="RAG-only query — no LLM synthesis, returns full untruncated context")
async def api_rag_query(query: str, top_k: int = 50, sheet: Optional[str] = None):
    """
    Used by the ECS Knowledge Bot tab. top_k controls how many rows are retrieved
    per search pass (unresolved + all). Range: 10–500. Default: 50.
    Results are not re-capped after dedup, so effective output can be up to 2×top_k.

    curl -s "http://localhost:8000/api/rag/query?query=longhorn+pvc+stuck&top_k=100&sheet=Known+Issues"
    """
    if not query.strip():
        return {"answer": "", "context": ""}
    top_k = max(10, min(top_k, 500))   # hard clamp: 10–500
    try:
        context = rag_retrieve(query=query, top_k=top_k, sheet=sheet)
        if not context.strip():
            context = "No matching entries found in the knowledge base for this query."
        return {"answer": context, "query": query, "sheet": sheet or "all"}
    except Exception as e:
        return _JSONResponse(status_code=500, content={"error": str(e)})


class KbAskRequest(BaseModel):
    q: str
    top_k: int = 50
    sheet: Optional[str] = None

@api.post("/kb/ask", summary="ECS Knowledge Bot — RAG retrieval + LLM synthesis, no cluster tools")
async def api_kb_ask(req: KbAskRequest):
    """
    Retrieves relevant context from the LanceDB knowledge base, then asks the LLM
    to synthesise a focused answer. No cluster tools are called.

    curl -s -X POST http://localhost:8000/api/kb/ask \\
         -H 'Content-Type: application/json' \\
         -d '{"q":"list all longhorn known issues","top_k":50}'
    """
    if not req.q.strip():
        return _JSONResponse(status_code=400, content={"error": "q must not be empty"})

    top_k = max(10, min(req.top_k, 500))
    logger.info(f"[API] POST /api/kb/ask  q={req.q!r:.120}  top_k={top_k}")

    KB_SYSTEM_PROMPT = (
        "You are the ECS Knowledge Bot, a read-only assistant that answers questions "
        "strictly from the provided knowledge base context. "
        "The context contains runbooks, known issues, dos and don'ts, prerequisites, and past learnings "
        "for a Cloudera ECS Kubernetes cluster.\n\n"
        "Rules:\n"
        "- Answer only from the context provided. Do not use general knowledge.\n"
        "- If the context contains no relevant information, say so clearly.\n"
        "- For Known Issues, present each issue as a structured block:\n"
        "  **[Issue ID] Summary** | Severity | Status\n"
        "  - Symptom   : ...\n"
        "  - Root Cause: ...\n"
        "  - Fix       : ...\n"
        "  - Jira      : ...\n"
        "- Flag UNRESOLVED issues with ⚠️.\n"
        "- Be concise. Do not repeat the question. Do not add preamble.\n"
        "- Never suggest live cluster operations — this bot has no cluster access."
    )

    try:
        context = await asyncio.get_event_loop().run_in_executor(
            None, lambda: rag_retrieve(query=req.q, top_k=top_k, sheet=req.sheet)
        )
        if not context.strip():
            return {"answer": "No matching entries found in the knowledge base for this query.",
                    "query": req.q, "top_k": top_k}

        user_msg = f"Context from knowledge base:\n\n{context}\n\nQuestion: {req.q}"

        # ── GGUF path ──────────────────────────────────────────────────────────
        if tokenizer is None:
            resp = await asyncio.get_event_loop().run_in_executor(None, lambda: model.create_chat_completion(
                messages=[
                    {"role": "system", "content": KB_SYSTEM_PROMPT},
                    {"role": "user",   "content": user_msg},
                ],
                max_tokens=max(512, _MAX_NEW_TOKENS),
                temperature=0.3,
                top_p=0.9,
            ))
            answer = resp["choices"][0]["message"].get("content", "").strip()

        # ── HuggingFace Transformers path ──────────────────────────────────────
        else:
            import torch
            chat_msgs = [
                {"role": "system", "content": KB_SYSTEM_PROMPT},
                {"role": "user",   "content": user_msg},
            ]
            template_kwargs = {"add_generation_prompt": True}
            if _is_qwen3:
                template_kwargs["enable_thinking"] = False

            encoded = tokenizer.apply_chat_template(
                chat_msgs, tokenize=True, return_tensors="pt", **template_kwargs
            )
            input_ids = (encoded["input_ids"] if hasattr(encoded, "__getitem__") and not hasattr(encoded, "shape")
                         else encoded).to(model.device)

            _max_new = max(512, _MAX_NEW_TOKENS)
            model_max = getattr(tokenizer, "model_max_length", None) or 32768
            if model_max > 131072:
                model_max = 32768
            budget = model_max - _max_new - 64
            if input_ids.shape[-1] > budget:
                keep_head = budget // 2
                keep_tail = budget - keep_head
                input_ids = torch.cat([input_ids[:, :keep_head], input_ids[:, -keep_tail:]], dim=-1)

            with torch.no_grad():
                out = model.generate(
                    input_ids,
                    max_new_tokens=_max_new,
                    do_sample=True,
                    temperature=0.3,
                    top_p=0.9,
                    pad_token_id=tokenizer.eos_token_id,
                )
            new_tokens = out[0][input_ids.shape[-1]:]
            answer = tokenizer.decode(new_tokens, skip_special_tokens=True).strip()

        return {"answer": answer, "query": req.q, "top_k": top_k}

    except Exception as e:
        logger.error(f"[API/kb/ask] {e}", exc_info=True)
        return _JSONResponse(status_code=500, content={"error": str(e)})

@api.get("/system", summary="Live CPU / RAM / GPU utilisation metrics")
async def api_system():
    """
    curl -s http://localhost:8000/api/system
    """
    cpu_per = psutil.cpu_percent(interval=0.2, percpu=True)
    mem     = psutil.virtual_memory()
    freq    = psutil.cpu_freq()
    return {
        "cpu_total_pct":  round(psutil.cpu_percent(interval=None), 1),
        "cpu_per_core":   [round(p, 1) for p in cpu_per],
        "cpu_count":      psutil.cpu_count(logical=True),
        "freq_mhz":       round(freq.current) if freq else 0,
        "load_avg_1_5_15": [round(x, 2) for x in psutil.getloadavg()],
        "ram_total_gb":   round(mem.total / 1e9, 1),
        "ram_used_gb":    round(mem.used  / 1e9, 1),
        "ram_pct":        mem.percent,
        "gpus":           _gpu_metrics(),
        "num_gpu":        NUM_GPU,
    }

@api.get("/prompt", summary="Read the current system_prompt.txt contents")
async def api_get_prompt():
    """
    curl -s http://localhost:8000/api/prompt
    """
    if not _PROMPT_FILE.exists():
        return _JSONResponse(status_code=404, content={"error": "config/system_prompt.txt not found"})
    return {
        "file":    str(_PROMPT_FILE),
        "content": _PROMPT_FILE.read_text(encoding="utf-8"),
    }

class PromptUpdateRequest(BaseModel):
    content: str

@api.put("/prompt", summary="Overwrite system_prompt.txt and hot-reload the agent")
async def api_put_prompt(req: PromptUpdateRequest):
    """
    Update system_prompt.txt in-place and immediately rebuild the agent.
    The new prompt is active for the very next /api/ask call.

    curl -s -X PUT http://localhost:8000/api/prompt \\
         -H 'Content-Type: application/json' \\
         -d '{"content":"You are a K8s expert...\\n{custom_rules}"}'
    """
    if not req.content.strip():
        return _JSONResponse(status_code=400, content={"error": "content must not be empty"})
    if "{custom_rules}" not in req.content:
        return _JSONResponse(status_code=400, content={
            "error": "Prompt must contain the {custom_rules} placeholder."
        })
    _PROMPT_FILE.write_text(req.content, encoding="utf-8")
    global _agent
    _agent = None
    logger.info("[Prompt] system_prompt.txt updated and agent cache cleared via API")
    return {"ok": True, "chars": len(req.content), "message": "Prompt saved. Agent will rebuild on next request."}

@api.post("/reload-prompt", summary="Hot-reload system_prompt.txt without restarting")
async def api_reload_prompt():
    """
    Re-reads system_prompt.txt from disk and clears the agent cache so the
    next request picks up any manual edits you made to the file.

    curl -s -X POST http://localhost:9000/api/reload-prompt
    """
    if not _PROMPT_FILE.exists():
        return _JSONResponse(status_code=404, content={"error": "config/system_prompt.txt not found"})
    global _agent
    _agent = None
    text = _PROMPT_FILE.read_text(encoding="utf-8")
    logger.info(f"[Prompt] Hot-reload triggered via API — {len(text)} chars")
    return {"ok": True, "chars": len(text), "message": "Agent cache cleared. New prompt active on next request."}


@api.get("/config", summary="Read runtime configuration (e.g. KUBECTL_MAX_CHARS)")
async def api_get_config():
    """Return the current live runtime config values that can be changed without restart."""
    import tools.tools_k8s as _tk
    return {
        "kubectl_max_chars": _tk._KUBECTL_MAX_OUT,
        "max_new_tokens":    _MAX_NEW_TOKENS,
        "llm_timeout":       _LLM_TIMEOUT,
    }


@api.post("/config", summary="Update runtime configuration (e.g. KUBECTL_MAX_CHARS)")
async def api_set_config(body: dict):
    """
    Update live config values without restarting the server.

    curl -s -X POST http://localhost:9000/api/config \\
         -H 'Content-Type: application/json' \\
         -d '{"kubectl_max_chars": 30000}'
    """
    import tools.tools_k8s as _tk
    updated = {}
    if "kubectl_max_chars" in body:
        val = int(body["kubectl_max_chars"])
        val = max(1000, min(val, 200000))   # clamp 1 000 – 200 000
        _tk._KUBECTL_MAX_OUT = val
        updated["kubectl_max_chars"] = val
        logger.info(f"[Config] KUBECTL_MAX_CHARS updated to {val}")
    if "max_new_tokens" in body:
        global _MAX_NEW_TOKENS
        val = int(body["max_new_tokens"])
        val = max(256, min(val, 16384))     # clamp 256 – 16 384
        _MAX_NEW_TOKENS = val
        updated["max_new_tokens"] = val
        logger.info(f"[Config] MAX_NEW_TOKENS updated to {val}")
    if "llm_timeout" in body:
        global _LLM_TIMEOUT
        val = int(body["llm_timeout"])
        val = max(30, min(val, 1800))       # clamp 30s – 1800s (30 min)
        _LLM_TIMEOUT = val
        updated["llm_timeout"] = val
        logger.info(f"[Config] LLM_TIMEOUT updated to {val}s")
    if not updated:
        return _JSONResponse(status_code=400, content={"error": "No recognised config keys in body"})
    return {"ok": True, "updated": updated}

app.include_router(api)

if _HERE.joinpath("web", "static").exists(): app.mount("/static", StaticFiles(directory=str(_HERE / "web" / "static")), name="static")

@app.get("/", response_class=FileResponse)
async def serve_ui():
    if _HERE.joinpath("web", "index.html").exists(): return FileResponse(str(_HERE / "web" / "index.html"), media_type="text/html")
    return {"error": "web/index.html not found"}

if __name__ == "__main__":
    if _ARGS.ingest:
        print(f"\n📂 Ingesting documents from: {_ARGS.ingest}  (force={_ARGS.force})")
        init_db()
        results = ingest_directory(_ARGS.ingest, force=_ARGS.force)
        total   = sum(r.get("chunks", 0) for r in results)
        print(f"\n✅  {len(results)} file(s)  |  {total} total chunks stored in LanceDB\n")
        for r in results:
            icon = ("✓" if r["status"] == "ingested" else "—" if r["status"] == "skipped" else "✗")
            print(f"  {icon}  {r['file']:<42} {r['status']:<10} ({r['chunks']} chunks)")
        print()

    import uvicorn
    gpu_str    = (f"{NUM_GPU} GPU(s) — GPU inference" if NUM_GPU > 0 else "None — CPU inference")
    tool_count = len(K8S_TOOLS)

    print(f"""
╔════════════════════════════════════════════════════════════╗
║            Cloudera ECS AI Ops  v2.0 (Transformers)        ║
╠════════════════════════════════════════════════════════════╣
║  LLM      : {LLM_MODEL:<46} ║
║  Embed    : {EMBED_MODEL:<46} ║
║  GPU      : {gpu_str:<46} ║
║  Tools    : {tool_count} kubectl tools registered{'':<26} ║
║  LanceDB  : {LANCEDB_DIR:<46} ║
║  Server   : http://{_ARGS.host}:{_ARGS.port:<38} ║
╚════════════════════════════════════════════════════════════╝
""")

    uvicorn.run("app:app", host=_ARGS.host, port=_ARGS.port, reload=_ARGS.reload, log_level="warning")
