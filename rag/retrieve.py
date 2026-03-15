from typing import Optional
from pathlib import Path
import config
from .store import embed_text, _get_lancedb

TOP_K = 10

_MSG_NO_INGEST = (
    "No results found. The knowledge base appears to be empty — "
    "no documents have been ingested yet. "
    "Please go to ⚙ Settings → RAG Documents to upload and ingest your knowledge base files."
)

_KB_TOPIC_KEYWORDS = [
    "longhorn", "ecs", "cdp", "cloudera", "rancher", "vault", "prometheus",
    "grafana", "cert-manager", "coredns", "ingress", "pvc", "pv", "storageclass",
    "known issue", "known problem", "list issue", "unresolved", "open issue",
    "dos and don", "best practice", "prerequisite", "prereq",
    "past learning", "postmortem", "incident", "what went wrong",
    "runbook", "playbook", "troubleshoot", "fix", "remediation",
    "crashloop", "oomkill", "imagepull", "pending", "evicted",
    "not running", "not ready", "node pressure", "1.5", "1.6", "sp1", "sp2", "upgrade",
]

def _is_kb_topic(question: str) -> bool:
    ql = question.lower()
    return any(k in ql for k in _KB_TOPIC_KEYWORDS)

def rag_retrieve(query: str, top_k: int = TOP_K, doc_type: Optional[str] = None, sheet: Optional[str] = None) -> str:
    _, docs_tbl, excel_tbl = _get_lancedb()
    sections = []

    _SHEET_ALIASES = {
        "dos": "Dos and Donts", "donts": "Dos and Donts", "dos and donts": "Dos and Donts", "dos & donts": "Dos and Donts",
        "known issues": "Known Issues", "known": "Known Issues", "issues": "Known Issues",
        "prerequisites": "Prerequisites", "prereq": "Prerequisites",
        "past learnings": "Past Learnings", "learnings": "Past Learnings", "past": "Past Learnings",
    }
    if sheet: sheet = _SHEET_ALIASES.get(sheet.lower().strip(), sheet)

    try: excel_count = excel_tbl.count_rows()
    except Exception: excel_count = 0

    if excel_count > 0:
        try:
            qvec = embed_text(query)
            sheet_filter = f"sheet = '{sheet}'" if sheet else None
            _aq = excel_tbl.search(qvec, vector_column_name="vector")
            if sheet_filter: _aq = _aq.where(sheet_filter)
            all_hits = _aq.limit(top_k * 2).to_list()

            seen, merged = set(), []
            for r in all_hits:
                if r["id"] not in seen:
                    seen.add(r["id"])
                    merged.append(r)
            merged = merged[:top_k]

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
                            f"  Problem  : {hit.get('problem','')}\n  Symptom  : {hit.get('symptom','')}\n"
                            f"  RootCause: {hit.get('root_cause','')}\n  Fix      : {hit.get('fix','')}\n"
                            + (f"  Notes    : {hit.get('notes','')}\n" if hit.get("notes") else "")
                        )
                    elif sheet == "Dos and Donts":
                        lines.append(f"[{sheet}] {hit.get('category','')} | relevance:{sim}\n  ✅ DO   : {hit.get('do_text','')}\n  ❌ DON'T: {hit.get('dont_text','')}\n  Why     : {hit.get('rationale','')}\n")
                    elif sheet == "Prerequisites":
                        lines.append(f"[{sheet}] {hit.get('category','')} | relevance:{sim}\n  Prerequisite : {hit.get('prerequisite','')}\n  Why it matters: {hit.get('rationale','')}\n  How to verify : {hit.get('how_to_verify','')}\n")
                    elif sheet == "Past Learnings":
                        lines.append(f"[{sheet}] {hit.get('discovered','')} | relevance:{sim}\n  Incident         : {hit.get('problem','')}\n  Potential Cause  : {hit.get('root_cause','')}\n  Resolution/Fix   : {hit.get('fix','') or hit.get('learning','')}\n  Action Taken     : {hit.get('action_taken','')}\n" + (f"  Jira             : {hit.get('jira','')}\n" if hit.get('jira') else ""))
                    else:
                        lines.append(f"[{sheet}] relevance:{sim}\n  {hit.get('symptom','')}\n")
                sections.append("\n".join(lines))
        except Exception as e:
            config._log_rag.warning(f"[RAG/Excel] Search failed: {e}")

    try: docs_count = docs_tbl.count_rows()
    except Exception: docs_count = 0

    if docs_count > 0:
        try:
            qvec  = embed_text(query)
            srch  = docs_tbl.search(qvec, vector_column_name="vector")
            if doc_type: srch = srch.where(f"doc_type = '{doc_type}'")
            hits = srch.limit(top_k).to_list()
            if hits:
                lines = [f"📄 Documentation ({len(hits)} chunk(s)):\n"]
                for hit in hits:
                    sim = round(1 - hit.get("_distance", 1.0), 3)
                    src = Path(hit.get("source", "?")).name
                    lines.append(f"[{src}] relevance:{sim}\n{hit.get('text','')}\n")
                sections.append("\n".join(lines))
        except Exception as e: config._log_rag.warning(f"[RAG/Docs] Search failed: {e}")

    if sections: return "\n\n---\n\n".join(sections)
    if excel_count == 0 and docs_count == 0: return "KB_EMPTY: No documents have been ingested into the knowledge base."
    return "No relevant documentation found."

def get_doc_stats() -> dict:
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
            except Exception: pass
        docs_by_type: dict = {}
        if docs_count > 0:
            try:
                from collections import Counter
                rows = docs_tbl.search().limit(docs_count + 1).to_list()
                docs_by_type = dict(Counter(r.get("doc_type", "general") for r in rows))
            except Exception: pass
        return {
            "total_chunks": docs_count + excel_count, "docs_chunks": docs_count, "excel_rows": excel_count,
            "docs_by_type": docs_by_type, "excel_by_sheet": excel_by_sheet,
        }
    except Exception as e: return {"total_chunks": 0, "docs_chunks": 0, "excel_rows": 0, "docs_by_type": {}, "excel_by_sheet": {}, "error": str(e)}

RAG_TOOLS = {
    "rag_search": {
        "fn": rag_retrieve,
        "description": (
            "Search the internal knowledge base for known issues, runbooks, troubleshooting guides, "
            "dos and don'ts, prerequisites, past learnings, and operational best practices. "
            "Call this tool in two situations: "
            "(1) AFTER get_unhealthy_pods_detail when a pod shows OOMKilled, CrashLoopBackOff, "
            "ImagePullBackOff, Pending, or any error — use the specific error and component as the query. "
            "(2) DIRECTLY when the user asks about: known issues, problems, documentation, runbooks, "
            "best practices, dos and don'ts, prerequisites, past learnings, postmortems, or WHY "
            "something might be happening. "
            "Call rag_search BEFORE live tools when query contains 'issues', 'problems', "
            "'known issues', 'what could cause', 'best practice', 'how to fix'. "
            "Call live tools BEFORE rag_search when query is about current cluster state "
            "('is X healthy', 'how many pods', 'list pvcs'). "
            "Pass sheet= ONLY when the user explicitly asks for a specific category. "
            "Leave sheet= empty (default) for all other queries — this searches ALL sheets "
            "and returns the best semantic match regardless of which table it comes from. "
            "Valid sheet values: 'Known Issues', 'Dos and Donts', 'Prerequisites', 'Past Learnings'. "
            "Examples: "
            "rag_search(query='CrashLoopBackOff cdp-cadence') — no sheet, searches everything "
            "rag_search(query='OOMKilled sense-db') — no sheet, searches everything "
            "rag_search(query='longhorn storage issues') — no sheet, searches everything "
            "rag_search(query='what are the dos and donts', sheet='Dos and Donts') — user asked explicitly "
            "rag_search(query='prerequisites before deploy', sheet='Prerequisites') — user asked explicitly "
            "rag_search(query='vault incident 2024', sheet='Past Learnings') — user asked explicitly "
            "IMPORTANT: if the result starts with KB_EMPTY: the knowledge base has not been ingested yet. "
            "In that case relay the message as-is — do NOT answer from training data."
        ),
        "parameters": {
            "query": {"type": "string", "description": "Search query — use specific error names, component names, or symptoms."},
            "top_k": {"type": "integer", "default": 10},
            "doc_type": {"type": "string", "default": None},
            "sheet": {"type": "string", "default": None, "description": "Optional sheet filter. Valid values: 'Known Issues', 'Dos and Donts', 'Prerequisites', 'Past Learnings'."},
        },
    },
}
