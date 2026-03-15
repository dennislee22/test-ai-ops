import hashlib, re
from pathlib import Path
import config.config as config
from rag.store import embed_text, _get_lancedb

CHUNK_SIZE    = 512
CHUNK_OVERLAP = 64

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
    path  = Path(file_path)
    fhash = hashlib.md5(path.read_bytes()).hexdigest()
    _, docs_tbl, _ = _get_lancedb()

    if not force:
        try:
            existing = docs_tbl.search().where(
                f"source = '{str(path)}' AND file_hash = '{fhash}'"
            ).limit(1).to_list()
            if existing:
                config._log_rag.info(f"[RAG] Skip (unchanged): {path.name}")
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
    config._log_rag.info(f"[RAG] {path.name}: {len(chunks)} chunks  type={doc_type}")

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

_SHEET_ROLES = {
    "Known Issues": {
        "symptom":    ["symptom", "observable", "error message", "what breaks",
                       "description", "what happens"],
        "problem":    ["problem", "summary", "title", "issue name",
                       "incident summary", "description"],
        "root_cause": ["root cause", "cause", "reason", "why", "root"],
        "fix":        ["remediation", "fix", "resolution", "solution",
                       "how to fix", "steps", "corrective"],
        "issue_id":   ["issue id", "id", "ticket", "issue #", "#"],
        "category":   ["category", "type", "component", "area"],
        "severity":   ["severity", "priority", "impact", "level"],
        "present":    ["present", "unresolved", "open", "active", "status"],
        "jira":       ["jira", "ticket", "postmortem", "link", "url"],
        "discovered": ["discovered", "found", "created", "reported", "date",
                       "incident date"],
        "resolved":   ["resolved", "closed", "fixed date"],
        "notes":      ["notes", "lessons", "comments", "additional",
                       "remarks", "observation"],
    },
    "Dos and Donts": {
        "do_text":    ["do", "should", "recommended", "best practice",
                       "correct", "✅", "yes", "good"],
        "dont_text":  ["don", "avoid", "never", "not", "incorrect",
                       "wrong", "❌", "bad", "prohibited"],
        "rationale":  ["rationale", "reason", "why", "explanation",
                       "impact", "because"],
        "category":   ["category", "type", "area", "component"],
        "jira":       ["jira", "related", "ticket", "issue", "link"],
    },
    "Prerequisites": {
        "prerequisite": ["prerequisite", "requirement", "condition",
                         "must have", "needed", "dependency", "what"],
        "rationale":    ["why", "matters", "reason", "rationale",
                         "impact", "importance", "because"],
        "how_to_verify": ["verify", "check", "validate", "confirm",
                          "how to", "test", "proof"],
        "category":     ["category", "type", "area", "component"],
    },
    "Past Learnings": {
        "problem":    ["incident", "summary", "what happened", "event",
                       "description", "title", "subject"],
        "root_cause": ["went wrong", "cause", "reason", "why", "root",
                       "potential cause", "failure"],
        "fix":        ["worked well", "resolution", "fix", "solution",
                       "corrective", "action", "potential resolution",
                       "key learning", "learning"],
        "learning":   ["learning", "lesson", "takeaway", "insight",
                       "key learning", "potential resolution",
                       "what we learned"],
        "action_taken": ["action", "taken", "implemented", "change",
                          "what was done", "resolution", "potential resolution"],
        "present":    ["prevented", "recurrence", "recur", "status",
                       "resolved", "fixed"],
        "jira":       ["jira", "postmortem", "ticket", "link", "url"],
        "discovered": ["date", "incident date", "when", "discovered",
                       "occurred"],
    },
}

_INDEX_HINTS = {"#", "no", "num", "index", "row", "sl no", "s.no"}

def _resolve_col(row: dict, *hints: str, cols: list = None) -> str:
    search_cols = cols or list(row.keys())
    for hint in hints:
        hint_l = hint.lower()
        for col in search_cols:
            if hint_l in col.lower() and row.get(col, "").strip():
                return row[col].strip()
    return ""

def _best_col(row: dict, role_hints: list, cols: list) -> str:
    return _resolve_col(row, *role_hints, cols=cols)

def _all_values(row: dict, cols: list, exclude_roles: set) -> str:
    parts = []
    for col in cols:
        col_l = col.lower().strip()
        if col_l in _INDEX_HINTS or col_l.rstrip(".") in _INDEX_HINTS:
            continue
        if any(hint in col_l for role in exclude_roles
               for hint in _SHEET_ROLES.get(role, {}).get("symptom", [])):
            continue
        v = str(row.get(col, "")).strip()
        if v and v.lower() not in ("none", "nan", "n/a", "-", ""):
            parts.append(v)
    return " / ".join(parts)

def _warn_unmatched(sn: str, cols: list, matched: set, log_fn):
    unmatched = [
        c for c in cols
        if c not in matched
        and c.lower().strip() not in _INDEX_HINTS
        and c.lower().strip().rstrip(".") not in _INDEX_HINTS
    ]
    if unmatched:
        log_fn(f"[RAG/Excel] Sheet '{sn}' — unrecognised columns (stored as-is): {unmatched}")

def _map_row(row: dict, sheet_type: str, cols: list) -> dict:
    roles = _SHEET_ROLES.get(sheet_type, {})
    resolved = {role: _best_col(row, hints, cols) for role, hints in roles.items()}

    if sheet_type == "Known Issues":
        primary = resolved.get("symptom", "") or resolved.get("problem", "")
        secondary = resolved.get("problem", "") if primary else ""
        search_text = " / ".join(t for t in [primary, secondary] if t).strip(" /")
    elif sheet_type == "Dos and Donts":
        search_text = " / ".join(
            t for t in [resolved.get("do_text",""), resolved.get("dont_text","")]
            if t
        ).strip(" /")
    elif sheet_type == "Prerequisites":
        search_text = resolved.get("prerequisite", "")
    else:
        search_text = " / ".join(
            t for t in [
                resolved.get("problem",""),
                resolved.get("root_cause",""),
                resolved.get("fix","") or resolved.get("learning",""),
            ] if t
        ).strip(" /")

    if not search_text:
        search_text = _all_values(row, cols, exclude_roles=set())

    return resolved, search_text

def ingest_excel(file_path: str, force: bool = False) -> dict:
    try:
        import pandas as pd
    except ImportError:
        return {"file": Path(file_path).name, "status": "error", "chunks": 0,
                "error": "pandas not installed — pip install pandas openpyxl"}

    path  = Path(file_path)
    fhash = hashlib.md5(path.read_bytes()).hexdigest()
    _, _, excel_tbl = _get_lancedb()

    rows  = []
    total = 0

    try:
        xl = pd.read_excel(str(path), sheet_name=None, dtype=str)
    except Exception as e:
        return {"file": path.name, "status": "error", "chunks": 0, "error": str(e)}

    for sheet_name, df in xl.items():
        sn   = sheet_name.strip()
        df.columns = [c.strip() for c in df.columns]

        sn_l = sn.lower()
        if "known" in sn_l or "issue" in sn_l:
            sheet_type = "Known Issues"
            id_prefix  = "ki"
        elif "dos" in sn_l or "don" in sn_l or "donts" in sn_l or "practice" in sn_l:
            sheet_type = "Dos and Donts"
            id_prefix  = "dd"
        elif "prereq" in sn_l or "prerequisite" in sn_l or "requirement" in sn_l:
            sheet_type = "Prerequisites"
            id_prefix  = "pr"
        elif "learn" in sn_l or "past" in sn_l or "incident" in sn_l or "postmortem" in sn_l:
            sheet_type = "Past Learnings"
            id_prefix  = "pl"
        else:
            config._log_rag.info(f"[RAG/Excel] Skipping unrecognised sheet '{sn}'")
            continue

        config._log_rag.info(f"[RAG/Excel] Sheet '{sn}' → {sheet_type} ({len(df)} rows)")
        cols = list(df.columns)

        matched_cols = set()
        for role, hints in _SHEET_ROLES.get(sheet_type, {}).items():
            for hint in hints:
                for col in cols:
                    if hint.lower() in col.lower():
                        matched_cols.add(col)

        _warn_unmatched(sn, cols, matched_cols, config._log_rag.warning)

        for _, raw_row in df.iterrows():
            row = {col: (str(v).strip() if v is not None else "")
                   for col, v in raw_row.items()}
            row = {k: ("" if v.lower() in ("none","nan","n/a","-","nat") else v)
                   for k, v in row.items()}

            resolved, search_text = _map_row(row, sheet_type, cols)
            if not search_text:
                continue

            rows.append({
                "id":          f"{id_prefix}-{fhash}-{total}",
                "vector":      embed_text(search_text),
                "source_file": path.name,
                "file_hash":   fhash,
                "sheet":       sheet_type,
                "symptom":     search_text,
                "issue_id":    resolved.get("issue_id",    ""),
                "category":    resolved.get("category",    ""),
                "problem":     resolved.get("problem",     ""),
                "root_cause":  resolved.get("root_cause",  ""),
                "fix":         resolved.get("fix",         ""),
                "severity":    resolved.get("severity",    ""),
                "present":     resolved.get("present",     ""),
                "jira":        resolved.get("jira",        ""),
                "discovered":  resolved.get("discovered",  ""),
                "resolved":    resolved.get("resolved",    ""),
                "notes":       resolved.get("notes",       ""),
                "do_text":     resolved.get("do_text",     ""),
                "dont_text":   resolved.get("dont_text",   ""),
                "rationale":   resolved.get("rationale",   ""),
                "prerequisite": resolved.get("prerequisite",""),
                "how_to_verify": resolved.get("how_to_verify",""),
                "learning":    resolved.get("learning",    ""),
                "action_taken": resolved.get("action_taken",""),
            })
            total += 1

    if not rows:
        return {"file": path.name, "status": "empty", "chunks": 0}

    try:
        excel_tbl.delete(f"id LIKE 'ki-{fhash}%' OR id LIKE 'dd-{fhash}%' OR id LIKE 'pr-{fhash}%' OR id LIKE 'pl-{fhash}%'")
    except Exception:
        pass

    excel_tbl.add(rows)
    config._log_rag.info(f"[RAG/Excel] {path.name}: {total} rows ingested")
    return {"file": path.name, "status": "ingested", "chunks": total, "doc_type": "excel"}

def ingest_directory(docs_dir: str, force: bool = False) -> list:
    p = Path(docs_dir)
    results = []
    for f in sorted(p.glob("**/*.md")) + sorted(p.glob("**/*.pdf")) + sorted(p.glob("**/*.txt")):
        results.append(ingest_file(str(f), force=force))
    for f in sorted(p.glob("**/*.xlsx")) + sorted(p.glob("**/*.xls")):
        results.append(ingest_excel(str(f), force=force))
    return results
