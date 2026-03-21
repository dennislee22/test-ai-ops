"""
pdf_report.py — ReportLab-based cluster health PDF generator.

Replaces WeasyPrint.  Key advantages:
  • No system font dependency for emoji — we use DejaVuSans which ships with
    most Linux distros and covers the Unicode symbols we use (✓ ✗ ⚠ ● etc.)
  • Emoji like ✅ / ⚠️ / 🔴 are mapped to safe Unicode equivalents that
    DejaVuSans actually contains.
  • The PV-usage table is built directly from k8s data — not from HTML fragments
    that WeasyPrint would silently drop.
  • No CSS compatibility issues.

Public API
----------
    build_pdf(filepath: str | Path) -> Path
        Runs all data-gathering, builds the PDF, writes it to *filepath*.
        Returns the Path on success, raises on error.
"""

from __future__ import annotations

import datetime
import re
import textwrap
from pathlib import Path
from typing import List, Tuple, Optional
from datetime import timezone

# ── ReportLab imports ─────────────────────────────────────────────────────────
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate, Frame, HRFlowable, PageTemplate,
    Paragraph, Spacer, Table, TableStyle,
)
from reportlab.platypus.flowables import KeepTogether

# ── Font setup ────────────────────────────────────────────────────────────────
_FONT_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
]
_FONT_MONO_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
]

_fonts_registered = False

def _ensure_fonts():
    global _fonts_registered
    if _fonts_registered:
        return
    for name, path in [
        ("DejaVuSans",       _FONT_PATHS[0]),
        ("DejaVuSans-Bold",  _FONT_PATHS[1]),
        ("DejaVuMono",       _FONT_MONO_PATHS[0]),
        ("DejaVuMono-Bold",  _FONT_MONO_PATHS[1]),
    ]:
        if Path(path).exists():
            try:
                pdfmetrics.registerFont(TTFont(name, path))
            except Exception:
                pass
    _fonts_registered = True


# ── Emoji → Unicode symbol mapping ───────────────────────────────────────────
# DejaVuSans has good coverage of Misc Symbols (U+2600-U+26FF) and
# Geometric Shapes (U+25A0-U+25FF) but NOT colour emoji (U+1F300+).
_EMOJI_MAP = {
    "✅": "✓",   # U+2713 CHECK MARK
    "⚠️": "⚠",   # U+26A0 WARNING SIGN
    "⚠":  "⚠",
    "🔴": "✗",   # U+2717 BALLOT X
    "🟢": "●",   # U+25CF BLACK CIRCLE  (used for "green = good" in context)
    "❌": "✗",
    "⏸": "||",
    "▶":  "▶",   # U+25B6 — in DejaVu Geometric Shapes
    "★":  "★",   # U+2605 BLACK STAR
    "●":  "●",
    "•":  "•",
    "—":  "—",
    "💬": "",
    "📋": "",
}

# Matches only high-codepoint colour emoji that DejaVuSans cannot render.
# Applied AFTER the _EMOJI_MAP substitutions so our wanted replacements are kept.
_COLOUR_EMOJI_RE = re.compile(
    "["
    "\U0001F300-\U0001F9FF"   # Misc symbols & pictographs (colour emoji block)
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "]+",
    flags=re.UNICODE
)

def _safe(text: str) -> str:
    """
    1. Strip HTML tags and numeric entities.
    2. Apply the emoji→Unicode symbol map (dict lookups, so ✅ → ✓ etc.)
    3. Remove any remaining high-codepoint colour emoji that DejaVuSans can't render.
    """
    if not text:
        return ""
    # 1. HTML cleanup first so entity-encoded emoji don't confuse step 2
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"&#\d+;", "", text)          # numeric entities e.g. &#128172;
    text = re.sub(r"&[a-z]+;", "", text)        # named entities &amp; etc.
    # 2. Known emoji → safe Unicode substitution (longest keys first to avoid partial match)
    for emoji_char, replacement in sorted(_EMOJI_MAP.items(), key=lambda kv: -len(kv[0])):
        text = text.replace(emoji_char, replacement)
    # 3. Strip residual colour emoji that DejaVuSans cannot render
    text = _COLOUR_EMOJI_RE.sub("", text)
    return text.strip()


def _cap(items, n=5):
    shown = items[:n]
    rest  = len(items) - n
    result = ", ".join(str(i) for i in shown)
    if rest > 0:
        result += f" … and {rest} more"
    return result


# ── Style sheet ───────────────────────────────────────────────────────────────

def _make_styles():
    _ensure_fonts()
    base = getSampleStyleSheet()

    NAVY   = colors.HexColor("#1e3a5f")
    TEAL   = colors.HexColor("#0369a1")
    GREEN  = colors.HexColor("#16a34a")
    AMBER  = colors.HexColor("#d97706")
    RED    = colors.HexColor("#dc2626")
    LGRAY  = colors.HexColor("#f8fafc")
    MGRAY  = colors.HexColor("#e2e8f0")
    DGRAY  = colors.HexColor("#64748b")

    def ps(name, **kw):
        defaults = dict(fontName="DejaVuSans", fontSize=9, leading=13,
                        textColor=colors.HexColor("#1e293b"))
        defaults.update(kw)
        return ParagraphStyle(name, **defaults)

    styles = {
        "title":    ps("title",    fontSize=22, fontName="DejaVuSans-Bold",
                       textColor=NAVY, spaceBefore=0, spaceAfter=4, leading=28),
        "subtitle": ps("subtitle", fontSize=11, textColor=DGRAY, spaceAfter=2),
        "section":  ps("section",  fontSize=13, fontName="DejaVuSans-Bold",
                       textColor=NAVY, spaceBefore=14, spaceAfter=4, leading=18),
        "subsect":  ps("subsect",  fontSize=10, fontName="DejaVuSans-Bold",
                       textColor=TEAL, spaceBefore=8, spaceAfter=3),
        "body":     ps("body",     spaceAfter=3),
        "ok":       ps("ok",       textColor=GREEN, spaceAfter=2),
        "warn":     ps("warn",     textColor=AMBER, spaceAfter=2),
        "err":      ps("err",      textColor=RED,   spaceAfter=2),
        "note":     ps("note",     fontSize=8, textColor=DGRAY, spaceAfter=2),
        "mono":     ps("mono",     fontName="DejaVuMono", fontSize=8, leading=11),
        "footer":   ps("footer",   fontSize=7, textColor=DGRAY),
    }
    styles["_GREEN"]  = GREEN
    styles["_AMBER"]  = AMBER
    styles["_RED"]    = RED
    styles["_NAVY"]   = NAVY
    styles["_TEAL"]   = TEAL
    styles["_LGRAY"]  = LGRAY
    styles["_MGRAY"]  = MGRAY
    styles["_DGRAY"]  = DGRAY
    return styles


# ── Table builder ─────────────────────────────────────────────────────────────

def _make_table(headers: List[str], rows: List[List[str]], styles_map: dict,
                cap: int = 20, col_widths=None) -> Optional[Table]:
    if not rows:
        return None
    display_rows = rows[:cap]

    GREEN = styles_map["_GREEN"]
    AMBER = styles_map["_AMBER"]
    RED   = styles_map["_RED"]
    NAVY  = styles_map["_NAVY"]
    LGRAY = styles_map["_LGRAY"]
    MGRAY = styles_map["_MGRAY"]

    body_style = styles_map["body"]
    mono_style = styles_map["mono"]

    def _cell(val):
        s = _safe(str(val)) if val is not None else ""
        # colour coding based on prefix markers
        if s.startswith("✓"):
            return Paragraph(s, ParagraphStyle("cok", parent=body_style,
                                                textColor=GREEN, fontSize=8))
        if s.startswith("⚠"):
            return Paragraph(s, ParagraphStyle("cwn", parent=body_style,
                                                textColor=AMBER, fontSize=8))
        if s.startswith("✗"):
            return Paragraph(s, ParagraphStyle("cer", parent=body_style,
                                                textColor=RED, fontSize=8))
        return Paragraph(s, ParagraphStyle("cd", parent=body_style, fontSize=8))

    header_row = [Paragraph(_safe(h),
                             ParagraphStyle("th", parent=body_style,
                                            fontName="DejaVuSans-Bold",
                                            fontSize=8, textColor=colors.white))
                  for h in headers]
    data = [header_row] + [[_cell(c) for c in row] for row in display_rows]

    ts = TableStyle([
        # Header
        ("BACKGROUND",  (0,0), (-1,0),  NAVY),
        ("TEXTCOLOR",   (0,0), (-1,0),  colors.white),
        ("FONTNAME",    (0,0), (-1,0),  "DejaVuSans-Bold"),
        ("FONTSIZE",    (0,0), (-1,0),  8),
        ("BOTTOMPADDING",(0,0),(-1,0),  5),
        ("TOPPADDING",   (0,0),(-1,0),  5),
        # Body alternating rows
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, LGRAY]),
        ("FONTNAME",    (0,1), (-1,-1), "DejaVuSans"),
        ("FONTSIZE",    (0,1), (-1,-1), 8),
        ("TOPPADDING",  (0,1), (-1,-1), 3),
        ("BOTTOMPADDING",(0,1),(-1,-1), 3),
        ("LEFTPADDING", (0,0), (-1,-1), 5),
        ("RIGHTPADDING",(0,0), (-1,-1), 5),
        # Grid
        ("GRID",        (0,0), (-1,-1), 0.25, MGRAY),
        ("LINEBELOW",   (0,0), (-1,0),  0.75, NAVY),
        # Alignment
        ("VALIGN",      (0,0), (-1,-1), "TOP"),
        ("ALIGN",       (0,0), (-1,-1), "LEFT"),
    ])

    page_w = A4[0] - 28*mm
    if col_widths:
        cw = col_widths
    else:
        n = len(headers)
        cw = [page_w / n] * n

    t = Table(data, colWidths=cw, repeatRows=1)
    t.setStyle(ts)
    return t


def _overflow_note(rows, cap, styles_map):
    over = len(rows) - cap
    if over > 0:
        return Paragraph(f"… and {over} more row(s) not shown.",
                         styles_map["note"])
    return None


# ── Page template ─────────────────────────────────────────────────────────────

def _make_doc(filepath: str) -> BaseDocTemplate:
    doc = BaseDocTemplate(
        str(filepath),
        pagesize=A4,
        leftMargin=14*mm, rightMargin=14*mm,
        topMargin=18*mm,  bottomMargin=16*mm,
        title="ECS Cluster Health Report",
        author="AI-Ops",
    )
    frame = Frame(doc.leftMargin, doc.bottomMargin,
                  doc.width, doc.height, id="main")

    def _header_footer(canvas, doc):
        canvas.saveState()
        # Header bar
        canvas.setFillColor(colors.HexColor("#1e3a5f"))
        canvas.rect(doc.leftMargin, A4[1] - 12*mm,
                    doc.width, 8*mm, stroke=0, fill=1)
        canvas.setFont("DejaVuSans-Bold", 8)
        canvas.setFillColor(colors.white)
        canvas.drawString(doc.leftMargin + 3*mm, A4[1] - 8*mm,
                          "ECS Cluster Health Report")
        canvas.setFont("DejaVuSans", 7)
        canvas.drawRightString(A4[0] - doc.rightMargin - 3*mm, A4[1] - 8*mm,
                               f"Page {doc.page}")
        # Footer line
        canvas.setStrokeColor(colors.HexColor("#e2e8f0"))
        canvas.setLineWidth(0.5)
        canvas.line(doc.leftMargin, 12*mm, A4[0] - doc.rightMargin, 12*mm)
        canvas.setFont("DejaVuSans", 7)
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.drawString(doc.leftMargin, 9*mm, "Generated by AI-Ops — confidential")
        canvas.restoreState()

    doc.addPageTemplates([PageTemplate(id="main", frames=[frame],
                                       onPage=_header_footer)])
    return doc


# ── Section helpers ───────────────────────────────────────────────────────────

def _section(title: str, S: dict) -> List:
    return [
        Spacer(1, 4*mm),
        HRFlowable(width="100%", thickness=1.5, color=S["_NAVY"], spaceAfter=3),
        Paragraph(_safe(title), S["section"]),
    ]

def _subsect(title: str, S: dict) -> Paragraph:
    return Paragraph(_safe(title), S["subsect"])

def _ok(msg: str, S: dict)   -> Paragraph:
    return Paragraph(f"✓ {_safe(msg)}", S["ok"])

def _warn(msg: str, S: dict) -> Paragraph:
    return Paragraph(f"⚠ {_safe(msg)}", S["warn"])

def _err(msg: str, S: dict)  -> Paragraph:
    return Paragraph(f"✗ {_safe(msg)}", S["err"])

def _body(msg: str, S: dict) -> Paragraph:
    return Paragraph(_safe(msg), S["body"])

def _note(msg: str, S: dict) -> Paragraph:
    return Paragraph(_safe(msg), S["note"])


# ── Data helpers (copied from tools_k8s helpers) ──────────────────────────────

def _parse_cpu_cores(val) -> float:
    try:
        s = str(val)
        if s.endswith("m"):
            return float(s[:-1]) / 1000
        return float(s)
    except Exception:
        return 0.0

def _parse_mem_gib(val) -> float:
    try:
        s = str(val)
        if s.endswith("Ki"):
            return int(s[:-2]) / (1024**2)
        if s.endswith("Mi"):
            return int(s[:-2]) / 1024
        if s.endswith("Gi"):
            return float(s[:-2])
        return float(s) / (1024**3)
    except Exception:
        return 0.0

def _parse_cpu_millicores(val) -> int:
    try:
        s = str(val)
        if s.endswith("m"):
            return int(s[:-1])
        return int(float(s) * 1000)
    except Exception:
        return 0

def _parse_mem_mib(val) -> float:
    try:
        s = str(val)
        if s.endswith("Ki"):
            return int(s[:-2]) / 1024
        if s.endswith("Mi"):
            return float(s[:-2])
        if s.endswith("Gi"):
            return float(s[:-2]) * 1024
        return float(s) / (1024**2)
    except Exception:
        return 0.0

def _age_str(ts) -> str:
    if not ts:
        return "-"
    try:
        delta = datetime.datetime.now(timezone.utc) - ts.replace(tzinfo=timezone.utc)
        h, rem = divmod(int(delta.total_seconds()), 3600)
        m = rem // 60
        if h >= 24:
            return f"{h//24}d {h%24}h"
        return f"{h}h {m}m"
    except Exception:
        return "-"


# ── Main builder ──────────────────────────────────────────────────────────────

def build_pdf(filepath) -> Path:
    """
    Gather live cluster data and write a PDF health report to *filepath*.
    Returns the Path on success.
    """
    _ensure_fonts()
    S = _make_styles()
    filepath = Path(filepath)

    # Import k8s clients (same pattern as tools_k8s.py)
    import kubernetes as _k8s
    _k8s.config.load_incluster_config() if _incluster() else _k8s.config.load_kube_config()
    _core    = _k8s.client.CoreV1Api()
    _apps    = _k8s.client.AppsV1Api()
    _storage = _k8s.client.StorageV1Api()
    _batch   = _k8s.client.BatchV1Api()
    _version = _k8s.client.VersionApi()

    now_str = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    MAX_ROWS = 20
    W = A4[0] - 28*mm   # usable width

    story = []

    # ── Cover / Title ─────────────────────────────────────────────────────────
    story.append(Spacer(1, 8*mm))
    story.append(Paragraph("ECS Cluster Health Report", S["title"]))
    story.append(Paragraph(f"Generated: {now_str}", S["subtitle"]))
    story.append(Spacer(1, 3*mm))
    story.append(HRFlowable(width="100%", thickness=2, color=S["_TEAL"]))
    story.append(Spacer(1, 2*mm))

    k8s_version = "unknown"
    try:
        k8s_version = _version.get_code().git_version
    except Exception:
        pass

    # ── Section 1 · Node Infrastructure ──────────────────────────────────────
    story += _section("1. Node Infrastructure", S)
    try:
        nodes = _core.list_node().items
        if not nodes:
            story.append(_warn("No nodes found.", S))
        else:
            story.append(_subsect(f"Nodes ({len(nodes)} total)  —  K8s {k8s_version}", S))
            rows = []
            for node in sorted(nodes, key=lambda n: n.metadata.name):
                roles = (",".join(k.split("/")[-1] for k in (node.metadata.labels or {})
                                  if "node-role.kubernetes.io" in k) or "worker")
                conds  = {c.type: c.status for c in (node.status.conditions or [])}
                status = "✓ Ready" if conds.get("Ready") == "True" else "✗ NotReady"
                alloc  = node.status.allocatable or {}
                cpu    = alloc.get("cpu", "?")
                mem_ki = alloc.get("memory", "0Ki")
                try:
                    mem_gib = f"{round(int(mem_ki.rstrip('Ki')) / (1024*1024), 1)} GiB"
                except Exception:
                    mem_gib = mem_ki
                gpu    = next((alloc[k] for k in alloc
                               if "nvidia.com/gpu" in k or "amd.com/gpu" in k), "-")
                flags  = []
                if conds.get("MemoryPressure") == "True": flags.append("MemPressure")
                if conds.get("DiskPressure")   == "True": flags.append("DiskPressure")
                status_str = status + (f"  ⚠ {','.join(flags)}" if flags else "")
                rows.append([node.metadata.name, roles, status_str, cpu, mem_gib, gpu])
            cw = [W*0.28, W*0.12, W*0.22, W*0.10, W*0.16, W*0.12]
            t = _make_table(["NAME","ROLES","STATUS","CPU","MEMORY","GPU"], rows, S,
                            col_widths=cw)
            if t:
                story.append(Spacer(1, 2*mm))
                story.append(t)
            over = _overflow_note(rows, MAX_ROWS, S)
            if over: story.append(over)

            # Node capacity
            story.append(Spacer(1, 3*mm))
            story.append(_subsect("Node Capacity (Allocatable vs Requested)", S))
            rows2 = []
            for node in sorted(nodes, key=lambda n: n.metadata.name):
                alloc     = node.status.allocatable or {}
                cpu_alloc = _parse_cpu_cores(alloc.get("cpu", 0))
                mem_alloc = _parse_mem_gib(alloc.get("memory", "0Ki"))
                pods_on   = _core.list_pod_for_all_namespaces(
                    field_selector=f"spec.nodeName={node.metadata.name}").items
                cpu_req = sum(_parse_cpu_cores((c.resources.requests or {}).get("cpu", 0))
                              for p in pods_on for c in (p.spec.containers or []) if c.resources)
                mem_req = sum(_parse_mem_gib((c.resources.requests or {}).get("memory", "0"))
                              for p in pods_on for c in (p.spec.containers or []) if c.resources)
                pct = lambda a, b: f"({round(a/b*100,1)}%)" if b > 0 else "(0%)"
                rows2.append([
                    node.metadata.name,
                    str(round(cpu_alloc, 2)),
                    f"{round(cpu_req,2)} {pct(cpu_req,cpu_alloc)}",
                    f"{round(cpu_alloc-cpu_req,2)} {pct(cpu_alloc-cpu_req,cpu_alloc)}",
                    str(round(mem_alloc, 2)),
                    f"{round(mem_req,2)} {pct(mem_req,mem_alloc)}",
                    f"{round(mem_alloc-mem_req,2)} {pct(mem_alloc-mem_req,mem_alloc)}",
                ])
            cw2 = [W*0.20, W*0.10, W*0.16, W*0.14, W*0.12, W*0.14, W*0.14]
            t2 = _make_table(["NODE","CPU ALLOC","CPU REQ","CPU AVAIL",
                               "RAM(Gi) ALLOC","RAM(Gi) REQ","RAM(Gi) AVAIL"],
                              rows2, S, col_widths=cw2)
            if t2:
                story.append(Spacer(1, 2*mm))
                story.append(t2)

            # Taints
            tainted = [(n.metadata.name, t) for n in nodes for t in (n.spec.taints or [])]
            if tainted:
                story.append(Spacer(1, 3*mm))
                story.append(_subsect("Node Taints", S))
                taint_rows = [[nm, t.key or "<any>", t.value or "-", t.effect or "-"]
                               for nm, t in tainted]
                cw3 = [W*0.30, W*0.28, W*0.22, W*0.20]
                t3 = _make_table(["NODE","KEY","VALUE","EFFECT"], taint_rows, S,
                                 col_widths=cw3)
                if t3:
                    story.append(Spacer(1, 2*mm))
                    story.append(t3)
            else:
                story.append(_ok("No node taints defined.", S))

            # GPU nodes
            gpu_nodes = [n for n in nodes if any("gpu" in k.lower()
                         for k in (n.status.allocatable or {}))]
            if gpu_nodes:
                story.append(Spacer(1, 3*mm))
                story.append(_subsect("GPU Nodes", S))
                gpu_rows = []
                for node in gpu_nodes:
                    labels  = node.metadata.labels or {}
                    alloc   = node.status.allocatable or {}
                    gpu_key = next((k for k in alloc if "gpu" in k.lower()), None)
                    gpu_alloc = alloc.get(gpu_key, "0") if gpu_key else "0"
                    gpu_rows.append([
                        node.metadata.name,
                        labels.get("gpu.product", "Unknown"),
                        labels.get("gpu.count", gpu_alloc),
                        f"{labels.get('gpu.memory','n/a')}Mi",
                        gpu_alloc,
                    ])
                cw4 = [W*0.30, W*0.25, W*0.15, W*0.15, W*0.15]
                t4 = _make_table(["NODE","PRODUCT","COUNT","VRAM","ALLOCATABLE"],
                                  gpu_rows, S, col_widths=cw4)
                if t4:
                    story.append(Spacer(1, 2*mm))
                    story.append(t4)
            else:
                story.append(_ok("No GPU nodes detected.", S))

    except Exception as exc:
        story.append(_warn(f"Could not gather node data: {exc}", S))

    # ── Section 2 · Resource Capacity ────────────────────────────────────────
    story += _section("2. Resource Capacity", S)
    try:
        all_ns  = [ns.metadata.name for ns in _core.list_namespace().items]
        ns_data = []
        for ns in all_ns:
            try:
                pods = _core.list_namespaced_pod(namespace=ns, limit=1000).items
                cpu_req = mem_req = cpu_lim = mem_lim = 0
                for pod in pods:
                    for c in list(pod.spec.containers or []) + list(pod.spec.init_containers or []):
                        req = (c.resources.requests or {}) if c.resources else {}
                        lim = (c.resources.limits   or {}) if c.resources else {}
                        cpu_req += _parse_cpu_millicores(req.get("cpu",    "0"))
                        mem_req += _parse_mem_mib(req.get("memory", "0"))
                        cpu_lim += _parse_cpu_millicores(lim.get("cpu",    "0"))
                        mem_lim += _parse_mem_mib(lim.get("memory", "0"))
                if pods:
                    ns_data.append((ns, len(pods), cpu_req, mem_req, cpu_lim, mem_lim))
            except Exception:
                pass
        ns_data.sort(key=lambda x: x[2], reverse=True)
        story.append(_subsect("Namespace Resource Requests vs Limits (top by CPU request)", S))
        rows = []
        for ns, pod_count, cpu_req, mem_req, cpu_lim, mem_lim in ns_data:
            rows.append([ns, str(pod_count),
                         f"{cpu_req}m", f"{mem_req:.0f}Mi",
                         f"{cpu_lim}m" if cpu_lim else "none",
                         f"{mem_lim:.0f}Mi"])
        cw5 = [W*0.28, W*0.09, W*0.14, W*0.14, W*0.17, W*0.18]
        t5 = _make_table(["NAMESPACE","PODS","CPU REQ","MEM REQ","CPU LIM","MEM LIM"],
                          rows, S, col_widths=cw5)
        if t5:
            story.append(Spacer(1, 2*mm))
            story.append(t5)
        over = _overflow_note(rows, MAX_ROWS, S)
        if over: story.append(over)
    except Exception as exc:
        story.append(_warn(f"Could not gather resource data: {exc}", S))

    try:
        q_items = _core.list_resource_quota_for_all_namespaces().items
        if q_items:
            story.append(Spacer(1, 3*mm))
            story.append(_subsect("Resource Quotas", S))
            rows = []
            for q in sorted(q_items, key=lambda x: (x.metadata.namespace, x.metadata.name)):
                hard = q.status.hard or {}
                used = q.status.used or {}
                for res in sorted(hard.keys()):
                    hv = hard.get(res, "0")
                    uv = used.get(res, "0")
                    try:
                        if "cpu" in res:
                            pct = round(_parse_cpu_millicores(str(uv)) /
                                        max(_parse_cpu_millicores(str(hv)), 1) * 100, 1)
                        elif "memory" in res:
                            pct = round(_parse_mem_mib(str(uv)) /
                                        max(_parse_mem_mib(str(hv)), 1) * 100, 1)
                        else:
                            pct = round(int(str(uv).split(".")[0]) /
                                        max(int(str(hv).split(".")[0]), 1) * 100, 1)
                        flag = "✗" if pct >= 90 else ("⚠" if pct >= 80 else "✓")
                        pct_str = f"{flag} {pct}%"
                    except Exception:
                        pct_str = "-"
                    rows.append([q.metadata.namespace, q.metadata.name,
                                  res, str(uv), str(hv), pct_str])
            cw6 = [W*0.18, W*0.18, W*0.24, W*0.13, W*0.13, W*0.14]
            t6 = _make_table(["NAMESPACE","QUOTA","RESOURCE","USED","HARD","%"],
                              rows, S, col_widths=cw6)
            if t6:
                story.append(Spacer(1, 2*mm))
                story.append(t6)
        else:
            story.append(_ok("No ResourceQuotas defined.", S))
    except Exception as exc:
        story.append(_warn(f"Could not gather quota data: {exc}", S))

    try:
        lr_items = _core.list_limit_range_for_all_namespaces().items
        if lr_items:
            story.append(Spacer(1, 3*mm))
            story.append(_subsect("LimitRanges", S))
            def _g(v, key): return (v or {}).get(key, "-")
            rows = []
            for lr in sorted(lr_items, key=lambda x: (x.metadata.namespace, x.metadata.name)):
                for item in (lr.spec.limits or []):
                    rows.append([lr.metadata.namespace, lr.metadata.name, item.type,
                                  _g(item.default,"cpu"), _g(item.default,"memory"),
                                  _g(item.max,"cpu"), _g(item.max,"memory")])
            cw7 = [W*0.16, W*0.17, W*0.12, W*0.14, W*0.14, W*0.13, W*0.14]
            t7 = _make_table(["NAMESPACE","LIMITRANGE","TYPE","CPU DEF","MEM DEF",
                               "CPU MAX","MEM MAX"], rows, S, col_widths=cw7)
            if t7:
                story.append(Spacer(1, 2*mm))
                story.append(t7)
        else:
            story.append(_ok("No LimitRanges defined.", S))
    except Exception as exc:
        story.append(_warn(f"Could not gather LimitRange data: {exc}", S))

    # ── Section 3 · Storage Health ────────────────────────────────────────────
    story += _section("3. Storage Health", S)

    try:
        scs = _storage.list_storage_class().items
        if scs:
            story.append(_subsect("Storage Classes", S))
            sc_rows = []
            for sc in scs:
                is_default = (sc.metadata.annotations or {}).get(
                    "storageclass.kubernetes.io/is-default-class") == "true"
                sc_rows.append([sc.metadata.name, sc.provisioner,
                                 sc.reclaim_policy or "Delete",
                                 "✓ Yes" if sc.allow_volume_expansion else "No",
                                 "Default" if is_default else "-"])
            cw_sc = [W*0.20, W*0.32, W*0.16, W*0.14, W*0.18]
            t_sc = _make_table(["NAME","PROVISIONER","RECLAIM","EXPANSION","DEFAULT"],
                                sc_rows, S, col_widths=cw_sc)
            if t_sc:
                story.append(Spacer(1, 2*mm))
                story.append(t_sc)
        else:
            story.append(_ok("No StorageClasses defined.", S))
    except Exception as exc:
        story.append(_warn(f"Could not gather StorageClass data: {exc}", S))

    try:
        pvcs    = _core.list_persistent_volume_claim_for_all_namespaces().items
        bound   = [p for p in pvcs if p.status.phase == "Bound"]
        unbound = [p for p in pvcs if p.status.phase != "Bound"]
        if unbound:
            story.append(_warn(f"{len(unbound)} PVC(s) not Bound / "
                                f"{len(bound)} Bound / {len(pvcs)} total", S))
            ub_rows = []
            for p in unbound:
                ub_rows.append([p.metadata.namespace, p.metadata.name,
                                  p.status.phase or "Unknown",
                                  p.spec.storage_class_name or "-",
                                  (p.status.capacity or {}).get("storage", "?")])
            cw_ub = [W*0.22, W*0.28, W*0.14, W*0.20, W*0.16]
            t_ub = _make_table(["NAMESPACE","PVC","PHASE","CLASS","CAPACITY"],
                                ub_rows, S, col_widths=cw_ub)
            if t_ub:
                story.append(Spacer(1, 2*mm))
                story.append(t_ub)
        else:
            story.append(_ok(f"All {len(pvcs)} PVCs Bound — 0 not Bound.", S))
    except Exception as exc:
        story.append(_warn(f"Could not gather PVC data: {exc}", S))

    # ── PV Disk Usage table — the section that was missing from WeasyPrint ────
    story.append(Spacer(1, 3*mm))
    story.append(_subsect("PV Disk Usage (requires running pod with mounted volume)", S))
    try:
        from kubernetes.stream import stream as _k8s_stream

        def _exec_df(pod_name, ns, mount_path, container=None):
            try:
                kwargs = dict(
                    command=["/bin/sh", "-c",
                              f"df -k --output=used,avail {mount_path} 2>/dev/null | tail -1"],
                    stderr=False, stdin=False, stdout=True, tty=False, _preload_content=True)
                if container:
                    kwargs["container"] = container
                resp = _k8s_stream(_core.connect_get_namespaced_pod_exec,
                                   pod_name, ns, **kwargs)
                return resp.strip() if isinstance(resp, str) else ""
            except Exception:
                return ""

        pv_rows  = []
        pv_skip  = 0
        all_pvcs = _core.list_persistent_volume_claim_for_all_namespaces().items

        for pvc in all_pvcs:
            ns, pvc_name = pvc.metadata.namespace, pvc.metadata.name
            pod_hit = pod_container = mount_path = None
            try:
                for pod in _core.list_namespaced_pod(namespace=ns).items:
                    if pod.status.phase != "Running":
                        continue
                    for vol in (pod.spec.volumes or []):
                        if (vol.persistent_volume_claim and
                                vol.persistent_volume_claim.claim_name == pvc_name):
                            for container in (pod.spec.containers or []):
                                for vm in (container.volume_mounts or []):
                                    if vm.name == vol.name:
                                        pod_hit       = pod.metadata.name
                                        pod_container = container.name
                                        mount_path    = vm.mount_path
                                        break
                                if pod_hit: break
                        if pod_hit: break
            except Exception:
                pass

            if not pod_hit:
                pv_skip += 1
                continue

            df_out = _exec_df(pod_hit, ns, mount_path, container=pod_container)
            if not df_out:
                pv_skip += 1
                continue

            parts = df_out.split()
            if len(parts) < 2:
                pv_skip += 1
                continue

            try:
                used_kb  = int(parts[0])
                avail_kb = int(parts[1])
                total_kb = used_kb + avail_kb
                pct      = round(used_kb / total_kb * 100, 1) if total_kb > 0 else 0.0
            except (ValueError, ZeroDivisionError):
                pv_skip += 1
                continue

            used_gib  = round(used_kb  / (1024**2), 2)
            avail_gib = round(avail_kb / (1024**2), 2)
            total_gib = round(total_kb / (1024**2), 2)
            free_pct  = round(avail_kb / total_kb * 100, 1) if total_kb > 0 else 0.0
            flag      = "✗" if pct >= 90 else ("⚠" if pct >= 80 else "✓")
            pv_rows.append((pct, flag, ns, pvc_name, used_gib, total_gib, avail_gib, free_pct))

        pv_rows.sort(key=lambda x: x[0], reverse=True)

        if pv_rows:
            table_rows = []
            for pct, flag, ns, pvc_name, used, total, avail, fpct in pv_rows:
                table_rows.append([
                    f"{flag} {pct}%",
                    f"{ns}/{pvc_name}",
                    f"{used} GiB",
                    f"{total} GiB",
                    f"{avail} GiB ({fpct}%)",
                ])
            cw_pv = [W*0.12, W*0.38, W*0.16, W*0.16, W*0.18]
            t_pv = _make_table(["USAGE%","NAMESPACE / PVC","USED","TOTAL","FREE"],
                                table_rows, S, cap=40, col_widths=cw_pv)
            if t_pv:
                story.append(Spacer(1, 2*mm))
                story.append(t_pv)
            over = _overflow_note(pv_rows, 40, S)
            if over: story.append(over)
        else:
            story.append(_ok("No volume usage data available (no mounted running pods found).", S))

        if pv_skip:
            story.append(_note(f"({pv_skip} PVC(s) skipped — no running pod with mount found.)", S))

    except Exception as exc:
        story.append(_warn(f"Could not gather PV usage data: {exc}", S))

    # ── Section 4 · Workload Health ───────────────────────────────────────────
    story += _section("4. Workload Health", S)

    def _workload_block(kind, items, desired_fn, ready_fn, avail_fn):
        if not items:
            story.append(_ok(f"No {kind}s found.", S))
            return
        degraded = [i for i in items
                    if (desired_fn(i) or 0) > 0 and (ready_fn(i) or 0) < (desired_fn(i) or 0)]
        if not degraded:
            story.append(_ok(f"{len(items)} {kind}(s) — all healthy.", S))
            return
        story.append(_warn(f"{len(degraded)}/{len(items)} {kind}(s) degraded:", S))
        rows = [[i.metadata.namespace, i.metadata.name,
                 str(desired_fn(i) or 0), str(ready_fn(i) or 0), str(avail_fn(i) or 0)]
                for i in degraded]
        cw_w = [W*0.25, W*0.33, W*0.14, W*0.14, W*0.14]
        t = _make_table(["NAMESPACE","NAME","DESIRED","READY","AVAILABLE"],
                         rows, S, col_widths=cw_w)
        if t:
            story.append(Spacer(1, 2*mm))
            story.append(t)

    for kind, getter, dfn, rfn, afn in [
        ("Deployment",
         lambda: _apps.list_deployment_for_all_namespaces().items,
         lambda d: d.spec.replicas or 0,
         lambda d: d.status.ready_replicas or 0,
         lambda d: d.status.available_replicas or 0),
        ("DaemonSet",
         lambda: _apps.list_daemon_set_for_all_namespaces().items,
         lambda d: d.status.desired_number_scheduled or 0,
         lambda d: d.status.number_ready or 0,
         lambda d: d.status.number_available or 0),
        ("StatefulSet",
         lambda: _apps.list_stateful_set_for_all_namespaces().items,
         lambda s: s.spec.replicas or 0,
         lambda s: s.status.ready_replicas or 0,
         lambda s: getattr(s.status,"available_replicas",None) or 0),
    ]:
        try:
            _workload_block(kind, getter(), dfn, rfn, afn)
        except Exception as exc:
            story.append(_warn(f"{kind}s: {exc}", S))

    try:
        rss = _apps.list_replica_set_for_all_namespaces().items
        orphaned = [r for r in rss
                    if (r.spec.replicas or 0) > 0
                    and (r.status.ready_replicas or 0) < (r.spec.replicas or 0)
                    and not r.metadata.owner_references]
        if orphaned:
            story.append(_warn(f"{len(orphaned)} orphaned/degraded ReplicaSet(s):", S))
            rows = [[r.metadata.namespace, r.metadata.name,
                     str(r.spec.replicas or 0), str(r.status.ready_replicas or 0)]
                    for r in orphaned]
            cw_rs = [W*0.25, W*0.45, W*0.15, W*0.15]
            t = _make_table(["NAMESPACE","NAME","DESIRED","READY"], rows, S, col_widths=cw_rs)
            if t:
                story.append(Spacer(1, 2*mm))
                story.append(t)
        else:
            story.append(_ok("No orphaned/degraded ReplicaSets.", S))
    except Exception as exc:
        story.append(_warn(f"ReplicaSets: {exc}", S))

    try:
        jobs = _batch.list_job_for_all_namespaces().items
        failed_jobs = [j for j in jobs
                       if (j.status.failed or 0) > 0 and not j.status.completion_time]
        if failed_jobs:
            story.append(_warn(f"{len(failed_jobs)} failed Job(s):", S))
            rows = [[j.metadata.namespace, j.metadata.name, str(j.status.failed or 0)]
                    for j in failed_jobs]
            cw_j = [W*0.25, W*0.60, W*0.15]
            t = _make_table(["NAMESPACE","NAME","FAILURES"], rows, S, col_widths=cw_j)
            if t:
                story.append(Spacer(1, 2*mm))
                story.append(t)
        else:
            story.append(_ok("No failed Jobs.", S))
    except Exception as exc:
        story.append(_warn(f"Jobs: {exc}", S))

    try:
        cronjobs = _batch.list_cron_job_for_all_namespaces().items
        if cronjobs:
            story.append(Spacer(1, 3*mm))
            story.append(_subsect(f"CronJobs ({len(cronjobs)} total)", S))
            rows = []
            for cj in sorted(cronjobs, key=lambda x: (x.metadata.namespace, x.metadata.name)):
                last = cj.status.last_schedule_time
                if last:
                    delta = datetime.datetime.now(timezone.utc) - last.replace(tzinfo=timezone.utc)
                    h, m = divmod(int(delta.total_seconds()) // 60, 60)
                    last_str = f"{h}h {m}m ago"
                else:
                    last_str = "Never"
                suspended = "|| Yes" if cj.spec.suspend else "No"
                rows.append([cj.metadata.namespace, cj.metadata.name,
                              cj.spec.schedule, suspended, last_str])
            cw_cj = [W*0.20, W*0.28, W*0.22, W*0.12, W*0.18]
            t = _make_table(["NAMESPACE","NAME","SCHEDULE","SUSPENDED","LAST RUN"],
                             rows, S, col_widths=cw_cj)
            if t:
                story.append(Spacer(1, 2*mm))
                story.append(t)
    except Exception as exc:
        story.append(_warn(f"CronJobs: {exc}", S))

    try:
        all_pods   = _core.list_pod_for_all_namespaces().items
        non_running = [p for p in all_pods
                       if p.status.phase not in ("Running", "Succeeded")
                       and not any(c.type == "Ready" and c.status == "True"
                                   for c in (p.status.conditions or []))]
        if non_running:
            story.append(Spacer(1, 3*mm))
            story.append(_warn(f"Non-Running Pods ({len(non_running)} total)", S))
            rows = []
            for p in non_running[:MAX_ROWS]:
                cs       = p.status.container_statuses or []
                ready    = sum(1 for c in cs if c.ready)
                restarts = sum(c.restart_count or 0 for c in cs)
                reason   = (p.status.reason or
                            next((s.state.waiting.reason for c in cs
                                  if (s := c) and c.state and c.state.waiting
                                  and c.state.waiting.reason), "") or
                            p.status.phase or "")
                rows.append([p.metadata.namespace, p.metadata.name,
                              p.status.phase or "Unknown",
                              f"{ready}/{len(p.spec.containers)}",
                              str(restarts), reason])
            cw_p = [W*0.18, W*0.28, W*0.12, W*0.09, W*0.10, W*0.23]
            t = _make_table(["NAMESPACE","NAME","PHASE","READY","RST","REASON"],
                             rows, S, col_widths=cw_p)
            if t:
                story.append(Spacer(1, 2*mm))
                story.append(t)
            over = _overflow_note(non_running, MAX_ROWS, S)
            if over: story.append(over)
        else:
            story.append(_ok("All pods running.", S))
    except Exception as exc:
        story.append(_warn(f"Pod status: {exc}", S))

    # ── Section 5 · Networking & DNS ─────────────────────────────────────────
    story += _section("5. Networking & DNS", S)
    try:
        DNS_NS       = "kube-system"
        DNS_PATTERNS = ["coredns", "core-dns", "kube-dns"]
        dns_pods = [p for p in _core.list_namespaced_pod(namespace=DNS_NS).items
                    if any(pat in p.metadata.name.lower() for pat in DNS_PATTERNS)
                    and "autoscaler" not in p.metadata.name.lower()
                    and (p.status.phase or "").lower() != "succeeded"]
        if dns_pods:
            not_ready_dns = [p for p in dns_pods
                             if sum(1 for cs in (p.status.container_statuses or []) if cs.ready)
                             < len(p.spec.containers or [])]
            if not_ready_dns:
                story.append(_warn(f"CoreDNS: {len(not_ready_dns)}/{len(dns_pods)} pods not ready — "
                                    f"{_cap([p.metadata.name for p in not_ready_dns])}", S))
            else:
                story.append(_ok(f"CoreDNS: {len(dns_pods)}/{len(dns_pods)} pods ready", S))

            dns_test_pod = next((p for p in dns_pods if p.status.phase == "Running"), None)
            if dns_test_pod:
                from kubernetes.stream import stream as _k8s_stream
                test_targets = ["kubernetes.default.svc.cluster.local",
                                 "kube-dns.kube-system.svc.cluster.local"]
                story.append(Spacer(1, 2*mm))
                story.append(_subsect("CoreDNS Resolution Tests", S))
                res_rows = []
                for target in test_targets:
                    try:
                        resp = _k8s_stream(
                            _core.connect_get_namespaced_pod_exec,
                            dns_test_pod.metadata.name, DNS_NS,
                            command=["/bin/sh", "-c", f"nslookup {target} 2>&1 | head -5"],
                            stderr=False, stdin=False, stdout=True, tty=False, _preload_content=True)
                        out = resp.strip() if isinstance(resp, str) else ""
                        resolved = "Address" in out or "answer" in out.lower()
                        flag = "✓" if resolved else "✗"
                        res_rows.append([flag, target, "Resolved" if resolved else "FAILED"])
                    except Exception as ex:
                        res_rows.append(["⚠", target, f"exec error: {ex}"])
                cw_dns = [W*0.08, W*0.55, W*0.37]
                t = _make_table(["", "Target", "Result"], res_rows, S, col_widths=cw_dns)
                if t:
                    story.append(Spacer(1, 2*mm))
                    story.append(t)
        else:
            story.append(_warn("CoreDNS: no DNS pods found in kube-system", S))
    except Exception as exc:
        story.append(_warn(f"CoreDNS check failed: {exc}", S))

    # ── Section 6 · Certificates & RBAC ──────────────────────────────────────
    story += _section("6. Certificates & RBAC", S)
    try:
        custom = _k8s.client.CustomObjectsApi()
        certs  = custom.list_cluster_custom_object(
            "cert-manager.io", "v1", "certificates").get("items", [])
        if certs:
            not_ready   = []
            expiring    = []
            for cert in certs:
                meta     = cert.get("metadata", {})
                status   = cert.get("status", {})
                ready_c  = next((c for c in status.get("conditions", [])
                                  if c.get("type") == "Ready"), None)
                is_ready = ready_c and ready_c.get("status") == "True"
                not_after = status.get("notAfter")
                if not is_ready:
                    not_ready.append(f"{meta.get('namespace')}/{meta.get('name')}")
                elif not_after:
                    try:
                        exp = datetime.datetime.fromisoformat(not_after.replace("Z", "+00:00"))
                        days_left = (exp - datetime.datetime.now(timezone.utc)).days
                        if days_left <= 30:
                            expiring.append(f"{meta.get('namespace')}/{meta.get('name')} ({days_left}d)")
                    except Exception:
                        pass
            if not_ready:
                story.append(_err(f"{len(not_ready)} cert(s) not Ready: {_cap(not_ready)}", S))
            if expiring:
                story.append(_warn(f"{len(expiring)} cert(s) expiring ≤30 days: {_cap(expiring)}", S))
            if not not_ready and not expiring:
                story.append(_ok(f"{len(certs)} cert-manager certificate(s) — all Ready, none expiring soon", S))
        else:
            story.append(_ok("No cert-manager Certificates found (cert-manager may not be installed)", S))
    except Exception as exc:
        if "404" in str(exc):
            story.append(_ok("cert-manager not installed — skipping certificate check", S))
        else:
            story.append(_warn(f"Certificates: {exc}", S))

    # ── Section 7 · Control Plane & Diagnostics ───────────────────────────────
    story += _section("7. Kubernetes Control Plane & Recent Diagnostics", S)

    try:
        cs_items = _core.list_component_status().items
        if cs_items:
            story.append(_subsect("Component Statuses", S))
            rows = []
            for c in cs_items:
                cond   = c.conditions[0] if c.conditions else None
                status = cond.status if cond else "Unknown"
                flag   = "✓" if status == "True" else "✗"
                rows.append([f"{flag} {c.metadata.name}",
                              cond.message if cond else "-",
                              cond.error if cond else "-"])
            cw_cs = [W*0.28, W*0.42, W*0.30]
            t = _make_table(["COMPONENT","MESSAGE","ERROR"], rows, S, col_widths=cw_cs)
            if t:
                story.append(Spacer(1, 2*mm))
                story.append(t)
        else:
            story.append(_ok("Component statuses not available (normal on managed clusters)", S))
    except Exception:
        story.append(_ok("Component statuses not available (normal on managed clusters)", S))

    try:
        pods = _core.list_namespaced_pod(namespace="kube-system").items
        core_components = ["kube-apiserver","etcd","kube-controller-manager","kube-scheduler"]
        cp_pods = [p for p in pods if any(comp in p.metadata.name for comp in core_components)]
        story.append(Spacer(1, 3*mm))
        story.append(_subsect("Control Plane Pods (kube-system)", S))
        if cp_pods:
            rows = []
            for pod in cp_pods:
                phase    = pod.status.phase or "Unknown"
                ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                total    = len(pod.spec.containers)
                restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
                flag     = "✓" if phase == "Running" and ready == total else "✗"
                comp     = next((c for c in core_components if c in pod.metadata.name),
                                pod.metadata.name.split("-")[0])
                rows.append([comp, pod.metadata.name, f"{flag} {phase}",
                              f"{ready}/{total}", str(restarts)])
            cw_cp = [W*0.20, W*0.38, W*0.16, W*0.12, W*0.14]
            t = _make_table(["COMPONENT","POD NAME","STATUS","READY","RESTARTS"],
                             rows, S, col_widths=cw_cp)
            if t:
                story.append(Spacer(1, 2*mm))
                story.append(t)
        else:
            story.append(_ok("No control plane pods visible (managed cluster)", S))
    except Exception as exc:
        story.append(_warn(f"Control plane pods: {exc}", S))

    try:
        events = _core.list_event_for_all_namespaces(
            field_selector="type=Warning", limit=500).items
        _NOISY = re.compile(
            r"(Readiness probe|Liveness probe|Back-off pulling|"
            r"pulling image|Successfully pulled|Created container|Started container)",
            re.I)
        seen: dict = {}
        for e in events:
            msg = e.message or ""
            if _NOISY.search(msg):
                continue
            key  = (e.reason or "", getattr(e.involved_object, "name", ""))
            prev = seen.get(key)
            if prev is None or (e.count or 1) > (prev[0] or 1):
                seen[key] = (e.count or 1, e.metadata.namespace,
                              e.reason or "-", key[1], msg)
        top = sorted(seen.values(), key=lambda x: x[0], reverse=True)[:15]
        if top:
            story.append(Spacer(1, 3*mm))
            story.append(_subsect(f"Top Warning Events ({len(top)} unique)", S))
            rows = []
            for count, ns, reason, obj, msg in top:
                short_msg = msg[:80] + "…" if len(msg) > 80 else msg
                rows.append([f"x{count}", reason, f"{ns}/{obj}", short_msg])
            cw_ev = [W*0.08, W*0.17, W*0.28, W*0.47]
            t = _make_table(["CNT","REASON","NS / OBJECT","MESSAGE"], rows, S, col_widths=cw_ev)
            if t:
                story.append(Spacer(1, 2*mm))
                story.append(t)
        else:
            story.append(_ok("No recent warning events", S))
    except Exception as exc:
        story.append(_warn(f"Warning events: {exc}", S))

    # ── Footer note ───────────────────────────────────────────────────────────
    story.append(Spacer(1, 6*mm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=S["_MGRAY"]))
    story.append(Spacer(1, 2*mm))
    story.append(_note("This is your complete health check report. "
                        "Ask the chatbot to drill into any flagged area for deeper diagnostics.", S))

    # ── Build PDF ─────────────────────────────────────────────────────────────
    doc = _make_doc(str(filepath))
    doc.build(story)
    return filepath


def _incluster() -> bool:
    """Return True if running inside a k8s pod (service account token present)."""
    return Path("/var/run/secrets/kubernetes.io/serviceaccount/token").exists()
