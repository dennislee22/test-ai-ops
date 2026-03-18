import re
import logging

_log = logging.getLogger("agent.bypass")

NEVER_BYPASS = {
    #"get_deployment_status",
}

BYPASSABLE_TOOLS = {
    "get_namespace_status",
    "get_namespace_resource_summary",
    "get_persistent_volumes",
    "get_pv_usage",
    "get_cluster_role_bindings",
    "get_service_accounts",
    "get_pod_status",
    "get_pvc_status",
    "get_service_status",
    "get_ingress_status",
    "get_secrets",
    "get_configmap_list",
    "get_pod_images",
    "kubectl_exec",
    "query_prometheus_metrics",
    "describe_pod",
    "get_pod_logs",
}

LIST_INTENTS = (
    r"\blist all\b", r"\bshow all\b", r"\bget all\b", r"\bdisplay all\b",
    r"\ball pods\b", r"\ball services\b", r"\ball svc\b", r"\ball pv\b", r"\ball persistent volume\b",
    r"\blist entire\b", r"\bshow entire\b", r"\bget entire\b", r"\bdisplay entire\b",
)

ALWAYS_SYNTHESISE = (
    r"\bwhy\b", r"\bhow\b", r"\bwhen\b", r"\bwhere\b", r"\bwho\b",
    r"\bhealth\b", r"\bhealthy\b", r"\bok\b", r"\bokay\b",
    r"\bissue\b", r"\bproblem\b", r"\berror\b", r"\bwarning\b",
    r"\bfail", r"\bbroken\b", r"\bdown\b", r"\bcrash",
    r"\bcondition\b", r"\bpending\b", r"\bcrashloop\b", 
    r"\bnot running\b", r"\bnot ready\b",
    r"\btell me\b", r"\bexplain\b", r"\bdescribe\b", r"\bsummar",
    r"\bmost\b", r"\bleast\b", r"\bcompare\b", r"\brank\b",
    r"\bhow many\b", r"\bcount\b", r"\btop\b",
)
def should_bypass_llm(tool_name: str, args: dict,
                      output: str, user_q: str,
                      req_id: str = "") -> bool:
    tag = f"[REQ:{req_id}] " if req_id else ""

    if tool_name in NEVER_BYPASS:
        _log.debug(f"{tag}[bypass] SKIP — {tool_name!r} is in NEVER_BYPASS")
        return False

    if tool_name not in BYPASSABLE_TOOLS:
        _log.debug(f"{tag}[bypass] SKIP — {tool_name!r} not in BYPASSABLE_TOOLS → LLM synthesis")
        return False

    if (output.startswith("K8s API error") or output.startswith("[ERROR]")
            or "not found" in output.lower() or output.lower().startswith("no ")):
        _log.info(f"{tag}[bypass] SKIP — {tool_name!r} output contains error/not-found → LLM synthesis")
        return False

    lq = user_q.lower()

    _SUMMARY_PATTERNS = (
        "all pods are healthy",
        "all pods are in running",
        "no unhealthy pods",
        "no pods found",
    )
    if any(p in output.lower() for p in _SUMMARY_PATTERNS):
        _log.info(f"{tag}[bypass] PASS — {tool_name!r} returned self-contained summary")
        return True

    if tool_name == "get_pv_usage":
        _log.info(f"{tag}[bypass] PASS — {tool_name!r} is structured report (unconditional)")
        return True

    if tool_name in ("query_prometheus_metrics", "get_pod_logs"):
        _log.info(f"{tag}[bypass] PASS — {tool_name!r} is structured report/logs (unconditional)")
        return True
    
    matched_synth = next((p for p in ALWAYS_SYNTHESISE if re.search(p, lq)), None)
    if matched_synth:
        _log.info(f"{tag}[bypass] SKIP — {tool_name!r} question matched ALWAYS_SYNTHESISE pattern {matched_synth!r}")
        return False

    if args.get("filter_keys") and not args.get("decode"):
        if any(re.search(pat, lq) for pat in LIST_INTENTS):
            _log.info(f"{tag}[bypass] PASS — {tool_name!r} filter_keys list intent")
            return True

    if args.get("decode"):
        _log.info(f"{tag}[bypass] SKIP — {tool_name!r} decode=True → LLM synthesis")
        return False

    matched_list = next((p for p in LIST_INTENTS if re.search(p, lq)), None)
    if not matched_list:
        _log.info(f"{tag}[bypass] SKIP — {tool_name!r} no list intent matched → LLM synthesis")
        return False

    _log.info(f"{tag}[bypass] PASS — {tool_name!r} list intent matched {matched_list!r}")
    return True