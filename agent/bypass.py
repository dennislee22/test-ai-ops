import re
import logging

_log = logging.getLogger("agent.bypass")

NEVER_BYPASS = {
    "get_secret_list",
    "exec_db_query",
}

BYPASSABLE_TOOLS = {
    "run_cluster_health",
    "get_node_info",
    "get_node_taints",
    "get_namespace_status",
    "get_namespace_resource_summary",
    "get_persistent_volumes",
    "get_pv_usage",
    "get_cluster_role_bindings",
    "get_serviceaccounts",
    "get_pod_status",
    "get_pvc_status",
    "get_service",
    "get_ingress",
    "get_configmap_list",
    "get_pod_images",
    "kubectl_exec",
    "get_pod_logs",
    "describe_pod",
    "describe_pvc",
    "describe_pv",
    "describe_sc",
    "get_events",
    "get_top_pods",
    "get_top_nodes",
    "find_resource",
}

# Note: They must ALSO be listed in BYPASSABLE_TOOLS above.
UNCONDITIONAL_BYPASS = {
    "run_cluster_health",
    #"get_node_info", --> test cordon 
    "get_node_taints",
    "get_pod_logs",
    "describe_pod",
    "describe_pvc",
    "describe_pv",
    "describe_sc",
    "get_events",
    "get_top_pods",
    "get_top_nodes",
    "find_resource",
}

LIST_INTENTS = (
    r"\blist all\b", r"\bshow all\b", r"\bget all\b", r"\bdisplay all\b",
    r"\ball pods\b", r"\ball services\b", r"\ball svc\b", r"\ball pv\b", r"\ball persistent volume\b",
    r"\ball log\b", r"\ball events\b",
    r"\blist entire\b", r"\bshow entire\b", r"\bget entire\b", r"\bdisplay entire\b",
)

ALWAYS_SYNTHESISE = (
    r"\bwhy\b", r"\bhow\b", r"\bwhen\b", r"\bwhere\b", r"\bwho\b",
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
        "no unhealthy pods",
        "no pods found",
    )
    if any(p in output.lower() for p in _SUMMARY_PATTERNS):
        _log.info(f"{tag}[bypass] PASS — {tool_name!r} returned self-contained summary")
        return True

    # Check against the new upfront set
    if tool_name in UNCONDITIONAL_BYPASS:
        _log.info(f"{tag}[bypass] PASS — {tool_name!r} is in UNCONDITIONAL_BYPASS")
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
