import re
import logging

_log = logging.getLogger("agent.routing")

NS_ALIASES = {
    "vault":      "vault-system",
    "longhorn":   "longhorn-system",
    "cattle":     "cattle-system",
    "rancher":    "cattle-system",
    "cert":       "cert-manager",
    "coredns":    "kube-system",
    "core-dns":   "kube-system",
    "dns":        "kube-system",
    "kube-proxy": "kube-system",
    "kubedns":    "kube-system",
    "kube-dns":   "kube-system",
    "etcd":       "kube-system",
    "scheduler":  "kube-system",
    "controller": "kube-system",
    "apiserver":  "kube-system",
    "api-server": "kube-system",
    "prometheus": "monitoring",
    "alertmanager": "monitoring",
}


def resolve_namespace(lm: str, req_id: str = "") -> str:
    tag = f"[REQ:{req_id}] " if req_id else ""
    m = re.search(r'(?:^|\s)(?:in|for|namespace|ns)\s+([a-z0-9-]+)', lm)
    if m:
        raw = m.group(1)
        if raw not in ("all", "namespace", "ns", "the", "this"):
            resolved = NS_ALIASES.get(raw, raw)
            _log.debug(f"{tag}[routing] namespace resolved: {raw!r} → {resolved!r} (explicit pattern)")
            return resolved

    for keyword, real_ns in NS_ALIASES.items():
        if keyword in lm:
            _log.debug(f"{tag}[routing] namespace resolved: {keyword!r} keyword → {real_ns!r}")
            return real_ns

    _log.debug(f"{tag}[routing] namespace resolved: no match → 'all'")
    return "all"


def default_tools_for(user_msg: str, req_id: str = "") -> list:
    tag = f"[REQ:{req_id}] " if req_id else ""
    lm = user_msg.lower().strip()
    ns = resolve_namespace(lm, req_id=req_id)

    # "don't run" / "just explain" phrasing — BUT only a pure how-to, not
    # a follow-up asking for cluster-specific output (e.g. "can you make the yaml
    # specific based on the current cluster state?" needs live tools).
    _explicit_no_run = bool(
        re.search(r"don\'?t\s+run|do\s+not\s+run|without\s+running|just\s+explain", lm)
        or re.search(r"don\'?t\s+execute|no\s+tools|no\s+commands", lm)
    )
    # A follow-up asking for specificity / current state overrides the no-run flag
    _wants_cluster_specific = bool(
        re.search(r"more\s+specific|based\s+on\s+(the\s+)?current|specific.*yaml|yaml.*specific", lm)
        or re.search(r"current\s+state|actual\s+(cluster|config|setup|environment)", lm)
        or re.search(r"(make|can\s+you|could\s+you).*specific", lm)
    )
    _is_howto = (
        not _wants_cluster_specific  # cluster-specific follow-up re-engages tools
        and (
            _explicit_no_run
            or re.search(r'\b(teach|explain|show)\s+(me\s+)?(how\s+(you|to|do\s+you)|step\s+by\s+step)', lm)
            or re.search(r'\bhow\s+do\s+you\b', lm)
            or re.search(r'\bhow\s+(can|does)\s+(you|the\s+(bot|assistant|chatbot))\b', lm)
            or re.search(r'\bwhat\s+(can|do)\s+you\s+(do|support|handle|know)\b', lm)
            or re.search(r'\b(demonstrate|walkthrough|walk\s+me\s+through)\b', lm)
        )
    )
    if _is_howto:
        _log.info(f"{tag}[routing] FALLBACK → __conversational__ (how-to / self-referential)")
        return [("__conversational__", {})]

    _is_gibberish = (
        len(lm) < 4
        or not any(c.isalpha() for c in lm)
        or (len(lm.split()) <= 2 and not any(k in lm for k in [
            "pod", "node", "pvc", "pv", "svc", "ns", "secret", "deploy",
            "job", "log", "event", "quota", "rbac", "ingress", "dns",
        ]))
    )
    if _is_gibberish:
        _log.info(f"{tag}[routing] FALLBACK → __conversational__ (gibberish / too short)")
        return [("__conversational__", {})]

    _is_image_query = any(k in lm for k in [
        "image version", "image tag", "what image", "which image",
        "image of", "running image", "deployed image", "container image",
        "what version", "which version", "version of",
    ]) and any(k in lm for k in ["pod", "pods", "container", "containers", "longhorn", "namespace"])
    if _is_image_query:
        _log.info(f"{tag}[routing] FALLBACK → get_pod_images(namespace={ns!r})")
        return [("get_pod_images", {"namespace": ns})]

    _is_coredns_query = any(k in lm for k in [
        "coredns", "core-dns", "kube-dns", "dns resolution", "dns health",
        "nslookup", "dns ok", "dns running", "dns working", "dns doing",
        "is dns", "is coredns", "is the dns", "is the coredns",
    ])
    if _is_coredns_query:
        _log.info(f"{tag}[routing] FALLBACK → get_coredns_health()")
        return [("get_coredns_health", {})]

    _is_diagnostic = any(k in lm for k in [
        "why", "reason", "cause", "elaborate", "explain", "diagnose",
        "investigate", "root cause", "what is wrong", "whats wrong",
        "what went wrong", "in trouble", "troubleshoot", "more detail",
        "problem to start", "fail to start", "failed to start",
        "cannot start", "not starting", "unable to start",
        "have problem", "having problem", "has problem",
        "stuck", "not coming up", "not come up",
    ]) and any(k in lm for k in ["pod", "pods", "container", "containers"])

    if _is_diagnostic:
        _log.info(f"{tag}[routing] FALLBACK → get_unhealthy_pods_detail(namespace={ns!r}) (diagnostic)")
        return [("get_unhealthy_pods_detail", {"namespace": ns})]

    _is_pod_health = any(k in lm for k in [
        "pod", "pods", "container", "crashloop", "oomkill", "crashing",
        "not running", "not ready", "failing", "unhealthy", "restart",
        "pending", "evicted",
    ])
    if _is_pod_health:
        _log.info(f"{tag}[routing] FALLBACK → get_pod_status(namespace={ns!r}) (pod health)")
        return [("get_pod_status", {"namespace": ns, "show_all": False})]

    _is_cluster_health = any(k in lm for k in [
        "cluster", "health", "healthy", "issue", "problem", "anything wrong",
        "overall", "status", "ok", "okay", "all ok", "what is wrong",
        "whats wrong", "check",
    ])
    if _is_cluster_health:
        _log.info(f"{tag}[routing] FALLBACK → broad cluster health tools (namespace={ns!r})")
        return [
            ("get_node_health", {}),
            ("get_pod_status", {"namespace": ns, "show_all": False}),
            ("get_deployment_status", {"namespace": ns}),
            ("get_pvc_status", {"namespace": ns}),
            ("get_events", {"namespace": ns, "warning_only": True}),
        ]

    _log.info(f"{tag}[routing] FALLBACK → default (get_node_health + get_pod_status, namespace={ns!r})")
    return [("get_node_health", {}), ("get_pod_status", {"namespace": ns, "show_all": False})]
