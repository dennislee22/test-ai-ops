import re

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
    "grafana":    "monitoring",
    "alertmanager": "monitoring",
}


def resolve_namespace(lm: str) -> str:
    m = re.search(r'(?:^|\s)(?:in|for|namespace|ns)\s+([a-z0-9-]+)', lm)
    if m:
        raw = m.group(1)
        if raw not in ("all", "namespace", "ns", "the", "this"):
            return NS_ALIASES.get(raw, raw)

    for keyword, real_ns in NS_ALIASES.items():
        if keyword in lm:
            return real_ns

    return "all"


def default_tools_for(user_msg: str) -> list:
    lm = user_msg.lower().strip()
    ns = resolve_namespace(lm)

    _is_howto = (
        re.search(r'\b(teach|explain|show)\s+(me\s+)?(how\s+(you|to|do\s+you)|step\s+by\s+step)', lm)
        or re.search(r'\bhow\s+do\s+you\b', lm)
        or re.search(r'\bhow\s+(can|does)\s+(you|the\s+(bot|assistant|chatbot))\b', lm)
        or re.search(r'\bwhat\s+(can|do)\s+you\s+(do|support|handle|know)\b', lm)
        or re.search(r'\b(demonstrate|walkthrough|walk\s+me\s+through)\b', lm)
    )
    if _is_howto:
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
        return [("__conversational__", {})]

    _is_image_query = any(k in lm for k in [
        "image version", "image tag", "what image", "which image",
        "image of", "running image", "deployed image", "container image",
        "what version", "which version", "version of",
    ]) and any(k in lm for k in ["pod", "pods", "container", "containers", "longhorn", "namespace"])
    if _is_image_query:
        return [("get_pod_images", {"namespace": ns})]

    _is_coredns_query = any(k in lm for k in [
        "coredns", "core-dns", "kube-dns", "dns resolution", "dns health",
        "nslookup", "dns ok", "dns running", "dns working", "dns doing",
        "is dns", "is coredns", "is the dns", "is the coredns",
    ])
    if _is_coredns_query:
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
        return [("get_unhealthy_pods_detail", {"namespace": ns})]

    _is_pod_health = any(k in lm for k in [
        "pod", "pods", "container", "crashloop", "oomkill", "crashing",
        "not running", "not ready", "failing", "unhealthy", "restart",
        "pending", "evicted",
    ])
    if _is_pod_health:
        return [("get_pod_status", {"namespace": ns, "show_all": False})]

    _is_cluster_health = any(k in lm for k in [
        "cluster", "health", "healthy", "issue", "problem", "anything wrong",
        "overall", "status", "ok", "okay", "all ok", "what is wrong",
        "whats wrong", "check",
    ])
    if _is_cluster_health:
        return [
            ("get_node_health", {}),
            ("get_pod_status", {"namespace": ns, "show_all": False}),
            ("get_deployment_status", {"namespace": ns}),
            ("get_pvc_status", {"namespace": ns}),
            ("get_events", {"namespace": ns, "warning_only": True}),
        ]

    return [("get_node_health", {}), ("get_pod_status", {"namespace": ns, "show_all": False})]
