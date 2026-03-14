import re
import logging

_log = logging.getLogger("agent.routing")

# ── Namespace alias registry ──────────────────────────────────────────────────
# _K8S_NS_ALIASES: Kubernetes-standard component namespaces mandated by the
# Kubernetes spec — safe to hardcode since they never change regardless of how
# the cluster is deployed.
_K8S_NS_ALIASES = {
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
}

# _TOPOLOGY_NS_ALIASES: deployment-specific namespace mappings that vary per
# customer, ECS version, and Helm values.  These are NOT hardcoded — they are
# loaded from the NS_ALIASES environment variable so each deployment can
# configure its own topology without touching code.
#
# Set NS_ALIASES in your .env file:
#   NS_ALIASES="vault=vault-system,longhorn=longhorn-system,alertmanager=monitoring,cdp=cdp"
#
# The defaults below match the reference ECS deployment; override with env var.
import os as _os

def _load_topology_aliases() -> dict:
    raw = _os.getenv("NS_ALIASES", "").strip()
    if raw:
        result = {}
        for pair in raw.split(","):
            pair = pair.strip()
            if "=" in pair:
                k, v = pair.split("=", 1)
                result[k.strip().lower()] = v.strip()
        if result:
            return result
    # Default fallback — should be overridden via NS_ALIASES env var
    return {
        "vault":    "vault-system",
        "longhorn": "longhorn-system",
        "cdp":      "cdp",
    }

_TOPOLOGY_NS_ALIASES = _load_topology_aliases()

# Merged alias map — K8s-standard aliases always win over topology overrides
NS_ALIASES = {**_TOPOLOGY_NS_ALIASES, **_K8S_NS_ALIASES}



def resolve_namespace(lm: str, req_id: str = "") -> str:
    tag = f"[REQ:{req_id}] " if req_id else ""
    m = re.search(r'(?:^|\s)(?:in|for|namespace|ns)\s+([a-z0-9-]+)', lm)
    if m:
        raw = m.group(1)
        # Exclude PVC/pod state words and pod-name-shaped tokens
        # (pod names have 3+ hyphens e.g. "cdp-release-svc-abc-123" — never a namespace)
        _STATE_WORDS = {"all", "namespace", "ns", "the", "this",
                        "pending", "lost", "bound", "unbound", "stuck",
                        "running", "failed", "unknown", "error", "ready"}
        if raw not in _STATE_WORDS and raw.count("-") < 3:
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

    # Restart-threshold query: "pods restarted more than N times", "restarted more than 5"
    # Needs get_unhealthy_pods_detail (has logs + exit codes) not get_pod_status.
    _is_restart_threshold = (
        any(k in lm for k in ["restart", "restarted", "restarting"])
        and any(k in lm for k in ["more than", "over", "greater than",
                                   "at least", "exceeded", "times"])
    )
    if _is_restart_threshold:
        _log.info(f"{tag}[routing] FALLBACK → get_unhealthy_pods_detail(namespace={ns!r}) (restart threshold)")
        return [("get_unhealthy_pods_detail", {"namespace": ns})]

    # PVC state query: "which pvc is not bound?", "any unbound pvcs?", "pvc stuck/pending/lost"
    # Must fire BEFORE _is_pod_health because "pending" is in both PVC and pod keyword sets.
    _is_pvc_state = (
        any(k in lm for k in ["pvc", "pvcs", "persistent volume claim", "persistent volume"])
        and any(k in lm for k in [
            "not bound", "unbound", "not-bound", "pending", "lost",
            "stuck", "which pvc", "any pvc", "pvc state", "pvc status",
        ])
    )
    if _is_pvc_state:
        _log.info(f"{tag}[routing] FALLBACK → get_pvc_status(namespace={ns!r}) (PVC state query)")
        return [("get_pvc_status", {"namespace": ns, "detail": True})]

    # Pod log query: "show me the log of pod X", "logs for X", "get pod logs"
    # Pod-name pattern: lowercase letters/digits/hyphens with at least 2 hyphens
    import re as _re2
    _has_pod_name = bool(_re2.search(
        r'[a-z][a-z0-9-]+-[a-z0-9]+-[a-z0-9]+', lm))
    _is_pod_log = (
        any(k in lm for k in ["log of ", "log for ", "logs of ", "logs for ",
                               "show log", "show me log", "get log", "get logs",
                               "pod log", "container log", " logs "])
        and (any(k in lm for k in ["pod", "container"]) or _has_pod_name)
    )
    if _is_pod_log:
        # Route to kubectl_exec so output is raw and bypasses LLM synthesis.
        # Only call get_pod_logs (which goes through synthesis) if user asks
        # to analyse/explain the logs — that is handled by the LLM itself
        # when it selects get_pod_logs on its own from the tool descriptions.
        import re as _re
        # Match full pod names (2+ segments) OR simple names like vault-0, db-0
        pod_match = (
            _re.search(r'[a-z][a-z0-9-]*-[a-z0-9]+-[a-z0-9]+(?:-[a-z0-9]+)*', lm)
            or _re.search(r'[a-z][a-z0-9]+-\d+', lm)
        )
        pod_name = pod_match.group(0) if pod_match else ""
        if pod_name:
            cmd = f"kubectl logs -n {ns} {pod_name} --tail=100"
            _log.info(f"{tag}[routing] FALLBACK → kubectl_exec logs (pod={pod_name!r}, ns={ns!r})")
            return [("kubectl_exec", {"command": cmd})]
        # No pod name found — let LLM sort it out
        return [("get_pod_logs", {"pod_name": "", "namespace": ns})]

    # Pod describe query: "describe pod X", "show pod description of X"
    _is_pod_describe = (
        any(k in lm for k in ["describe pod", "pod description", "description of pod",
                               "describe the pod", "show description", "pod detail",
                               "detail of pod", "details of pod", "details for pod"])
    )
    if _is_pod_describe:
        # Route to kubectl_exec so output is the raw kubectl describe output.
        # First lookup the namespace if not explicit — search all namespaces.
        import re as _re
        pod_match = (
            _re.search(r'[a-z][a-z0-9-]*-[a-z0-9]+-[a-z0-9]+(?:-[a-z0-9]+)*', lm)
            or _re.search(r'[a-z][a-z0-9]+-\d+', lm)
        )
        pod_name = pod_match.group(0) if pod_match else ""
        if pod_name:
            if ns and ns != "all":
                # Namespace known — describe directly
                cmd = f"kubectl -n {ns} describe pod {pod_name}"
            else:
                # Namespace not specified — search all namespaces first
                # The LLM will see the pod location and can describe it next
                cmd = f"kubectl get pod -A -o wide"
            _log.info(f"{tag}[routing] FALLBACK → kubectl_exec describe (pod={pod_name!r}, ns={ns!r})")
            return [("kubectl_exec", {"command": cmd})]
        return [("describe_pod", {"pod_name": "", "namespace": ns})]

    # Resource calculation query: "calculate CPU/memory requests", "total CPU in namespace X"
    _is_resource_calc = (
        any(k in lm for k in ["calculate", "total cpu", "total mem", "sum of",
                               "cpu request", "memory request", "resource request",
                               "cpu limit", "memory limit", "resource limit",
                               "how much cpu", "how much memory", "how much mem"])
        and any(k in lm for k in ["namespace", "pods", "all pods", ns])
    )
    if _is_resource_calc and ns != "all":
        _log.info(f"{tag}[routing] FALLBACK → get_namespace_resource_summary(namespace={ns!r})")
        return [("get_namespace_resource_summary", {"namespace": ns})]

    _is_pod_health = any(k in lm for k in [
        "pod", "pods", "container", "crashloop", "oomkill", "crashing",
        "not running", "not ready", "failing", "unhealthy", "restart",
        "pending", "evicted",
    ])
    if _is_pod_health:
        _log.info(f"{tag}[routing] FALLBACK → get_pod_status(namespace={ns!r}) (pod health)")
        return [("get_pod_status", {"namespace": ns, "show_all": False})]

    # Storage-class-usage query: "what SC is X using?" / "which storage class does vault use?"
    # These need live PVC data — NOT the static storage class table in the system prompt.
    # Storage-class-usage: "what SC is X using?" needs live PVC data.
    # Distinguish from "what storage classes exist?" (static) by requiring
    # a usage verb — "using/uses/use/used by" — alongside the SC keyword.
    _is_sc_usage = (
        any(k in lm for k in ["storage class", "storageclass", "storage-class", " sc "])
        and any(k in lm for k in ["using", "use", "uses", "used by", "is using",
                                   "does use", "which sc does", "which storage class does",
                                   "what sc is", "what storage class is"])
    )
    if _is_sc_usage:
        _log.info(f"{tag}[routing] FALLBACK → get_pvc_status(namespace={ns!r}, detail=True) (SC usage query)")
        return [("get_pvc_status", {"namespace": ns, "detail": True})]

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
