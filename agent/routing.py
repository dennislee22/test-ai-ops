"""
agent/routing.py — Fallback tool routing when the LLM does not select tools.

_default_tools_for() maps a user message to a list of (tool_name, args) pairs
using keyword matching. It fires when Qwen3 fails to emit a <tool_call> block
on iteration 1, ensuring the cluster is always queried.

_resolve_namespace() extracts and resolves a namespace from the user message,
including alias expansion (e.g. "vault" → "vault-system").
"""

import re

# ── Namespace aliases ─────────────────────────────────────────────────────────
NS_ALIASES = {
    "vault":    "vault-system",
    "longhorn": "longhorn-system",
    "cattle":   "cattle-system",
    "rancher":  "cattle-system",
    "cert":     "cert-manager",
}


def resolve_namespace(lm: str) -> str:
    """
    Extract a namespace from a lowercased user message and resolve known aliases.
    Returns 'all' if no namespace is found.
    """
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
    """
    Map a user message to a list of (tool_name, args) tuples.

    Called as a fallback when the LLM produces no tool calls on iteration 1.
    Returns a list so multiple tools can be called in parallel where needed
    (e.g. RBAC queries call both get_cluster_role_bindings and get_service_accounts).
    """
    lm = user_msg.lower()
    ns = resolve_namespace(lm)

    # ── Namespace queries ─────────────────────────────────────────────────────
    is_ns_query = (
        any(k in lm for k in ["how many namespace", "list namespace", "how many ns",
                               "all namespace", "terminating namespace", "namespace phase",
                               "namespace stuck"])
        or (any(k in lm for k in ["namespace", "namespaces"])
            and any(k in lm for k in ["how many", "list", "count", "stuck",
                                       "terminating", "active", "phase"])
            and "pod" not in lm)
    )
    if is_ns_query:
        return [("get_namespace_status", {})]

    # ── Warning / cluster events ──────────────────────────────────────────────
    if any(k in lm for k in ["warning event", "warn event", "recent event",
                               "fetch event", "cluster event", "critical error",
                               "any event", "show event", "get event",
                               "events across", "events in the cluster"]):
        return [("get_events", {"namespace": ns, "warning_only": True})]

    # ── Pod listing / raw output ──────────────────────────────────────────────
    is_pod_output_query = (
        any(k in lm for k in ["output of", "show me the output", "show output",
                               "display output", "kubectl get pod", "get pods output",
                               "show all pods", "all pods output", "pods output",
                               "show pods", "display pods", "list all pods output"])
        and any(k in lm for k in ["pod", "pods"])
    )
    if is_pod_output_query:
        return [("get_pod_status", {"namespace": ns, "show_all": True, "raw_output": True})]

    # ── Pod count / health / comparison ──────────────────────────────────────
    is_pod_count_query = any(k in lm for k in [
        "how many pod", "list pod", "list all pod", "list pods",
        "pods in", "pod in", "count pod", "failing pod", "unhealthy pod",
        "oomkill", "crashloop", "crashing pod",
        "not running", "not run", "not ready",
        "any pod", "pod.*not", "pod health",
        "all pods", "show pods", "show all pod",
        # Comparison / ranking queries
        "most pod", "most pods", "least pod", "least pods",
        "fewest pod", "fewest pods", "most running", "namespace.*pod",
        "which namespace", "namespace.*most", "namespace.*least",
    ])
    if is_pod_count_query:
        is_health_check = any(k in lm for k in [
            "not running", "not run", "not ready", "any pod",
            "failing", "unhealthy", "crashloop", "oomkill", "crashing",
        ])
        # Comparison queries always need show_all=True — full counts needed to compare
        is_comparison = any(k in lm for k in [
            "most pod", "most pods", "least pod", "least pods",
            "fewest pod", "fewest pods", "which namespace", "most running",
            "namespace.*most", "namespace.*least",
        ])
        if is_health_check:
            show = False
        elif is_comparison:
            show = True
        elif ns != "all":
            show = any(k in lm for k in ["how many", "list", "count", "all pod", "all pods"])
        else:
            show = any(k in lm for k in ["how many", "list", "count", "all pod", "all pods"])
        return [("get_pod_status", {"namespace": ns, "show_all": show})]

    # ── Nodes ─────────────────────────────────────────────────────────────────
    if any(k in lm for k in ["node", "pressure", "allocatable", "node metric",
                               "node health", "node status"]):
        return [("get_node_health", {})]

    # ── Deployments ───────────────────────────────────────────────────────────
    if any(k in lm for k in ["deployment", "deploy ", "replica", "desired replica",
                               "ready replica", "degraded"]):
        return [("get_deployment_status", {"namespace": ns})]

    # ── DaemonSets ────────────────────────────────────────────────────────────
    if any(k in lm for k in ["daemonset", "daemon set", "agent pod", "missing agent",
                               "scheduling health", "ds "]):
        return [("get_daemonset_status", {"namespace": ns})]

    # ── StatefulSets ──────────────────────────────────────────────────────────
    if any(k in lm for k in ["statefulset", "stateful set"]):
        return [("get_statefulset_status", {"namespace": ns})]

    # ── Jobs / CronJobs ───────────────────────────────────────────────────────
    if any(k in lm for k in ["job", "cronjob", "cron job", "batch", "failed job",
                               "failed batch"]):
        return [("get_job_status", {"namespace": ns})]

    # ── HPA ───────────────────────────────────────────────────────────────────
    if any(k in lm for k in ["hpa", "autoscal", "horizontal pod"]):
        return [("get_hpa_status", {"namespace": ns})]

    # ── Persistent Volumes / PVCs ─────────────────────────────────────────────
    # KEY INSIGHT: get_pvc_status already includes access modes (RWO/RWX).
    # get_persistent_volumes dumps ALL PVs globally — very large, slow to read.
    # Only call it when the user explicitly asks for PV-level details
    # (reclaim policy, cross-namespace PV mapping) rather than access modes.
    is_access_mode_query = any(k in lm for k in [
        "rwo", "rwx", "rox", "rwop", "access mode", "readwriteonce",
        "readwritemany", "storage type", "storage types",
    ])
    is_explicit_pv_query = any(k in lm for k in [
        "persistent volume", " pv ", "pv globally", "all pv",
        "reclaim policy", "which pod is using pv", "who is using pv",
    ])

    if is_access_mode_query and ns != "all":
        return [("get_pvc_status", {"namespace": ns})]

    if is_access_mode_query:
        return [("get_pvc_status", {"namespace": "all"})]

    if is_explicit_pv_query:
        return [("get_persistent_volumes", {})]

    # ── PVCs ──────────────────────────────────────────────────────────────────
    if any(k in lm for k in ["pvc", "persistent volume claim", "volume claim",
                               "storage class", "storage", "vault", "volume",
                               "longhorn volume", "pending pvc", "lost pvc"]):
        return [("get_pvc_status", {"namespace": ns})]

    # ── Services ──────────────────────────────────────────────────────────────
    if any(k in lm for k in ["service", "svc", "pod selector", "clusterip",
                               "nodeport", "loadbalancer", "endpoint"]):
        return [("get_service_status", {"namespace": ns})]

    # ── Ingress ───────────────────────────────────────────────────────────────
    if any(k in lm for k in ["ingress", "load balancer ip", "lb ip", "hostname",
                               "routing rule", "ingressclass"]):
        # Detect specific ingress name e.g. "cmlwb1/apiv2-ingress" or "apiv2-ingress"
        ingress_name = ""
        m = re.search(r'(?:[a-z0-9-]+/)?([a-z0-9][a-z0-9-]*ingress[a-z0-9-]*)', lm)
        if m:
            ingress_name = m.group(1)
        return [("get_ingress_status", {"namespace": ns, "name": ingress_name})]

    # ── Resource Quotas / Limits ──────────────────────────────────────────────
    if any(k in lm for k in ["quota", "resource quota", "limit range", "hard limit",
                               "cpu limit", "memory limit", "resource limit"]):
        return [("get_resource_quotas", {"namespace": ns})]

    # ── RBAC ──────────────────────────────────────────────────────────────────
    if any(k in lm for k in ["rbac", "role binding", "service account",
                               "cluster role", "permission"]):
        return [("get_cluster_role_bindings", {}), ("get_service_accounts", {"namespace": ns})]

    # ── Secrets ───────────────────────────────────────────────────────────────
    if any(k in lm for k in ["secret", "credential", "tls cert", "ssl cert",
                               "ssl certificate", "certificate", "x509",
                               "auth token", "api key", "private key",
                               "imagepullsecret", "dockerconfig",
                               "username", "password", "user credential"]):
        # Detect specific secret name — must look like a k8s resource name.
        # Patterns tried in order:
        #   1. secret named/called "foo"  |  secret "foo"  |  secret foo
        #   2. quoted name anywhere: "foo-secret" / 'foo-secret'
        #   3. hyphenated k8s-style name that isn't a stopword
        secret_name = ""
        _stopwords = {"in", "for", "the", "all", "from", "with", "that", "has",
                      "any", "which", "what", "show", "list", "get", "cdp",
                      "namespace", "tls", "ssl", "cert", "secret", "secrets"}

        # Pattern 1: explicit "secret <name>" or "secret named <name>"
        m = re.search(r'secret\s+(?:named?\s+|called?\s+)?["\']?([a-z0-9][a-z0-9._-]{2,})["\']?', lm)
        if m and m.group(1) not in _stopwords:
            secret_name = m.group(1)

        # Pattern 2: quoted name anywhere in the message
        if not secret_name:
            m = re.search(r'["\']([a-z0-9][a-z0-9._-]{2,})["\']', lm)
            if m and m.group(1) not in _stopwords:
                secret_name = m.group(1)

        # decode flag: controlled entirely by the UI Security toggle.
        # _wants_decode is only a fallback if the ContextVar can't be read
        # (e.g. unit tests / direct invocation outside the HTTP request context).
        # It must NOT override an explicit toggle-off from the user.
        _wants_decode = any(k in lm for k in [
            "username", "password", "credential", "show me", "display", "decode", "value",
        ])
        try:
            from app import get_decode_secrets
            import logging as _log
            _rlog = _log.getLogger("agent.routing")
            _toggle = get_decode_secrets()
            _rlog.debug(f"[routing] get_decode_secrets()={_toggle}  _wants_decode={_wants_decode}")
            decode = _toggle
        except ImportError:
            decode = _wants_decode
            import logging as _log
            _log.getLogger("agent.routing").debug(f"[routing] ImportError fallback: decode={decode}")

        _is_cert_query = any(k in lm for k in [
            "ssl", "tls", "certificate", "x509", "ca bundle", "ca cert",
            "tls cert", "ssl cert", "ssl certificate",
        ])
        if _is_cert_query and not secret_name:
            _cert_filter = ["tls", "cert", "ca.", "ssl", ".crt", ".pem"]
            return [
                ("get_secrets",        {"namespace": ns, "name": "", "decode": False,
                                        "filter_keys": _cert_filter}),
                ("get_configmap_list", {"namespace": ns,
                                        "filter_keys": _cert_filter}),
            ]

        _is_credential_query = any(k in lm for k in [
            "user credential", "username", "password", "user and pass",
            "user/pass", "login", "auth credential",
        ])
        if _is_credential_query and not secret_name:
            _cred_filter = ["username", "user", "password", "pass", "login",
                            "auth", "credential", "access_key", "secret_key"]
            import logging as _log
            _log.getLogger("agent.routing").debug(
                f"[routing] credential query → get_secrets(decode={decode}, filter_keys={_cred_filter})")
            return [
                ("get_secrets", {"namespace": ns, "name": "", "decode": decode,
                                 "filter_keys": _cred_filter}),
            ]

        import logging as _log
        _log.getLogger("agent.routing").debug(
            f"[routing] secret query → get_secrets(name={secret_name!r}, decode={decode})")
        return [("get_secrets", {"namespace": ns, "name": secret_name, "decode": decode})]

    # ── ConfigMaps ────────────────────────────────────────────────────────────
    if any(k in lm for k in ["configmap", "config map"]):
        return [("get_configmap_list", {"namespace": ns})]

    # ── DB / SQL queries ──────────────────────────────────────────────────────
    # Must be checked BEFORE the cluster health fallback — "access database",
    # "sql tables", "how many tables" etc. must never fall through to health check.
    _is_db_query = any(k in lm for k in [
        "sql", "database", "db-", " db ", "db pod",
        "access db", "access database", "query db", "query database",
        "how many table", "show table", "list table", "count table",
        "show database", "list database", "mysql", "postgres",
        "mariadb", "psql", "select ", "insert ", "update ",
        "sql table", "sql query", "run query", "execute query",
        "check table", "find table", "tables in",
    ])
    if _is_db_query:
        # Extract pod name hint (e.g. "db-0", "model-metrics-db-0")
        pod_hint = ""
        m = re.search(r'\b([a-z0-9][a-z0-9-]*db[a-z0-9-]*-\d+|db-\d+)\b', lm)
        if m:
            pod_hint = m.group(1)
        return [("exec_db_query", {"namespace": ns, "pod_name": pod_hint, "sql": "SHOW TABLES"})]
    # Broad questions about overall cluster health fire ALL health tools so the
    # LLM can give a comprehensive assessment across nodes, pods, deployments,
    # PVCs, and recent warning events.
    _is_cluster_health = any(k in lm for k in [
        "cluster having issue", "cluster ok", "cluster healthy", "cluster health",
        "cluster status", "cluster doing", "anything wrong", "any issue",
        "any problem", "overall health", "overall status", "how is my cluster",
        "is my cluster", "check my cluster", "cluster check",
        "everything ok", "everything okay", "all ok", "all okay",
        "what is wrong", "what's wrong", "whats wrong",
    ])
    if _is_cluster_health:
        return [
            ("get_node_health",        {}),
            ("get_pod_status",         {"namespace": "all", "show_all": False}),
            ("get_deployment_status",  {"namespace": "all"}),
            ("get_pvc_status",         {"namespace": "all"}),
            ("get_events",             {"namespace": "all", "warning_only": True}),
        ]

    # ── Default: cluster-wide health ──────────────────────────────────────────
    return [("get_node_health", {}), ("get_pod_status", {"namespace": ns, "show_all": False})]
