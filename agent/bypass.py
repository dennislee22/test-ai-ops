import re

NEVER_BYPASS = {
    "get_node_health",
    "get_deployment_status",
    "get_daemonset_status",
    "get_statefulset_status",
    "get_job_status",
    "get_hpa_status",
    "get_resource_quotas",
    "get_pv_usage",
    "get_coredns_health",
    "get_unhealthy_pods_detail",
}

BYPASSABLE_TOOLS = {
    "get_namespace_status",
    "get_persistent_volumes",
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
}

LIST_INTENTS = (
    r"\blist\b",
    r"\bshow all\b",
    r"\bget all\b",
    r"\bdisplay all\b",
    r"\ball pods\b", r"\ball secrets\b", r"\ball services\b",
    r"\ball namespaces\b", r"\ball nodes\b", r"\ball pvcs?\b",
    r"\ball deployments\b", r"\ball configmaps?\b",
    r"\ball replicasets?\b", r"\ball statefulsets?\b", r"\ball daemonsets?\b",
    r"\bshow pods\b", r"\bshow secrets\b", r"\bshow services\b",
    r"\bshow nodes\b", r"\bshow pvcs?\b", r"\bshow replicasets?\b",
    r"\benumerate\b",
)

ALWAYS_SYNTHESISE = (

    r"\?",
    r"\bwhy\b", r"\bwhat\b", r"\bhow\b", r"\bwhen\b", r"\bwhere\b",
    r"\bwhich\b", r"\bwho\b",

    r"\bhealth\b", r"\bhealthy\b", r"\bok\b", r"\bokay\b",
    r"\bissue\b", r"\bproblem\b", r"\berror\b", r"\bwarning\b",
    r"\bfail", r"\bbroken\b", r"\bdown\b", r"\bcrash",
    r"\bstatus\b", r"\bstate\b", r"\bcondition\b",
    r"\bpending\b", r"\bcrashloop\b", r"\bnot running\b", r"\bnot ready\b",

    r"\btell me\b", r"\bexplain\b", r"\bdescribe\b", r"\bsummar",
    r"\bgive me\b", r"\bshow me\b",

    r"\bmost\b", r"\bleast\b", r"\bcompare\b", r"\brank\b",
    r"\bhow many\b", r"\bcount\b", r"\btop\b",

    r"\bcertificate\b", r"\bssl\b", r"\btls\b", r"\bcert\b",
)

NS_SPECIFIED_KEYWORDS = (
    "namespace", " ns=", " ns ", "cdp", "cmlwb", "longhorn",
    "vault", "cattle", "rancher", "cert", "default namespace",
    "coredns", "kube-system", "dns", "kube-proxy", "etcd",
    "scheduler", "controller", "apiserver",
)

NS_SCOPED_TOOLS = {
    "get_pod_status", "get_deployment_status", "get_daemonset_status",
    "get_statefulset_status", "get_job_status", "get_hpa_status",
    "get_pvc_status", "get_service_status", "get_ingress_status",
    "get_resource_quotas", "get_configmap_list", "get_service_accounts",
    "get_secrets",
}

def should_bypass_llm(tool_name: str, args: dict,
                      output: str, user_q: str) -> bool:
    if tool_name in NEVER_BYPASS:
        return False

    if tool_name not in BYPASSABLE_TOOLS:
        return False

    if (output.startswith("K8s API error") or output.startswith("[ERROR]")
            or "not found" in output.lower() or output.lower().startswith("no ")):
        return False

    lq = user_q.lower()

    if any(re.search(pat, lq) for pat in ALWAYS_SYNTHESISE):
        return False

    if args.get("filter_keys") and not args.get("decode"):
        if any(re.search(pat, lq) for pat in LIST_INTENTS):
            return True

    if args.get("decode"):
        return False

    if not any(re.search(pat, lq) for pat in LIST_INTENTS):
        return False

    return True

def build_direct_answer(tool_name: str, output: str, user_q: str) -> str:
    lq = user_q.lower()
    ns_specified = any(k in lq for k in NS_SPECIFIED_KEYWORDS)
    if tool_name in NS_SCOPED_TOOLS and not ns_specified:
        return ("As no namespace was specified, I am assuming "
                "you are requesting for all namespaces.\n\n" + output)
    return output
