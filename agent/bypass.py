"""
agent/bypass.py — LLM synthesis bypass detection.

Philosophy: synthesise by DEFAULT. Bypass is the narrow exception, only for
queries where the user explicitly asked to "list" or "show all" something and
the tool output is a flat enumeration that needs no interpretation.

The old approach — bypass everything then blocklist patterns — caused synthesis
to be skipped for almost all queries, producing raw data dumps instead of
conversational answers. This version inverts that: bypass requires a positive
signal (explicit list/show intent) AND a bypassable tool AND clean output.
"""

import re

# ── Tools that are NEVER bypassed ─────────────────────────────────────────────
# These always need LLM synthesis to produce a useful answer.
NEVER_BYPASS = {
    "get_node_health",          # health + capacity — always needs interpretation
    "get_deployment_status",    # replica state, conditions — needs explanation
    "get_daemonset_status",
    "get_statefulset_status",
    "get_job_status",           # succeeded/failed counts — needs context
    "get_hpa_status",           # scaling metrics — needs explanation
    "get_resource_quotas",      # usage vs limits — always needs interpretation
}

# ── Tools eligible for bypass (only under strict conditions below) ─────────────
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
}

# ── Explicit list/show intent — the ONLY time bypass is allowed ───────────────
# User must have clearly asked to enumerate/list something, not asked a question.
LIST_INTENTS = (
    r"\blist\b",
    r"\bshow all\b",
    r"\bget all\b",
    r"\bdisplay all\b",
    r"\ball pods\b", r"\ball secrets\b", r"\ball services\b",
    r"\ball namespaces\b", r"\ball nodes\b", r"\ball pvcs?\b",
    r"\ball deployments\b", r"\ball configmaps?\b",
    r"\bshow pods\b", r"\bshow secrets\b", r"\bshow services\b",
    r"\bshow nodes\b", r"\bshow pvcs?\b",
    r"\benumerate\b",
)

# ── Patterns that always force synthesis regardless of anything else ───────────
ALWAYS_SYNTHESISE = (
    # Questions
    r"\?",
    r"\bwhy\b", r"\bwhat\b", r"\bhow\b", r"\bwhen\b", r"\bwhere\b",
    r"\bwhich\b", r"\bwho\b",
    # Health / diagnosis
    r"\bhealth\b", r"\bhealthy\b", r"\bok\b", r"\bokay\b",
    r"\bissue\b", r"\bproblem\b", r"\berror\b", r"\bwarning\b",
    r"\bfail", r"\bbroken\b", r"\bdown\b", r"\bcrash",
    r"\bstatus\b", r"\bstate\b", r"\bcondition\b",
    r"\bpending\b", r"\bcrashloop\b", r"\bnot running\b", r"\bnot ready\b",
    # Conversational
    r"\btell me\b", r"\bexplain\b", r"\bdescribe\b", r"\bsummar",
    r"\bgive me\b", r"\bshow me\b",
    # Analysis
    r"\bmost\b", r"\bleast\b", r"\bcompare\b", r"\brank\b",
    r"\bhow many\b", r"\bcount\b", r"\btop\b",
    # Credentials / certs — synthesis needed only for analysis questions about them
    r"\bcertificate\b", r"\bssl\b", r"\btls\b", r"\bcert\b",
)

# Namespace keywords — used by build_direct_answer for the NS assumption prefix.
NS_SPECIFIED_KEYWORDS = (
    "namespace", " ns=", " ns ", "cdp", "cmlwb", "longhorn",
    "vault", "cattle", "rancher", "cert", "default namespace",
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
    """
    Return True ONLY if ALL of the following are true:
      1. The tool is not in NEVER_BYPASS
      2. The tool is in BYPASSABLE_TOOLS
      3. The output is clean (not an error or empty result)
      4. The user question has explicit list/show intent (LIST_INTENTS)
      5. The user question has NO analysis/question/conversational signal (ALWAYS_SYNTHESISE)
    """
    # 1. Hard never-bypass tools
    if tool_name in NEVER_BYPASS:
        return False

    # 2. Must be a bypassable tool
    if tool_name not in BYPASSABLE_TOOLS:
        return False

    # 3. Never bypass error or empty outputs
    if (output.startswith("K8s API error") or output.startswith("[ERROR]")
            or "not found" in output.lower() or output.lower().startswith("no ")):
        return False

    lq = user_q.lower()

    # 4. Any analysis/question/conversational signal → always synthesise.
    # This check must come BEFORE any early-return bypass logic.
    if any(re.search(pat, lq) for pat in ALWAYS_SYNTHESISE):
        return False

    # 5. filter_keys queries with hidden output: bypass directly so the user
    # sees the ❗ reminder line without LLM paraphrasing it away.
    # Never bypass when decode=True — decoded plaintext passwords must go
    # through synthesis so the LLM can present them in a structured response.
    if args.get("filter_keys") and not args.get("decode"):
        if any(re.search(pat, lq) for pat in LIST_INTENTS):
            return True

    # 6. Never bypass when decode=True — decoded values must always go through
    # synthesis so the LLM presents them in a structured response.
    if args.get("decode"):
        return False

    # 7. Must have explicit list intent to bypass
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

