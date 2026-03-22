from tools.tools_k8s import (
    get_pod_status, get_pod_logs, describe_pod, get_node_info, get_gpu_info,
    get_node_labels, get_node_taints, get_events, get_deployment, describe_sc,
    get_daemonset, get_statefulset, get_adhoc_job_status, get_hpa_status, describe_pvc,
    get_pvc_status, get_cluster_version, get_storage_classes, get_endpoints,
    get_node_capacity, get_persistent_volumes, get_service, get_ingress, describe_pv,
    get_configmap_list, get_secret_list, get_resource_quotas, get_limit_ranges,
    get_serviceaccounts, get_cluster_role_bindings, get_namespace_status,
    get_pod_tolerations, get_pod_resource_requests, run_cluster_health, get_replicaset,
    get_namespace_resource_summary, get_pod_images, get_unhealthy_pods_detail,
    get_coredns_health, get_pv_usage, find_resource, get_pod_containers_resources, get_cronjob_status,
    kubectl_exec, exec_db_query, get_pod_storage, get_pdb_status,
    get_certificate_status, get_control_plane_status, get_network_policy_status, get_webhook_health,
    get_top_pods, get_top_nodes,
)

_P_NS = {
    "type":        "string",
    "default":     "all",
    "description": "Namespace to query. Defaults to 'all' — only override when the user explicitly names a namespace.",
}

_P_SEARCH = {
    "type":        "string",
    "default":     None,
    "description": "Optional keyword to filter by name (partial, case-insensitive match).",
}

_P_YAML = {
    "type":        "boolean",
    "default":     False,
    "description": "If true, output the full object as YAML instead of the human-readable summary.",
}

_VERBATIM = (
    "CRITICAL: Output the exact Markdown table returned by this tool. "
    "Do NOT reformat, summarise, or remove table headers."
)

K8S_TOOL_METADATA: dict = {

    "find_resource": {
        "fn":          find_resource,
        "description": (
            "Search for Kubernetes resources by name (partial, case-insensitive match) across "
            "all major resource types: pods, deployments, daemonsets, statefulsets, replicasets, "
            "services, ingresses, PVCs, configmaps, and secrets. "
            "Use this as the PRIMARY tool whenever the user mentions a specific resource name or "
            "asks where something is running. Examples: "
            "'where is grafana', 'find the nginx deployment', 'is there a redis statefulset', "
            "'which node is prometheus running on', 'show me the grafana service', "
            "'locate ingress Y', 'is there anything named prometheus', 'find all resources named cdp'. "
            "Also use this when the user asks to list ALL resources with no specific name — "
            "pass name_substring='' to show everything. "
            "Vague intent words like 'all', 'any', 'everything' are automatically treated as "
            "no filter, so the full resource list is returned. "
            "The tool uses a two-stage fallback: if a typed search yields no matches it "
            "automatically widens to all resource types and searches again, then falls back "
            "to showing everything if still nothing is found. "
            "The fallback messages are user-facing — do NOT mention resource_type in your response. "
            "Do NOT use get_pod_status when a specific resource name is mentioned — use this tool. "
            "Do NOT use get_deployment, get_daemonset, get_statefulset when searching by name — use this tool. "
            "Returns: scope header, resource type, namespace, name, and status/details per row."
        ),
        "parameters":  {
            "name_substring": {
                "type":        "string",
                "description": (
                    "Partial name to search for (e.g., 'grafana', 'nginx', 'redis', 'cdp'). "
                    "Pass an empty string '' to list all resources with no name filter. "
                    "Do NOT pass intent words like 'all', 'any', or 'everything' — "
                    "pass '' instead. These words are stripped automatically but '' is cleaner."
                ),
            },
            "resource_type":  {
                "type":        "string",
                "default":     None,
                "description": (
                    "Optional resource type to restrict the search. Accepted values: "
                    "'pod', 'deployment' or 'deploy', 'daemonset' or 'ds', "
                    "'statefulset' or 'sts', 'replicaset' or 'rs', "
                    "'svc' or 'service', 'ingress', 'pvc', 'configmap' or 'cm', 'secret'. "
                    "Omit (None) to search all supported types at once. "
                    "IMPORTANT: only set this if the user explicitly mentioned a resource type "
                    "(e.g. 'find the grafana deployment', 'is there a grafana service'). "
                    "If the user says 'find grafana', 'where is grafana', or any phrasing "
                    "without a specific type, pass None — the tool searches all types automatically. "
                    "Never infer a type from context."
                ),
            },
            "namespace":      _P_NS,
        },
    },

    "get_pod_containers_resources": {
        "fn":          get_pod_containers_resources,
        "description": (
            "List all containers in pods across a namespace (or all namespaces). "
            "Shows container name, image, CPU and memory requests/limits, and attached GPUs if any. "
            "Supports partial matching on pod names or namespaces using the `search` parameter. "
            "Use for queries like: 'list all containers in pod X', "
            "'what images are running in namespace Y', or 'show CPU/memory allocated for containers'. "
            "Always includes requested CPU in m and memory in Mi, as defined in pod.spec.resources."
        ),
        "parameters":  {
            "namespace": _P_NS,
            "search":    {**_P_SEARCH, "description": "Partial pod name or namespace to filter results. Leave empty to show all pods."},
        },
    },

    "get_pod_status": {
        "fn":          get_pod_status,
        "description": (
            "List Kubernetes pods with their phase, readiness, restart count, and conditions. "
            "This is the PRIMARY tool for listing pods and checking pod health. "
            "Use this tool for queries like: "
            "'list pods', 'list all pods', 'list pods in namespace X', "
            "'show pods in cdp namespace', 'any pod not running', "
            "'any broken or stuck pods', 'show me failed pods', 'are there any pending pods'. "
            "Supports filtering by partial pod name via search, and by phase via the phase parameter. "
            "When phase='notrunning', fetches only Pending/Failed/Unknown pods using server-side "
            "field selectors — efficient even on large clusters. "
            "Non-running results include an extra REASON column (e.g. CrashLoopBackOff, "
            "ImagePullBackOff, OOMKilled) so the cause is visible without a second tool call. "
            "Namespace defaults to 'all' unless the user explicitly specifies one. "
            "Do NOT use this tool for detailed per-container resource requests or limits — "
            "use get_pod_resource_requests for that purpose. "
            "Do NOT use get_unhealthy_pods_detail unless the user explicitly asks for logs "
            "or deep diagnostics on a specific pod."
        ),
        "parameters":  {
            "namespace": _P_NS,
            "search":    {**_P_SEARCH, "description": "Optional keyword to filter pods by name (partial, case-insensitive match). Omit when not filtering by a specific name."},
            "phase":     {
                "type":        "string",
                "default":     None,
                "description": (
                    "Optional phase filter. Use this whenever the user asks about pod health "
                    "or non-running pods. "
                    "Pass 'notrunning' for: 'any pod not running', 'broken pods', 'stuck pods', "
                    "'unhealthy pods', 'failed pods', 'any issues', 'pods with problems'. "
                    "Also accepts natural variants: 'unhealthy', 'failed', 'failing', "
                    "'stuck', 'broken', 'down', 'pending', 'unknown'. "
                    "For a specific phase pass it directly: 'Running', 'Pending', 'Failed', 'Unknown'. "
                    "Omit entirely to show all pods regardless of phase."
                ),
            },
        },
    },

    "get_pod_storage": {
        "fn":          get_pod_storage,
        "description": (
            "Show storage types (PVC access modes like ReadWriteOnce/ReadWriteMany) used by pods in a namespace. "
            "Supports searching by pod name or namespace — if no matches are found, falls back to all pods. "
            "Returns a Markdown table listing pods with their attached PVCs, access modes, and storage class, "
            "and a summary of storage types and storage classes used across pods. "
            "Use this for queries like: 'which pods use RWX', 'list all storage types in namespace X', "
            "'what PVCs are attached to pod Y', 'storage summary for pods in namespace Z'."
        ),
        "parameters":  {
            "namespace": _P_NS,
            "search":    _P_SEARCH,
        },
    },

    "get_pod_logs": {
        "fn":          get_pod_logs,
        "description": (
            "Fetch recent log lines from pods. "
            "Supports filtering by pod name or namespace. "
            "Use for: 'show me the log of pod X', 'get logs for X', 'what does pod X log say?'. "
            "For multi-container pods the correct container is auto-selected — "
            "only pass container= if the user asks for a specific container's logs. "
            "Defaults to searching across all namespaces if namespace is not specified."
        ),
        "parameters":  {
            "search":     {**_P_SEARCH, "description": "Partial pod name to search for (e.g., 'prometheus-server')."},
            "namespace":  _P_NS,
            "tail_lines": {"type": "integer", "default": 50, "description": "Number of log lines to return (max 100)."},
            "container":  {"type": "string",  "default": "", "description": "Container name. Leave empty to auto-select the main app container."},
        },
    },

    "describe_pod": {
        "fn":          describe_pod,
        "description": (
            "Get detailed info about pods: container states, restart count, "
            "last termination reason (e.g., OOMKilled, Error), and CPU/memory requests and limits per container. "
            "Supports searching by exact pod name or partial pod name across namespaces. "
            "Optionally output the full YAML of the pod. "
            "Use for: 'what are the resource limits for pod X', 'why did pod X crash', "
            "'what is the memory limit for pod X', or any OOMKilled diagnosis. "
            "This is the ONLY tool that shows per-pod resource limits and termination reasons."
        ),
        "parameters":  {
            "pod_name":  {"type": "string", "description": "Exact pod name to fetch details for. Optional if using 'search'."},
            "search":    {**_P_SEARCH, "description": "Partial pod name to search for across namespaces. Optional if using 'pod_name'."},
            "namespace": _P_NS,
            "show_yaml": _P_YAML,
        },
    },

    "get_node_info": {
        "fn":          get_node_info,
        "description": (
            "Check Kubernetes node health, resources, and scheduling status. "
            "Returns a Markdown table with columns: NODE, ROLES, STATUS (including Ready/NotReady and Cordon/SchedulingDisabled), CPU, RAM (Gi), GPU. "
            "Supports filtering for a specific node by partial name match. "
            "Use for questions like: "
            "'are nodes healthy', 'list all nodes', 'node status', "
            "'is ecs-w-01 cordoned', 'which node is cordoned', 'is any node cordoned', "
            "'which node is unschedulable', 'is scheduling disabled on any node', "
            "'can pods be scheduled on this node', 'why are pods pending'. "
            "IMPORTANT: Always use this tool — NOT get_node_taints — for cordon, "
            "unschedulable, or SchedulingDisabled questions. "
            "Cordon sets spec.unschedulable=true and is shown in the STATUS column here. "
            + _VERBATIM
        ),
        "parameters":  {
            "node_name": {
                "type":        "string",
                "default":     None,
                "description": (
                    "Optional specific node to query (partial match supported). "
                    "Leave empty or null to list ALL nodes. "
                    "If a search yields no matches, it automatically falls back to listing all nodes."
                ),
            },
        },
    },

    "get_gpu_info": {
        "fn":          get_gpu_info,
        "description": (
            "List nodes with GPU hardware and their technical specifications. "
            "Returns a Markdown table showing GPU product name, total count, memory per card (GRAM/VRAM), "
            "current allocatable capacity (from the device plugin), which pods are attached to or using the GPU, "
            "and whether the GPU is currently in use. "
            "Use this to answer: 'what kind of GPUs do we have', 'how much VRAM is on ecs-w-03', "
            "'which pod is attached to GPU', 'which pod is using GPU', or 'is the GPU in use'. "
            + _VERBATIM
        ),
        "parameters":  {},
    },

    "get_node_labels": {
        "fn":          get_node_labels,
        "description": (
            "Show labels for Kubernetes nodes in the cluster. "
            "Returns a structured Markdown list mapping nodes to their labels. "
            "The `search` parameter is highly flexible: you can pass a partial/full 'node_name' "
            "to get ALL labels for that specific node, OR pass a label keyword (e.g., 'gpu', 'cde') "
            "to find which nodes have that specific label. "
            "IMPORTANT: If the user asks for 'labels', 'label', 'all', or similar general terms, do NOT pass these words as the search term. Leave the search parameter empty (null). "
            "CRITICAL: Output the exact text returned by this tool. Use bulleted list. Do NOT convert to a table, modify formatting, summarise, or omit ANY labels."
        ),
        "parameters":  {
            "search": {**_P_SEARCH, "description": "Optional keyword to filter by node name OR label content. Leave empty (or null) to list ALL nodes and ALL their labels."},
        },
    },

    "get_node_taints": {
        "fn":          get_node_taints,
        "description": (
            "List taints on all Kubernetes nodes. "
            "Returns the taint key, value, and effect for each node. "
            "A taint is a key/value/effect label on a node that repels pods unless they have a matching toleration. "
            "Use this for questions like: "
            "'which node is tainted', 'show node taints', 'are any nodes tainted', "
            "'what taints are on the cluster', 'show me the NoSchedule taints', "
            "'which nodes have gpu taint', 'list nodes tainted with cde', "
            "'which nodes have a dedicated taint', 'what toleration do I need for node X'. "
            "Do NOT use for cordon or unschedulable questions — use get_node_info for those. "
            "PARAMETER ROUTING — choose carefully: "
            "search = partial NODE NAME filter (e.g. 'ecs-w-01'). "
            "taint_search = partial TAINT KEY or VALUE filter (e.g. 'cde', 'gpu', 'dedicated'). "
            "tainted_only = True to show only nodes that have at least one taint. "
            "When the user asks for nodes tainted WITH a specific word like 'cde' or 'gpu', "
            "always use taint_search — NOT search."
        ),
        "parameters":  {
            "search":       {**_P_SEARCH, "description": "Optional NODE NAME filter (partial match, e.g. 'ecs-m-01'). Do NOT use for taint content — use taint_search instead."},
            "taint_search": {
                "type":        "string",
                "default":     None,
                "description": (
                    "Optional TAINT KEY or VALUE content filter (partial, case-insensitive). "
                    "Use when the user asks for nodes tainted WITH a specific key or value: "
                    "'tainted with cde' → taint_search='cde', "
                    "'nodes with gpu taint' → taint_search='gpu', "
                    "'dedicated taint' → taint_search='dedicated'. "
                    "This filters taint rows, not node names."
                ),
            },
            "tainted_only": {
                "type":        "boolean",
                "default":     False,
                "description": "When True, only show nodes that have at least one taint. Set True for: 'which node is tainted', 'show tainted nodes', 'are any nodes tainted'.",
            },
        },
    },

    "describe_sc": {
        "fn":          describe_sc,
        "description": (
            "Get detailed info about a Kubernetes StorageClass, including provisioner, parameters, "
            "volume binding mode, and reclaim policy. Supports partial name search if needed. "
            "Use for: 'what is the configuration of StorageClass X', 'show me details of my storage class', "
            "or 'is this the default storage class?'."
        ),
        "parameters":  {
            "name":      {"type": "string", "description": "Name of the StorageClass to describe."},
            "show_yaml": _P_YAML,
        },
    },

    "describe_pvc": {
        "fn":          describe_pvc,
        "description": (
            "Get detailed info about a PersistentVolumeClaim (PVC), including status, storage class, bound volume, "
            "capacity, access modes, labels, annotations, and finalizers. Supports partial name search and namespace selection. "
            "Use for: 'show me details of PVC X', 'which pod is using PVC X?', or 'what is the storage class and size of PVC X?'."
        ),
        "parameters":  {
            "name":      {"type": "string", "description": "Name of the PVC to describe."},
            "namespace": _P_NS,
            "show_yaml": _P_YAML,
        },
    },

    "describe_pv": {
        "fn":          describe_pv,
        "description": (
            "Get detailed info about a PersistentVolume (PV): status, storage class, "
            "access modes, capacity, reclaim policy, volume source, node affinity, and events. "
            "Supports partial PV name search and optional full YAML output. "
            "Use for: 'what is the status of PV X', 'which PVC is bound to PV X', "
            "or inspecting PV configuration and events."
        ),
        "parameters":  {
            "name":      {"type": "string",  "description": "Partial or full name of the PersistentVolume to describe."},
            "show_yaml": _P_YAML,
        },
    },

    "get_events": {
        "fn":          get_events,
        "description": (
            "Fetch recent Kubernetes events. Use for diagnosing issues, errors, or warnings. "
            "Supports searching by namespace, involved object, or message content (partial matches). "
            "type='Warning' returns Warning events (falls back to Normal if none found). "
            "type='Normal' returns only Normal events. "
            "type='All' (default) returns all events."
        ),
        "parameters":  {
            "namespace": _P_NS,
            "search":    {**_P_SEARCH, "description": "Optional search term to filter events by pod, namespace, object, or message."},
            "type":      {"type": "string", "default": "All", "description": "Event type to fetch: 'Warning', 'Normal', or 'All' (default)."},
        },
    },

    "get_deployment": {
        "fn":          get_deployment,
        "description": (
            "List Deployments and their health status (desired, ready, available pods). "
            "Supports filtering by partial name match. " + _VERBATIM
        ),
        "parameters":  {
            "namespace": _P_NS,
            "search":    {**_P_SEARCH, "description": "Optional keyword to filter deployments by name (partial match)."},
        },
    },

    "get_statefulset": {
        "fn":          get_statefulset,
        "description": "List StatefulSets and their health status (desired vs ready pods). " + _VERBATIM,
        "parameters":  {"namespace": _P_NS},
    },

    "get_daemonset": {
        "fn":          get_daemonset,
        "description": "List DaemonSets and their health status (desired, ready, available pods). " + _VERBATIM,
        "parameters":  {"namespace": _P_NS},
    },

    "get_replicaset": {
        "fn":          get_replicaset,
        "description": "List ReplicaSets and their health status (desired, ready, available pods). " + _VERBATIM,
        "parameters":  {"namespace": _P_NS},
    },

    "get_pdb_status": {
        "fn":          get_pdb_status,
        "description": (
            "List all PodDisruptionBudgets (PDBs) across a namespace (or all namespaces). "
            "Shows minimum available, maximum unavailable, allowed disruptions, and current/desired healthy counts. "
            "Flags PDBs that are blocking node evictions or upgrades (Allowed Disruptions = 0). "
            "Use for queries like: 'show me pod disruption budgets', 'why can't I drain this node', or 'are there any PDBs blocking evictions'."
        ),
        "parameters":  {"namespace": _P_NS},
    },

    "get_webhook_health": {
        "fn":          get_webhook_health,
        "description": (
            "List all Mutating and Validating Admission Webhook Configurations in the cluster. "
            "Shows webhook name, target service/URL, and explicitly flags webhooks with 'failurePolicy: Fail'. "
            "Use for queries like: 'check admission webhooks', 'what webhooks are active', or 'are there any webhooks that could break the cluster if they go down'."
        ),
        "parameters":  {},
    },

    "get_cronjob_status": {
        "fn":          get_cronjob_status,
        "description": (
            "List all CronJobs across a namespace (or all namespaces). "
            "Shows the schedule, whether it is suspended, the number of currently active jobs, and the time since the last run. "
            "Supports partial matching on CronJob names using the `search` parameter. "
            "Use for queries like: 'show me cronjobs', 'are any cronjobs suspended', or 'when did my nightly batch last run'."
        ),
        "parameters":  {
            "namespace": _P_NS,
            "search":    {**_P_SEARCH, "description": "Partial CronJob name to filter results. Leave empty to show all CronJobs."},
        },
    },

    "get_network_policy_status": {
        "fn":          get_network_policy_status,
        "description": (
            "Audit NetworkPolicies across a namespace (or all namespaces). "
            "Shows the policy name, pod selector, and policy types (Ingress/Egress). "
            "When checking all namespaces, it outputs a critical warning listing namespaces that have zero network policies securing them. "
            "Use for queries like: 'show network policies', 'audit cluster network security', or 'which namespaces are open to lateral movement'."
        ),
        "parameters":  {"namespace": _P_NS},
    },

    "get_control_plane_status": {
        "fn":          get_control_plane_status,
        "description": (
            "Check the health of core Kubernetes control plane components (etcd, kube-apiserver, kube-controller-manager, kube-scheduler). "
            "Reads ComponentStatuses and inspects core pods running in the kube-system namespace. "
            "Use for queries like: 'is the control plane healthy', 'check etcd status', or 'is the api server running'."
        ),
        "parameters":  {},
    },

    "get_certificate_status": {
        "fn":          get_certificate_status,
        "description": (
            "List cert-manager Certificates across a namespace (or all namespaces). "
            "Shows the Certificate's Ready status, target secret name, and exact expiration date (notAfter). "
            "Requires cert-manager custom resource definitions (CRDs) to be installed on the cluster. "
            "Use for queries like: 'check certificate expirations', 'are any cert-manager certificates failing', or 'show TLS cert status'."
        ),
        "parameters":  {"namespace": _P_NS},
    },

    "get_adhoc_job_status": {
        "fn":          get_adhoc_job_status,
        "description": (
            "List standalone or ad-hoc Jobs across a namespace (or all namespaces). "
            "By default, this excludes Jobs spawned by CronJobs to prevent log spam. "
            "Shows active, succeeded, and failed counts for each job. "
            "Use for queries like: 'show failed jobs', 'list running jobs', or 'check one-off job status'."
        ),
        "parameters":  {
            "namespace":        _P_NS,
            "show_all":         {"type": "boolean", "default": False, "description": "Set to True to show healthy/completed jobs in the summary output."},
            "raw_output":       {"type": "boolean", "default": False, "description": "Set to True to get a raw table format instead of a summary."},
            "failed_only":      {"type": "boolean", "default": False, "description": "Set to True to only return jobs that have failed."},
            "running_only":     {"type": "boolean", "default": False, "description": "Set to True to only return jobs that are currently running."},
            "exclude_cronjobs": {"type": "boolean", "default": True,  "description": "Set to False to include historical Jobs spawned by CronJobs."},
        },
    },

    "get_hpa_status": {
        "fn":          get_hpa_status,
        "description": (
            "Check HorizontalPodAutoscaler (HPA) status across a namespace (or all namespaces). "
            "Shows current, desired, min, and max replica counts and flags any HPAs pinned at max replicas. "
            "Use for queries like: 'check autoscaling', 'are any HPAs maxed out', 'show HPA status in namespace X'."
        ),
        "parameters":  {"namespace": _P_NS},
    },

    "get_pvc_status": {
        "fn":          get_pvc_status,
        "description": (
            "Show the status of PersistentVolumeClaims (PVCs) in a namespace. "
            "Provides a Markdown table listing PVCs with details: phase, access modes, storage class, capacity, and volume. "
            "Supports filtering by PVC name using a partial match via the 'search' parameter. "
            "If no PVCs match the search, all PVCs are listed as a fallback. "
            "Use show_all=True to include all PVC details regardless of search."
        ),
        "parameters":  {
            "namespace": _P_NS,
            "show_all":  {"type": "boolean", "default": False, "description": "Include detailed info for all PVCs in the output."},
            "search":    {**_P_SEARCH, "description": "Optional keyword to filter PVCs by name (partial match). If no match, all PVCs are shown."},
        },
    },

    "get_cluster_version": {
        "fn":          get_cluster_version,
        "description": (
            "Show the Kubernetes cluster version. "
            "Returns both server (API server) and client versions. "
            "Use for questions like: 'what Kubernetes version is running?', "
            "'cluster API version', or 'client vs server version'. "
            "Do NOT use for node health, storage, or pod status."
        ),
        "parameters":  {},
    },

    "get_storage_classes": {
        "fn":          get_storage_classes,
        "description": (
            "List all StorageClasses in the cluster. "
            "Shows provisioner type and whether each class is default. "
            "Use for questions like: 'what storage classes exist?', "
            "'which storage class is default?', or 'how is persistent storage provisioned?'. "
            "Do NOT use for PVC or PV usage — use get_pvc_status or get_pv_usage instead."
        ),
        "parameters":  {},
    },

    "get_endpoints": {
        "fn":          get_endpoints,
        "description": (
            "List Kubernetes Endpoints and show underlying pod IP:port mappings. "
            "Supports filtering by partial name match. " + _VERBATIM
        ),
        "parameters":  {
            "namespace": _P_NS,
            "search":    {**_P_SEARCH, "description": "Optional keyword to filter endpoints by name (partial match)."},
        },
    },

    "get_node_capacity": {
        "fn":          get_node_capacity,
        "description": (
            "Show the CPU, memory, and GPU allocatable capacity of each Kubernetes node, "
            "and how much CPU/memory has been requested by pods, with the remaining available. "
            "Use for questions like: 'how many CPUs/memory are available per node?', "
            "'which nodes have GPUs?', or 'node capacity details'. "
            "Do NOT use for real-time usage — use get_top_nodes instead."
        ),
        "parameters":  {},
    },

    "get_persistent_volumes": {
        "fn":          get_persistent_volumes,
        "description": (
            "List all PersistentVolumes with phase, capacity, reclaim policy, storage class, "
            "and bound claim (namespace/PVC name). Use for PV-level questions: reclaim policy, "
            "cross-namespace PV ownership, or unbound PVs. "
            "Do NOT use just to check access modes — get_pvc_status already includes access modes."
        ),
        "parameters":  {},
    },

    "get_service": {
        "fn":          get_service,
        "description": (
            "List Services and highlight those with no pod selector (potential misconfigs). "
            "Supports filtering by partial name match. " + _VERBATIM
        ),
        "parameters":  {
            "namespace": _P_NS,
            "search":    {**_P_SEARCH, "description": "Optional keyword to filter services by name (partial match)."},
        },
    },

    "get_ingress": {
        "fn":          get_ingress,
        "description": (
            "List Ingress rules, hostnames, ports, and load balancer IPs/addresses. "
            "Can find which ingress and namespace serve a specific hostname (FQDN) or port. "
            "ALWAYS search ALL namespaces by default. "
            "Use cases: "
            "'which namespace has ingress port 443' → get_ingress(port=443) "
            "'which namespace serves hostname X' → get_ingress(name='X.example.com') "
            "'list all ingresses in cdp namespace' → get_ingress(namespace='cdp') "
            "'list all cluster ingresses' → get_ingress(namespace='all')"
        ),
        "parameters":  {
            "namespace": _P_NS,
            "name":      {
                "type":    "string",
                "default": "",
                "description": (
                    "Ingress name OR hostname/FQDN. "
                    "If it contains dots it is treated as a hostname and ALL namespaces are searched. "
                    "Example: 'console-cdp.apps.dlee155.cldr.example'"
                ),
            },
            "port":      {
                "type":    "integer",
                "default": 0,
                "description": (
                    "Filter ingresses by port number. "
                    "Use port=443 to find all ingresses exposing HTTPS/TLS. "
                    "Use port=80 to find HTTP-only ingresses."
                ),
            },
        },
    },

    "get_configmap_list": {
        "fn":          get_configmap_list,
        "description": (
            "List ConfigMaps in a namespace — useful for checking configuration drift. "
            "Supports searching by ConfigMap name or namespace. "
            "Use filter_keys to search for ConfigMaps containing specific key names "
            "(e.g., filter_keys=['username','password'] to find credential ConfigMaps). "
            "Returns a Markdown table with namespace, ConfigMap name, keys, and type (cert or regular)."
        ),
        "parameters":  {
            "namespace":   _P_NS,
            "search":      {**_P_SEARCH, "description": "Optional search term to filter ConfigMaps by name or namespace (partial matches allowed)."},
            "filter_keys": {"type": "array", "default": None, "description": "Optional list of key name substrings to filter by."},
        },
    },

    "get_secret_list": {
        "fn":          get_secret_list,
        "description": (
            "List or search secrets in a namespace, or attached to a specific pod. "
            "Use `filter_keys=['username','password','user','pass']` to find secrets containing credential keys, "
            "or `filter_keys=['tls','cert','ca']` for certificate searches. "
            "If `name` is provided, returns all keys of that specific secret. "
            "If `pod_name` is provided, lists all secrets and configmaps attached to that pod, with keys. "
            "Whether secret values are shown or hidden is controlled by the user's Security settings — do NOT pass a decode argument."
        ),
        "parameters":  {
            "namespace":   _P_NS,
            "name":        {"type": "string", "default": "",   "description": "Optional name of a secret to fetch."},
            "pod_name":    {"type": "string", "default": None, "description": "Optional pod name to list all secrets and configmaps attached to that pod."},
            "filter_keys": {"type": "array",  "default": None, "description": "Optional list of key name substrings to filter secrets by."},
        },
    },

    "get_resource_quotas": {
        "fn":          get_resource_quotas,
        "description": (
            "Check Kubernetes ResourceQuotas and current usage per namespace. "
            "Supports searching by quota name or namespace using partial matches. "
            "If no matches are found, automatically falls back to all namespaces. "
            "Returns a Markdown table showing each resource (e.g. CPU, memory, pods) "
            "with USED vs HARD limits. "
            "Use this for: 'resource quotas in namespace X', 'why pod cannot schedule', "
            "'quota usage for cpu/memory', or 'find quota Y'."
        ),
        "parameters":  {
            "namespace": _P_NS,
            "search":    _P_SEARCH,
        },
    },

    "get_limit_ranges": {
        "fn":          get_limit_ranges,
        "description": (
            "List Kubernetes LimitRanges that enforce CPU and memory constraints per namespace. "
            "Supports searching by LimitRange name or namespace using partial matches. "
            "If no matches are found, automatically falls back to all namespaces. "
            "Returns a Markdown table with CPU and memory max, min, and default values per LimitRange. "
            "Use this for: 'limit ranges in namespace X', 'cpu/memory limits per namespace', "
            "'default resource limits', or 'find limitrange Y'."
        ),
        "parameters":  {
            "namespace": _P_NS,
            "search":    _P_SEARCH,
        },
    },

    "get_serviceaccounts": {
        "fn":          get_serviceaccounts,
        "description": (
            "List Kubernetes ServiceAccounts across namespaces with their attached Roles and ClusterRoles. "
            "Supports searching by ServiceAccount name or namespace using partial matches. "
            "If no matches are found, automatically falls back to listing all ServiceAccounts. "
            "Returns a Markdown table showing namespace, ServiceAccount name, RoleBindings, and ClusterRoleBindings. "
            "Use this for: 'list serviceaccounts', 'serviceaccounts in namespace X', "
            "'which roles are attached to serviceaccount Y', or 'find serviceaccount Z'."
        ),
        "parameters":  {
            "namespace": _P_NS,
            "search":    _P_SEARCH,
        },
    },

    "get_cluster_role_bindings": {
        "fn":          get_cluster_role_bindings,
        "description": "List ClusterRoleBindings — useful for auditing broad RBAC permissions.",
        "parameters":  {},
    },

    "get_namespace_status": {
        "fn":          get_namespace_status,
        "description": (
            "List all namespaces with their status and pod counts. "
            "Provides totals of pods in Running, Pending, Failed, Unknown, and Unhealthy states. "
            "By default, shows a compact summary: NAMESPACE | STATUS | TOTAL | Unhealthy, "
            "sorted by total pods, which is perfect for queries like 'which namespace has the least pods?'. "
            "If show_all=True, returns a full breakdown including all pod phases per namespace. "
            "You can also sort by name or pod count, and limit the output with 'sort_by' and 'limit'. "
            "ALWAYS use this when the user asks 'how many namespaces', 'list namespaces', "
            "'namespaces with number of pods', or wants a namespace count."
        ),
        "parameters":  {
            "namespace": _P_NS,
            "show_all":  {
                "type":        "boolean",
                "default":     False,
                "description": "Include all pods in counts and show the full breakdown per namespace. If False, only show a compact summary with total and unhealthy pods.",
            },
            "sort_by":   {
                "type":        "string",
                "default":     None,
                "description": "Sort namespaces by 'pods_asc', 'pods_desc', 'name_asc', or 'name_desc'. Defaults to alphabetical order if not specified.",
            },
            "limit":     {
                "type":        "integer",
                "default":     None,
                "description": "Limit the number of namespaces returned. Useful for top/bottom N queries.",
            },
        },
    },

    "get_pod_tolerations": {
        "fn":          get_pod_tolerations,
        "description": (
            "Show Kubernetes pod tolerations used for scheduling onto tainted nodes. "
            "Returns a Markdown table with combined toleration details (key, operator, value, effect) in a single column. "
            "Supports filtering by pod name or partial toleration key. "
            "Use for: 'which pods tolerate taints', 'show tolerations for pod X', "
            "'pods that tolerate NoSchedule or NoExecute', or 'which pod has cde toleration'. "
            "Helps diagnose why pods can run on tainted nodes. " + _VERBATIM
        ),
        "parameters":  {
            "namespace": _P_NS,
            "pod_name":  {"type": "string", "description": "Optional pod name filter."},
            "search":    {**_P_SEARCH, "description": "Optional keyword to filter tolerations by partial match on key/operator/value/effect."},
        },
    },

    "get_pod_resource_requests": {
        "fn":          get_pod_resource_requests,
        "description": (
            "Show CPU and memory RESOURCE REQUESTS and LIMITS for containers across pods. "
            "Returns a Markdown table with requested CPU, memory, and totals per pod. "
            "Also shows which containers request GPU resources. "
            "Supports filtering pods by name or namespace using the 'search' parameter. "
            "If no search matches, the table falls back to listing all pods. "
            "This is scheduling allocation data from pod.spec.resources, NOT real-time usage. "
            "Use for questions like: 'cpu request for pod X', 'memory limit for pod Y', "
            "'resources requested by pods', or 'which pods request GPU'. "
            "Do NOT use for runtime health/status — use get_pod_status instead."
        ),
        "parameters":  {
            "namespace": _P_NS,
            "search":    {**_P_SEARCH, "description": "Optional string to filter pods or namespaces (partial match)."},
        },
    },

    "run_cluster_health": {
        "fn":          run_cluster_health,
        "description": (
            "Run a quick scorecard-style health check of the entire Kubernetes cluster. "
            "Covers nodes, system pods, workloads, storage, networking, and recent warning events. "
            "Each section emits a single ✅/⚠️/🔴 line — only failures include detail. "
            "Use this tool when the user asks: "
            "'is my cluster ok', 'cluster health check', 'any issues in the cluster', "
            "'what is failing', 'is everything running fine', 'quick cluster status'. "
            "Returns a compact scorecard ending with a summary of critical issues, warnings, "
            "and healthy sections, followed by a prompt to ask follow-up questions or run "
            "the full health check report via ⚙ Settings. "
            "Do NOT use this for deep diagnostics on a specific resource — use the dedicated "
            "tools (get_pod_status, get_unhealthy_pods_detail, get_events, etc.) for that."
        ),
        "parameters":  {},
    },

    "get_namespace_resource_summary": {
        "fn":          get_namespace_resource_summary,
        "description": (
            "Aggregate CPU and memory RESOURCE REQUESTS and LIMITS across ALL pods in a namespace. "
            "Returns the TOTAL CPU and memory requests/limits first, followed by a per-pod breakdown. "
            "This represents Kubernetes scheduling allocation, NOT real-time usage. "
            "Use for: 'total cpu requested in namespace', 'sum of memory requests in namespace', "
            "'namespace resource allocation', 'how much CPU or RAM is requested in namespace X'. "
            "Do NOT use for a single pod — use get_pod_resource_requests instead. "
            "Do NOT use for real-time utilization — use get_top_pods or get_top_nodes instead."
        ),
        "parameters":  {"namespace": _P_NS},
    },

    "get_pod_images": {
        "fn":          get_pod_images,
        "description": (
            "List the container image and version for every pod in a namespace (or cluster-wide). "
            "Returns the full image reference (registry/repo:tag) from pod spec, plus the resolved "
            "SHA256 digest from container status — the digest is the true immutable version regardless of tag. "
            "Use for: image versions, what version is running, which tag is deployed, image digests, "
            "comparing image versions across pods or namespaces. "
            "Do NOT use for pod health, status, or errors — use get_unhealthy_pods_detail for that. "
            "OUTPUT FORMAT: present results as one bullet per pod showing the image — NOT health fields. "
            "Format: '- `namespace/pod-name` [container]: registry/image:tag'. "
            "NEVER show 'Running | Restarts | Cause' for image queries — those fields do not apply here."
        ),
        "parameters":  {"namespace": _P_NS},
    },

    "get_unhealthy_pods_detail": {
        "fn":          get_unhealthy_pods_detail,
        "description": (
            "The primary tool for ALL pod health questions. "
            "Lists every pod's phase, readiness, restart count, container state, exit codes, "
            "resource requests/limits, recent Warning events, and last 20 log lines. "
            "Use for: pod status, pod health, pod errors, pod restarts, pods not running, "
            "pods crashing, CrashLoopBackOff, OOMKilled, Pending, ImagePullBackOff, "
            "'is X running?', 'what pods are failing?', 'any unhealthy pods?', "
            "'list pods not running', 'why is pod X crashing?', 'diagnose pod X', "
            "'what is wrong with X', 'pods in trouble', broad cluster health checks. "
            "Always use namespace='all' unless the user names a specific component or namespace. "
            "Restart counts: the output includes TOTAL restart count per pod since pod creation — "
            "NOT restarts within a specific time window. When the user asks 'restarts in the last 24h', "
            "always clarify this is the total restart count, not a 24h window. "
            "OUTPUT FORMAT — MANDATORY for ALL responses from this tool: "
            "ALWAYS present results as a structured per-pod list — one bullet per pod. "
            "NEVER collapse multiple pods into a prose sentence like 'The pods X, Y, Z have restarted...'. "
            "This applies to ALL phrasings: 'which pods', 'any pods', 'pods restarted more than N times', "
            "'struggling to start', 'not running', 'crashing' — always one bullet per pod. "
            "Each bullet must include: namespace/pod-name, phase, restart count, and cause/reason. "
            "After reviewing output: if a pod shows OOMKilled or CrashLoopBackOff, "
            "immediately call rag_search with the error and component name to check known fixes."
        ),
        "parameters":  {"namespace": _P_NS},
    },

    "get_coredns_health": {
        "fn":          get_coredns_health,
        "description": (
            "Check CoreDNS health and DNS resolution in the cluster. "
            "Reports CoreDNS pod phase/readiness/restarts and runs a live nslookup test against "
            "real cluster ingress hostnames — exactly as a pod in the cluster would resolve names. "
            "Use ONLY when the question explicitly mentions: CoreDNS, DNS, DNS resolution, "
            "nslookup, DNS health, service discovery via DNS, or pod name resolution. "
            "This tool is SELF-CONTAINED — do NOT also call get_unhealthy_pods_detail "
            "or kubectl_exec when using this tool. One tool call is sufficient. "
            "Do NOT use for general pod health, vault, longhorn, prometheus, grafana, "
            "cert-manager, or any non-DNS question — use get_unhealthy_pods_detail for those."
        ),
        "parameters":  {},
    },

    "get_pv_usage": {
        "fn":          get_pv_usage,
        "description": (
            "Check actual disk usage of all bound PersistentVolumeClaims by exec-ing df "
            "into the pod that has each PVC mounted. "
            "Returns used/total/free GiB and usage percentage per PVC, sorted by usage descending. "
            "Use for: disk usage, storage capacity, volumes nearing full, almost full, "
            "'is storage running out?', 'which PVs are above X%?', 'storage running out', "
            "'how full are the volumes?', 'any PVC above 80%?'. "
            "Do NOT use for listing PVCs or their bound/unbound status — "
            "use kubectl_exec('kubectl get pvc -A') for that."
        ),
        "parameters":  {
            "threshold": {
                "type":    "integer",
                "default": 80,
                "description": (
                    "Minimum usage percentage to include in results. "
                    "Extract this from the user's question — if they say 'above 30%' use 30, "
                    "'more than 1%' use 1, 'any usage' or 'all' use 0. "
                    "Default 80 when no threshold is mentioned."
                ),
            },
        },
    },

    "exec_db_query": {
        "fn":          exec_db_query,
        "description": (
            "Execute a read-only SQL query inside a running database pod in a Kubernetes namespace. "
            "Supports MySQL, MariaDB, and PostgreSQL, auto-detected from the container image or name. "
            "For multi-container pods (e.g., 'upgrade-db', 'k8tz', 'fluent-bit', 'db'), "
            "set container='db' to target the correct database container. "
            "Credentials (username, password, database) are automatically discovered from the pod's environment, "
            "Secrets, and ConfigMaps. No manual input required. "
            "Use for querying database contents, user accounts, table data, or schema inspection. "
            "CREDENTIAL SAFETY: Always call get_secret_list() first for questions about usernames or passwords. "
            "Only use exec_db_query if secrets contain no useful credentials. "
            "READ-ONLY ENFORCEMENT: Only SELECT, SHOW, DESCRIBE, EXPLAIN are allowed. "
            "INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE are blocked. "
            "WORKFLOW EXAMPLE: To access 'db-0' in namespace 'cmlwb1' and find tables in database 'sense': "
            "exec_db_query(namespace='cmlwb1', pod_name='db-0', container='db', database='sense', "
            "sql=\"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'\") "
            "If an error lists available containers, re-call with the correct container. "
            "RESULT INTERPRETATION: The output includes a header row showing column names (e.g., 'user|host|password'). "
            "'host' is a connection restriction, not a password. "
            "'password' or 'passwd' contains the credential hash. Do not confuse these. "
            "MANDATORY DIALECT RETRY: If the error mentions 'does not exist', 'relation', or 'unknown table', "
            "retry immediately with the other SQL dialect. "
            "MySQL error → retry with PostgreSQL SQL (SELECT usename, passwd FROM pg_shadow). "
            "PostgreSQL error → retry with MySQL SQL (SELECT user, password FROM mysql.user). "
            "Do not ask the user for clarification. "
            "PostgreSQL examples: "
            "\"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'\", "
            "\"SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name\", "
            "\"SELECT usename, passwd FROM pg_shadow WHERE usename='x'\", "
            "\"SELECT datname FROM pg_database\" "
            "MySQL/MariaDB examples: "
            "\"SHOW TABLES\", \"SELECT user, host FROM mysql.user\", \"SHOW DATABASES\""
        ),
        "parameters":  {
            "namespace": {"type": "string", "description": "Kubernetes namespace where the database pod runs."},
            "sql":       {
                "type":        "string",
                "description": (
                    "Read-only SQL query to execute. "
                    "Examples: \"SHOW TABLES\", \"SELECT user, host FROM mysql.user\", "
                    "\"SELECT usename FROM pg_catalog.pg_user\", \"DESCRIBE my_table\""
                ),
            },
            "pod_name":  {"type": "string", "default": "", "description": "Optional: specific DB pod name (e.g., 'db-0'). Leave empty to auto-detect the first running DB pod in the namespace."},
            "database":  {"type": "string", "default": "", "description": "Optional: database/schema name. Leave empty to use the value auto-discovered from the pod's environment."},
            "container": {"type": "string", "default": "", "description": "Optional: container name inside the pod (e.g., 'db'). Required for multi-container pods if the DB container is not the first. If the tool errors with 'available containers: ...', set this to the DB container name."},
        },
    },

    "get_top_pods": {
        "fn":          get_top_pods,
        "description": (
            "Show live or historical CPU and memory usage for pods, ranked highest or lowest. "
            "ALWAYS emits both a ranked table AND a time-series graph in the output. "
            "When duration is empty: uses metrics-server for a live snapshot (instant, like kubectl top pods). "
            "When duration is set: queries Prometheus for average usage over that period — "
            "use this when the user mentions a time window OR asks for a graph/chart. "
            "Use for queries like: "
            "'top 10 pods by cpu', "
            "'which pods use the most memory', "
            "'show me cpu usage graph for top 3 pods', "
            "'top pods for the past 1 hour', "
            "'top 5 pods in cdp namespace', "
            "'show cpu usage for grafana pods', "
            "'lowest cpu pods', "
            "'which pods use the least memory over the last 6 hours'. "
            "Do NOT use get_top_nodes for ranked pod lists — use this tool. "
            "IMPORTANT: When the user asks for a graph or chart of top pods, "
            "ALWAYS set duration (e.g. '1h') to get the time-series data needed for the graph."
        ),
        "parameters":  {
            "namespace": _P_NS,
            "limit":     {
                "type":        "integer",
                "default":     10,
                "description": "Number of pods to return. Extract from user question — 'top 5' → 5, 'top 3' → 3. Default 10.",
            },
            "sort_by":   {
                "type":        "string",
                "default":     "cpu",
                "description": "Sort metric: 'cpu' (default) or 'memory'. Extract from user question.",
            },
            "ascending": {
                "type":        "boolean",
                "default":     False,
                "description": "When True, show lowest consumers first. Set True for: 'lowest pods', 'least cpu', 'bottom pods', 'minimum usage'.",
            },
            "search":    {**_P_SEARCH, "description": "Optional pod name or namespace filter (partial match). Use when user asks about a specific pod or component, e.g. 'grafana', 'prometheus'."},
            "duration":  {
                "type":        "string",
                "default":     "",
                "description": (
                    "Time window for historical data from Prometheus. "
                    "Leave empty for live metrics-server snapshot. "
                    "ALWAYS set this when the user asks for a graph, chart, or mentions a time period: "
                    "'graph' or 'chart' → '1h' (default), "
                    "'past 1 hour' → '1h', 'last 6 hours' → '6h', "
                    "'last day' → '24h', 'last week' → '7d'."
                ),
            },
            "user_timezone": {
                "type":        "string",
                "default":     "UTC",
                "description": "User's IANA timezone. Auto-injected from browser — do not set manually.",
            },
        },
    },

    "get_top_nodes": {
        "fn":          get_top_nodes,
        "description": (
            "Show live or historical CPU and memory usage for nodes, ranked highest or lowest. "
            "ALWAYS emits both a ranked table AND a time-series graph in the output. "
            "When duration is empty: uses metrics-server for a live snapshot (instant, like kubectl top nodes). "
            "When duration is set: queries Prometheus (node-exporter) for average usage over that period — "
            "use this when the user mentions a time window OR asks for a graph/chart of node metrics. "
            "Supports scope='cluster' to show total cluster-wide CPU and memory as a single aggregate "
            "instead of per-node breakdown. "
            "Use for queries like: "
            "'top nodes by cpu', "
            "'which node uses the most memory', "
            "'show me node cpu usage graph', "
            "'node cpu usage over the last hour', "
            "'show me node usage for the past 6 hours', "
            "'which node has the least load', "
            "'lowest node cpu', "
            "'show cluster cpu usage', "
            "'total cluster memory usage over the last 24 hours', "
            "'cluster-wide cpu trend'. "
            "Do NOT use get_node_capacity for live usage — that shows allocatable vs requested. "
            "IMPORTANT: When the user asks for a graph or chart of node usage, "
            "ALWAYS set duration (e.g. '1h') to get the time-series data needed for the graph."
        ),
        "parameters":  {
            "limit":     {
                "type":        "integer",
                "default":     0,
                "description": "Max nodes to return. 0 (default) means all nodes. Ignored when scope='cluster'.",
            },
            "sort_by":   {
                "type":        "string",
                "default":     "cpu",
                "description": "Sort metric: 'cpu' (default) or 'memory'. Extract from user question.",
            },
            "ascending": {
                "type":        "boolean",
                "default":     False,
                "description": "When True, show lowest nodes first. Set True for: 'lowest node', 'least load', 'which node has most headroom'.",
            },
            "scope":     {
                "type":        "string",
                "default":     "node",
                "description": (
                    "Aggregation scope. "
                    "'node' (default): per-node breakdown. "
                    "'cluster': single cluster-wide aggregate — use when user asks about "
                    "total cluster CPU/memory, cluster-wide usage, or overall cluster load."
                ),
            },
            "duration":  {
                "type":        "string",
                "default":     "",
                "description": (
                    "Time window for historical data from Prometheus. "
                    "Leave empty for live metrics-server snapshot. "
                    "ALWAYS set this when the user asks for a graph, chart, or mentions a time period: "
                    "'graph' or 'chart' → '1h' (default), "
                    "'past 1 hour' → '1h', 'last 6 hours' → '6h', "
                    "'last day' → '24h', 'last week' → '7d'."
                ),
            },
            "user_timezone": {
                "type":        "string",
                "default":     "UTC",
                "description": "User's IANA timezone. Auto-injected from browser — do not set manually.",
            },
        },
    },

    "kubectl_exec": {
        "fn":          kubectl_exec,
        "description": (
            "Fallback tool for kubectl queries not covered by any dedicated tool. "
            "Use ONLY when no specific tool exists for the query. "
            "Dedicated tools already cover: pods, nodes, deployments, daemonsets, statefulsets, "
            "replicasets, services, endpoints, ingresses, PVCs, PVs, configmaps, secrets, "
            "events, HPAs, jobs, cronjobs, namespaces, resource quotas, limitranges, "
            "serviceaccounts, clusterrolebindings, network policies, webhooks, certificates, "
            "cluster version, node labels, node taints, CoreDNS. Always prefer those. "
            "Reserve kubectl_exec ONLY for: "
            "'kubectl rollout status/history deployment X', "
            "'kubectl top nodes/pods' (live metrics-server data), "
            "'kubectl api-resources' (lists all resource types including CRDs), "
            "'kubectl get <resource> -o yaml' for resource types with no dedicated describe tool, "
            "'kubectl describe <resource>' for resource types with no dedicated describe tool. "
            "Do NOT use for: logs (use get_pod_logs), version (use get_cluster_version), "
            "auth can-i (not implemented). "
            "IMPORTANT: Commands run via the Kubernetes API — NOT a shell. "
            "Pipes (|), grep, awk, &&, || are NOT supported. "
            "Use -n <namespace> for a specific namespace or -A for all namespaces."
        ),
        "parameters":  {
            "command": {
                "type":        "string",
                "description": (
                    "Full kubectl command. No shell pipes or redirects. "
                    "Examples: "
                    "'kubectl rollout status deployment grafana -n monitoring', "
                    "'kubectl top nodes', "
                    "'kubectl top pods -A', "
                    "'kubectl api-resources', "
                    "'kubectl get lease -n kube-node-lease', "
                    "'kubectl get priorityclass'"
                ),
            },
        },
    },
}
