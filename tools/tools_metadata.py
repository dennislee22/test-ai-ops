from tools.tools_k8s import (
    get_pod_status, get_pod_logs, describe_pod, get_node_info, get_gpu_info,
    get_node_labels, get_node_taints, get_events, get_deployment, describe_sc,
    get_daemonset, get_statefulset, get_job_status, get_hpa_status, describe_pvc,
    get_pvc_status, get_cluster_version, get_storage_classes, get_endpoints,
    get_node_capacity, get_persistent_volumes, get_service, get_ingress, describe_pv,
    get_configmap_list, get_secret_list, get_resource_quotas, get_limit_ranges,
    get_serviceaccounts, get_cluster_role_bindings, get_namespace_status,
    get_pod_tolerations, get_pod_resource_requests, run_cluster_health, get_replicaset,
    get_namespace_resource_summary, get_pod_images, get_unhealthy_pods_detail,
    get_coredns_health, get_pv_usage, find_resource, get_pod_containers_resources,
    query_prometheus_metrics, kubectl_exec, exec_db_query, get_pod_storage,
)

K8S_TOOL_METADATA: dict = {
    "find_resource": {
        "fn":          find_resource,
        "description": (
            "Find and locate Kubernetes resources by NAME (partial match). "
            "This is the PRIMARY tool for searching resources when the user mentions a specific name. "
            "Especially useful for locating pods and answering WHERE they are running. "

            "Use this tool when the query includes a resource name, such as: "
            "'where is grafana pod', "
            "'find pod nginx', "
            "'search for redis', "
            "'which node is pod X running on', "
            "'show me grafana', "
            "'locate service Y'. "

            "Returns matching resources with namespace, name, node (for pods), and status. "
            "Supports pods, services, ingresses, and PVCs. "

            "If a name or partial name is provided, ALWAYS use this tool instead of get_pod_status. "
            "If no matches are found, falls back to listing all resources of the specified type."
        ),
        "parameters":  {
            "name_substring": {
                "type": "string",
                "description": "Partial name of the resource to search for (e.g., 'grafana', 'nginx')."
            },
            "resource_type":  {
                "type": "string",
                "default": None,
                "description": "Optional resource type to filter (pod, svc/service, ingress, pvc). Defaults to all supported types."
            },
            "namespace":      {
                "type": "string",
                "default": None,
                "description": "Optional namespace to restrict the search. Defaults to all namespaces."
            },
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
            "namespace": {"type": "string", "default": "all", "description": "Namespace to query. Defaults to all namespaces — only override when the user explicitly names a namespace."},
            "search":    {"type": "string", "description": "Partial pod name or namespace to filter results. Leave empty to show all pods."}
        },
    },

    "get_pod_status": {
        "fn": get_pod_status,
        "description": (
            "List and check runtime STATUS of Kubernetes pods. "
            "This is the PRIMARY tool for listing pods in a namespace or across the cluster. "

            "Use this tool for queries like: "
            "'list pods', "
            "'list all pods in namespace X', "
            "'show pods in longhorn-system', "
            "'which pods are unhealthy', "
            "'search pods by partial name'. "

            "Supports namespace filtering and search by partial pod name. "
            "Returns matching pods only (no hidden fallback). "
            "If no pods match the search, the tool returns a 'no results' message. "

            "Use show_all=true to include all pods instead of filtered results. "

            "Shows pod phase (Running/Pending/Failed/Unknown), container readiness, restart counts, "
            "and unhealthy conditions. "

            "Do NOT use this tool for detailed per-container resource requests or limits — "
            "use get_pod_resource_requests for that purpose."
        ),
        "parameters": {
            "namespace": {
                "type": "string",
                "default": "all",
                "description": "Namespace to query. Defaults to 'all' namespaces — set when user specifies a namespace."
            },
            "search": {
                "type": "string",
                "description": "Optional search string to match pod names (partial match only)."
            },
            "show_all": {
                "type": "boolean",
                "default": False,
                "description": "Set true to include ALL pods (otherwise returns filtered results)."
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
            "namespace": {"type": "string", "default": "all",
                          "description": "Namespace to query. Defaults to 'all' namespaces — override only when explicitly specified."},
            "search":    {"type": "string", "default": None,
                          "description": "Optional search term to filter pods by name or namespace (partial matches allowed)."},
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
            "search":     {"type": "string", "description": "Partial pod name to search for (e.g., 'prometheus-server')."},
            "namespace":  {"type": "string", "default": "all", "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."},
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
            "pod_name":   {"type": "string",  "description": "Exact pod name to fetch details for. Optional if using 'search'."},
            "search":     {"type": "string",  "description": "Partial pod name to search for across namespaces. Optional if using 'pod_name'."},
            "namespace":  {"type": "string",  "default": "all", "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."},
            "show_yaml":  {"type": "boolean", "default": False, "description": "true = output full pod YAML instead of human-readable describe-style summary."},
        },
    },
    
    "get_node_info": {
        "fn":          get_node_info,
        "description": (
            "Check Kubernetes node health, resources, and scheduling status. "
            "Returns a Markdown table with columns: NODE, ROLES, STATUS (including Ready/NotReady and Cordon/SchedulingDisabled), CPU, RAM (Gi), GPU. "
            "Supports filtering for a specific node by partial name match. "
            "Use for questions like: 'are nodes healthy', 'is ecs-w-01 cordoned', or 'why are pods pending'. "
            "CRITICAL: You must output the exact Markdown table returned by this tool. Do NOT modify the formatting, summarize the data, or remove the table headers."
        ),
        "parameters":  {
            "node_name": {
                "type": "string",
                "default": None,
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
            "CRITICAL: You must output the exact Markdown table returned by this tool. Do NOT modify the formatting, summarize the data, or remove the table headers."
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
            "CRITICAL: You must output the exact text returned by this tool. Use bulleted list. Do NOT try to convert this list into a table, modify the formatting, summarize the data, or omit ANY labels."
        ),
        "parameters":  {
            "search": {
                "type": "string", 
                "description": "Optional keyword to filter by node name OR label content. Leave empty (or null) to list ALL nodes and ALL their labels."
            },
        },
    },

    "get_node_taints": {
        "fn":          get_node_taints,
        "description": (
            "Show all taints for Kubernetes nodes in the cluster. "
            "Returns key/value/effect strings describing node taints that restrict pod scheduling. "
            "You can optionally filter taints by a keyword using the `search` parameter. "
            "Do NOT pass a node_name — this tool only supports search by taint content. "
            "Use for questions like: "
            "'which nodes have taints?', "
            "'show nodes tainted with GPU?', "
            "'which nodes prevent pods from scheduling?', "
            "'find nodes with a specific taint keyword like cde'."
        ),
        "parameters":  {
            "search": {"type": "string", "description": "Optional keyword to filter taints (e.g., 'cde')."},
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
            "name":       {"type": "string", "description": "Name of the StorageClass to describe."},
            "show_yaml":  {"type": "boolean", "default": False,
                           "description": "If true, output full YAML of the StorageClass instead of human-readable summary."},
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
            "name":       {"type": "string", "description": "Name of the PVC to describe."},
            "namespace":  {"type": "string", "default": "all",
                           "description": "Namespace of the PVC. Defaults to 'all' — only override if a specific namespace is needed."},
            "show_yaml":  {"type": "boolean", "default": False,
                           "description": "If true, output full YAML of the PVC instead of human-readable summary."},
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
            "show_yaml": {"type": "boolean", "default": False, "description": "If true, returns the full PV object as YAML."},
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
            "namespace": {"type": "string", "default": "all",
                          "description": "Namespace to query. Defaults to 'all' namespaces — override only when explicitly specified."},
            "search":    {"type": "string", "default": None,
                          "description": "Optional search term to filter events by pod, namespace, object, or message."},
            "type":      {"type": "string", "default": "All",
                          "description": "Event type to fetch: 'Warning', 'Normal', or 'All' (default)."},
        },
    },

    "get_deployment": {
        "fn":          get_deployment,
        "description": (
            "List Deployments and their health status (desired, ready, available pods). "
            "Supports filtering by partial name match. "
            "CRITICAL: You must output the exact Markdown table returned by this tool. Do NOT modify the formatting, summarize the data, or remove the table headers."
        ),
        "parameters": {
            "namespace": {
                "type": "string", 
                "default": "all", 
                "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."
            },
            "search": {
                "type": "string", 
                "description": "Optional keyword to filter deployments by name (partial match)."
            }
        },
    },
    
    "get_statefulset": {
        "fn":          get_statefulset,
        "description": (
            "List StatefulSets and their health status (desired vs ready pods). "
            "CRITICAL: You must output the exact Markdown table returned by this tool. Do NOT modify the formatting, summarize the data, or remove the table headers."
        ),
        "parameters": {
            "namespace": {
                "type": "string", 
                "default": "all", 
                "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."
            }
        },
    },

    "get_daemonset": {
        "fn":          get_daemonset,
        "description": (
            "List DaemonSets and their health status (desired, ready, available pods). "
            "CRITICAL: You must output the exact Markdown table returned by this tool. Do NOT modify the formatting, summarize the data, or remove the table headers."
        ),
        "parameters": {
            "namespace": {
                "type": "string", 
                "default": "all", 
                "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."
            }
        },
    },

    "get_replicaset": {
        "fn":          get_replicaset,
        "description": (
            "List ReplicaSets and their health status (desired, ready, available pods). "
            "CRITICAL: You must output the exact Markdown table returned by this tool. Do NOT modify the formatting, summarize the data, or remove the table headers."
        ),
        "parameters": {
            "namespace": {
                "type": "string", 
                "default": "all", 
                "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."
            }
        },
    },
    
    "get_job_status": {
        "fn":          get_job_status,
        "description": (
            "Check Kubernetes Jobs in a namespace or across all namespaces. "
            "By default, only FAILED jobs are returned (active jobs with failures). "
            "Set show_all=true to include all jobs including complete ones. "
            "Set failed_only=true to return only failed jobs. "
            "Set running_only=true to return only currently active/running jobs. "
            "Set raw_output=true for kubectl-style table output. "
            "NAMESPACE RULE — CRITICAL: if the user does not name a specific namespace, "
            "ALWAYS use namespace='all'. Only scope to a specific namespace when the user explicitly names one."
        ),
        "parameters":  {
            "namespace":    {"type": "string", "default": "all", "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."},
            "show_all":     {"type": "boolean", "default": False, "description": "Include all jobs including healthy/complete ones."},
            "failed_only":  {"type": "boolean", "default": False, "description": "Return only failed jobs (ignores running or complete jobs)."},
            "running_only": {"type": "boolean", "default": False, "description": "Return only currently active/running jobs (status.active > 0)."},
            "raw_output":   {"type": "boolean", "default": False, "description": "Return kubectl-style table output."},
        },
    },
    
    "get_hpa_status": {
        "fn":          get_hpa_status,
        "description": "Check HorizontalPodAutoscaler targets and whether any are pinned at max replicas.",
        "parameters":  {"namespace": {"type": "string", "default": "all", "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."}},
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
            "namespace": {"type": "string", "default": "all",
                          "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."},
            "show_all":  {"type": "boolean", "default": False,
                          "description": "Include detailed info for all PVCs in the output."},
            "search":    {"type": "string",
                          "description": "Optional keyword to filter PVCs by name (partial match). If no match, all PVCs are shown."}
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
            "Supports filtering by partial name match. "
            "CRITICAL: You must output the exact Markdown table returned by this tool. Do NOT modify the formatting, summarize the data, or remove the table headers."
        ),
        "parameters": {
            "namespace": {
                "type": "string", 
                "default": "all", 
                "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."
            },
            "search": {
                "type": "string", 
                "description": "Optional keyword to filter endpoints by name (partial match)."
            }
        },
    },
    
    "get_node_capacity": {
        "fn":          get_node_capacity,
        "description": (
            "Show the CPU, memory, and GPU allocatable capacity of each Kubernetes node, "
            "and how much CPU/memory has been requested by pods, with the remaining available. "
            "Use for questions like: 'how many CPUs/memory are available per node?', "
            "'which nodes have GPUs?', or 'node capacity details'. "
            "Do NOT use for real-time usage — use get_node_health or query_prometheus_metrics instead."
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
            "Supports filtering by partial name match. "
            "CRITICAL: You must output the exact Markdown table returned by this tool. Do NOT modify the formatting, summarize the data, or remove the table headers."
        ),
        "parameters": {
            "namespace": {
                "type": "string", 
                "default": "all", 
                "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."
            },
            "search": {
                "type": "string", 
                "description": "Optional keyword to filter services by name (partial match)."
            }
        },
    },
    
    "get_ingress": {
        "fn":          get_ingress,
        "description": (
            "List Ingress rules, hostnames, ports, and load balancer IPs/addresses. "
            "Can find which ingress and namespace serve a specific hostname (FQDN) or port. "
            "ALWAYS search ALL namespaces by default. "
            "Use cases: "
            "'which namespace has ingress port 443' → get_ingress_status(port=443) "
            "'which namespace serves hostname X' → get_ingress_status(name='X.example.com') "
            "'list all ingresses in cdp namespace' → get_ingress_status(namespace='cdp') "
            "'list all cluster ingresses' → get_ingress_status(namespace='all')"
        ),
        "parameters":  {
            "namespace": {"type": "string", "default": "all", "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."},
            "name":      {"type": "string", "default": "",
                          "description": (
                              "Ingress name OR hostname/FQDN. "
                              "If it contains dots it is treated as a hostname and ALL namespaces are searched. "
                              "Example: 'console-cdp.apps.dlee155.cldr.example'"
                          )},
            "port":      {"type": "integer", "default": 0,
                          "description": (
                              "Filter ingresses by port number. "
                              "Use port=443 to find all ingresses exposing HTTPS/TLS. "
                              "Use port=80 to find HTTP-only ingresses."
                          )},
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
            "namespace":   {"type": "string", "default": "all",
                            "description": "Namespace to query. Defaults to 'all' namespaces — only override when explicitly specified."},
            "search":      {"type": "string", "default": None,
                            "description": "Optional search term to filter ConfigMaps by name or namespace (partial matches allowed)."},
            "filter_keys": {"type": "array",  "default": None,
                            "description": "Optional list of key name substrings to filter by."},
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
            "namespace":   {"type": "string", "default": "all",
                            "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."},
            "name":        {"type": "string", "default": "",
                            "description": "Optional name of a secret to fetch."},
            "pod_name":    {"type": "string", "default": None,
                            "description": "Optional pod name to list all secrets and configmaps attached to that pod."},
            "filter_keys": {"type": "array",  "default": None,
                            "description": "Optional list of key name substrings to filter secrets by."},
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
            "namespace": {
                "type": "string",
                "default": "all",
                "description": "Namespace to query. Defaults to 'all' namespaces — override only when explicitly specified."
            },
            "search": {
                "type": "string",
                "default": None,
                "description": "Optional search term to filter quotas by name or namespace (partial matches allowed)."
            },
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
            "namespace": {
                "type": "string",
                "default": "all",
                "description": "Namespace to query. Defaults to 'all' namespaces — override only when explicitly specified."
            },
            "search": {
                "type": "string",
                "default": None,
                "description": "Optional search term to filter LimitRanges by name or namespace (partial matches allowed)."
            },
        },
    },

    "get_servicesaccounts": {
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
            "namespace": {
                "type": "string",
                "default": "all",
                "description": "Namespace to query. Defaults to 'all' namespaces — override only when explicitly specified."
            },
            "search": {
                "type": "string",
                "default": None,
                "description": "Optional search term to filter ServiceAccounts by name or namespace (partial matches allowed)."
            },
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
            "namespace": {
                "type": "string",
                "default": "all",
                "description": (
                    "Namespace to query. Defaults to 'all' namespaces — only override when "
                    "the user explicitly names a namespace."
                )
            },
            "show_all": {
                "type": "boolean",
                "default": False,
                "description": (
                    "Include all pods in counts and show the full breakdown per namespace. "
                    "If False, only show a compact summary with total and unhealthy pods."
                )
            },
            "sort_by": {
                "type": "string",
                "default": None,
                "description": (
                    "Sort namespaces by 'pods_asc', 'pods_desc', 'name_asc', or 'name_desc'. "
                    "Defaults to alphabetical order if not specified."
                )
            },
            "limit": {
                "type": "integer",
                "default": None,
                "description": (
                    "Limit the number of namespaces returned. Useful for top/bottom N queries."
                )
            }
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
            "Helps diagnose why pods can run on tainted nodes. "
            "CRITICAL: You must output the exact Markdown table returned by this tool. Do NOT modify the formatting, summarize the data, or remove the table headers."
        ),
        "parameters":  {
            "namespace": {"type": "string", "default": "all", "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."},
            "pod_name":  {"type": "string", "description": "Optional pod name filter."},
            "search":    {"type": "string", "description": "Optional keyword to filter tolerations by partial match on key/operator/value/effect."},
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
            "namespace": {"type": "string", "default": "all",
                          "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."},
            "search":    {"type": "string",
                          "description": "Optional string to filter pods or namespaces (partial match)."}
        },
    },

    "run_cluster_health": {
        "fn":          run_cluster_health,
        "description": (
            "Summarize the overall health of the Kubernetes cluster by aggregating issues "
            "across namespaces, pods, nodes, storage, ingresses, and critical system components. "
            "Reports counts of Critical and Moderate issues, resource usage, and summaries per component. "
            "Includes CPU/memory requested vs. capacity, pods in non-Running phases, "
            "unbound PVCs, failed ingresses, unhealthy system DaemonSets, CoreDNS, kube-proxy, "
            "and any namespace-level quota breaches. "
            "Useful for: 'cluster health check', 'what is failing in the cluster', "
            "'which components have issues', 'summary of resource usage'."
        ),
        "parameters":  {
            "namespace":  {"type": "string", "default": "all", "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."},
            "show_all":   {"type": "boolean", "default": False, "description": "Include all pods, namespaces, PVCs, and system components in counts, even if healthy."},
            "raw_output": {"type": "boolean", "default": False, "description": "Return detailed per-object output for PVCs, ingresses, and system components instead of a summarized report."}
        },
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
            "Do NOT use for real-time utilization — use query_prometheus_metrics instead."
        ),
        "parameters":  {
            "namespace": {"type": "string", "default": "all", "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."}
        },
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
        "parameters": {
            "namespace": {"type": "string", "default": "all", "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."},
        },
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
        "parameters": {
            "namespace": {"type": "string", "default": "all", "description": "Namespace to query. Defaults to 'all' namespaces — only override when the user explicitly names a namespace."},
        },
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
        "parameters": {},
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
        "parameters": {
            "threshold": {
                "type": "integer",
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

    "query_prometheus_metrics": {
        "fn":          query_prometheus_metrics,
        "description": (
            "Query Prometheus for real-time usage metrics and render an inline time-series chart. "
            "Use for any question about actual usage, load, consumption, or trends — regardless of "
            "whether the user says 'nodes' or 'pods'. "
            "IMPORTANT: this cluster has no node-exporter installed, so per-node CPU/memory consumption "
            "is not available. All metrics are pod-level. When a user asks for node usage, use this tool "
            "and note that pod-level data is the closest available proxy. "
            "Available metrics: 'cpu'/'pod_cpu' (pod CPU in millicores), 'memory'/'pod_memory' "
            "(pod memory in MiB), 'cluster_cpu', 'cluster_memory'. "
            "Disk I/O and network metrics are unavailable (no node-exporter). "
            "duration sets the time window (e.g. '1h', '6h', '24h', '7d'). "
            "namespace filters to a specific namespace (leave empty for all)."
        ),
        "parameters": {
            "metric": {
                "type": "string",
                "default": "cpu",
                "description": (
                    "Metric shortcut or raw PromQL. Shortcuts: cpu, memory, pod_cpu, pod_memory, "
                    "disk_io, network_in, network_out. Extract from user question — "
                    "'CPU usage' → 'cpu', 'memory' → 'memory', 'pod memory' → 'pod_memory', "
                    "'disk I/O' or 'PVC I/O' → 'disk_io'. Default: 'cpu'."
                ),
            },
            "duration": {
                "type": "string",
                "default": "1h",
                "description": (
                    "Time window to query. Extract from user question — "
                    "'last hour' → '1h', 'last 6 hours' → '6h', 'today' / 'last 24 hours' → '24h', "
                    "'last week' → '7d'. Default: '1h'."
                ),
            },
            "step": {
                "type": "string",
                "default": "60s",
                "description": (
                    "Query resolution. Use '60s' for ≤6h windows, '5m' for ≤24h, '15m' for >24h. "
                    "Auto-scale: if duration is >24h, use '15m'; if >6h, use '5m'; else '60s'."
                ),
            },
            "namespace": {
                "type": "string",
                "default": "",
                "description": (
                    "Filter results to a specific Kubernetes namespace. "
                    "Extract from user question — 'in cdp namespace' → 'cdp', "
                    "'in the vault namespace' → 'vault'. "
                    "Leave EMPTY (do not pass anything) when the question is about all namespaces, "
                    "all nodes, or does not mention a specific namespace. "
                    "NEVER pass 'all', 'any', 'cluster', or similar — use empty string instead."
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
        "parameters": {
            "namespace": {
                "type": "string",
                "description": "Kubernetes namespace where the database pod runs."
            },
            "sql": {
                "type": "string",
                "description": (
                    "Read-only SQL query to execute. "
                    "Examples: \"SHOW TABLES\", \"SELECT user, host FROM mysql.user\", "
                    "\"SELECT usename FROM pg_catalog.pg_user\", \"DESCRIBE my_table\""
                )
            },
            "pod_name": {
                "type": "string",
                "default": "",
                "description": (
                    "Optional: specific DB pod name (e.g., 'db-0'). "
                    "Leave empty to auto-detect the first running DB pod in the namespace."
                )
            },
            "database": {
                "type": "string",
                "default": "",
                "description": (
                    "Optional: database/schema name. "
                    "Leave empty to use the value auto-discovered from the pod's environment."
                )
            },
            "container": {
                "type": "string",
                "default": "",
                "description": (
                    "Optional: container name inside the pod (e.g., 'db'). "
                    "Required for multi-container pods if the DB container is not the first. "
                    "If the tool errors with 'available containers: ...', set this to the DB container name."
                )
            }
        },
    },
    
    "kubectl_exec": {
        "fn":          kubectl_exec,
        "description": (
            "Execute a read-only kubectl command against the cluster. Use this as the general-purpose "
            "tool for any cluster state query not covered by a more specific tool. "
            "IMPORTANT: Commands run via the Kubernetes API — NOT a shell. "
            "Pipes (|), grep, awk, &&, || are NOT supported. Use -n <namespace> or -A for all namespaces. "
            "Use for the following (with example commands): "
            "• Node health/status/conditions: 'kubectl get nodes -o wide' or 'kubectl describe node <name>' "
            "• Pod location ('where is X?', 'which node is X on?', 'find X pod'): "
            "  ALWAYS use 'kubectl get pod -A -o wide' — never assume the namespace. "
            "  The -A flag searches all namespaces so grafana/vault/etc will be found regardless of namespace. "
            "• Deployments/replicas: 'kubectl get deployments -n <ns>' "
            "• ReplicaSets: 'kubectl get replicasets -n <ns>' "
            "• DaemonSets: 'kubectl get daemonsets -A' "
            "• StatefulSets: 'kubectl get statefulsets -A' "
            "• Jobs/CronJobs: 'kubectl get jobs -A' or 'kubectl get cronjobs -A' "
            "• HPA/autoscaling: 'kubectl get hpa -A' "
            "• Services/endpoints: 'kubectl get services -A' or 'kubectl get endpoints -n <ns>' "
            "• Ingress: 'kubectl get ingress -A' "
            "• ConfigMaps: 'kubectl get configmaps -n <ns>' "
            "• Secrets (names only, not values): 'kubectl get secrets -n <ns>' "
            "• RBAC: 'kubectl get clusterrolebindings' or 'kubectl get rolebindings -n <ns>' "
            "• ServiceAccounts: 'kubectl get serviceaccounts -n <ns>' "
            "• Namespaces: 'kubectl get namespaces' "
            "• Resource quotas: 'kubectl get resourcequota -n <ns>' "
            "• LimitRanges: 'kubectl get limitrange -n <ns>' "
            "• PVCs (list/status): 'kubectl get pvc -A' "
            "• PVs: 'kubectl get pv' "
            "• Events: 'kubectl get events -n <ns> --sort-by=.lastTimestamp' "
            "• GPU info: 'kubectl describe nodes | grep -A5 nvidia' — NOTE: grep not supported, "
            "  use 'kubectl describe node <nodename>' instead "
            "• Cluster version: 'kubectl version' "
            "• API resources: 'kubectl api-resources' "
            "Resolve namespace aliases before calling: "
            "vault → vault-system, longhorn → longhorn-system, rancher/cattle → cattle-system, "
            "cert-manager/cert → cert-manager, coredns/dns → kube-system, "
            "prometheus/grafana/alertmanager/monitoring → monitoring."
        ),
        "parameters": {
            "command": {
                "type": "string",
                "description": (
                    "Full kubectl command. No shell pipes or redirects. "
                    "Examples: 'kubectl get nodes -o wide', 'kubectl get pod -A -o wide', "
                    "'kubectl describe node ecs-w-01.dlee155.cldr.example', "
                    "'kubectl get deployments -n cdp', 'kubectl get events -n vault-system --sort-by=.lastTimestamp'"
                ),
            },
        },
    },
}