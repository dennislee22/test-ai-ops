import os
import re
import shlex
import logging
import json as _json
import yaml as _yaml
import base64
from pathlib import Path
from kubernetes import client as _k8s, config as _k8s_cfg
from kubernetes.client.rest import ApiException

_KUBECTL_MAX_OUT = 4000
_KUBECTL_READ_VERBS = {"get", "describe", "logs", "top", "rollout", "auth", "api-resources", "version"}

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger("k8s")

def _load_initial_k8s():
    kc = os.getenv("KUBECONFIG_PATH", "").strip()
    
    if kc and Path(os.path.expanduser(kc)).exists():
        try:
            _k8s_cfg.load_kube_config(config_file=os.path.expanduser(kc))
            _log.info(f"Loaded initial kubeconfig from disk: {kc}")
        except Exception as e:
            _log.error(f"Failed to load disk kubeconfig: {e}")
    else:
        _log.warning("No valid KUBECONFIG_PATH found at startup. Awaiting dynamic reload_kubeconfig().")

_load_initial_k8s()

_version_api = _k8s.VersionApi()
_storage = _k8s.StorageV1Api()
_core   = _k8s.CoreV1Api()
_apps   = _k8s.AppsV1Api()
_batch  = _k8s.BatchV1Api()
_rbac   = _k8s.RbacAuthorizationV1Api()
_net    = _k8s.NetworkingV1Api()
_autoscaling = _k8s.AutoscalingV2Api()

def reload_kubeconfig(yaml_content: str) -> dict:
    import tempfile, re as _re
    global _core, _apps, _batch, _rbac, _net, _autoscaling

    if not yaml_content.strip():
        raise ValueError("Empty kubeconfig.")

    m = _re.search(r'server\s*:\s*(https?://[^\s\n]+)', yaml_content)
    server_url = m.group(1).strip() if m else None

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml',
                                     delete=False, encoding='utf-8') as f:
        f.write(yaml_content)
        tmp_path = f.name

    try:
        _k8s_cfg.load_kube_config(config_file=tmp_path)
    except Exception as e:
        raise ValueError(f"Invalid kubeconfig: {e}")

    _core        = _k8s.CoreV1Api()
    _apps        = _k8s.AppsV1Api()
    _batch       = _k8s.BatchV1Api()
    _rbac        = _k8s.RbacAuthorizationV1Api()
    _net         = _k8s.NetworkingV1Api()
    _autoscaling = _k8s.AutoscalingV2Api()

    try:
        _core.list_namespace(_request_timeout=5)
    except Exception as e:
        raise ValueError(f"Kubeconfig loaded but cluster unreachable: {e}")

    _log.info(f"[kubeconfig] Reloaded — server={server_url}")
    return {"ok": True, "server": server_url or "unknown"}

def _is_high_restart(pod, restart_count: int) -> bool:
    import datetime as _dt

    if restart_count == 0:
        return False

    now = _dt.datetime.now(_dt.timezone.utc)

    last_restart_time = None
    for cs in (pod.status.container_statuses or []):
        if cs.last_state and cs.last_state.terminated:
            t = cs.last_state.terminated.finished_at
            if t:
                if last_restart_time is None or t > last_restart_time:
                    last_restart_time = t

    if last_restart_time is not None:
        hours_since_last = (now - last_restart_time).total_seconds() / 3600
        if hours_since_last > 24:
            return False

    if restart_count > 100:
        return True

    run_start = None
    for cs in (pod.status.container_statuses or []):
        if cs.state and cs.state.running and cs.state.running.started_at:
            t = cs.state.running.started_at
            if run_start is None or t > run_start:
                run_start = t

    if run_start is None and pod.status and pod.status.start_time:
        run_start = pod.status.start_time

    if run_start is None:
        return restart_count > 10

    run_days = max((now - run_start).total_seconds() / 86400, 0.1)
    return (restart_count / run_days) > 3.0

def get_pod_tolerations(namespace: str = "all",
                        pod_name: str | None = None,
                        search: str | None = None) -> str:
    try:
        if namespace != "all":
            try:
                _core.read_namespace(name=namespace)
            except ApiException as e:
                if e.status == 404:
                    return (f"Namespace '{namespace}' does not exist in this cluster. "
                            f"Cannot report pod tolerations.")
                raise

        pods = (_core.list_pod_for_all_namespaces()
                if namespace == "all"
                else _core.list_namespaced_pod(namespace=namespace))

        if not pods.items:
            return f"No pods found in namespace '{namespace}'."

        if pod_name:
            pods.items = [p for p in pods.items if pod_name in p.metadata.name]

        if not pods.items:
            return f"No pods matching '{pod_name}' found in namespace '{namespace}'."

        table_rows = []
        for pod in sorted(pods.items, key=lambda p: (p.metadata.namespace, p.metadata.name)):
            ns = pod.metadata.namespace
            name = pod.metadata.name
            tolerations = pod.spec.tolerations or []

            if not tolerations:
                table_rows.append((ns, name, "<none>"))
                continue

            for t in tolerations:
                key = t.key or "<any>"
                op = t.operator or "Equal"
                val = t.value or "-"
                eff = t.effect or "Any"

                tol_str = f"key:{key} op:{op} value:{val} effect:{eff}"
                if search and search.lower() not in tol_str.lower():
                    continue

                table_rows.append((ns, name, tol_str))

        if search and not table_rows:
            for pod in sorted(pods.items, key=lambda p: (p.metadata.namespace, p.metadata.name)):
                ns = pod.metadata.namespace
                name = pod.metadata.name
                tolerations = pod.spec.tolerations or []

                if not tolerations:
                    table_rows.append((ns, name, "<none>"))
                    continue

                for t in tolerations:
                    key = t.key or "<any>"
                    op = t.operator or "Equal"
                    val = t.value or "-"
                    eff = t.effect or "Any"
                    tol_str = f"key:{key} op:{op} value:{val} effect:{eff}"
                    table_rows.append((ns, name, tol_str))

        md_lines = ["| NAMESPACE | POD | TOLERATION |",
                    "|---|---|---|"]
        for ns, name, tol_str in table_rows:
            md_lines.append(f"| `{ns}` | `{name}` | {tol_str} |")

        return "\n".join(md_lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"
        
def get_pod_resource_requests(namespace: str = "all", search: str | None = None) -> str:
    try:
        pods = (_core.list_pod_for_all_namespaces().items
                if namespace == "all"
                else _core.list_namespaced_pod(namespace=namespace).items)

        if not pods:
            return f"No pods found in namespace '{namespace}'."

        filtered_pods = []
        if search:
            search_lower = search.lower()
            for pod in pods:
                if search_lower in pod.metadata.name.lower() or (namespace != "all" and search_lower in pod.metadata.namespace.lower()):
                    filtered_pods.append(pod)
        else:
            filtered_pods = pods

        if search and not filtered_pods:
            filtered_pods = pods

        md_lines = []
        md_lines.append("As no namespace was mentioned, I checked across all namespaces.")
        md_lines.append("| NAMESPACE | POD | CONTAINER | CPU_REQ | CPU_LIM | MEM_REQ | MEM_LIM | ATTACHED GPU |")
        md_lines.append("|---|---|---|---|---|---|---|---|")

        for pod in sorted(filtered_pods, key=lambda p: (p.metadata.namespace, p.metadata.name)):
            ns = pod.metadata.namespace
            podn = pod.metadata.name

            cpu_req_total_m = 0
            cpu_lim_total_m = 0
            mem_req_total_mi = 0
            mem_lim_total_mi = 0

            gpu_reqs = []
            for c in pod.spec.containers or []:
                req = c.resources.requests or {}
                for k, v in req.items():
                    if "gpu" in k.lower():
                        gpu_reqs.append(f"{c.name}:{v}")
            attached_gpu = ", ".join(gpu_reqs) if gpu_reqs else "-"

            for c in pod.spec.containers or []:
                req = c.resources.requests or {}
                lim = c.resources.limits or {}

                cpu_req = req.get("cpu", "0")
                cpu_lim = lim.get("cpu", "0")
                mem_req = req.get("memory", "0")
                mem_lim = lim.get("memory", "0")

                cpu_req_m = cpu_req if cpu_req.endswith("m") else f"{int(float(cpu_req)*1000)}m"
                cpu_lim_m = cpu_lim if cpu_lim.endswith("m") else f"{int(float(cpu_lim)*1000)}m"

                def to_mi(val):
                    if val.endswith("Ki"):
                        return f"{int(int(val[:-2])/1024)}Mi"
                    if val.endswith("Gi"):
                        return f"{int(float(val[:-2])*1024)}Mi"
                    if val.endswith("Ti"):
                        return f"{int(float(val[:-2])*1024*1024)}Mi"
                    if val.endswith("Mi"):
                        return val
                    return f"{int(int(val)/1024/1024)}Mi"

                mem_req_mi = to_mi(mem_req)
                mem_lim_mi = to_mi(mem_lim)

                cpu_req_total_m += int(cpu_req_m.rstrip("m"))
                cpu_lim_total_m += int(cpu_lim_m.rstrip("m"))
                mem_req_total_mi += int(mem_req_mi.rstrip("Mi"))
                mem_lim_total_mi += int(mem_lim_mi.rstrip("Mi"))

                md_lines.append(
                    f"| `{ns}` | `{podn}` | `{c.name}` | {cpu_req_m} | {cpu_lim_m} | {mem_req_mi} | {mem_lim_mi} | {attached_gpu} |"
                )

            md_lines.append(
                f"| `{ns}` | `{podn}` | **TOTAL** | {cpu_req_total_m}m | {cpu_lim_total_m}m | {mem_req_total_mi}Mi | {mem_lim_total_mi}Mi | {attached_gpu} |"
            )

        return "\n".join(md_lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_pod_containers_resources(namespace: str = "all", search: str | None = None) -> str:
    try:
        pods = (_core.list_pod_for_all_namespaces().items
                if namespace == "all"
                else _core.list_namespaced_pod(namespace=namespace).items)

        if not pods:
            return f"No pods found in namespace '{namespace}'."

        filtered_pods = []
        if search:
            search_lower = search.lower()
            for pod in pods:
                if search_lower in pod.metadata.name.lower() or (namespace != "all" and search_lower in pod.metadata.namespace.lower()):
                    filtered_pods.append(pod)
        else:
            filtered_pods = pods

        if search and not filtered_pods:
            filtered_pods = pods

        md_lines = []
        md_lines.append("As no namespace was mentioned, I checked across all namespaces.")
        md_lines.append("| NAMESPACE | POD | CONTAINER | IMAGE | CPU_REQ | CPU_LIM | MEM_REQ | MEM_LIM | ATTACHED GPU |")
        md_lines.append("|---|---|---|---|---|---|---|---|---|")

        def _parse_cpu(v):
            if not v:
                return "0m"
            return v

        def _parse_mem(v):
            if not v:
                return "0Mi"
            return v

        for pod in sorted(filtered_pods, key=lambda p: (p.metadata.namespace, p.metadata.name)):
            ns = pod.metadata.namespace
            podn = pod.metadata.name

            gpu_reqs = []
            for c in pod.spec.containers or []:
                req = c.resources.requests or {}
                for k, v in req.items():
                    if "gpu" in k.lower():
                        gpu_reqs.append(f"{c.name}:{v}")
            attached_gpu = ", ".join(gpu_reqs) if gpu_reqs else "-"

            for c in pod.spec.containers or []:
                req = c.resources.requests or {}
                lim = c.resources.limits or {}

                cpu_req = _parse_cpu(req.get("cpu", "0m"))
                cpu_lim = _parse_cpu(lim.get("cpu", "0m"))
                mem_req = _parse_mem(req.get("memory", "0Mi"))
                mem_lim = _parse_mem(lim.get("memory", "0Mi"))
                image   = c.image or "<none>"

                md_lines.append(
                    f"| `{ns}` | `{podn}` | `{c.name}` | `{image}` | {cpu_req} | {cpu_lim} | {mem_req} | {mem_lim} | {attached_gpu} |"
                )

        return "\n".join(md_lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"
    
def get_pod_status(namespace: str = "all", search: str | None = None) -> str:
    try:
        pods = (_core.list_pod_for_all_namespaces().items
                if namespace == "all"
                else _core.list_namespaced_pod(namespace=namespace).items)

        if not pods:
            return f"No pods found in '{namespace}'."

        table_rows = []
        for pod in pods:
            if search:
                s = search.lower()
                if s not in pod.metadata.name.lower() and s not in pod.metadata.namespace.lower():
                    continue

            phase = pod.status.phase or "Unknown"
            restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
            ready = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
            total = len(pod.spec.containers)

            conditions = [f"{c.type}={c.status}" for c in (pod.status.conditions or []) if c.status != "True"]
            cond_str = ", ".join(conditions) if conditions else "-"

            table_rows.append((
                pod.metadata.namespace,
                pod.metadata.name,
                phase,
                f"{ready}/{total}",
                restarts,
                cond_str
            ))

        fallback_msg = ""
        if search and not table_rows:
            table_rows = []
            for pod in pods:
                phase = pod.status.phase or "Unknown"
                restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
                ready = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                total = len(pod.spec.containers)

                conditions = [f"{c.type}={c.status}" for c in (pod.status.conditions or []) if c.status != "True"]
                cond_str = ", ".join(conditions) if conditions else "-"

                table_rows.append((
                    pod.metadata.namespace,
                    pod.metadata.name,
                    phase,
                    f"{ready}/{total}",
                    restarts,
                    cond_str
                ))

        md_lines = [fallback_msg] if fallback_msg else []

        if namespace == "all":
            md_lines.append("| NAMESPACE | NAME | STATUS | READY | RESTARTS | CONDITIONS |")
            md_lines.append("|---|---|---|---|---|---|")
            for ns, name, phase, ready, restarts, cond in table_rows:
                md_lines.append(f"| `{ns}` | `{name}` | {phase} | {ready} | {restarts} | {cond} |")
        else:
            md_lines.append("| NAME | STATUS | READY | RESTARTS | CONDITIONS |")
            md_lines.append("|---|---|---|---|---|")
            for _, name, phase, ready, restarts, cond in table_rows:
                md_lines.append(f"| `{name}` | {phase} | {ready} | {restarts} | {cond} |")

        return "\n".join(md_lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"
    
def get_pod_logs(namespace: str = "all", search: str | None = None,
                 tail_lines: int = 50, container: str = "") -> str:
    tail_lines = min(tail_lines, 100)
    try:
        pods = (_core.list_pod_for_all_namespaces().items
                if namespace == "all"
                else _core.list_namespaced_pod(namespace=namespace).items)

        if not pods:
            return f"No pods found in namespace '{namespace}'."

        matching_pods = []
        if search:
            search_lower = search.lower()
            for pod in pods:
                if search_lower in pod.metadata.name.lower() or (namespace != "all" and search_lower in pod.metadata.namespace.lower()):
                    matching_pods.append(pod)
        else:
            matching_pods = pods

        if not matching_pods:
            return f"No pods matching '{search}' found in namespace '{namespace}'."

        log_entries = []
        for pod in sorted(matching_pods, key=lambda p: (p.metadata.namespace, p.metadata.name)):
            pod_name = pod.metadata.name
            ns_name = pod.metadata.namespace

            kw: dict = {"tail_lines": tail_lines, "timestamps": True}

            if container:
                kw["container"] = container
            else:
                containers = [c.name for c in (pod.spec.containers or [])]
                if containers:
                    if len(containers) > 1:
                        pod_stem = pod_name.rsplit("-", 2)[0] if pod_name.count("-") >= 2 else pod_name
                        preferred = next(
                            (c for c in containers if pod_stem in c or c in pod_stem),
                            containers[-1]
                        )
                        kw["container"] = preferred
                    else:
                        kw["container"] = containers[0]

            try:
                logs = _core.read_namespaced_pod_log(
                    name=pod_name, namespace=ns_name, **kw)
                container_label = f" [{kw['container']}]" if "container" in kw else ""
                if logs.strip():
                    log_entries.append(f"### `{ns_name}/{pod_name}`{container_label}\n```\n{logs}\n```")
                else:
                    log_entries.append(f"### `{ns_name}/{pod_name}`{container_label}\n_No logs available._")
            except ApiException as e:
                log_entries.append(f"### `{ns_name}/{pod_name}`\n_Error fetching logs: {e.reason}_")

        header = ""
        if namespace == "all":
            header = "As no namespace was specified, I searched all namespaces and below is the result.\n\n"

        return header + "\n\n".join(log_entries)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def describe_pv(name: str, show_yaml: bool = False) -> str:
    try:
        pvs = _core.list_persistent_volume().items
        if not pvs:
            return "`No PVs found.`"

        matching_pvs = [p for p in pvs if name.lower() in p.metadata.name.lower()]
        if not matching_pvs:
            return f"`No PV matching '{name}' found.`"

        pv = matching_pvs[0]

        if show_yaml:
            import yaml
            return f"`{yaml.safe_dump(pv.to_dict(), sort_keys=False)}`"

        lines = ["```"]
        lines.append(f"Name:            {pv.metadata.name}")

        if pv.metadata.labels:
            lines.append("Labels:          " + "\n                 ".join(f"{k}={v}" for k,v in pv.metadata.labels.items()))
        else:
            lines.append("Labels:          <none>")

        if pv.metadata.annotations:
            lines.append("Annotations:     " + "\n                 ".join(f"{k}: {v}" for k,v in pv.metadata.annotations.items()))
        else:
            lines.append("Annotations:     <none>")

        lines.append(f"Finalizers:      {pv.metadata.finalizers or []}")
        lines.append(f"StorageClass:    {pv.spec.storage_class_name or '<none>'}")
        lines.append(f"Status:          {pv.status.phase}")
        claim_ref = f"{pv.spec.claim_ref.namespace}/{pv.spec.claim_ref.name}" if pv.spec.claim_ref else "<none>"
        lines.append(f"Claim:           {claim_ref}")
        lines.append(f"Reclaim Policy:  {pv.spec.persistent_volume_reclaim_policy}")
        access_modes = ", ".join(pv.spec.access_modes or [])
        lines.append(f"Access Modes:    {access_modes or '<none>'}")
        lines.append(f"VolumeMode:      {pv.spec.volume_mode or '<none>'}")
        capacity = pv.spec.capacity.get("storage") if pv.spec.capacity else "<none>"
        lines.append(f"Capacity:        {capacity}")
        lines.append(f"Node Affinity:   {pv.spec.node_affinity or '<none>'}")
        lines.append(f"Message:         {pv.status.message or ''}")

        if pv.spec.csi:
            csi = pv.spec.csi
            lines.append("Source:")
            lines.append("    Type:              CSI (a Container Storage Interface (CSI) volume source)")
            lines.append(f"    Driver:            {csi.driver}")
            lines.append(f"    FSType:            {csi.fs_type or '<none>'}")
            lines.append(f"    VolumeHandle:      {csi.volume_handle}")
            lines.append(f"    ReadOnly:          {csi.read_only}")
            if csi.volume_attributes:
                lines.append("    VolumeAttributes:      " + "\n                           ".join(f"{k}={v}" for k,v in csi.volume_attributes.items()))
        else:
            lines.append(f"Source:           <unknown>")

        try:
            events = _core.list_event_for_all_namespaces(limit=500).items
            pv_events = [e for e in events if getattr(e.involved_object, "name", "") == pv.metadata.name and e.involved_object.kind == "PersistentVolume"]
            if pv_events:
                lines.append("Events:                ")
                for e in sorted(pv_events, key=lambda x: x.last_timestamp or x.event_time or ""):
                    lines.append(f"  {e.last_timestamp} {e.type} {e.reason} — {e.message} (x{e.count or 1})")
            else:
                lines.append("Events:                <none>")
        except Exception:
            lines.append("Events:                <error fetching events>")

        lines.append("```")
        return "\n".join(lines)

    except Exception as e:
        return f"`Error fetching PV: {str(e)}`"
    
def describe_pvc(name: str, namespace: str = "all", show_yaml: bool = False) -> str:
    try:
        # Fetch PVCs
        pvcs = (_core.list_persistent_volume_claim_for_all_namespaces().items
                if namespace == "all"
                else _core.list_namespaced_persistent_volume_claim(namespace=namespace).items)

        if not pvcs:
            return f"`No PVCs found in namespace '{namespace}'.`"

        # Match PVC by name (supports partial match)
        matching_pvcs = [p for p in pvcs if name.lower() in p.metadata.name.lower()]
        if not matching_pvcs:
            return f"`No PVC matching '{name}' found in namespace '{namespace}'.`"

        pvc = matching_pvcs[0]

        if show_yaml:
            import yaml
            return f"`{yaml.safe_dump(pvc.to_dict(), sort_keys=False)}`"

        lines = ["```"]
        lines.append(f"Name:          {pvc.metadata.name}")
        lines.append(f"Namespace:     {pvc.metadata.namespace}")
        lines.append(f"StorageClass:  {pvc.spec.storage_class_name or '<none>'}")
        lines.append(f"Status:        {pvc.status.phase}")
        lines.append(f"Volume:        {pvc.spec.volume_name or '<none>'}")

        # Labels
        if pvc.metadata.labels:
            lines.append("Labels:        " + "\n               ".join(f"{k}={v}" for k,v in pvc.metadata.labels.items()))
        else:
            lines.append("Labels:        <none>")

        # Annotations
        if pvc.metadata.annotations:
            lines.append("Annotations:   " + "\n               ".join(f"{k}: {v}" for k,v in pvc.metadata.annotations.items()))
        else:
            lines.append("Annotations:   <none>")

        # Finalizers
        lines.append(f"Finalizers:    {pvc.metadata.finalizers or []}")

        # Capacity
        capacity = pvc.status.capacity.get("storage") if pvc.status.capacity else "<none>"
        lines.append(f"Capacity:      {capacity}")

        # Access Modes
        access_modes = ", ".join(pvc.status.access_modes or [])
        lines.append(f"Access Modes:  {access_modes or '<none>'}")

        # Volume Mode
        lines.append(f"VolumeMode:    {pvc.spec.volume_mode or '<none>'}")

        # Used By (find pods using this PVC)
        try:
            if namespace == "all":
                pods = _core.list_pod_for_all_namespaces().items
            else:
                pods = _core.list_namespaced_pod(namespace=namespace).items
            used_by = []
            for pod in pods:
                for c in pod.spec.volumes or []:
                    if getattr(c, "persistent_volume_claim", None):
                        claim_name = c.persistent_volume_claim.claim_name
                        if claim_name == pvc.metadata.name:
                            used_by.append(pod.metadata.name)
            lines.append(f"Used By:       {', '.join(used_by) if used_by else '<none>'}")
        except Exception:
            lines.append("Used By:       <error fetching pods>")

        # Events
        try:
            events = (_core.list_event_for_all_namespaces(limit=500).items
                      if namespace == "all"
                      else _core.list_namespaced_event(namespace=namespace, limit=500).items)
            pvc_events = [e for e in events if getattr(e.involved_object, "name", "") == pvc.metadata.name and e.involved_object.kind == "PersistentVolumeClaim"]
            if pvc_events:
                lines.append("Events:")
                for e in sorted(pvc_events, key=lambda x: x.last_timestamp or x.event_time or ""):
                    lines.append(f"  {e.last_timestamp} {e.type} {e.reason} — {e.message} (x{e.count or 1})")
            else:
                lines.append("Events:        <none>")
        except Exception:
            lines.append("Events:        <error fetching events>")

        lines.append("```")
        return "\n".join(lines)

    except Exception as e:
        return f"`Error fetching PVC: {str(e)}`"
    
def describe_sc(name: str, show_yaml: bool = False) -> str:
    try:
        sc_list = _storage.list_storage_class().items
        matching_scs = [sc for sc in sc_list if sc.metadata.name == name]

        if not matching_scs:
            return f"`No StorageClass named '{name}' found.`"

        sc = matching_scs[0]

        if show_yaml:
            import yaml
            return f"`{yaml.safe_dump(sc.to_dict(), sort_keys=False)}`"

        lines = ["```"]
        lines.append(f"Name:            {sc.metadata.name}")
        is_default = sc.metadata.annotations.get("storageclass.kubernetes.io/is-default-class", "false") == "true"
        lines.append(f"IsDefaultClass:  {'Yes' if is_default else 'No'}")
        if sc.metadata.annotations:
            lines.append("Annotations:     " +
                         "\n                  ".join(f"{k}={v}" for k,v in sc.metadata.annotations.items()))

        lines.append(f"Provisioner:           {sc.provisioner}")
        lines.append("Parameters:            " +
                     ",".join(f"{k}={v}" for k,v in (sc.parameters or {}).items()))
        lines.append(f"AllowVolumeExpansion:  {sc.allow_volume_expansion or False}")
        lines.append(f"MountOptions:          {', '.join(sc.mount_options) if sc.mount_options else '<none>'}")
        lines.append(f"ReclaimPolicy:         {sc.reclaim_policy or '<none>'}")
        lines.append(f"VolumeBindingMode:     {sc.volume_binding_mode or '<none>'}")

        # StorageClass events are not usually generated, but placeholder for uniformity
        lines.append(f"Events:                <none>")

        lines.append("```")
        return "\n".join(lines)

    except Exception as e:
        return f"`Error fetching StorageClass: {str(e)}`"
    
def describe_pod(pod_name: str, namespace: str = "all", search: str | None = None, show_yaml: bool = False) -> str:
    try:
        pods = (_core.list_pod_for_all_namespaces().items
                if namespace == "all"
                else _core.list_namespaced_pod(namespace=namespace).items)

        if not pods:
            return f"`No pods found in namespace '{namespace}'.`"

        matching_pods = []
        if search:
            search_lower = search.lower()
            for pod in pods:
                if search_lower in pod.metadata.name.lower() or (namespace != "all" and search_lower in pod.metadata.namespace.lower()):
                    matching_pods.append(pod)
        else:
            matching_pods = [p for p in pods if p.metadata.name == pod_name]

        if not matching_pods:
            return f"`No pods matching '{pod_name if not search else search}' found in namespace '{namespace}'.`"

        pod = matching_pods[0]

        if show_yaml:
            import yaml
            return f"`{yaml.safe_dump(pod.to_dict(), sort_keys=False)}`"

        lines = ["```"]
        lines.append(f"Name:             {pod.metadata.name}")
        lines.append(f"Namespace:        {pod.metadata.namespace}")
        lines.append(f"Priority:         {pod.spec.priority or 0}")
        lines.append(f"Service Account:  {pod.spec.service_account_name or pod.spec.service_account}")
        lines.append(f"Node:             {pod.spec.node_name or '<none>'}")
        lines.append(f"Start Time:       {pod.status.start_time}")
        lines.append("Labels:           " + 
                     "\n                  ".join(f"{k}={v}" for k,v in (pod.metadata.labels or {}).items()))
        lines.append("Annotations:      " +
                     "\n                  ".join(f"{k}: {v}" for k,v in (pod.metadata.annotations or {}).items()))
        lines.append(f"Status:           {pod.status.phase}")
        lines.append(f"IP:               {pod.status.pod_ip}")

        pod_ips = getattr(pod.status, "pod_ips", None)
        if pod_ips:
            lines.append("IPs:")
            for ip in pod_ips:
                lines.append(f"  IP:           {ip.ip}")

        if pod.metadata.owner_references:
            owner = pod.metadata.owner_references[0]
            lines.append(f"Controlled By:  {owner.kind}/{owner.name}")

        if pod.spec.init_containers:
            lines.append("Init Containers:")
            for c in pod.spec.init_containers:
                lines.append(f"  {c.name}:")
                lines.append(f"    Image:        {c.image}")
                lines.append(f"    Resources:    limits={c.resources.limits}, requests={c.resources.requests}")
                lines.append(f"    Mounts:       " + ", ".join(m.mount_path for m in c.volume_mounts or []))
                status = next((s for s in pod.status.init_container_statuses or [] if s.name == c.name), None)
                if status:
                    if status.state.running:
                        lines.append(f"    State:        Running (Started: {status.state.running.started_at})")
                    elif status.state.terminated:
                        lines.append(f"    State:        Terminated (Exit: {status.state.terminated.exit_code}, Reason: {status.state.terminated.reason})")
                    elif status.state.waiting:
                        lines.append(f"    State:        Waiting ({status.state.waiting.reason})")
                    lines.append(f"    Ready:        {status.ready}")
                    lines.append(f"    Restart Count: {status.restart_count}")

        if pod.spec.containers:
            lines.append("Containers:")
            for c in pod.spec.containers:
                lines.append(f"  {c.name}:")
                lines.append(f"    Image:        {c.image}")
                status = next((s for s in pod.status.container_statuses or [] if s.name == c.name), None)
                if status:
                    if status.state.running:
                        lines.append(f"    State:        Running (Started: {status.state.running.started_at})")
                    elif status.state.terminated:
                        lines.append(f"    State:        Terminated (Exit: {status.state.terminated.exit_code}, Reason: {status.state.terminated.reason})")
                    elif status.state.waiting:
                        lines.append(f"    State:        Waiting ({status.state.waiting.reason})")
                    lines.append(f"    Ready:        {status.ready}")
                    lines.append(f"    Restart Count: {status.restart_count}")
                lines.append(f"    Requests:     {c.resources.requests or {}}")
                lines.append(f"    Limits:       {c.resources.limits or {}}")
                lines.append(f"    Mounts:       " + ", ".join(m.mount_path for m in c.volume_mounts or []))

        if pod.status.conditions:
            lines.append("Conditions:")
            for cond in pod.status.conditions:
                lines.append(f"  Type: {cond.type:25} Status: {cond.status}")

        if pod.spec.volumes:
            lines.append("Volumes:")
            for v in pod.spec.volumes:
                vtype = next((k for k in v.to_dict() if k not in ("name",)), "unknown")
                lines.append(f"  {v.name}: {vtype}")

        lines.append(f"QoS Class:       {pod.status.qos_class or 'None'}")
        if pod.spec.node_selector:
            lines.append("Node-Selectors:  " + ", ".join(f"{k}={v}" for k,v in pod.spec.node_selector.items()))
        if pod.spec.tolerations:
            lines.append("Tolerations:     " + ", ".join(str(t) for t in pod.spec.tolerations))

        try:
            events = (_core.list_event_for_all_namespaces(limit=500)
                      if namespace == "all"
                      else _core.list_namespaced_event(namespace=namespace, limit=500))
            pod_events = [e for e in (events.items or []) if e.involved_object.name == pod.metadata.name and e.involved_object.kind == "Pod"]
            if pod_events:
                lines.append("Events:")
                for e in sorted(pod_events, key=lambda x: x.last_timestamp or x.event_time or ""):
                    lines.append(f"  {e.last_timestamp} {e.type} {e.reason} — {e.message} (x{e.count or 1})")
            else:
                lines.append("Events:          <none>")
        except Exception:
            lines.append("Events:          <error fetching events>")

        lines.append("```")
        return "\n".join(lines)

    except Exception as e:
        return f"`Error fetching pod: {str(e)}`"

def get_unhealthy_pods_detail(namespace: str = "all") -> str:
    import datetime as _dt
    from datetime import timezone

    def _collect_unhealthy():
        found = []
        # Collect pods in non-Running phases
        for phase in ("Pending", "Failed", "Unknown"):
            try:
                result = (_core.list_pod_for_all_namespaces(field_selector=f"status.phase={phase}")
                          if namespace == "all"
                          else _core.list_namespaced_pod(namespace=namespace, field_selector=f"status.phase={phase}"))
                found.extend(result.items)
            except ApiException:
                pass

        # Check Running pods for high restarts or not-ready containers
        _cont = None
        while True:
            try:
                kw = {"field_selector": "status.phase=Running", "limit": 100}
                if namespace != "all":
                    kw["namespace"] = namespace
                if _cont:
                    kw["_continue"] = _cont
                page = (_core.list_pod_for_all_namespaces(**kw)
                        if namespace == "all"
                        else _core.list_namespaced_pod(**kw))
                for pod in page.items:
                    restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
                    ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                    tot      = len(pod.spec.containers)
                    if ready < tot or _is_high_restart(pod, restarts):
                        found.append(pod)
                _cont = page.metadata._continue if page.metadata and page.metadata._continue else None
                if not _cont:
                    break
            except ApiException:
                break
        return found

    def _describe_detail(pod) -> list:
        lines = []
        phase    = pod.status.phase or "Unknown"
        restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
        ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
        tot      = len(pod.spec.containers)
        lines.append(f"  Phase: {phase} | Ready: {ready}/{tot} | Total restarts: {restarts}")

        bad_conds = [(c.type, c.reason or "", c.message or "")
                     for c in (pod.status.conditions or []) if c.status != "True"]
        if bad_conds:
            lines.append("  Conditions:")
            for ctype, reason, msg in bad_conds:
                lines.append(f"    {ctype}: {reason} — {msg}" if reason or msg else f"    {ctype}: False")

        lines.append("  Containers:")
        for cs in (pod.status.container_statuses or []):
            state_dict = cs.state.to_dict() if cs.state else {}
            state_key  = next((k for k, v in state_dict.items() if v), "unknown")
            state_val  = state_dict.get(state_key) or {}
            reason     = state_val.get("reason", "")
            message    = state_val.get("message", "")
            exit_code  = state_val.get("exit_code", "")
            lines.append(f"    [{cs.name}] ready={cs.ready} state={state_key} restarts={cs.restart_count}"
                         + (f" reason={reason}" if reason else "")
                         + (f" exit_code={exit_code}" if exit_code != "" else ""))
            if message:
                for mline in message.strip().splitlines()[:5]:
                    lines.append(f"      {mline}")
            if cs.last_state and cs.last_state.terminated:
                lt = cs.last_state.terminated
                lines.append(f"      Last exit: code={lt.exit_code} reason={lt.reason}"
                              + (f" message={lt.message.strip()[:200]}" if lt.message else ""))

        for c in (pod.spec.containers or []):
            req = (c.resources.requests or {}) if c.resources else {}
            lim = (c.resources.limits   or {}) if c.resources else {}
            if req or lim:
                lines.append(f"    [{c.name}] resources: "
                              f"requests=cpu:{req.get('cpu','none')}/mem:{req.get('memory','none')} "
                              f"limits=cpu:{lim.get('cpu','none')}/mem:{lim.get('memory','none')}")

        events = []
        try:
            ev = _core.list_namespaced_event(
                namespace=pod.metadata.namespace,
                field_selector=f"involvedObject.name={pod.metadata.name}")
            warning_events = sorted(
                [e for e in ev.items if e.type == "Warning"],
                key=lambda e: (e.last_timestamp or e.event_time or _dt.datetime.min.replace(tzinfo=timezone.utc)),
                reverse=True)
            for e in warning_events[:5]:
                events.append(f"    [{e.reason}] {e.message}")
        except ApiException:
            pass
        if events:
            lines.append("  Warning events:")
            lines.extend(events)

        return lines

    def _get_logs(pod_name: str, ns: str) -> str:
        try:
            logs = _core.read_namespaced_pod_log(name=pod_name, namespace=ns, tail_lines=20, timestamps=False)
            if logs and logs.strip():
                return logs.strip()
        except ApiException:
            pass
        try:
            logs = _core.read_namespaced_pod_log(name=pod_name, namespace=ns, tail_lines=20, timestamps=False, previous=True)
            if logs and logs.strip():
                return "(previous container)\n" + logs.strip()
        except ApiException:
            pass
        return ""
    try:
        unhealthy = _collect_unhealthy()
    except Exception as e:
        return f"[ERROR] Failed to collect unhealthy pods: {e}"

    if not unhealthy:
        scope = f"namespace '{namespace}'" if namespace != "all" else "all namespaces"
        return f"No unhealthy pods found in {scope}."

    out = ["### Unhealthy Pods Summary", ""]
    out.append("| Namespace | Pod | Phase | Ready | Restarts |")
    out.append("|---|---|---|---|---|")

    for pod in unhealthy:
        ns_name  = pod.metadata.namespace
        pod_name = pod.metadata.name
        phase    = pod.status.phase or "Unknown"
        restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
        ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
        tot      = len(pod.spec.containers)
        out.append(f"| `{ns_name}` | `{pod_name}` | {phase} | {ready}/{tot} | {restarts} |")

    for pod in unhealthy:
        ns_name  = pod.metadata.namespace
        pod_name = pod.metadata.name
        out.append("\n" + "="*70)
        out.append(f"POD: {ns_name}/{pod_name}")
        out.extend(_describe_detail(pod))

        logs = _get_logs(pod_name, ns_name)
        if logs:
            out.append("  Recent logs (last 20 lines):")
            for line in logs.splitlines()[-20:]:
                out.append(f"    {line}")
        else:
            out.append("  Logs: none available")
        out.append("")

    return "\n".join(out)

def get_cluster_version() -> str:
    try:
        version_info = _version_api.get_code()
        return (
            f"Major: {version_info.major}, Minor: {version_info.minor}, "
            f"GitVersion: {version_info.git_version}, GitCommit: {version_info.git_commit}, "
            f"GitTreeState: {version_info.git_tree_state}, BuildDate: {version_info.build_date}"
        )
    except Exception as e:
        return f"[ERROR] Failed to get cluster version: {e}"

def get_storage_classes() -> str:
    try:
        scs = _storage.list_storage_class()
        if not scs.items:
            return "No StorageClasses found in the cluster."
        
        lines = ["### StorageClasses\n"]
        lines.extend(["| NAME | PROVISIONER | RECLAIM POLICY | EXPANSION | DEFAULT |", "|---|---|---|---|---|"])

        for sc in scs.items:
            name = sc.metadata.name
            provisioner = getattr(sc, "provisioner", "unknown")
            reclaim = getattr(sc, "reclaim_policy", "Delete")
            allow_expand = getattr(sc, "allow_volume_expansion", False)
            expand_str = "✓ Yes" if allow_expand else "No"
            
            annotations = sc.metadata.annotations or {}
            is_default = annotations.get("storageclass.kubernetes.io/is-default-class") == "true"
            default_str = "★ Yes" if is_default else ""

            lines.append(
                f"| {name} | {provisioner} | {reclaim} | {expand_str} | {default_str} |"
            )
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_endpoints(namespace: str = "all", search: str | None = None) -> str:
    try:
        eps_list = (_core.list_endpoints_for_all_namespaces().items
                    if namespace == "all"
                    else _core.list_namespaced_endpoints(namespace=namespace).items)

        if not eps_list:
            return f"No endpoints found in '{namespace}'."

        table_rows = []
        for ep in eps_list:
            if search and search.lower() not in ep.metadata.name.lower():
                continue

            ns_name = ep.metadata.namespace
            name = ep.metadata.name

            if not ep.subsets:
                continue

            for subset in ep.subsets:
                ports = subset.ports or []
                addresses = subset.addresses or []

                for addr in addresses:
                    ip = addr.ip
                    for port in ports:
                        table_rows.append((ns_name, name, f"{ip}:{port.port}"))

        fallback_msg = ""
        if search and not table_rows:
            for ep in eps_list:
                ns_name = ep.metadata.namespace
                name = ep.metadata.name

                if not ep.subsets:
                    continue

                for subset in ep.subsets:
                    ports = subset.ports or []
                    addresses = subset.addresses or []

                    for addr in addresses:
                        ip = addr.ip
                        for port in ports:
                            table_rows.append((ns_name, name, f"{ip}:{port.port}"))

        if not table_rows:
            return "No active Endpoints (no addresses) found."

        md_lines = []

        if namespace == "all":
            md_lines.append("As no namespace was mentioned, I checked across all namespaces.")
            md_lines.append("| NAMESPACE | NAME | ADDRESS |")
            md_lines.append("|---|---|---|")
            for ns, name, addr in table_rows:
                md_lines.append(f"| `{ns}` | `{name}` | `{addr}` |")
        else:
            md_lines.append("| NAME | ADDRESS |")
            md_lines.append("|---|---|")
            for _, name, addr in table_rows:
                md_lines.append(f"| `{name}` | `{addr}` |")

        return "\n".join(md_lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"
    
def get_node_capacity() -> str:
    def _parse_cpu(val) -> float:
        if not val: return 0.0
        val_str = str(val)
        if val_str.endswith("m"):
            return float(val_str[:-1]) / 1000.0
        return float(val_str)

    def _parse_mem_gib(val) -> float:
        if not val: return 0.0
        val_str = str(val)
        if val_str.endswith("Ki"): return float(val_str[:-2]) / (1024**2)
        if val_str.endswith("Mi"): return float(val_str[:-2]) / 1024.0
        if val_str.endswith("Gi"): return float(val_str[:-2])
        if val_str.endswith("Ti"): return float(val_str[:-2]) * 1024.0
        if val_str.isdigit(): return float(val_str) / (1024**3) # Plain bytes
        return 0.0

    try:
        nodes = _core.list_node()
        if not nodes.items:
            return "No nodes found."

        lines = ["### Node Capacity Overview\n"]

        headers = ["NODE", "CPU ALLOC", "CPU REQ", "CPU AVAIL", "RAM ALLOC (Gi)", "RAM REQ (Gi)", "RAM AVAIL (Gi)", "GPU"]
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("|" + "|".join(["---"] * len(headers)) + "|")

        for node in sorted(nodes.items, key=lambda n: n.metadata.name):
            alloc = node.status.allocatable or {}
            
            cpu_alloc = _parse_cpu(alloc.get("cpu", 0))
            mem_alloc_gib = _parse_mem_gib(alloc.get("memory", "0Ki"))

            gpu = 0
            for key in alloc:
                if "nvidia.com/gpu" in key or "amd.com/gpu" in key:
                    try:
                        gpu += int(alloc[key])
                    except (ValueError, TypeError):
                        pass

            cpu_req_total = 0.0
            mem_req_total_gib = 0.0
            pods = _core.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={node.metadata.name}")
            
            for pod in pods.items:
                for c in pod.spec.containers or []:
                    if c.resources and c.resources.requests:
                        cpu_req_total += _parse_cpu(c.resources.requests.get("cpu", 0))
                        mem_req_total_gib += _parse_mem_gib(c.resources.requests.get("memory", "0"))

            cpu_avail = round(cpu_alloc - cpu_req_total, 2)
            mem_avail = round(mem_alloc_gib - mem_req_total_gib, 2)

            cpu_req_pct = f"({round((cpu_req_total / cpu_alloc) * 100, 1)}%)" if cpu_alloc > 0 else "(0%)"
            cpu_avail_pct = f"({round((cpu_avail / cpu_alloc) * 100, 1)}%)" if cpu_alloc > 0 else "(0%)"
            
            mem_req_pct = f"({round((mem_req_total_gib / mem_alloc_gib) * 100, 1)}%)" if mem_alloc_gib > 0 else "(0%)"
            mem_avail_pct = f"({round((mem_avail / mem_alloc_gib) * 100, 1)}%)" if mem_alloc_gib > 0 else "(0%)"

            lines.append(
                f"| {node.metadata.name} "
                f"| {round(cpu_alloc, 2)} "
                f"| {round(cpu_req_total, 2)} {cpu_req_pct} "
                f"| {cpu_avail} {cpu_avail_pct} "
                f"| {round(mem_alloc_gib, 2)} "
                f"| {round(mem_req_total_gib, 2)} {mem_req_pct} "
                f"| {mem_avail} {mem_avail_pct} "
                f"| {gpu} |"
            )

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_node_labels(search: str = None) -> str:
    try:
        nodes = _core.list_node().items
        if not nodes:
            return "No nodes found."

        if search:
            search_lower = search.lower().strip()
            if search_lower in ("*", "all", "label", "labels", "node", "nodes"):
                search = None

        results = []

        for node in nodes:
            labels = node.metadata.labels or {}
            node_name = node.metadata.name

            label_lines = [f"  - {k}={v}" for k, v in labels.items()] or ["  - <none>"]

            if not search:
                results.append(f"**Node: {node_name}**\n" + "\n".join(label_lines) + "\n")
                continue

            search_lower = search.lower()

            if search_lower in node_name.lower():
                results.append(f"**Node: {node_name}**\n" + "\n".join(label_lines) + "\n")
                continue

            filtered = [l for l in label_lines if search_lower in l.lower()]
            if filtered:
                results.append(f"**Node: {node_name}**\n" + "\n".join(filtered) + "\n")

        if not results:
            return f"No nodes or labels found matching '{search}'."

        return "\n".join(results)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_node_taints(search: str = None) -> str:
    try:
        nodes = _core.list_node().items
        if not nodes:
            return "No nodes found."

        table_rows = []

        for node in nodes:
            node_taints = node.spec.taints or []
            if not node_taints:
                continue

            for t in node_taints:
                taint_str = f"{t.key}={t.value}:{t.effect}" if t.value else f"{t.key}:{t.effect}"
                if search and search.lower() not in taint_str.lower():
                    continue
                table_rows.append((node.metadata.name, taint_str))

        # Fallback: if search applied and no matches, show all taints
        if search and not table_rows:
            for node in nodes:
                node_taints = node.spec.taints or []
                if not node_taints:
                    continue
                for t in node_taints:
                    taint_str = f"{t.key}={t.value}:{t.effect}" if t.value else f"{t.key}:{t.effect}"
                    table_rows.append((node.metadata.name, taint_str))
            if table_rows:
                md_lines = [f"No matches for '{search}'. Showing all tainted nodes:", ""]
            else:
                return "No tainted nodes found at all."
        elif not table_rows:
            return "No tainted nodes found."
        else:
            md_lines = []

        # Build Markdown table
        if table_rows:
            md_lines.append("| Node | Taint |")
            md_lines.append("|---|---|")
            for node_name, taint in table_rows:
                md_lines.append(f"| `{node_name}` | {taint} |")

        return "\n".join(md_lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_node_info(node_name: str = None) -> str:
    """Show nodes with roles, readiness, CPU, memory (Gi), GPU."""
    def _mem_to_gib(mem_str: str) -> str:
        """Convert K8s memory string to GiB with 2 decimal precision."""
        if not mem_str:
            return "n/a"
        try:
            mem_str = mem_str.strip()
            if mem_str.endswith("Ki"):
                return f"{int(mem_str[:-2]) / (1024**2):.2f}Gi"
            if mem_str.endswith("Mi"):
                return f"{int(mem_str[:-2]) / 1024:.2f}Gi"
            if mem_str.endswith("Gi"):
                return f"{float(mem_str[:-2]):.2f}Gi"
            if mem_str.endswith("Ti"):
                return f"{float(mem_str[:-2]) * 1024:.2f}Gi"
            # fallback: assume bytes
            return f"{int(mem_str) / (1024**3):.2f}Gi"
        except ValueError:
            return mem_str

    try:
        nodes = _core.list_node().items
        if not nodes:
            return "No nodes found."

        headers = ["NODE", "ROLES", "STATUS", "CPU", "RAM (Gi)", "GPU"]
        
        def _build_row(node) -> str:
            # 1. Parse Roles
            roles = [k.replace("node-role.kubernetes.io/", "") 
                     for k in (node.metadata.labels or {}) 
                     if k.startswith("node-role.kubernetes.io/")]
            roles_str = ",".join(roles) if roles else "worker"

            # 2. Parse Readiness
            ready_status = "Unknown"
            for cond in node.status.conditions or []:
                if cond.type == "Ready":
                    ready_status = "Ready" if cond.status == "True" else "NotReady"
                    break
            
            # 3. Apply Cordon Status (Unschedulable)
            if node.spec.unschedulable:
                ready_status += ",SchedulingDisabled"

            # 4. Parse Resources
            alloc = node.status.allocatable or {}
            cpu = alloc.get("cpu", "n/a")
            mem = _mem_to_gib(alloc.get("memory", ""))
            
            gpu = "0"
            for key in alloc:
                if "nvidia.com/gpu" in key or "amd.com/gpu" in key:
                    gpu = alloc[key]
                    break

            return f"| {node.metadata.name} | {roles_str} | {ready_status} | {cpu} | {mem} | {gpu} |"

        # Filter nodes based on search parameter
        filtered_nodes = [n for n in nodes if node_name and node_name.lower() in n.metadata.name.lower()]
        
        # Handle fallback if search yields no results
        if node_name and not filtered_nodes:
            out = [f"### No matches for '{node_name}'. Showing all nodes:\n"]
            target_nodes = nodes
        else:
            title = f"### Node Info" + (f" (matching '{node_name}')" if node_name else "")
            out = [title + "\n"]
            target_nodes = filtered_nodes if node_name else nodes

        # Build Table
        out.append("| " + " | ".join(headers) + " |")
        out.append("|" + "|".join(["---"] * len(headers)) + "|")

        for node in target_nodes:
            out.append(_build_row(node))

        return "\n".join(out)

    except Exception as e:
        return f"Unexpected error: {str(e)}"
    
def find_resource(name_substring: str, resource_type: str = None, namespace: str = None) -> str:
    try:
        resource_type = resource_type.lower() if resource_type else None
        lines = ["| Resource Type | Namespace | Name | Status/Details |",
                 "|---------------|----------|------|----------------|"]
        results = []

        def add_resources(kind: str, items, get_details_fn):
            for item in items:
                if not name_substring or name_substring.lower() in item.metadata.name.lower():
                    details = get_details_fn(item)
                    results.append(f"| {kind} | {item.metadata.namespace} | {item.metadata.name} | {details} |")

        resources = []

        if not resource_type or resource_type == "pod":
            pods = _core.list_namespaced_pod(namespace).items if namespace else _core.list_pod_for_all_namespaces().items
            resources.append(("Pod", pods, lambda p: f"{p.status.phase or 'Unknown'} on {p.spec.node_name or 'n/a'}"))

        if not resource_type or resource_type in ("svc", "service"):
            svcs = _core.list_namespaced_service(namespace).items if namespace else _core.list_service_for_all_namespaces().items
            resources.append(("Service", svcs, lambda s: f"{s.spec.type} {s.spec.cluster_ip}"))

        if not resource_type or resource_type == "ingress":
            ingresses = _net.list_namespaced_ingress(namespace).items if namespace else _net.list_ingress_for_all_namespaces().items
            resources.append(("Ingress", ingresses, lambda i: ", ".join([h.host for h in i.spec.rules or []])))

        if not resource_type or resource_type == "pvc":
            pvcs = _core.list_namespaced_persistent_volume_claim(namespace).items if namespace else _core.list_persistent_volume_claim_for_all_namespaces().items
            resources.append(("PVC", pvcs, lambda pvc: f"{pvc.status.phase or 'Unknown'} {pvc.spec.resources.requests.get('storage', 'n/a') if pvc.spec.resources and pvc.spec.resources.requests else 'n/a'}"))

        for kind, items, fn in resources:
            add_resources(kind, items, fn)

        if not results:
            results.append(f"| No matches for '{name_substring}'. Showing all resources. | | | |")
            for kind, items, fn in resources:
                add_resources(kind, items, fn)

        return "\n".join(lines + results)

    except Exception as e:
        return f"Unexpected error: {str(e)}"

def get_gpu_info() -> str:
    try:
        nodes = _core.list_node().items
        if not nodes:
            return "No nodes found."

        pods = _core.list_pod_for_all_namespaces().items

        lines = ["### GPU Node Overview\n"]
        lines.extend(["| NODE | PRODUCT | COUNT | VRAM/GRAM | ALLOCATABLE | ATTACHED PODS |", "|---|---|---|---|---|---|"])

        has_gpu = False
        for node in sorted(nodes, key=lambda n: n.metadata.name):
            labels = node.metadata.labels or {}
            alloc  = node.status.allocatable or {}

            gpu_keys = [k for k in alloc.keys() if "gpu" in k.lower()]
            if not gpu_keys:
                continue

            gpu_key = gpu_keys[0]
            gpu_alloc_val = alloc.get(gpu_key, "0")

            product = (
                labels.get(f"{gpu_key}.product") or
                labels.get("gpu.product") or
                "Unknown"
            )

            count = labels.get(f"{gpu_key}.count") or labels.get("gpu.count") or gpu_alloc_val
            memory = labels.get(f"{gpu_key}.memory") or labels.get("gpu.memory") or "n/a"

            if gpu_alloc_val == "0" and product == "Unknown":
                continue

            pod_list = []
            for pod in pods:
                if pod.status.phase not in ("Running", "Pending"):
                    continue
                if pod.spec.node_name != node.metadata.name:
                    continue
                for container in pod.spec.containers or []:
                    resources = container.resources or {}
                    limits = resources.limits or {}
                    requests = resources.requests or {}
                    if any("gpu" in k.lower() and int(v) > 0 for k, v in {**limits, **requests}.items()):
                        pod_list.append(f"{pod.metadata.namespace}/{pod.metadata.name}")
                        break

            pods_str = ", ".join(pod_list) if pod_list else "-"

            lines.append(
                f"| {node.metadata.name} | {product} | {count} | {memory}Mi | {gpu_alloc_val} | {pods_str} |"
            )
            has_gpu = True

        if not has_gpu:
            return "No GPU nodes detected in the cluster."

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

_EVENT_NOISE_PATTERNS = [
    "cgroup",
    "cgroupv",
    "cgroup v1",
    "cgroup v2",
]

def _is_noisy_event(message: str) -> bool:
    msg_lower = (message or "").lower()
    return any(pat in msg_lower for pat in _EVENT_NOISE_PATTERNS)

def get_events(namespace: str = "all", search: str | None = None, type: str = "All") -> str:
    try:
        def fetch_events(ns: str, fs: str):
            if ns == "all":
                return _core.list_event_for_all_namespaces(field_selector=fs, limit=500).items
            return _core.list_namespaced_event(namespace=ns, field_selector=fs, limit=500).items

        type_upper = type.capitalize()
        field_selector = f"type={type_upper}" if type_upper in ("Warning", "Normal") else ""
        events = fetch_events(namespace, field_selector)

        if not events:
            return f"`No {type_upper.lower() if type_upper != 'All' else ''} events in '{namespace}'.`"

        search_lower = search.lower() if search else None
        matching_events = []
        for e in events:
            ns_name = e.metadata.namespace
            obj_name = getattr(e.involved_object, "name", "")
            if not search_lower or (search_lower in e.message.lower()
                                    or search_lower in ns_name.lower()
                                    or search_lower in obj_name.lower()):
                matching_events.append(e)

        if not matching_events:
            return f"`No events matching '{search}' found in namespace '{namespace}'.`"

        sorted_events = sorted(
            matching_events,
            key=lambda e: e.last_timestamp or e.event_time or "",
            reverse=True
        )

        lines = ["```"]
        shown = 0
        suppressed = 0
        for e in sorted_events:
            if shown >= 20:
                break
            if _is_noisy_event(e.message):
                suppressed += 1
                continue
            obj_kind = getattr(e.involved_object, "kind", "Unknown")
            obj_name = getattr(e.involved_object, "name", "Unknown")
            lines.append(f"{e.last_timestamp or e.event_time or 'N/A'}  "
                         f"{e.type:<7}  {e.reason:<18}  {namespace}/{obj_kind}/{obj_name}  {e.message} (x{e.count or 1})")
            shown += 1

        if suppressed:
            lines.append(f"_({suppressed} noisy/background event(s) suppressed)_")
        if shown == 0:
            return f"`No actionable events in '{namespace}' (all were background noise).`"

        lines.append("```")
        return "\n".join(lines)

    except ApiException as e:
        return f"`K8s API error: {e.reason}`"
    except Exception as e:
        return f"`Error fetching events: {str(e)}`"
    
def get_deployment(namespace: str = "all", search: str = None) -> str:
    try:
        deps = (_apps.list_deployment_for_all_namespaces()
                if namespace == "all"
                else _apps.list_namespaced_deployment(namespace=namespace))
        
        if not deps.items:
            return f"No deployments in '{namespace}'."
            
        filtered_deps = []
        for dep in deps.items:
            if search and search.lower() not in dep.metadata.name.lower():
                continue
            filtered_deps.append(dep)
            
        if not filtered_deps:
            search_term = f" matching '{search}'" if search else ""
            return f"No deployments{search_term} found in '{namespace}'."
        
        title = f"### Deployments in '{namespace}'"
        if search:
            title += f" (matching '{search}')"
        lines = [title + "\n"]
        
        if namespace == "all":
            lines.extend(["| NAMESPACE | NAME | STATUS | DESIRED | READY | AVAILABLE |", "|---|---|---|---|---|---|"])
        else:
            lines.extend(["| NAME | STATUS | DESIRED | READY | AVAILABLE |", "|---|---|---|---|---|"])

        for dep in filtered_deps:
            desired = dep.spec.replicas or 0
            ready   = dep.status.ready_replicas or 0
            avail   = dep.status.available_replicas or 0
            
            if desired == 0 and ready == 0:
                status = "✓ Scaled to 0"
            else:
                status = "✓ Healthy" if ready == desired else "⚠ Degraded"

            if namespace == "all":
                lines.append(f"| {dep.metadata.namespace} | {dep.metadata.name} | {status} | {desired} | {ready} | {avail} |")
            else:
                lines.append(f"| {dep.metadata.name} | {status} | {desired} | {ready} | {avail} |")
                
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_daemonset(namespace: str = "all") -> str:
    try:
        ds = (_apps.list_daemon_set_for_all_namespaces()
              if namespace == "all"
              else _apps.list_namespaced_daemon_set(namespace=namespace))
        if not ds.items:
            return f"No DaemonSets in '{namespace}'."
        
        lines = [f"### DaemonSets in '{namespace}'\n"]
        if namespace == "all":
            lines.extend(["| NAMESPACE | NAME | STATUS | DESIRED | READY | AVAILABLE |", "|---|---|---|---|---|---|"])
        else:
            lines.extend(["| NAME | STATUS | DESIRED | READY | AVAILABLE |", "|---|---|---|---|---|"])

        for d in ds.items:
            desired   = d.status.desired_number_scheduled or 0
            ready     = d.status.number_ready or 0
            available = d.status.number_available or 0
            
            if desired == 0 and ready == 0:
                status = "✓ Scaled to 0"
            else:
                status = "✓ Healthy" if ready == desired else "⚠ Degraded"

            if namespace == "all":
                lines.append(f"| {d.metadata.namespace} | {d.metadata.name} | {status} | {desired} | {ready} | {available} |")
            else:
                lines.append(f"| {d.metadata.name} | {status} | {desired} | {ready} | {available} |")
                
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"
    

def get_replicaset(namespace: str = "all", search: str = None) -> str:
    try:
        rs = (_apps.list_replica_set_for_all_namespaces()
              if namespace == "all"
              else _apps.list_namespaced_replica_set(namespace=namespace))
        
        if not rs.items:
            return f"No ReplicaSets in '{namespace}'."
            
        filtered_rs = []
        for r in rs.items:
            if search and search.lower() not in r.metadata.name.lower():
                continue
            filtered_rs.append(r)
            
        if not filtered_rs:
            search_term = f" matching '{search}'" if search else ""
            return f"No ReplicaSets{search_term} found in '{namespace}'."
        
        title = f"### ReplicaSets in '{namespace}'"
        if search:
            title += f" (matching '{search}')"
        lines = [title + "\n"]

        # Always include NAMESPACE column for consistency
        lines.extend(["| NAMESPACE | NAME | STATUS | DESIRED | READY | AVAILABLE |", "|---|---|---|---|---|---|"])

        for r in filtered_rs:
            desired   = r.status.replicas or 0
            ready     = r.status.ready_replicas or 0
            available = r.status.available_replicas or 0
            
            if desired == 0 and ready == 0:
                status = "✓ Scaled to 0"
            else:
                status = "✓ Healthy" if ready == desired else "⚠ Degraded"

            lines.append(f"| {r.metadata.namespace} | {r.metadata.name} | {status} | {desired} | {ready} | {available} |")
                
        return "\n".join(lines)
    except Exception as e:
        return f"Unexpected error: {str(e)}"

def get_statefulset(namespace: str = "all", search: str = None) -> str:
    try:
        sts = (_apps.list_stateful_set_for_all_namespaces()
               if namespace == "all"
               else _apps.list_namespaced_stateful_set(namespace=namespace))
               
        if not sts.items:
            return f"No StatefulSets in '{namespace}'."
            
        filtered_sts = []
        for s in sts.items:
            if search and search.lower() not in s.metadata.name.lower():
                continue
            filtered_sts.append(s)
            
        if not filtered_sts:
            search_term = f" matching '{search}'" if search else ""
            return f"No StatefulSets{search_term} found in '{namespace}'."

        title = f"### StatefulSets in '{namespace}'"
        if search:
            title += f" (matching '{search}')"
        lines = [title + "\n"]

        # Always include NAMESPACE and AVAILABLE columns for consistency
        lines.extend(["| NAMESPACE | NAME | STATUS | DESIRED | READY | AVAILABLE |", "|---|---|---|---|---|---|"])

        for s in filtered_sts:
            desired   = s.spec.replicas or 0
            ready     = s.status.ready_replicas or 0
            available = getattr(s.status, 'available_replicas', None) or 0
            
            if desired == 0 and ready == 0:
                status = "✓ Scaled to 0"
            else:
                status = "✓ Healthy" if ready == desired else "⚠ Degraded"

            lines.append(f"| {s.metadata.namespace} | {s.metadata.name} | {status} | {desired} | {ready} | {available} |")
                
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_job_status(namespace: str = "all",
                   show_all: bool = False,
                   raw_output: bool = False,
                   failed_only: bool = False,
                   running_only: bool = False) -> str:
    try:
        jobs = (_batch.list_job_for_all_namespaces()
                if namespace == "all"
                else _batch.list_namespaced_job(namespace=namespace))

        if not jobs.items:
            return f"No Jobs in '{namespace}'."

        running_jobs = []
        failed_jobs = []
        healthy_jobs = []

        for j in sorted(jobs.items, key=lambda j: (j.metadata.namespace, j.metadata.name)):
            active    = j.status.active    or 0
            succeeded = j.status.succeeded or 0
            failed    = j.status.failed    or 0

            # Determine status symbol
            if succeeded > 0 and active == 0:
                status = "✓ Complete"
            elif failed > 0:
                status = "⚠ Failed"
            else:
                status = "⏳ Running"

            entry = f"{j.metadata.namespace}/{j.metadata.name}: {status} | Active:{active} Succeeded:{succeeded} Failed:{failed}"

            # Filter for running_only
            if running_only:
                if active > 0:
                    running_jobs.append(entry)
                continue

            # Filter for failed_only
            if failed_only:
                if failed > 0:
                    failed_jobs.append(entry)
            else:
                if status == "⚠ Failed":
                    failed_jobs.append(entry)
                else:
                    healthy_jobs.append(entry)

        if raw_output:
            hdr = f"{'NAMESPACE':<22} {'NAME':<55} {'STATUS':<12} {'ACTIVE':<7} {'SUCCEEDED':<9} {'FAILED':<7}"
            rows = [hdr, "-"*len(hdr)]
            for j in sorted(jobs.items, key=lambda j: (j.metadata.namespace, j.metadata.name)):
                active    = j.status.active    or 0
                succeeded = j.status.succeeded or 0
                failed    = j.status.failed    or 0
                if succeeded > 0 and active == 0:
                    status = "Complete"
                elif failed > 0:
                    status = "Failed"
                else:
                    status = "Running"
                if running_only and active == 0:
                    continue
                rows.append(
                    f"{j.metadata.namespace:<22} {j.metadata.name:<55} {status:<12} "
                    f"{active:<7} {succeeded:<9} {failed:<7}"
                )
            if len(rows) == 2:  # only header exists
                return "No Jobs are currently running."
            return "\n".join(rows)

        # Human-readable output
        lines = []
        total_jobs = len(jobs.items)
        lines.append(f"Jobs in '{namespace}': {total_jobs} total.")

        if running_only:
            if running_jobs:
                lines.append(f"\nRunning jobs ({len(running_jobs)}):")
                lines.extend(running_jobs)
            else:
                lines.append("\nRunning jobs (0):")
                lines.append("  None")
            return "\n".join(lines)

        if failed_jobs:
            lines.append(f"\nFailed/Unhealthy jobs ({len(failed_jobs)}):")
            lines.extend(failed_jobs)
        else:
            lines.append("\nFailed/Unhealthy jobs (0):")
            lines.append("  None")

        if show_all and healthy_jobs:
            lines.append(f"\nHealthy/Complete jobs ({len(healthy_jobs)}):")
            lines.extend(healthy_jobs)

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_hpa_status(namespace: str = "all") -> str:
    try:
        hpas = (_autoscaling.list_horizontal_pod_autoscaler_for_all_namespaces()
                if namespace == "all"
                else _autoscaling.list_namespaced_horizontal_pod_autoscaler(namespace=namespace))
        if not hpas.items:
            return f"No HPAs in '{namespace}'."

        # Table header
        lines = [
            f"| Namespace | HPA Name | Current | Desired | Min | Max | Status |",
            f"|-----------|----------|---------|---------|-----|-----|--------|"
        ]

        # Table rows
        for h in hpas.items:
            cur = h.status.current_replicas or 0
            des = h.status.desired_replicas or 0
            mn  = h.spec.min_replicas or 1
            mx  = h.spec.max_replicas or "?"
            at_max = cur >= h.spec.max_replicas if h.spec.max_replicas else False
            flag = "⚠ AT MAX" if at_max else "OK"
            lines.append(
                f"| {h.metadata.namespace} | {h.metadata.name} | {cur} | {des} | {mn} | {mx} | {flag} |"
            )

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_pvc_status(namespace: str = "all", show_all: bool = False, search: str | None = None) -> str:
    _AM = {"ReadWriteOnce": "RWO", "ReadWriteMany": "RWX"}

    def _access(pvc):
        modes = pvc.spec.access_modes or []
        filtered = [_AM[m] for m in modes if m in _AM]
        return ",".join(filtered) if filtered else "?"

    try:
        pvcs = (
            _core.list_persistent_volume_claim_for_all_namespaces()
            if namespace == "all"
            else _core.list_namespaced_persistent_volume_claim(namespace=namespace)
        )

        if not pvcs.items:
            return f"No PVCs found in namespace '{namespace}'."

        table_rows = []
        for pvc in sorted(pvcs.items, key=lambda x: (x.metadata.namespace, x.metadata.name)):
            ns = pvc.metadata.namespace
            name = pvc.metadata.name
            phase = pvc.status.phase or "Unknown"
            sc = pvc.spec.storage_class_name or "default"
            cap = (pvc.status.capacity or {}).get("storage", "?")
            vol = pvc.spec.volume_name or "<unbound>"
            am = _access(pvc)

            if search and search.lower() not in name.lower():
                continue

            table_rows.append((ns, name, phase, am, sc, cap, vol))

        # fallback if search applied but nothing matched
        if search and not table_rows:
            for pvc in sorted(pvcs.items, key=lambda x: (x.metadata.namespace, x.metadata.name)):
                ns = pvc.metadata.namespace
                name = pvc.metadata.name
                phase = pvc.status.phase or "Unknown"
                sc = pvc.spec.storage_class_name or "default"
                cap = (pvc.status.capacity or {}).get("storage", "?")
                vol = pvc.spec.volume_name or "<unbound>"
                am = _access(pvc)
                table_rows.append((ns, name, phase, am, sc, cap, vol))

        if not table_rows:
            return "No PVCs to display."

        md_lines = []

        if namespace == "all":
            md_lines.append("_As no namespace was specified, showing PVCs from all namespaces._\n")
            md_lines.append("| NAMESPACE | PVC | PHASE | ACCESS | CLASS | CAPACITY | VOLUME |")
            md_lines.append("|---|---|---|---|---|---|---|")
            for ns, name, phase, am, sc, cap, vol in table_rows:
                md_lines.append(f"| `{ns}` | `{name}` | {phase} | {am} | {sc} | {cap} | {vol} |")
        else:
            md_lines.append("| PVC | PHASE | ACCESS | CLASS | CAPACITY | VOLUME |")
            md_lines.append("|---|---|---|---|---|---|")
            for _, name, phase, am, sc, cap, vol in table_rows:
                md_lines.append(f"| `{name}` | {phase} | {am} | {sc} | {cap} | {vol} |")

        return "\n".join(md_lines)

    except ApiException as e:
        return f"K8s API error (PVC listing): {e.reason}"
    
def get_persistent_volumes() -> str:
    _AM = {"ReadWriteOnce": "RWO", "ReadWriteMany": "RWX", "ReadOnlyMany": "ROX",
           "ReadWriteOncePod": "RWOP"}
    try:
        pvs = _core.list_persistent_volume()
        if not pvs.items:
            return "No PersistentVolumes found."
        lines = ["PersistentVolumes:"]
        for pv in pvs.items:
            phase    = pv.status.phase or "Unknown"
            cap      = (pv.spec.capacity or {}).get("storage", "?")
            policy   = pv.spec.persistent_volume_reclaim_policy or "?"
            sc       = pv.spec.storage_class_name or "none"
            modes    = pv.spec.access_modes or []
            am       = ",".join(_AM.get(m, m) for m in modes) or "?"
            claim    = (f"{pv.spec.claim_ref.namespace}/{pv.spec.claim_ref.name}"
                        if pv.spec.claim_ref else "unbound")
            lines.append(
                f"  {pv.metadata.name}: {phase} | {cap} | access:{am} | "
                f"class:{sc} policy:{policy} claim:{claim}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_pv_usage(threshold: int = 80) -> str:
    from kubernetes.stream import stream as _k8s_stream
    from kubernetes.client.rest import ApiException
    from kubernetes import client as _k8s
    _core = _k8s.CoreV1Api()

    def _parse_storage_to_gib(s: str) -> float:
        s = s.strip()
        try:
            if s.endswith("Ti"): return float(s[:-2]) * 1024
            if s.endswith("Gi"): return float(s[:-2])
            if s.endswith("Mi"): return float(s[:-2]) / 1024
            if s.endswith("Ki"): return float(s[:-2]) / (1024 * 1024)
            if s.endswith("T"):  return float(s[:-1]) * 1000
            if s.endswith("G"):  return float(s[:-1])
            if s.endswith("M"):  return float(s[:-1]) / 1000
        except ValueError:
            pass
        return 0.0

    def _exec_df(pod_name: str, namespace: str, mount_path: str, container: str = None) -> str:
        try:
            kwargs = dict(
                command=["/bin/sh", "-c", f"df -k --output=used,avail {mount_path} 2>/dev/null | tail -1"],
                stderr=False, stdin=False, stdout=True, tty=False,
                _preload_content=True,
            )
            if container:
                kwargs["container"] = container
            resp = _k8s_stream(
                _core.connect_get_namespaced_pod_exec,
                pod_name, namespace,
                **kwargs,
            )
            return resp.strip() if isinstance(resp, str) else ""
        except Exception:
            return ""

    def _longhorn_crd_usage(vol_name: str, ns: str, pvc_name: str, sc: str, threshold: int):
        try:
            custom = _k8s.CustomObjectsApi()
            lh_vol = custom.get_namespaced_custom_object(
                "longhorn.io", "v1beta2", "longhorn-system", "volumes", vol_name
            )
            actual_raw = lh_vol.get("status", {}).get("actualSize")
            total_raw  = lh_vol.get("spec", {}).get("size")
            
            if actual_raw is None or total_raw is None:
                return (f"{ns}/{pvc_name}: Longhorn CRD found but actualSize/spec.size missing", True)
            
            actual = int(actual_raw or 0)
            total  = int(total_raw  or 0)
            
            if total == 0:
                return (f"{ns}/{pvc_name}: Longhorn CRD found but spec.size=0 (not provisioned)", True)
            if actual == 0:
                total_gib = round(total / (1024**3), 2)
                return (f"{ns}/{pvc_name}: Longhorn CRD spec.size={total_gib}Gi but actualSize=0", True)
                
            pct = round((actual / total) * 100, 1)
            used_gib  = round(actual / (1024**3), 2)
            total_gib = round(total  / (1024**3), 2)
            avail_gib = round(max(0, total - actual) / (1024**3), 2)
            
            display_pct = min(pct, 100.0)
            pct_label = f"{pct}% (replica overhead)" if pct > 100 else f"{pct}%"
            flag = "🔴" if display_pct >= 90 else ("🟠" if display_pct >= threshold else "🟢")
            
            entry_data = {
                "pct_val": display_pct,
                "flag": flag,
                "ns_pvc": f"{ns}/{pvc_name}",
                "pct_str": pct_label,
                "used": f"{used_gib}Gi ({pct}%)",
                "total": f"{total_gib}Gi",
                "free": f"{avail_gib}Gi ({100 - pct}%)",
                "sc": sc
            }
            return (entry_data, False)
        except Exception as e:
            return (f"{ns}/{pvc_name}: CRD Error - {str(e)}", True)

    try:
        pvcs = _core.list_persistent_volume_claim_for_all_namespaces()
        pvs  = {pv.metadata.name: pv for pv in _core.list_persistent_volume().items}
    except ApiException as e:
        return f"K8s API error: {e.reason}"

    results = []
    errors  = []

    for pvc in pvcs.items:
        if pvc.status.phase != "Bound":
            continue

        pvc_name  = pvc.metadata.name
        ns        = pvc.metadata.namespace
        vol_name  = pvc.spec.volume_name or ""
        cap_str   = (pvc.status.capacity or {}).get("storage", "")
        cap_gib   = _parse_storage_to_gib(cap_str)

        pv = pvs.get(vol_name)
        sc = pvc.spec.storage_class_name or "none"

        try:
            pods = _core.list_namespaced_pod(namespace=ns)
        except ApiException:
            continue

        pod_hit       = None
        pod_container = None
        mount_path    = None
        
        for pod in pods.items:
            if pod.status.phase != "Running":
                continue
            for vol in (pod.spec.volumes or []):
                if vol.persistent_volume_claim and vol.persistent_volume_claim.claim_name == pvc_name:
                    for container in (pod.spec.containers or []):
                        for vm in (container.volume_mounts or []):
                            if vm.name == vol.name:
                                pod_hit       = pod.metadata.name
                                pod_container = container.name
                                mount_path    = vm.mount_path
                                break
                        if pod_hit: break
                if pod_hit: break

        if not pod_hit or not mount_path:
            entry, is_error = _longhorn_crd_usage(vol_name, ns, pvc_name, sc, threshold)
            if not is_error:
                results.append(entry)
            else:
                errors.append(entry if is_error else f"{ns}/{pvc_name}: no running pod found and Longhorn CRD unavailable")
            continue

        df_out = _exec_df(pod_hit, ns, mount_path, container=pod_container)
        
        if not df_out:
            entry, is_error = _longhorn_crd_usage(vol_name, ns, pvc_name, sc, threshold)
            if not is_error:
                results.append(entry)
            else:
                errors.append(entry if is_error else f"{ns}/{pvc_name}: df exec failed on pod {pod_hit} and Longhorn CRD unavailable")
            continue

        parts = df_out.split()
        if len(parts) < 2:
            errors.append(f"{ns}/{pvc_name}: unexpected df output: {df_out!r}")
            continue

        try:
            used_kb  = int(parts[0])
            avail_kb = int(parts[1])
            total_kb = used_kb + avail_kb
            pct      = round((used_kb / total_kb) * 100, 1) if total_kb > 0 else 0.0
        except (ValueError, ZeroDivisionError):
            errors.append(f"{ns}/{pvc_name}: could not parse df numbers from: {df_out!r}")
            continue

        used_gib  = round(used_kb  / (1024 * 1024), 2)
        avail_gib = round(avail_kb / (1024 * 1024), 2)
        total_gib = round(total_kb / (1024 * 1024), 2)

        flag = "🔴" if pct >= 90 else ("🟠" if pct >= threshold else "🟢")

        results.append({
            "pct_val": pct,
            "flag": flag,
            "ns_pvc": f"{ns}/{pvc_name}",
            "pct_str": f"{pct}%",
            "used": f"{used_gib}Gi ({pct}%)",
            "total": f"{total_gib}Gi",
            "free": f"{avail_gib}Gi ({100 - pct}%)",
            "sc": sc
        })

    results.sort(key=lambda x: x["pct_val"], reverse=True)
    nearing = [r for r in results if r["pct_val"] >= threshold]
    ok      = [r for r in results if r["pct_val"] < threshold]

    # --- THE FIXED MARKDOWN RENDERER ---
    def build_md_table(title, data):
        if not data:
            return ""
        # Added explicit newlines to ensure clean separation from the text above
        lines = [f"\n### {title}\n"]
        lines.append("| Flag | Namespace / PVC | Usage | Used (GiB) | Total (GiB) | Free (GiB) |")
        # Fixed the mismatched columns (now exactly 6 separators)
        lines.append("|---|---|---|---|---|---|")
        for d in data:
            lines.append(f"| {d['flag']} | `{d['ns_pvc']}` | **{d['pct_str']}** | {d['used']} | {d['total']} | {d['free']} |")
        return "\n".join(lines)

    output = []
    
    if nearing:
        output.append(build_md_table(f"⚠️ Nearing or exceeding {threshold}% capacity ({len(nearing)} PVCs)", nearing))
    else:
        output.append(f"\n✅ **No PVCs are at or above {threshold}% capacity.**\n")

    if ok:
        output.append(build_md_table(f"✅ Within capacity ({len(ok)} PVCs)", ok))

    if errors:
        output.append(f"\n### ⏭️ Skipped ({len(errors)} PVCs)")
        output.append("No mounted pod or `df` unavailable:")
        for err in errors:
            output.append(f"- {err}")

    return "\n".join(output)

# ── Prometheus metrics tool ──────────────────────────────────────────────────

def query_prometheus_metrics(metric: str = "cpu", duration: str = "1h", step: str = "60s", namespace: str = "") -> str:
    """
    Query Prometheus for time-series performance metrics and return tagged JSON
    for inline SVG chart rendering in the frontend.

    Discovers the Prometheus server pod automatically (any namespace, any pod
    whose name contains 'prometheus-server'), then execs curl against the
    Prometheus HTTP API (query_range).

    Returns a §GRAPH§...§GRAPH§ tagged JSON block that the frontend renders
    as an inline SVG chart, plus a human-readable text summary.
    """
    import json as _json_local
    import time as _time
    from kubernetes.stream import stream as _k8s_stream

    # ── PromQL templates ────────────────────────────────────────────────────
    # ── Metric map tuned for this cluster's Prometheus (no node-exporter).
    # CPU/memory metrics come from the metrics-server-exporter sidecar,
    # exposed as container_cpu_usage (millicores gauge) and
    # container_memory_usage_bytes (bytes gauge), labelled by pod+namespace.
    METRIC_MAP = {
        # CPU — millicores per pod (this cluster has no node-exporter)
        "cpu":          ("Pod CPU Usage (millicores)",
                         "sum by (pod, namespace) (container_cpu_usage)",
                         "m"),
        "node_cpu":     ("Pod CPU Usage (millicores)",
                         "sum by (pod, namespace) (container_cpu_usage)",
                         "m"),
        "pod_cpu":      ("Pod CPU Usage (millicores)",
                         "sum by (pod, namespace) (container_cpu_usage)",
                         "m"),
        # Memory — MiB per pod
        "memory":       ("Pod Memory Usage (MiB)",
                         "sum by (pod, namespace) (container_memory_usage_bytes) / 1048576",
                         "MiB"),
        "node_memory":  ("Pod Memory Usage (MiB)",
                         "sum by (pod, namespace) (container_memory_usage_bytes) / 1048576",
                         "MiB"),
        "pod_memory":   ("Pod Memory Usage (MiB)",
                         "sum by (pod, namespace) (container_memory_usage_bytes) / 1048576",
                         "MiB"),
        # Cluster-wide CPU limit total (single scalar — no node breakdown available)
        "cluster_cpu":  ("Cluster CPU Limits (cores)",
                         "cluster:kube_pod_container_resource_limits_cpu_cores:sum",
                         "cores"),
        "cluster_memory": ("Cluster Memory Limits (bytes)",
                           "cluster:kube_pod_container_resource_limits_memory_bytes:sum",
                           "bytes"),
        # Disk I/O — not available without node-exporter; return helpful message
        "disk_io":      ("PVC Disk I/O",
                         "container_cpu_usage",   # placeholder — overridden below
                         "n/a"),
        "pvc_io":       ("PVC Disk I/O",
                         "container_cpu_usage",
                         "n/a"),
        # Network — not available without node-exporter
        "network_in":   ("Network Receive",
                         "container_cpu_usage",
                         "n/a"),
        "network_out":  ("Network Transmit",
                         "container_cpu_usage",
                         "n/a"),
    }
    # Fallback PromQLs: tried in order if primary returns empty
    FALLBACK_PROMQL = {
        "cpu":        ["sum by (pod, namespace) (kube_metrics_server_pods_cpu)"],
        "node_cpu":   ["sum by (pod, namespace) (kube_metrics_server_pods_cpu)"],
        "pod_cpu":    ["sum by (pod, namespace) (kube_metrics_server_pods_cpu)"],
        "memory":     ["sum by (pod, namespace) (container_memory_usage_bytes) / 1048576"],
        "node_memory":["sum by (pod, namespace) (container_memory_usage_bytes) / 1048576"],
        "pod_memory": ["sum by (pod, namespace) (container_memory_usage_bytes) / 1048576"],
    }

    key = metric.lower().replace(" ", "_").replace("-", "_")
    title, promql, unit = METRIC_MAP.get(key, (
        f"Metric: {metric}",
        metric,   # treat raw input as PromQL if not in map
        "value",
    ))

    # Metrics not available on this cluster (no node-exporter)
    _UNAVAILABLE = {"disk_io", "pvc_io", "network_in", "network_out"}
    if key in _UNAVAILABLE:
        return (f"The '{metric}' metric is not available on this cluster. "
                f"Node-exporter is not installed, so disk I/O and network metrics are not scraped. "
                f"Available metrics: cpu (pod CPU millicores), memory (pod memory MiB), "
                f"pod_cpu, pod_memory, cluster_cpu, cluster_memory.")

    # ── Namespace filter — inject into PromQL if specified ──────────────────
    _NS_IGNORE = {"all", "any", "every", "cluster", "cluster-wide", "clusterwide", "none", "global", "*"}
    ns = namespace.strip().lower()
    if ns in _NS_IGNORE:
        ns = ""
    if ns:
        # Insert {namespace="<ns>"} label selector into each metric name in the PromQL
        # Works for both container_cpu_usage{...} and bare container_cpu_usage
        import re as _re_ns
        def _inject_ns(pql, ns_val):
            ns_filter = f'namespace="{ns_val}"'
            # Already has a label selector — append inside braces
            def _add_to_existing(m):
                inner = m.group(1).strip()
                if inner:
                    return '{' + inner + ',' + ns_filter + '}'
                return '{' + ns_filter + '}'
            result = _re_ns.sub(r'\{([^}]*)\}', _add_to_existing, pql)
            # No label selector present — add one after each bare metric name
            if result == pql:
                result = _re_ns.sub(
                    r'(container_cpu_usage|container_memory_usage_bytes'
                    r'|kube_metrics_server_pods_cpu)',
                    r'\1{' + ns_filter + '}',
                    result
                )
            return result
        promql = _inject_ns(promql, ns)
        # Also update title to show namespace scope
        title = f"{title} — ns:{ns}"

    # ── Duration → seconds ─────────────────────────────────────────────────
    def _dur_seconds(s):
        s = s.strip().lower()
        mul = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        try:
            return int(s[:-1]) * mul.get(s[-1], 1)
        except Exception:
            return 3600

    dur_sec = _dur_seconds(duration)
    end_ts  = int(_time.time())
    start_ts = end_ts - dur_sec

    # ── Discover Prometheus pod ─────────────────────────────────────────────
    prom_pod = None
    prom_ns  = None
    prom_container = "prometheus-server"
    try:
        pods = _core.list_pod_for_all_namespaces(field_selector="status.phase=Running")
        for p in pods.items:
            n = p.metadata.name.lower()
            if "prometheus-server" in n and "operator" not in n:
                # pick the right container
                cnames = [c.name for c in (p.spec.containers or [])]
                cname  = "prometheus-server" if "prometheus-server" in cnames else (cnames[0] if cnames else "prometheus-server")
                prom_pod       = p.metadata.name
                prom_ns        = p.metadata.namespace
                prom_container = cname
                break
    except Exception as e:
        return f"Could not list pods to find Prometheus: {e}"

    if not prom_pod:
        return ("No running Prometheus server pod found. "
                "Ensure a pod with 'prometheus-server' in its name is running.")

    # ── Detect Prometheus base path ─────────────────────────────────────────
    # Some installs use /prometheus prefix, others use /
    def _exec(cmd):
        try:
            resp = _k8s_stream(
                _core.connect_get_namespaced_pod_exec,
                prom_pod, prom_ns,
                command=["/bin/sh", "-c", cmd],
                container=prom_container,
                stderr=False, stdin=False, stdout=True, tty=False,
                _preload_content=True,
            )
            # k8s client may return str, bytes, or a dict (deserialised by some versions)
            if isinstance(resp, bytes):
                resp = resp.decode("utf-8", errors="replace")
            elif not isinstance(resp, str):
                # Some k8s client versions return a dict-like object — re-serialise to JSON
                import json as _js
                try:
                    resp = _js.dumps(resp) if hasattr(resp, "__iter__") else str(resp)
                except Exception:
                    resp = str(resp)
            return resp.strip()
        except Exception as exc:
            return f"[exec error: {exc}]"

    # Try /prometheus/api/v1 first (like your cluster), then /api/v1
    base_url = "http://localhost:9090"
    probe = _exec(f"curl -s -o /dev/null -w '%{{http_code}}' '{base_url}/prometheus/api/v1/query?query=up'")
    if probe.strip() == "200":
        api_base = f"{base_url}/prometheus/api/v1"
    else:
        probe2 = _exec(f"curl -s -o /dev/null -w '%{{http_code}}' '{base_url}/api/v1/query?query=up'")
        if probe2.strip() == "200":
            api_base = f"{base_url}/api/v1"
        else:
            return (f"Prometheus API not reachable at {base_url}. "
                    f"HTTP probes returned: /prometheus → {probe}, / → {probe2}. "
                    f"Pod: {prom_pod} in {prom_ns}.")

    # ── Discover available labels on container_cpu_usage ─────────────────────
    import urllib.parse as _up
    label_url = f"{api_base}/labels?match[]=container_cpu_usage"
    label_raw = _exec(f"curl -s --max-time 10 '{label_url}'")
    try:
        import json as _jl2
        label_data = _jl2.loads(label_raw)
        available_labels = label_data.get("data", [])
    except Exception:
        available_labels = []
    has_node_label = "node" in available_labels

    # ── Rewrite PromQL to group by node if node label is available ───────────
    if has_node_label and not ns:
        # Remap to node-level aggregation
        if key in ("cpu", "node_cpu", "pod_cpu"):
            promql = "sum by (node) (container_cpu_usage)"
            title  = "Node CPU Usage (millicores)"
        elif key in ("memory", "node_memory", "pod_memory"):
            promql = "sum by (node) (container_memory_usage_bytes) / 1048576"
            title  = "Node Memory Usage (MiB)"
        node_label_note = ""  # accurate data — no caveat needed
    else:
        node_label_note = available_labels  # carry for debug in caveat

    # ── Query ────────────────────────────────────────────────────────────────
    encoded = _up.quote(promql, safe="")
    url = (f"{api_base}/query_range"
           f"?query={encoded}"
           f"&start={start_ts}&end={end_ts}&step={step}")
    raw = _exec(f"curl -s --max-time 15 '{url}'")
    if raw.startswith("[exec error"):
        return raw

    try:
        data = _json_local.loads(raw)
    except Exception:
        # Fallback: some k8s client versions return Python dict repr (single quotes)
        # Try ast.literal_eval to recover
        import ast as _ast
        try:
            data = _ast.literal_eval(raw)
        except Exception:
            return f"Prometheus returned non-JSON response:\n{raw[:500]}"

    if data.get("status") != "success":
        return f"Prometheus query failed: {data.get('error', data)}"

    results = data.get("data", {}).get("result", [])
    if not results:
        # Try fallback PromQLs for this metric key before giving up
        tried = [promql]
        for fallback_pql in FALLBACK_PROMQL.get(key, []):
            if ns:
                fallback_pql = _inject_ns(fallback_pql, ns)
            if fallback_pql in tried:
                continue
            tried.append(fallback_pql)
            fb_encoded = _up.quote(fallback_pql, safe="")
            fb_url = (f"{api_base}/query_range"
                      f"?query={fb_encoded}"
                      f"&start={start_ts}&end={end_ts}&step={step}")
            fb_raw = _exec(f"curl -s --max-time 15 '{fb_url}'")
            try:
                fb_data = _json_local.loads(fb_raw)
            except Exception:
                import ast as _ast2
                try:
                    fb_data = _ast2.literal_eval(fb_raw)
                except Exception:
                    continue
            fb_results = fb_data.get("data", {}).get("result", [])
            if fb_results:
                results = fb_results
                promql  = fallback_pql  # for summary line
                break

    if not results:
        # Try to discover what CPU/memory metrics are actually available
        disc_url = f"{api_base}/label/__name__/values"
        disc_raw = _exec(f"curl -s --max-time 10 '{disc_url}'")
        available_hint = ""
        try:
            disc_data = _json_local.loads(disc_raw)
            all_metrics = disc_data.get("data", [])
            # Filter to relevant ones
            relevant = [m for m in all_metrics if any(
                kw in m for kw in ["cpu", "memory", "mem", "node_", "container_"]
            )][:20]
            if relevant:
                available_hint = f"\nAvailable metrics on this cluster include: {', '.join(relevant[:15])}"
        except Exception:
            pass
        return (f"No data returned for metric '{metric}' over the last {duration}. "
                f"The PromQL may not match any active series on this cluster.\n"
                f"PromQL tried: {promql}"
                + available_hint)

    # ── Build chart payload ─────────────────────────────────────────────────
    # Sort by last value descending (highest consumers first) — replaces PromQL sort_desc()
    def _last_val(r):
        vals = r.get("values", [])
        try:
            return float(vals[-1][1]) if vals else 0.0
        except (ValueError, IndexError):
            return 0.0

    results = sorted(results, key=_last_val, reverse=True)
    total_series = len(results)
    CHART_CAP = 8  # max series in chart

    # Python-side fmt to match JS — auto-scale decimal places
    def _fmt(v, u):
        if u == "%":     return f"{v:.1f}%"
        if u == "cores": return f"{v:.3f}"
        if u == "m":
            if v == 0:   return "0m"
            if v < 0.01: return f"{v:.4f}m"
            if v < 0.1:  return f"{v:.3f}m"
            if v < 1:    return f"{v:.2f}m"
            return f"{v:.1f}m"
        if u == "MiB":   return f"{v:.0f}Mi"
        if u in ("bytes", "bytes/s"):
            if v >= 1e9: return f"{v/1e9:.2f}G"
            if v >= 1e6: return f"{v/1e6:.1f}M"
            if v >= 1e3: return f"{v/1e3:.0f}k"
            return f"{v:.0f}"
        return f"{v:.2f}"

    series_out = []
    for r in results[:CHART_CAP]:
        metric_labels = r.get("metric", {})
        ns_val  = metric_labels.get("namespace") or metric_labels.get("pod_namespace") or ""
        pod_val = metric_labels.get("pod")       or metric_labels.get("pod_name")      or ""
        if ns_val and pod_val:
            label = f"{ns_val}/{pod_val}"
        elif pod_val:
            label = pod_val
        elif ns_val:
            label = ns_val
        else:
            inst = metric_labels.get("instance", "")
            label = inst.split(".")[0] if inst else next(iter(metric_labels.values()), "unknown")

        values = r.get("values", [])
        series_out.append({
            "label": label,
            "values": [[float(ts), float(v)] for ts, v in values],
        })

    # Text summary — in sync with chart (same CHART_CAP series, same fmt)
    cap_note = f" (top {CHART_CAP} of {total_series})" if total_series > CHART_CAP else ""

    # Caveat when user likely asked for node-level data but we only have pod-level
    node_caveat = ""
    if not ns:
        if has_node_label:
            node_caveat = ""  # showing real node data — no caveat needed
        else:
            node_caveat = (
                "\nNote: per-node metrics are unavailable (container_cpu_usage has no 'node' label "
                f"on this cluster; available labels: {available_labels}). "
                "Showing pod-level data as the closest available proxy."
            )

    summary_lines = [f"📊 {title} — last {duration}{cap_note}{node_caveat}"]
    for s in series_out:
        if s["values"]:
            last_val = s["values"][-1][1]
            summary_lines.append(f"  {s['label']}: {_fmt(last_val, unit)}")

    graph_payload = _json_local.dumps({
        "title":    title,
        "unit":     unit,
        "duration": duration,
        "series":   series_out,
    }, separators=(",", ":"))

    summary = "\n".join(summary_lines)
    return f"{summary}\n§GRAPH§{graph_payload}§GRAPH§"

def get_coredns_health() -> str:
    from kubernetes.stream import stream as _k8s_stream

    DNS_NS = "kube-system"
    DNS_PATTERNS = ["coredns", "core-dns", "kube-dns"]

    lines = ["CoreDNS Health Check:"]

    try:
        all_pods = _core.list_namespaced_pod(namespace=DNS_NS)
    except ApiException as e:
        return f"K8s API error reading kube-system pods: {e.reason}"

    dns_pods = [
        p for p in all_pods.items
        if any(pat in p.metadata.name.lower() for pat in DNS_PATTERNS)
        and "autoscaler" not in p.metadata.name.lower()
        and not p.metadata.name.lower().startswith("helm-install-")
        and (p.status.phase or "").lower() != "succeeded"
    ]
    autoscaler_pods = [
        p for p in all_pods.items
        if any(pat in p.metadata.name.lower() for pat in DNS_PATTERNS)
        and "autoscaler" in p.metadata.name.lower()
        and not p.metadata.name.lower().startswith("helm-install-")
        and (p.status.phase or "").lower() != "succeeded"
    ]

    if not dns_pods:
        lines.append(f"\n  No CoreDNS pods found in '{DNS_NS}'. Check if CoreDNS is deployed.")
        return "\n".join(lines)

    lines.append(f"\n  CoreDNS pods in '{DNS_NS}':")
    running_pods = []
    for pod in dns_pods:
        phase     = pod.status.phase or "Unknown"
        ready_cs  = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
        total_cs  = len(pod.status.container_statuses or [])
        restarts  = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
        node      = pod.spec.node_name or "?"
        flag      = "✅" if phase == "Running" and ready_cs == total_cs else "❌"
        lines.append(
            f"    {flag} {pod.metadata.name}  phase:{phase}  "
            f"ready:{ready_cs}/{total_cs}  restarts:{restarts}  node:{node}"
        )
        if phase == "Running" and ready_cs == total_cs:
            running_pods.append(pod)

    if autoscaler_pods:
        lines.append(f"\n  CoreDNS autoscaler:")
        for pod in autoscaler_pods:
            phase    = pod.status.phase or "Unknown"
            ready_cs = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
            total_cs = len(pod.status.container_statuses or [])
            restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
            flag     = "✅" if phase == "Running" and ready_cs == total_cs else "❌"
            lines.append(
                f"    {flag} {pod.metadata.name}  phase:{phase}  "
                f"ready:{ready_cs}/{total_cs}  restarts:{restarts}"
            )

    try:
        svcs = _core.list_namespaced_service(namespace=DNS_NS)
        dns_svcs = [
            s for s in svcs.items
            if any(pat in s.metadata.name.lower() for pat in DNS_PATTERNS)
        ]
        if dns_svcs:
            lines.append("\n  CoreDNS service(s):")
            for svc in dns_svcs:
                cluster_ip = svc.spec.cluster_ip or "?"
                ports = ", ".join(
                    f"{p.port}/{p.protocol}" for p in (svc.spec.ports or [])
                )
                lines.append(f"    {svc.metadata.name}  clusterIP:{cluster_ip}  ports:[{ports}]")
        else:
            lines.append("\n  ⚠ No CoreDNS service found in kube-system.")
    except ApiException:
        lines.append("\n  ⚠ Could not retrieve CoreDNS service.")

    def _exec_in_pod(pod_name: str, ns: str, cmd: str) -> str:
        try:
            resp = _k8s_stream(
                _core.connect_get_namespaced_pod_exec,
                pod_name, ns,
                command=["/bin/sh", "-c", cmd],
                stderr=True, stdin=False, stdout=True, tty=False,
                _preload_content=True,
            )
            return resp.strip() if isinstance(resp, str) else ""
        except Exception as exc:
            return f"[exec failed: {exc}]"

    # Only use pods whose name matches the actual CoreDNS deployment pattern
    coredns_test_pods = [p for p in running_pods
                         if "coredns" in p.metadata.name.lower()
                         and "autoscaler" not in p.metadata.name.lower()
                         and not p.metadata.name.lower().startswith("helm-install-")]

    if coredns_test_pods:
        lines.append("\n  DNS resolution test:")

        test_pod_name = coredns_test_pods[0].metadata.name
        lines.append(f"  (running nslookup from pod: {test_pod_name})")

        resolv = _exec_in_pod(test_pod_name, DNS_NS, "cat /etc/resolv.conf 2>/dev/null")
        lines.append(f"  /etc/resolv.conf (from {test_pod_name}):")
        for rline in resolv.splitlines():
            lines.append(f"    {rline}")

        nameserver = ""
        search_domain = ""
        for rline in resolv.splitlines():
            rline = rline.strip()
            if rline.startswith("nameserver"):
                parts = rline.split()
                if len(parts) >= 2:
                    nameserver = parts[1]
            if rline.startswith("search"):
                parts = rline.split()
                if len(parts) >= 2:
                    search_domain = parts[1]

        test_targets = []
        if search_domain:
            test_targets.append(f"console-cdp.apps.{search_domain}")
        try:
            ingresses = _net.list_ingress_for_all_namespaces()
            for ing in (ingresses.items or []):
                for rule in (ing.spec.rules or []):
                    if rule.host:
                        test_targets.append(rule.host)
                        break
                if len(test_targets) >= 3:
                    break
        except Exception:
            pass

        if not test_targets:
            test_targets = ["kubernetes.default.svc.cluster.local"]

        import logging as _logging
        _dns_log = _logging.getLogger("tools.coredns")

        _dns_log.info(f"[coredns] test_pod={test_pod_name} nameserver={nameserver!r} search={search_domain!r}")
        _dns_log.info(f"[coredns] test_targets={test_targets[:3]}")

        lines.append(f"\n  nslookup tests (nameserver: {nameserver or 'default'}, search: {search_domain or 'none'}):")
        for target in test_targets[:3]:
            cmd = f"nslookup {target} 2>&1"
            _dns_log.info(f"[coredns] exec cmd: {cmd!r} in pod {test_pod_name}")
            output = _exec_in_pod(test_pod_name, DNS_NS, cmd)
            _dns_log.info(f"[coredns] raw output: {output!r}")
            _out_lower = output.lower()
            _is_failure = (
                not output
                or "[exec failed" in output
                or "bad address" in _out_lower
                or "can't resolve" in _out_lower
                or "nxdomain" in _out_lower
                or "server can't find" in _out_lower
                or "no answer" in _out_lower
                or "timed out" in _out_lower
                or "connection refused" in _out_lower
                or re.search(r'\bfailed\b', _out_lower) is not None
            )
            _has_address = bool(re.search(r'address[\s\d:]+\d+\.\d+\.\d+\.\d+', _out_lower))
            ok = _has_address and not _is_failure
            _dns_log.info(f"[coredns] has_address={_has_address} is_failure={_is_failure} ok={ok}")
            flag = "✅" if ok else "❌"
            lines.append(f"    {flag} nslookup {target}")
            for oline in output.splitlines():
                lines.append(f"       {oline}")
    else:
        lines.append("\n  ⚠ DNS resolution test skipped — no running CoreDNS pod available.")

    return "\n".join(lines)

def get_service(namespace: str = "all", search: str = None) -> str:
    """
    List Kubernetes services in a namespace or all namespaces.
    Optionally filter by a search term in the service name.
    Always shows a Markdown table, even if fallback to all services.
    """
    try:
        svcs = (_core.list_service_for_all_namespaces()
                if namespace == "all"
                else _core.list_namespaced_service(namespace=namespace))
        if not svcs.items:
            return f"No services found in '{namespace}'."

        table_rows = []
        for svc in svcs.items:
            if search and search.lower() not in svc.metadata.name.lower():
                continue
            stype    = svc.spec.type or "ClusterIP"
            ports    = ", ".join(f"{p.port}/{p.protocol}" for p in (svc.spec.ports or []))
            selector = svc.spec.selector or {}
            flag     = "✓ Present" if selector else "⚠ No selector"
            table_rows.append((svc.metadata.namespace, svc.metadata.name, stype, ports, flag))

        fallback_msg = ""
        if search and not table_rows:
            table_rows = []
            for svc in svcs.items:
                stype    = svc.spec.type or "ClusterIP"
                ports    = ", ".join(f"{p.port}/{p.protocol}" for p in (svc.spec.ports or []))
                selector = svc.spec.selector or {}
                flag     = "✓ Present" if selector else "⚠ No selector"
                table_rows.append((svc.metadata.namespace, svc.metadata.name, stype, ports, flag))

        md_lines = [fallback_msg] if fallback_msg else []
        if namespace == "all":
            md_lines.append("As no namespace was mentioned, I checked across all namespaces.")
            md_lines.append("| NAMESPACE | NAME | TYPE | PORTS | SELECTOR STATUS |")
            md_lines.append("|---|---|---|---|---|")
            for ns, name, stype, ports, flag in table_rows:
                md_lines.append(f"| `{ns}` | `{name}` | {stype} | {ports} | {flag} |")
        else:
            md_lines.append("| NAME | TYPE | PORTS | SELECTOR STATUS |")
            md_lines.append("|---|---|---|---|")
            for _, name, stype, ports, flag in table_rows:
                md_lines.append(f"| `{name}` | {stype} | {ports} | {flag} |")

        return "\n".join(md_lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_ingress(namespace: str = "all", name: str = "", port: int = 0) -> str:
    def _get_ports(ing) -> list:
        ports = set()
        if ing.spec.tls:
            ports.add(443)

        ann = ing.metadata.annotations or {}
        for v in ann.values():
            if "443" in str(v):
                ports.add(443)

        for rule in (ing.spec.rules or []):
            if rule.http:
                for path in (rule.http.paths or []):
                    svc = path.backend.service
                    if svc and svc.port:
                        p = svc.port.number
                        if p:
                            ports.add(p)

        if not ports:
            ports.add(80)
        return sorted(ports)

    def _fmt(ing) -> str:
        cls   = ing.spec.ingress_class_name or "default"
        hosts = [rule.host or "*" for rule in (ing.spec.rules or [])]
        ports = _get_ports(ing)
        
        lb = []
        if ing.status.load_balancer and ing.status.load_balancer.ingress:
            for status in ing.status.load_balancer.ingress:
                if status.ip: lb.append(status.ip)
                elif status.hostname: lb.append(status.hostname)

        out = [
            f"### Ingress: `{ing.metadata.namespace}/{ing.metadata.name}`",
            f"- **Class**: {cls}",
            f"- **Hosts**: {', '.join(hosts) or 'none'}",
            f"- **Ports**: {', '.join(str(p) for p in ports)}",
            f"- **LB IP**: {', '.join(lb) or 'pending'}",
        ]
        
        ann = ing.metadata.annotations or {}
        if ann:
            out.append("\n**Annotations:**")
            for k, v in ann.items():
                out.append(f"- `{k}`: {v}")
                
        for rule in (ing.spec.rules or []):
            if rule.http:
                host_str = rule.host or "*"
                out.append(f"\n**Rules ({host_str}):**")
                for path in (rule.http.paths or []):
                    svc = path.backend.service
                    port_str = svc.port.number or svc.port.name
                    path_str = path.path or "/"
                    out.append(f"- `{path_str}` ➔ Service `{svc.name}:{port_str}` ({path.path_type})")
                    
        tls = ing.spec.tls or []
        if tls:
            out.append("\n**TLS:**")
            for t in tls:
                hosts_str = ", ".join(t.hosts) if t.hosts else "none"
                out.append(f"- Secret `{t.secret_name}` for hosts: {hosts_str}")
                
        return "\n".join(out)

    try:
        all_ings = _net.list_ingress_for_all_namespaces()
        pool = all_ings.items

        if port:
            pool = [ing for ing in pool if port in _get_ports(ing)]
            if not pool:
                return f"No ingresses found exposing port {port} in any namespace."
            
            out = [f"### Ingresses exposing port {port}\n", "| NAMESPACE | NAME | HOSTS | PORTS |", "|---|---|---|---|"]
            for ing in pool:
                hosts = [rule.host or "*" for rule in (ing.spec.rules or [])]
                ports = _get_ports(ing)
                out.append(f"| {ing.metadata.namespace} | {ing.metadata.name} | {', '.join(hosts)} | {', '.join(str(p) for p in ports)} |")
            return "\n".join(out)

        if name:
            if "." in name:
                host_lower = name.lower()
                matches = []
                for ing in pool:
                    for rule in (ing.spec.rules or []):
                        if rule.host and rule.host.lower() == host_lower:
                            matches.append(ing)
                            break
                if not matches:
                    return (
                        f"No ingress found with hostname '{name}' in any namespace. "
                        f"Use `get_ingress_status(namespace='all')` to list all ingresses."
                    )
                return "\n\n---\n\n".join(_fmt(ing) for ing in matches)

            if namespace != "all":
                try:
                    ing = _net.read_namespaced_ingress(name=name, namespace=namespace)
                    return _fmt(ing)
                except ApiException as e:
                    if e.status == 404:
                        return f"Ingress '{name}' not found in namespace '{namespace}'."
                    raise
            else:
                matches = [i for i in pool if i.metadata.name == name]
                if not matches:
                    return f"Ingress '{name}' not found in any namespace."
                return "\n\n---\n\n".join(_fmt(ing) for ing in matches)

        if namespace != "all":
            pool = [i for i in pool if i.metadata.namespace == namespace]
            
        if not pool:
            return f"No Ingresses found in '{namespace}'."
            
        out = [f"### Ingresses in '{namespace}'\n"]
        if namespace == "all":
            out.extend(["| NAMESPACE | NAME | HOSTS | PORTS | LOAD BALANCER |", "|---|---|---|---|---|"])
        else:
            out.extend(["| NAME | HOSTS | PORTS | LOAD BALANCER |", "|---|---|---|---|"])
            
        for ing in pool:
            hosts = [rule.host or "*" for rule in (ing.spec.rules or [])]
            ports = _get_ports(ing)
            lb = []
            if ing.status.load_balancer and ing.status.load_balancer.ingress:
                for status in ing.status.load_balancer.ingress:
                    if status.ip: lb.append(status.ip)
                    elif status.hostname: lb.append(status.hostname)
                    
            if namespace == "all":
                out.append(f"| {ing.metadata.namespace} | {ing.metadata.name} | {', '.join(hosts) or '*'} | {', '.join(str(p) for p in ports)} | {', '.join(lb) or 'pending'} |")
            else:
                out.append(f"| {ing.metadata.name} | {', '.join(hosts) or '*'} | {', '.join(str(p) for p in ports)} | {', '.join(lb) or 'pending'} |")
                
        return "\n".join(out)
        
    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_configmap_list(namespace: str = "all", search: str | None = None, filter_keys: list = None) -> str:
    _CERT_KEY_HINTS = {"ca.crt", "tls.crt", "tls.key", "ca-bundle", "ca-certificates",
                       "ca.pem", "cert.pem", "certificate", "ssl.crt", "ssl.key"}
    try:
        if namespace != "all":
            try:
                cm_items = _core.list_namespaced_config_map(namespace=namespace).items
                if not cm_items:
                    cm_items = _core.list_config_map_for_all_namespaces().items
            except Exception:
                cm_items = _core.list_config_map_for_all_namespaces().items
        else:
            cm_items = _core.list_config_map_for_all_namespaces().items

        if not cm_items:
            return "No ConfigMaps found."

        skip = {"kube-root-ca.crt"}
        cm_items = [cm for cm in cm_items if cm.metadata.name not in skip]

        filtered = []
        if search:
            s = search.lower()
            for cm in cm_items:
                if s in cm.metadata.name.lower() or s in cm.metadata.namespace.lower():
                    filtered.append(cm)
        else:
            filtered = cm_items

        if search and not filtered:
            filtered = cm_items

        lines = []
        lines.append("| NAMESPACE | CONFIGMAP | KEYS | TYPE |")
        lines.append("|---|---|---|---|")

        for cm in sorted(filtered, key=lambda x: (x.metadata.namespace, x.metadata.name)):
            ns = cm.metadata.namespace
            name = cm.metadata.name
            data = cm.data or {}
            keys = list(data.keys())

            if filter_keys:
                fk = [f.lower() for f in filter_keys]
                keys = [k for k in keys if any(f in k.lower() for f in fk)]
                if not keys:
                    continue

            cert_keys = [k for k in keys
                         if k in _CERT_KEY_HINTS
                         or any(h in k.lower() for h in ("cert", "tls", "ssl", "ca.", ".crt", ".pem"))]

            tag = "cert" if cert_keys else "-"

            lines.append(
                f"| `{ns}` | `{name}` | {keys if keys else '-'} | {tag} |"
            )

        if len(lines) == 2:
            return "No matching ConfigMaps found."

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_secret_list(namespace: str = "all", name: str = "", pod_name: str = None,
                    filter_keys: list = None, decode: bool = False) -> str:
    import base64 as _b64

    def _decode(val: str) -> str:
        try:
            return _b64.b64decode(val).decode("utf-8", errors="replace")
        except Exception:
            return "<decode error>"

    hidden_msg = "<hidden> — enable 'Show Secret Values' in ⚙ Settings → Security to decode."

    CERT_KEY_HINTS = {"tls", "cert", "certs", "certificate", "crt", "pem", "key", "ca", "ssl", "x509"}

    try:
        if pod_name:
            try:
                pod = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
            except ApiException as e:
                if e.status == 404:
                    return f"Pod '{pod_name}' not found in namespace '{namespace}'."
                raise

            secrets = set()
            configmaps = set()

            for vol in pod.spec.volumes or []:
                if vol.secret:
                    secrets.add(vol.secret.secret_name)
                if vol.config_map:
                    configmaps.add(vol.config_map.name)

            lines = [f"Secrets and ConfigMaps attached to pod '{namespace}/{pod_name}':"]

            if secrets:
                lines.append("  Secrets:")
                for sname in sorted(secrets):
                    try:
                        s = _core.read_namespaced_secret(name=sname, namespace=namespace)
                        data = s.data or {}
                        lines.append(f"    {sname} [type={s.type}]")
                        for k, v in data.items():
                            if decode:
                                val_str = _decode(v)
                            else:
                                is_cert = any(h in k.lower() for h in CERT_KEY_HINTS)
                                val_str = _decode(v) if is_cert else hidden_msg
                            lines.append(f"      {k}: {val_str}")
                    except ApiException:
                        lines.append(f"    {sname}: <fetch error>")
            else:
                lines.append("  Secrets: None")

            if configmaps:
                lines.append("  ConfigMaps:")
                for cm_name in sorted(configmaps):
                    try:
                        cm = _core.read_namespaced_config_map(name=cm_name, namespace=namespace)
                        keys = list(cm.data.keys() if cm.data else [])
                        lines.append(f"    {cm_name}: keys={keys if keys else 'None'}")
                    except ApiException:
                        lines.append(f"    {cm_name}: <fetch error>")
            else:
                lines.append("  ConfigMaps: None")

            return "\n".join(lines)

        if name:
            try:
                secret = _core.read_namespaced_secret(name=name, namespace=namespace)
            except ApiException as e:
                if e.status == 404:
                    return f"Secret '{name}' not found in namespace '{namespace}'."
                raise

            data = secret.data or {}

            lines = [f"Secret: {namespace}/{name}", f"  Type: {secret.type}"]

            if data:
                lines.append("  Data:")
                for k, v in data.items():
                    is_cert = any(h in k.lower() for h in CERT_KEY_HINTS)
                    should_decode = decode or is_cert or any(h in name.lower() for h in CERT_KEY_HINTS)
                    val_str = _decode(v) if should_decode else hidden_msg
                    lines.append(f"    {k}: {val_str}")
            else:
                lines.append("  Data: None")

            return "\n".join(lines)

        secrets = _core.list_namespaced_secret(namespace=namespace)
        if not secrets.items:
            return f"No secrets in namespace '{namespace}'."

        _CERT_TYPES = {"kubernetes.io/tls", "helm.sh/release.v1"}

        by_type: dict = {}
        for s in secrets.items:
            t = s.type or "Opaque"
            keys = list((s.data or {}).keys())
            is_cert = (
                t in _CERT_TYPES
                or any(k.lower().find(x) != -1 for k in keys for x in CERT_KEY_HINTS)
                or any(h in k.lower() for k in keys for h in CERT_KEY_HINTS)
            )
            by_type.setdefault(t, []).append((s.metadata.name, keys, is_cert))

        lines = [f"Secrets in '{namespace}' ({len(secrets.items)} total):"]

        for stype, entries in sorted(by_type.items()):
            lines.append(f"  [{stype}] ({len(entries)})")
            for n, keys, is_cert in sorted(entries):
                tag = " [cert/TLS]" if is_cert else ""
                lines.append(f"    {n}: keys={keys}{tag}")

        if filter_keys:
            _fk_lower = [f.lower() for f in filter_keys]
            filtered_lines = [line for line in lines if any(f in line.lower() for f in _fk_lower)]
            if filtered_lines:
                lines = ["Filtered secrets:"] + filtered_lines
            else:
                lines = [f"No secrets in '{namespace}' match keys {filter_keys}."]

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_resource_quotas(namespace: str = "all", search: str | None = None) -> str:
    try:
        if namespace != "all":
            try:
                q_items = _core.list_namespaced_resource_quota(namespace=namespace).items
                if not q_items:
                    q_items = _core.list_resource_quota_for_all_namespaces().items
            except Exception:
                q_items = _core.list_resource_quota_for_all_namespaces().items
        else:
            q_items = _core.list_resource_quota_for_all_namespaces().items

        if not q_items:
            return "No ResourceQuotas found."

        filtered = []
        if search:
            s = search.lower()
            for q in q_items:
                if s in q.metadata.name.lower() or s in q.metadata.namespace.lower():
                    filtered.append(q)
        else:
            filtered = q_items

        if search and not filtered:
            filtered = q_items

        lines = []
        lines.append("| NAMESPACE | QUOTA | RESOURCE | USED | HARD |")
        lines.append("|---|---|---|---|---|")

        for q in sorted(filtered, key=lambda x: (x.metadata.namespace, x.metadata.name)):
            ns = q.metadata.namespace
            name = q.metadata.name
            hard = q.status.hard or {}
            used = q.status.used or {}

            for res in sorted(hard.keys()):
                lines.append(
                    f"| `{ns}` | `{name}` | {res} | {used.get(res, '0')} | {hard.get(res)} |"
                )

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_limit_ranges(namespace: str = "all", search: str | None = None) -> str:
    try:
        if namespace != "all":
            try:
                lrs_items = _core.list_namespaced_limit_range(namespace=namespace).items
                if not lrs_items:
                    lrs_items = _core.list_limit_range_for_all_namespaces().items
            except Exception:
                lrs_items = _core.list_limit_range_for_all_namespaces().items
        else:
            lrs_items = _core.list_limit_range_for_all_namespaces().items

        if not lrs_items:
            return "No LimitRanges found."

        filtered = []
        if search:
            s = search.lower()
            for lr in lrs_items:
                if s in lr.metadata.name.lower() or s in lr.metadata.namespace.lower():
                    filtered.append(lr)
        else:
            filtered = lrs_items

        if search and not filtered:
            filtered = lrs_items

        def _get(v, key):
            if not v:
                return "-"
            return v.get(key, "-")

        lines = []
        lines.append("| NAMESPACE | LIMITRANGE | TYPE | CPU_MAX | CPU_MIN | CPU_DEFAULT | MEM_MAX | MEM_MIN | MEM_DEFAULT |")
        lines.append("|---|---|---|---|---|---|---|---|---|")

        for lr in sorted(filtered, key=lambda x: (x.metadata.namespace, x.metadata.name)):
            ns = lr.metadata.namespace
            name = lr.metadata.name

            for item in (lr.spec.limits or []):
                lines.append(
                    f"| `{ns}` | `{name}` | {item.type} | "
                    f"{_get(item.max, 'cpu')} | {_get(item.min, 'cpu')} | {_get(item.default, 'cpu')} | "
                    f"{_get(item.max, 'memory')} | {_get(item.min, 'memory')} | {_get(item.default, 'memory')} |"
                )

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_serviceaccounts(namespace: str = "all", search: str | None = None) -> str:
    try:
        if namespace != "all":
            try:
                sa_items = _core.list_namespaced_service_account(namespace=namespace).items
                if not sa_items:
                    sa_items = _core.list_service_account_for_all_namespaces().items
            except Exception:
                sa_items = _core.list_service_account_for_all_namespaces().items
        else:
            sa_items = _core.list_service_account_for_all_namespaces().items

        if not sa_items:
            return "No ServiceAccounts found."

        filtered = []
        if search:
            s = search.lower()
            for sa in sa_items:
                if s in sa.metadata.name.lower() or s in sa.metadata.namespace.lower():
                    filtered.append(sa)
        else:
            filtered = sa_items

        if search and not filtered:
            filtered = sa_items

        rb_items = _rbac.list_role_binding_for_all_namespaces().items
        crb_items = _rbac.list_cluster_role_binding().items

        lines = []
        lines.append("| NAMESPACE | SERVICEACCOUNT | ROLES | CLUSTER ROLES |")
        lines.append("|---|---|---|---|")

        for sa in sorted(filtered, key=lambda x: (x.metadata.namespace, x.metadata.name)):
            ns = sa.metadata.namespace
            name = sa.metadata.name

            roles = []
            for rb in rb_items:
                if rb.metadata.namespace != ns:
                    continue
                for subj in rb.subjects or []:
                    if subj.kind == "ServiceAccount" and subj.name == name and subj.namespace == ns:
                        roles.append(rb.role_ref.name)

            cluster_roles = []
            for crb in crb_items:
                for subj in crb.subjects or []:
                    if subj.kind == "ServiceAccount" and subj.name == name and subj.namespace == ns:
                        cluster_roles.append(crb.role_ref.name)

            roles_str = ", ".join(sorted(set(roles))) if roles else "-"
            cr_str = ", ".join(sorted(set(cluster_roles))) if cluster_roles else "-"

            lines.append(f"| `{ns}` | `{name}` | {roles_str} | {cr_str} |")

        return "\n".join(lines)

    except Exception as e:
        return f"K8s error: {str(e)}"

def get_cluster_role_bindings() -> str:
    try:
        crbs = _rbac.list_cluster_role_binding()
        if not crbs.items:
            return "No ClusterRoleBindings found."
        lines = ["ClusterRoleBindings:"]
        for crb in crbs.items:
            role     = crb.role_ref.name
            subjects = [f"{s.kind}/{s.name}" for s in (crb.subjects or [])]
            lines.append(f"  {crb.metadata.name}: role={role} → {subjects}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_pod_images(namespace: str = "all", search: str | None = None) -> str:
    try:
        pods = (_core.list_pod_for_all_namespaces()
                if namespace == "all"
                else _core.list_namespaced_pod(namespace=namespace))
    except ApiException as e:
        return f"K8s API error: {e.reason}"

    if not pods.items:
        return f"No pods found in namespace '{namespace}'."

    rows = []
    for pod in sorted(pods.items, key=lambda p: (p.metadata.namespace, p.metadata.name)):
        ns_name  = pod.metadata.namespace
        pod_name = pod.metadata.name

        for c in (pod.spec.containers or []):
            image = c.image or "?"

            if search and search.lower() not in pod_name.lower() and search.lower() not in image.lower():
                continue

            rows.append(f"- `{ns_name}/{pod_name}` [{c.name}]: `{image}`")

    # fallback if search yields nothing
    if search and not rows:
        for pod in sorted(pods.items, key=lambda p: (p.metadata.namespace, p.metadata.name)):
            ns_name  = pod.metadata.namespace
            pod_name = pod.metadata.name

            for c in (pod.spec.containers or []):
                image = c.image or "?"
                rows.append(f"- `{ns_name}/{pod_name}` [{c.name}]: `{image}`")

    if not rows:
        return f"No containers found in namespace '{namespace}'."

    lines = []

    if namespace == "all":
        lines.append("As no namespace was specified, showing pod images from all namespaces.\n")

    lines.extend(rows)

    return "\n".join(lines)

def run_cluster_health(namespace: str = "all", show_all: bool = True, raw_output: bool = True) -> str:
    try:
        report = []

        nodes = _core.list_node().items
        critical_nodes, moderate_nodes = [], []
        node_capacity = {}

        for node in nodes:
            name = node.metadata.name
            conditions = {c.type: c.status for c in (node.status.conditions or [])}
            if conditions.get("Ready") != "True":
                critical_nodes.append(name)
            elif conditions.get("MemoryPressure") == "True" or conditions.get("DiskPressure") == "True":
                moderate_nodes.append(name)

            node_capacity[name] = {
                "cpu": node.status.capacity.get("cpu"),
                "memory": node.status.capacity.get("memory")
            }

        report.append(f"Nodes: {len(nodes)} total")
        if critical_nodes:
            report.append(f"  Critical (Not Ready): {len(critical_nodes)} — {', '.join(critical_nodes)}")
        if moderate_nodes:
            report.append(f"  Moderate (Pressure issues): {len(moderate_nodes)} — {', '.join(moderate_nodes)}")

        ns_status = get_namespace_status(namespace=namespace, show_all=show_all)
        report.append("\nNamespace/Pod summary:\n" + ns_status)

        pvcs = _core.list_persistent_volume_claim_for_all_namespaces().items
        unbound_pvcs = [f"{p.metadata.namespace}/{p.metadata.name}" for p in pvcs if p.status.phase != "Bound"]
        if unbound_pvcs:
            report.append(f"\nPersistentVolumeClaims not bound: {len(unbound_pvcs)}")
            if raw_output:
                report.extend(f"  {p}" for p in unbound_pvcs)

        _net = _k8s.NetworkingV1Api()
        ingresses = _net.list_ingress_for_all_namespaces().items
        failed_ing = [f"{i.metadata.namespace}/{i.metadata.name}" for i in ingresses if not i.status.load_balancer.ingress]
        if failed_ing:
            report.append(f"\nIngresses without active load balancer: {len(failed_ing)}")
            if raw_output:
                report.extend(f"  {i}" for i in failed_ing)

        core_system_ns = ["kube-system", "coredns", "longhorn-system", "ingress-nginx"]
        system_unhealthy = []
        for ns in core_system_ns:
            try:
                pods = _core.list_namespaced_pod(namespace=ns, limit=1000)
                for pod in pods.items:
                    ready = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                    total = len(pod.spec.containers)
                    if ready < total:
                        system_unhealthy.append(f"{ns}/{pod.metadata.name}")
            except Exception:
                pass

        if system_unhealthy:
            report.append(f"\nSystem components with unhealthy pods: {len(system_unhealthy)}")
            if raw_output:
                report.extend(f"  {p}" for p in system_unhealthy)

        # --- Cluster-wide resource usage ---
        cpu_total = 0
        mem_total = 0
        for ns in _core.list_namespace().items:
            try:
                pods = _core.list_namespaced_pod(ns.metadata.name, limit=1000)
                for pod in pods.items:
                    for c in pod.spec.containers:
                        req = c.resources.requests or {}
                        cpu_total += int(req.get("cpu", 0))
                        mem_total += int(req.get("memory", 0))
            except Exception:
                continue

        report.append(f"\nCluster resource requests:")
        report.append(f"  CPU requested: {cpu_total} cores")
        report.append(f"  Memory requested: {mem_total} bytes")

        return "\n".join(report)

    except Exception as e:
        return f"[ERROR] Unexpected error: {str(e)}"

def get_pod_storage(namespace: str = "all", search: str | None = None) -> str:
    try:
        pods = (
            _core.list_namespaced_pod(namespace=namespace).items
            if namespace != "all"
            else _core.list_pod_for_all_namespaces().items
        )

        if not pods:
            return f"No pods found in namespace '{namespace}'."

        pvc_pods = []

        for pod in pods:
            pod_entries = []

            for vol in pod.spec.volumes or []:
                if vol.persistent_volume_claim:
                    pvc_name = vol.persistent_volume_claim.claim_name
                    pvc = _core.read_namespaced_persistent_volume_claim(
                        pvc_name, pod.metadata.namespace
                    )

                    modes = pvc.spec.access_modes or []
                    mode_str = ",".join(
                        [m for m in modes if m in ["ReadWriteOnce", "ReadWriteMany"]]
                    ) or "Unknown"

                    storage_class = pvc.spec.storage_class_name or "Unknown"

                    pod_entries.append((pvc_name, mode_str, storage_class))

            if pod_entries:
                pvc_pods.append((pod, pod_entries))

        if not pvc_pods:
            return f"No pods with PVCs found in namespace '{namespace}'."

        filtered = []
        if search:
            s = search.lower()
            for pod, entries in pvc_pods:
                if s in pod.metadata.name.lower() or (
                    namespace != "all" and s in pod.metadata.namespace.lower()
                ):
                    filtered.append((pod, entries))
        else:
            filtered = pvc_pods

        if search and not filtered:
            filtered = pvc_pods

        lines = []
        lines.append("| NAMESPACE | POD | PVC | ACCESS MODE | STORAGE CLASS |")
        lines.append("|---|---|---|---|---|")

        for pod, entries in sorted(filtered, key=lambda x: (x[0].metadata.namespace, x[0].metadata.name)):
            for pvc_name, mode_str, storage_class in entries:
                lines.append(
                    f"| `{pod.metadata.namespace}` | `{pod.metadata.name}` | `{pvc_name}` | {mode_str} | {storage_class} |"
                )

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"
    
def get_namespace_status(namespace: str = "all", show_all: bool = False, sort_by: str | None = None, limit: int | None = None) -> str:
    try:
        if namespace != "all":
            try:
                _core.read_namespace(name=namespace)
            except ApiException as e:
                if e.status == 404:
                    return f"Namespace '{namespace}' does not exist."
                raise

        ns_items = _core.list_namespace().items if namespace == "all" else [_core.read_namespace(namespace)]
        ns_pod_info = {}

        for ns in ns_items:
            ns_name = ns.metadata.name
            pods = _core.list_namespaced_pod(ns_name, limit=1000).items
            running = 0
            other_states = {}

            for pod in pods:
                phase = pod.status.phase or "Unknown"
                if phase == "Running":
                    running += 1
                else:
                    other_states[phase] = other_states.get(phase, 0) + 1

            ns_pod_info[ns_name] = {"running": running, "other": other_states}

        if sort_by == "pods_asc":
            sorted_ns = sorted(ns_pod_info.items(), key=lambda x: x[1]["running"] + sum(x[1]["other"].values()))
        elif sort_by == "pods_desc":
            sorted_ns = sorted(ns_pod_info.items(), key=lambda x: x[1]["running"] + sum(x[1]["other"].values()), reverse=True)
        elif sort_by == "name_asc":
            sorted_ns = sorted(ns_pod_info.items(), key=lambda x: x[0])
        elif sort_by == "name_desc":
            sorted_ns = sorted(ns_pod_info.items(), key=lambda x: x[0], reverse=True)
        else:
            sorted_ns = sorted(ns_pod_info.items())

        if limit is not None:
            sorted_ns = sorted_ns[:limit]

        lines = [
            "| Namespace | Total Pods | Running | Other State |",
            "|-----------|------------|---------|--------------|"
        ]

        pod_totals = {}
        for ns_name, info in sorted_ns:
            total = info["running"] + sum(info["other"].values())
            pod_totals[ns_name] = total

            if info["other"]:
                other_str = ", ".join(f"{k}: {v}" for k, v in info["other"].items())
            else:
                other_str = "-"

            lines.append(
                f"| {ns_name} | {total} | {info['running']} | {other_str} |"
            )

        if show_all:
            lines.append("")
            lines.append("_Namespaces pod summary (all details included above)._")

        if pod_totals:
            least_ns = min(pod_totals, key=pod_totals.get)
            most_ns = max(pod_totals, key=pod_totals.get)
            lines.append("")
            lines.append(f"_Namespace with the least pods: {least_ns} ({pod_totals[least_ns]} pods)_")
            lines.append(f"_Namespace with the most pods: {most_ns} ({pod_totals[most_ns]} pods)_")

        return "\n".join(lines)

    except ApiException as e:
        return f"[ERROR] K8s API error listing namespaces: {e.reason}"

def _safe_reason(e) -> str:
    try:
        reason = getattr(e, "reason", None) or ""
        status = getattr(e, "status", 0)
        if reason:
            return f"HTTP {status} {reason}"
        return str(e).replace("\n", " ")[:80]
    except Exception:
        return "Unknown API error"
_ALLOW_WRITES     = os.getenv("KUBECTL_ALLOW_WRITES", "false").lower() in ("1", "true", "yes")

_KUBECTL_READ_VERBS  = {
    "get", "describe", "logs", "top", "rollout", "auth",
    "api-resources", "api-versions", "version", "cluster-info",
    "explain", "diff", "events",
}
_KUBECTL_WRITE_VERBS = {
    "apply", "create", "delete", "patch", "replace", "scale",
    "edit", "label", "annotate", "taint", "drain", "cordon",
    "uncordon", "set", "run", "expose", "autoscale",
}
_BLOCKED_VERBS = {
    "exec": "exec is not supported; use kubectl logs or describe instead",
    "port-forward": "port-forward is not supported in unattended mode",
    "attach": "attach is not supported",
    "proxy": "proxy is not supported",
}

def _get_resource_fns(resource: str):
    r = resource.lower()
    if r in ("pod", "pods", "po"):
        return (
            lambda fs="": _paginate(_core.list_pod_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_core.list_namespaced_pod, ns, field_selector=fs),
            lambda name, ns: _core.read_namespaced_pod(name, ns),
            "Pod",
        )
    if r in ("deployment", "deployments", "deploy"):
        return (
            lambda fs="": _paginate(_apps.list_deployment_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_apps.list_namespaced_deployment, ns, field_selector=fs),
            lambda name, ns: _apps.read_namespaced_deployment(name, ns),
            "Deployment",
        )
    if r in ("replicaset", "replicasets", "rs"):
        return (
            lambda fs="": _paginate(_apps.list_replica_set_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_apps.list_namespaced_replica_set, ns, field_selector=fs),
            lambda name, ns: _apps.read_namespaced_replica_set(name, ns),
            "ReplicaSet",
        )
    if r in ("statefulset", "statefulsets", "sts"):
        return (
            lambda fs="": _paginate(_apps.list_stateful_set_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_apps.list_namespaced_stateful_set, ns, field_selector=fs),
            lambda name, ns: _apps.read_namespaced_stateful_set(name, ns),
            "StatefulSet",
        )
    if r in ("daemonset", "daemonsets", "ds"):
        return (
            lambda fs="": _paginate(_apps.list_daemon_set_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_apps.list_namespaced_daemon_set, ns, field_selector=fs),
            lambda name, ns: _apps.read_namespaced_daemon_set(name, ns),
            "DaemonSet",
        )
    if r in ("service", "services", "svc"):
        return (
            lambda fs="": _paginate(_core.list_service_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_core.list_namespaced_service, ns, field_selector=fs),
            lambda name, ns: _core.read_namespaced_service(name, ns),
            "Service",
        )
    if r in ("configmap", "configmaps", "cm"):
        return (
            lambda fs="": _paginate(_core.list_config_map_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_core.list_namespaced_config_map, ns, field_selector=fs),
            lambda name, ns: _core.read_namespaced_config_map(name, ns),
            "ConfigMap",
        )
    if r in ("secret", "secrets"):
        return (
            lambda fs="": _paginate(_core.list_secret_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_core.list_namespaced_secret, ns, field_selector=fs),
            lambda name, ns: _core.read_namespaced_secret(name, ns),
            "Secret",
        )
    if r in ("persistentvolumeclaim", "persistentvolumeclaims", "pvc", "pvcs"):
        return (
            lambda fs="": _paginate(_core.list_persistent_volume_claim_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_core.list_namespaced_persistent_volume_claim, ns, field_selector=fs),
            lambda name, ns: _core.read_namespaced_persistent_volume_claim(name, ns),
            "PersistentVolumeClaim",
        )
    if r in ("persistentvolume", "persistentvolumes", "pv", "pvs"):
        return (
            lambda fs="": _paginate(_core.list_persistent_volume, field_selector=fs),
            None,
            lambda name, ns: _core.read_persistent_volume(name),
            "PersistentVolume",
        )
    if r in ("node", "nodes", "no"):
        return (
            lambda fs="": _paginate(_core.list_node, field_selector=fs),
            None,
            lambda name, ns: _core.read_node(name),
            "Node",
        )
    if r in ("namespace", "namespaces", "ns"):
        return (
            lambda fs="": _paginate(_core.list_namespace, field_selector=fs),
            None,
            lambda name, ns: _core.read_namespace(name),
            "Namespace",
        )
    if r in ("job", "jobs"):
        return (
            lambda fs="": _paginate(_batch.list_job_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_batch.list_namespaced_job, ns, field_selector=fs),
            lambda name, ns: _batch.read_namespaced_job(name, ns),
            "Job",
        )
    if r in ("cronjob", "cronjobs", "cj"):
        return (
            lambda fs="": _paginate(_batch.list_cron_job_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_batch.list_namespaced_cron_job, ns, field_selector=fs),
            lambda name, ns: _batch.read_namespaced_cron_job(name, ns),
            "CronJob",
        )
    if r in ("ingress", "ingresses", "ing"):
        return (
            lambda fs="": _paginate(_net.list_ingress_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_net.list_namespaced_ingress, ns, field_selector=fs),
            lambda name, ns: _net.read_namespaced_ingress(name, ns),
            "Ingress",
        )
    if r in ("horizontalpodautoscaler", "horizontalpodautoscalers", "hpa", "hpas"):
        return (
            lambda fs="": _paginate(_autoscaling.list_horizontal_pod_autoscaler_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_autoscaling.list_namespaced_horizontal_pod_autoscaler, ns, field_selector=fs),
            lambda name, ns: _autoscaling.read_namespaced_horizontal_pod_autoscaler(name, ns),
            "HorizontalPodAutoscaler",
        )
    if r in ("event", "events", "ev"):
        return (
            lambda fs="": _paginate(_core.list_event_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_core.list_namespaced_event, ns, field_selector=fs),
            lambda name, ns: _core.read_namespaced_event(name, ns),
            "Event",
        )
    if r in ("role", "roles"):
        return (
            lambda fs="": _paginate(_rbac.list_role_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_rbac.list_namespaced_role, ns, field_selector=fs),
            lambda name, ns: _rbac.read_namespaced_role(name, ns),
            "Role",
        )
    if r in ("clusterrole", "clusterroles"):
        return (
            lambda fs="": _paginate(_rbac.list_cluster_role, field_selector=fs),
            None,
            lambda name, ns: _rbac.read_cluster_role(name),
            "ClusterRole",
        )
    if r in ("rolebinding", "rolebindings"):
        return (
            lambda fs="": _paginate(_rbac.list_role_binding_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_rbac.list_namespaced_role_binding, ns, field_selector=fs),
            lambda name, ns: _rbac.read_namespaced_role_binding(name, ns),
            "RoleBinding",
        )
    if r in ("clusterrolebinding", "clusterrolebindings"):
        return (
            lambda fs="": _paginate(_rbac.list_cluster_role_binding, field_selector=fs),
            None,
            lambda name, ns: _rbac.read_cluster_role_binding(name),
            "ClusterRoleBinding",
        )
    if r in ("serviceaccount", "serviceaccounts", "sa"):
        return (
            lambda fs="": _paginate(_core.list_service_account_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_core.list_namespaced_service_account, ns, field_selector=fs),
            lambda name, ns: _core.read_namespaced_service_account(name, ns),
            "ServiceAccount",
        )
    if r in ("storageclass", "storageclasses", "sc"):
        return (
            lambda fs="": _paginate(_storage.list_storage_class, field_selector=fs),
            None,
            lambda name, ns: _storage.read_storage_class(name),
            "StorageClass",
        )
    if r in ("volumeattachment", "volumeattachments", "va"):
        return (
            lambda fs="": _paginate(_storage.list_volume_attachment, field_selector=fs),
            None,
            lambda name, ns: _storage.read_volume_attachment(name),
            "VolumeAttachment",
        )
    if r in ("endpoints", "endpoint", "ep"):
        return (
            lambda fs="": _paginate(_core.list_endpoints_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_core.list_namespaced_endpoints, ns, field_selector=fs),
            lambda name, ns: _core.read_namespaced_endpoints(name, ns),
            "Endpoints",
        )
    if r in ("networkpolicy", "networkpolicies", "np"):
        return (
            lambda fs="": _paginate(_net.list_network_policy_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_net.list_namespaced_network_policy, ns, field_selector=fs),
            lambda name, ns: _net.read_namespaced_network_policy(name, ns),
            "NetworkPolicy",
        )
    if r in ("poddisruptionbudget", "poddisruptionbudgets", "pdb"):
        return (
            lambda fs="": _paginate(_policy.list_pod_disruption_budget_for_all_namespaces, field_selector=fs),
            lambda ns, fs="": _paginate(_policy.list_namespaced_pod_disruption_budget, ns, field_selector=fs),
            lambda name, ns: _policy.read_namespaced_pod_disruption_budget(name, ns),
            "PodDisruptionBudget",
        )
    if r in ("volumesnapshot", "volumesnapshots", "vs"):
        version = "v1"
        group   = "snapshot.storage.k8s.io"
        plural  = "volumesnapshots"
        custom  = _k8s.CustomObjectsApi()
        return (
            lambda fs="", _p=plural, _g=group, _v=version: _list_custom_all(_p, _g, _v),
            lambda ns, fs="", _p=plural, _g=group, _v=version: _list_custom_ns(ns, _p, _g, _v),
            lambda name, ns, _p=plural, _g=group, _v=version: _get_custom(name, ns, _p, _g, _v),
            "VolumeSnapshot",
        )
    if "." in resource:
        parts = resource.split(".", 1)
        plural, group = parts[0], parts[1]
        custom = _k8s.CustomObjectsApi()
        version = _resolve_crd_version(group, plural)
        return (
            lambda fs="", _p=plural, _g=group, _v=version: _list_custom_all(_p, _g, _v),
            lambda ns, fs="", _p=plural, _g=group, _v=version: _list_custom_ns(ns, _p, _g, _v),
            lambda name, ns, _p=plural, _g=group, _v=version: _get_custom(name, ns, _p, _g, _v),
            resource,
        )
    return None    

def _paginate(list_fn, *args, field_selector="", **kwargs):
    items, _cont = [], None
    while True:
        kw = {"limit": 500, **kwargs}
        if field_selector:
            kw["field_selector"] = field_selector
        if _cont:
            kw["_continue"] = _cont
        if args:
            page = list_fn(*args, **kw)
        else:
            page = list_fn(**kw)
        items.extend(page.items)
        _cont = (page.metadata._continue
                 if page.metadata and page.metadata._continue else None)
        if not _cont:
            break
    return items

def _resolve_crd_version(group: str, plural: str) -> str:
    try:
        ext = _k8s.ApiextensionsV1Api()
        crd = ext.read_custom_resource_definition(f"{plural}.{group}")
        for v in crd.spec.versions:
            if v.storage:
                return v.name
        return crd.spec.versions[0].name
    except Exception:
        return "v1"

def _list_custom_all(plural: str, group: str, version: str) -> list:
    custom = _k8s.CustomObjectsApi()
    try:
        resp = custom.list_cluster_custom_object(group, version, plural)
        return resp.get("items", [])
    except Exception:
        return []

def _list_custom_ns(ns: str, plural: str, group: str, version: str) -> list:
    custom = _k8s.CustomObjectsApi()
    try:
        resp = custom.list_namespaced_custom_object(group, version, ns, plural)
        return resp.get("items", [])
    except Exception:
        return []

def _get_custom(name: str, ns: str, plural: str, group: str, version: str) -> dict:
    custom = _k8s.CustomObjectsApi()
    if ns:
        return custom.get_namespaced_custom_object(group, version, ns, plural, name)
    return custom.get_cluster_custom_object(group, version, plural, name)

def _parse_kubectl(command: str) -> dict:
    tokens = shlex.split(command.strip())
    if tokens and tokens[0] == "kubectl":
        tokens = tokens[1:]

    result = {
        "verb": "",
        "resource": "",
        "name": "",
        "namespace": "default",
        "all_namespaces": False,
        "output_format": "",
        "field_selector": "",
        "tail": 100,
        "container": "",
        "subcommand": "",
        "args": [],
        "flags": {},
    }

    if not tokens:
        return result

    result["verb"] = tokens[0]
    tokens = tokens[1:]

    i = 0
    positional = []
    while i < len(tokens):
        t = tokens[i]
        if t in ("-n", "--namespace") and i + 1 < len(tokens):
            result["namespace"] = tokens[i + 1]; i += 2
        elif t.startswith("--namespace="):
            result["namespace"] = t.split("=", 1)[1]; i += 1
        elif t.startswith("-n") and len(t) > 2:
            result["namespace"] = t[2:]; i += 1
        elif t in ("-A", "--all-namespaces"):
            result["all_namespaces"] = True; i += 1
        elif t in ("-o", "--output") and i + 1 < len(tokens):
            result["output_format"] = tokens[i + 1]; i += 2
        elif t.startswith("--output=") or t.startswith("-o"):
            result["output_format"] = t.split("=", 1)[-1].lstrip("-o"); i += 1
        elif t.startswith("--field-selector="):
            result["field_selector"] = t.split("=", 1)[1]; i += 1
        elif t == "--field-selector" and i + 1 < len(tokens):
            result["field_selector"] = tokens[i + 1]; i += 2
        elif t.startswith("--tail="):
            try: result["tail"] = int(t.split("=")[1])
            except ValueError: pass
            i += 1
        elif t in ("-c", "--container") and i + 1 < len(tokens):
            result["container"] = tokens[i + 1]; i += 2
        elif t.startswith("--container="):
            result["container"] = t.split("=", 1)[1]; i += 1
        elif t.startswith("--no-headers") or t in ("--show-kind", "--show-labels"):
            i += 1
        elif t.startswith("-"):
            if i + 1 < len(tokens) and not tokens[i + 1].startswith("-"):
                result["flags"][t] = tokens[i + 1]; i += 2
            else:
                result["flags"][t] = True; i += 1
        else:
            positional.append(t); i += 1

    result["args"] = positional
    if positional:
        result["resource"] = positional[0]
        if len(positional) >= 2:
            result["name"] = positional[1]
        if len(positional) >= 3:
            result["subcommand"] = positional[2]
    return result

def _fmt_pod(p) -> str:
    ns   = p.metadata.namespace or ""
    name = p.metadata.name
    phase = (p.status.phase or "Unknown")
    ready_cs = p.status.container_statuses or []
    ready    = sum(1 for c in ready_cs if c.ready)
    total    = len(ready_cs) or len(p.spec.containers or [])
    restarts = sum(c.restart_count or 0 for c in ready_cs)
    node     = p.spec.node_name or "<none>"
    age      = _age(p.metadata.creation_timestamp)
    if ns:
        return f"{ns:<30} {name:<50} {ready}/{total}  {restarts:<6} {phase:<12} {node:<30} {age}"
    return f"{name:<50} {ready}/{total}  {restarts:<6} {phase:<12} {node:<30} {age}"

def _fmt_node(n) -> str:
    name  = n.metadata.name
    role  = ",".join(k.split("/")[-1] for k in (n.metadata.labels or {})
                     if "node-role.kubernetes.io" in k) or "worker"
    conds = {c.type: c.status for c in (n.status.conditions or [])}
    ready = "Ready" if conds.get("Ready") == "True" else "NotReady"
    age   = _age(n.metadata.creation_timestamp)
    ver   = n.status.node_info.kubelet_version if n.status and n.status.node_info else ""
    return f"{name:<40} {role:<20} {ready:<10} {age:<10} {ver}"

def _fmt_deployment(d) -> str:
    ns    = d.metadata.namespace or ""
    name  = d.metadata.name
    desired   = d.spec.replicas or 0
    ready_r   = d.status.ready_replicas or 0
    available = d.status.available_replicas or 0
    age   = _age(d.metadata.creation_timestamp)
    if ns:
        return f"{ns:<30} {name:<50} {ready_r}/{desired}  available={available}  {age}"
    return f"{name:<50} {ready_r}/{desired}  available={available}  {age}"

def _age(ts) -> str:
    if not ts:
        return "<unknown>"
    import datetime
    try:
        now  = datetime.datetime.now(datetime.timezone.utc)
        diff = now - ts
        s    = int(diff.total_seconds())
        if s < 60:    return f"{s}s"
        if s < 3600:  return f"{s//60}m"
        if s < 86400: return f"{s//3600}h"
        return f"{s//86400}d"
    except Exception:
        return "<unknown>"

def _obj_to_yaml(obj) -> str:
    try:
        d = _k8s.ApiClient().sanitize_for_serialization(obj)
        return _yaml.dump(d, default_flow_style=False, allow_unicode=True)
    except Exception:
        return str(obj)

def _obj_to_table(items, kind: str) -> str:
    if not items:
        return f"No {kind} resources found."
    lines = []
    k = kind.lower()
    if k == "pod":
        ns_col = any(getattr(p.metadata, "namespace", None) for p in items)
        if ns_col:
            lines.append(f"{'NAMESPACE':<30} {'NAME':<50} {'READY':<8} {'RESTARTS':<8} {'STATUS':<12} {'NODE':<30} {'AGE'}")
        else:
            lines.append(f"{'NAME':<50} {'READY':<8} {'RESTARTS':<8} {'STATUS':<12} {'NODE':<30} {'AGE'}")
        for p in items:
            lines.append(_fmt_pod(p))
    elif k in ("deployment",):
        lines.append(f"{'NAMESPACE':<30} {'NAME':<50} {'READY':<8} {'AVAILABLE':<12} {'AGE'}")
        for d in items:
            lines.append(_fmt_deployment(d))
    elif k == "node":
        lines.append(f"{'NAME':<40} {'ROLES':<20} {'STATUS':<10} {'AGE':<10} {'VERSION'}")
        for n in items:
            lines.append(_fmt_node(n))
    elif k in ("namespace",):
        lines.append(f"{'NAME':<40} {'STATUS':<12} {'AGE'}")
        for ns in items:
            lines.append(f"  {ns.metadata.name:<40} {ns.status.phase or '':<12} {_age(ns.metadata.creation_timestamp)}")
    elif k == "event":
        lines.append(f"{'NAMESPACE':<25} {'LAST SEEN':<12} {'TYPE':<10} {'REASON':<25} {'OBJECT':<40} {'MESSAGE'}")
        for ev in items:
            obj_ref = f"{(ev.involved_object.kind or '').lower()}/{ev.involved_object.name or ''}"
            lines.append(
                f"  {ev.metadata.namespace or '':<25} "
                f"{_age(ev.last_timestamp or ev.first_timestamp or ev.metadata.creation_timestamp):<12} "
                f"{ev.type or '':<10} {ev.reason or '':<25} {obj_ref:<40} "
                f"{(ev.message or '')[:80]}"
            )
    else:
        has_ns = any(getattr(i.metadata, "namespace", None) for i in items)
        if has_ns:
            lines.append(f"{'NAMESPACE':<30} {'NAME':<50} {'AGE'}")
            for item in items:
                lines.append(f"  {item.metadata.namespace or '':<30} {item.metadata.name:<50} {_age(item.metadata.creation_timestamp)}")
        else:
            lines.append(f"{'NAME':<50} {'AGE'}")
            for item in items:
                lines.append(f"  {item.metadata.name:<50} {_age(item.metadata.creation_timestamp)}")
    return "\n".join(lines)

def _custom_to_table(items: list, kind: str) -> str:
    if not items:
        return f"No {kind} resources found."
    lines = []
    has_ns = any(i.get("metadata", {}).get("namespace") for i in items)
    if has_ns:
        lines.append(f"{'NAMESPACE':<30} {'NAME':<50} {'AGE'}")
        for item in items:
            meta = item.get("metadata", {})
            from datetime import datetime, timezone
            try:
                ts_str = meta.get("creationTimestamp")
                if ts_str:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    age = _age(ts)
                else:
                    age = "<unknown>"
            except Exception:
                age = "<unknown>"
            ns = meta.get("namespace", "")
            name = meta.get("name", "")
            status = item.get("status", {})
            state  = status.get("state", status.get("phase", status.get("robustness", "")))
            suffix = f"  state={state}" if state else ""
            lines.append(f"  {ns:<30} {name:<50} {age}{suffix}")
    else:
        lines.append(f"{'NAME':<50} {'AGE'}")
        for item in items:
            meta = item.get("metadata", {})
            lines.append(f"  {meta.get('name', ''):<50} <n/a>")
    return "\n".join(lines)

def _handle_get(p: dict) -> str:
    resource = p["resource"]
    name     = p["name"]
    ns       = p["namespace"]
    all_ns   = p["all_namespaces"]
    fmt      = p["output_format"]
    fs       = p["field_selector"]

    fns = _get_resource_fns(resource)
    if fns is None:
        return f"[ERROR] Unsupported resource type: {resource!r}. Use get_pod_status, get_node_info, or other specific tools."

    list_all, list_ns, get_one, kind = fns

    try:
        if name:
            obj = get_one(name, ns)
            if fmt in ("yaml", "-oyaml"):
                return _obj_to_yaml(obj)
            if fmt in ("json", "-ojson"):
                d = _k8s.ApiClient().sanitize_for_serialization(obj)
                return _json.dumps(d, indent=2)
            return _obj_to_yaml(obj)

        if all_ns or list_ns is None:
            items = list_all(fs)
        else:
            items = list_ns(ns, fs)

        if fmt in ("yaml", "-oyaml"):
            d = [_k8s.ApiClient().sanitize_for_serialization(o) for o in items]
            return _yaml.dump(d, default_flow_style=False, allow_unicode=True)
        if fmt in ("json", "-ojson"):
            d = [_k8s.ApiClient().sanitize_for_serialization(o) for o in items]
            return _json.dumps(d, indent=2)

        if items and isinstance(items[0], dict):
            return _custom_to_table(items, kind)
        return _obj_to_table(items, kind)

    except ApiException as e:
        return f"[ERROR] API error getting {resource}: {_safe_reason(e)}"

def _handle_describe(p: dict) -> str:
    resource = p["resource"]
    name     = p["name"]
    ns       = p["namespace"]

    fns = _get_resource_fns(resource)
    if fns is None:
        return f"[ERROR] Unsupported resource type: {resource!r}"

    _, _, get_one, kind = fns
    try:
        obj = get_one(name or "", ns)
        return _obj_to_yaml(obj)
    except ApiException as e:
        return f"[ERROR] API error describing {resource}/{name}: {e.reason}"

def _handle_logs(p: dict) -> str:
    pod_ref = p["resource"]
    if "/" in pod_ref:
        pod_name = pod_ref.split("/", 1)[1]
    else:
        pod_name = pod_ref
    ns        = p["namespace"]
    tail      = p["tail"]
    container = p["container"] or None
    try:
        kw: dict = {"tail_lines": tail}
        if container:
            kw["container"] = container
        logs = _core.read_namespaced_pod_log(pod_name, ns, **kw)
        return logs or "(empty log)"
    except ApiException as e:
        return f"[ERROR] Cannot get logs for {pod_name} in {ns}: {e.reason}"

def _handle_top(p: dict) -> str:
    resource = p["resource"]
    ns       = p["namespace"]
    all_ns   = p["all_namespaces"]
    try:
        custom = _k8s.CustomObjectsApi()
        if resource in ("node", "nodes", "no"):
            resp = custom.list_cluster_custom_object(
                "metrics.k8s.io", "v1beta1", "nodes"
            )
            lines = [f"{'NODE':<40} {'CPU':<12} {'MEMORY'}"]
            for item in resp.get("items", []):
                lines.append(
                    f"  {item['metadata']['name']:<40} "
                    f"{item['usage']['cpu']:<12} "
                    f"{item['usage']['memory']}"
                )
            return "\n".join(lines)
        else:
            if all_ns:
                resp = custom.list_cluster_custom_object(
                    "metrics.k8s.io", "v1beta1", "pods"
                )
                items = resp.get("items", [])
            else:
                resp = custom.list_namespaced_custom_object(
                    "metrics.k8s.io", "v1beta1", ns, "pods"
                )
                items = resp.get("items", [])
            lines = [f"{'NAMESPACE':<30} {'POD':<50} {'CPU':<12} {'MEMORY'}"]
            for item in items:
                meta = item["metadata"]
                containers = item.get("containers", [])
                cpu = sum(
                    int(c["usage"]["cpu"].rstrip("n")) for c in containers
                    if c["usage"].get("cpu", "").endswith("n")
                )
                mem = containers[0]["usage"].get("memory", "?") if containers else "?"
                lines.append(
                    f"  {meta.get('namespace',''):<30} {meta['name']:<50} "
                    f"{cpu}n{'':6} {mem}"
                )
            return "\n".join(lines)
    except ApiException as e:
        return f"[ERROR] Metrics not available: {e.reason}. Is metrics-server installed?"

def _handle_rollout(p: dict) -> str:
    subverb  = p["args"][1] if len(p["args"]) > 1 else p["subcommand"]
    ref      = p["args"][2] if len(p["args"]) > 2 else ""
    ns       = p["namespace"]
    if "/" in ref:
        _, name = ref.split("/", 1)
    else:
        name = ref or (p["name"] or "")
    try:
        d = _apps.read_namespaced_deployment(name, ns)
        if subverb == "status":
            ready     = d.status.ready_replicas or 0
            desired   = d.spec.replicas or 0
            available = d.status.available_replicas or 0
            updated   = d.status.updated_replicas or 0
            if ready == desired == available == updated:
                return f"deployment \"/{name}\" successfully rolled out ({ready}/{desired} ready)"
            return (
                f"Waiting: desired={desired} updated={updated} "
                f"available={available} ready={ready}"
            )
        elif subverb == "history":
            rev = d.metadata.annotations.get("deployment.kubernetes.io/revision", "?")
            return (
                f"deployment.apps/{name}\n"
                f"REVISION  CHANGE-CAUSE\n"
                f"{rev}         <none>\n"
                f"(Full history requires kubectl-based access with --record flag history)"
            )
        return f"[ERROR] rollout sub-command {subverb!r} not supported. Use: status, history"
    except ApiException as e:
        return f"[ERROR] rollout {subverb} {name}: {e.reason}"

def _handle_auth_cani(p: dict) -> str:
    args = p["args"]
    if len(args) < 3:
        return "[ERROR] Usage: kubectl auth can-i <verb> <resource>"
    verb     = args[1]
    resource = args[2]
    ns       = p["namespace"]
    try:
        auth = _k8s.AuthorizationV1Api()
        review = auth.create_self_subject_access_review(
            body=_k8s.V1SelfSubjectAccessReview(
                spec=_k8s.V1SelfSubjectAccessReviewSpec(
                    resource_attributes=_k8s.V1ResourceAttributes(
                        namespace=ns, verb=verb, resource=resource
                    )
                )
            )
        )
        allowed = review.status.allowed
        return f"{'yes' if allowed else 'no'} — {verb} {resource} in {ns}"
    except ApiException as e:
        return f"[ERROR] auth can-i failed: {e.reason}"

def _handle_api_resources() -> str:
    try:
        api_client = _k8s.ApiClient()
        resources_v1 = api_client.call_api(
            "/api/v1", "GET", response_type="object", auth_settings=["BearerToken"]
        )[0]
        lines = ["NAME                  SHORTNAMES  APIVERSION  NAMESPACED  KIND"]
        if isinstance(resources_v1, dict):
            for r in resources_v1.get("resources", []):
                if "/" not in r.get("name", ""):
                    lines.append(
                        f"  {r.get('name',''):<22} {','.join(r.get('shortNames',[])):<12} "
                        f"v1{'':10} {str(r.get('namespaced','')).lower():<12} {r.get('kind','')}"
                    )
        return "\n".join(lines[:60])
    except Exception as e:
        return f"[ERROR] api-resources: {e}"

def _handle_version() -> str:
    try:
        v = _k8s.VersionApi().get_code()
        return (
            f"Server Version: {v.git_version}\n"
            f"  Platform: {v.platform}\n"
            f"  Go: {v.go_version}"
        )
    except ApiException as e:
        return f"[ERROR] version: {e.reason}"

def _parse_kubectl(command: str) -> dict:
    """
    Safely parses a kubectl command string to extract the verb, resource, name, namespace, and output format.
    Handles variations like 'kubectl get pod/my-pod' vs 'kubectl get pod my-pod'.
    """
    parts = shlex.split(command)
    
    if parts and parts[0] == "kubectl":
        parts = parts[1:]

    p = {
        "verb": None, 
        "resource": None, 
        "name": None, 
        "namespace": "default", 
        "output": None
    }
    
    if not parts:
        return p

    p["verb"] = parts[0]
    args = parts[1:]
    
    i = 0
    positionals = []
    
    while i < len(args):
        arg = args[i]
        if arg in ("-n", "--namespace"):
            if i + 1 < len(args):
                p["namespace"] = args[i+1]
                i += 1
        elif arg.startswith("--namespace="):
            p["namespace"] = arg.split("=", 1)[1]
        elif arg in ("-o", "--output"):
            if i + 1 < len(args):
                p["output"] = args[i+1]
                i += 1
        elif arg.startswith("-o"):
            p["output"] = arg[2:]
        elif arg.startswith("--output="):
            p["output"] = arg.split("=", 1)[1]
        elif arg in ("-A", "--all-namespaces"):
            p["namespace"] = ""
        elif arg.startswith("-"):
            pass
        else:
            positionals.append(arg)
        i += 1

    if positionals:
        res_str = positionals[0]
        if "/" in res_str:
            p["resource"], p["name"] = res_str.split("/", 1)
        else:
            p["resource"] = res_str
            if len(positionals) > 1:
                p["name"] = positionals[1]

    return p


def _handle_get(p: dict, raw: bool = False):
    """
    Handles 'kubectl get' by routing to the correct K8s API client.
    Note: In a full production app, use kubernetes.dynamic.DynamicClient here instead.
    """
    core_v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    
    resource = p.get("resource", "").lower()
    name = p.get("name")
    namespace = p.get("namespace", "default")
    
    if resource in ("po", "pod", "pods"):
        if name:
            obj = core_v1.read_namespaced_pod(name=name, namespace=namespace)
            return obj.to_dict() if raw else f"Pod: {obj.metadata.name}, Status: {obj.status.phase}"
        else:
            if namespace == "":
                obj = core_v1.list_pod_for_all_namespaces()
            else:
                obj = core_v1.list_namespaced_pod(namespace=namespace)
            
            if raw: return obj.to_dict()
            items = [f"{item.metadata.name} ({item.status.phase})" for item in obj.items]
            return "Pods:\n" + "\n".join(items)
            
    elif resource in ("deploy", "deployment", "deployments"):
        if name:
            obj = apps_v1.read_namespaced_deployment(name=name, namespace=namespace)
            return obj.to_dict() if raw else f"Deployment: {obj.metadata.name}, Replicas: {obj.status.ready_replicas}/{obj.status.replicas}"
        else:
            obj = apps_v1.list_namespaced_deployment(namespace=namespace)
            if raw: return obj.to_dict()
            items = [f"{item.metadata.name} ({item.status.ready_replicas}/{item.status.replicas})" for item in obj.items]
            return "Deployments:\n" + "\n".join(items)

    return f"[ERROR] 'get' for resource '{resource}' is not mapped in this implementation."


def _handle_describe(p: dict) -> str:
    return f"Simulated describe output for {p.get('resource')} {p.get('name')} in namespace {p.get('namespace')}..."


def _handle_logs(p: dict) -> str:
    core_v1 = client.CoreV1Api()
    if not p.get("name"):
        return "[ERROR] Logs require a resource name."
    
    logs = core_v1.read_namespaced_pod_log(name=p["name"], namespace=p["namespace"], tail_lines=50)
    return logs

_log = logging.getLogger(__name__)

def kubectl_exec(command: str) -> str:
    command = command.strip()
    _log.info(f"[kubectl_exec] {command!r}")

    if not re.match(r"^kubectl(\s|$)", command):
        return "[ERROR] Command must start with 'kubectl'."

    _SHELL_OPS = re.compile(r'(\|\||&&|(?<!<)>(?!>)|\bawk\b|\bgrep\b|\bsed\b|\bcut\b|\bwc\b|2>/dev/null)')
    if _SHELL_OPS.search(command):
        return (
            "[ERROR] Shell operators and pipes (||, &&, |, awk, grep, 2>/dev/null) are NOT "
            "supported by kubectl_exec — it uses the Kubernetes Python API directly. "
            "Please use a dedicated tool instead: get_pod_status(namespace=..., show_all=True), "
            "get_pvc_status(namespace=...), get_events(namespace=...), etc."
        )

    p = _parse_kubectl(command)
    verb = p["verb"]

    if verb in _BLOCKED_VERBS:
        return f"[ERROR] {_BLOCKED_VERBS[verb]}"

    if verb in _KUBECTL_WRITE_VERBS and not _ALLOW_WRITES:
        return (
            f"[ERROR] Write operation '{verb}' is disabled. "
            "Set KUBECTL_ALLOW_WRITES=true in your env file to enable writes."
        )

    try:
        if verb == "get":
            out = _handle_get(p)
        elif verb == "describe":
            out = _handle_describe(p)
        elif verb == "logs":
            out = _handle_logs(p)
        elif verb == "top":
            out = _handle_top(p)
        elif verb == "rollout":
            out = _handle_rollout(p)
        elif verb == "auth":
            out = _handle_auth_cani(p)
        elif verb == "api-resources":
            out = _handle_api_resources()
        elif verb == "version":
            out = _handle_version()
        elif verb in _KUBECTL_READ_VERBS:
            out = f"[ERROR] kubectl {verb} is not yet implemented in API mode. Use a specific tool instead."
        else:
            out = f"[ERROR] Unknown kubectl verb: {verb!r}"
    except Exception as exc:
        _log.exception(f"[kubectl_exec] Unexpected error: {exc}")
        out = f"[ERROR] Unexpected error: {exc}"

    if len(out) > _KUBECTL_MAX_OUT:
        out = out[:_KUBECTL_MAX_OUT] + f"\n...[output truncated at {_KUBECTL_MAX_OUT} chars]"
    return out

def _parse_cpu_to_millicores(cpu_str: str) -> int:
    if not cpu_str or cpu_str in ("none", "<none>", "0"):
        return 0
    cpu_str = cpu_str.strip()
    try:
        if cpu_str.endswith("m"):
            return int(cpu_str[:-1])

        return int(float(cpu_str) * 1000)
    except (ValueError, TypeError):
        return 0

def _parse_mem_to_mib(mem_str: str) -> float:
    if not mem_str or mem_str in ("none", "<none>", "0"):
        return 0.0
    mem_str = mem_str.strip()
    try:
        _UNITS = {
            "Ki": 1 / 1024,
            "Mi": 1.0,
            "Gi": 1024.0,
            "Ti": 1024.0 * 1024,
            "K":  1 / 1024,
            "M":  1.0,
            "G":  1024.0,
        }
        for suffix, factor in _UNITS.items():
            if mem_str.endswith(suffix):
                return float(mem_str[: -len(suffix)]) * factor
        return float(mem_str) / (1024 * 1024)
    except (ValueError, TypeError):
        return 0.0

def get_namespace_resource_summary(namespace: str) -> str:
    try:
        pods = _core.list_namespaced_pod(namespace=namespace, limit=1000)
    except ApiException as e:
        return f"[ERROR] {_safe_reason(e)}"

    if not pods.items:
        return f"No pods found in namespace '{namespace}'."

    total_cpu_req_m   = 0
    total_cpu_lim_m   = 0
    total_mem_req_mib = 0.0
    total_mem_lim_mib = 0.0
    
    # List to hold our Markdown table rows
    table_rows = []

    for pod in pods.items:
        pod_cpu_req_m   = 0
        pod_cpu_lim_m   = 0
        pod_mem_req_mib = 0.0
        pod_mem_lim_mib = 0.0

        all_containers = list(pod.spec.containers or []) + list(pod.spec.init_containers or [])
        for c in all_containers:
            req = (c.resources.requests or {}) if c.resources else {}
            lim = (c.resources.limits   or {}) if c.resources else {}
            pod_cpu_req_m   += _parse_cpu_to_millicores(req.get("cpu",    "0"))
            pod_cpu_lim_m   += _parse_cpu_to_millicores(lim.get("cpu",    "0"))
            pod_mem_req_mib += _parse_mem_to_mib(req.get("memory", "0"))
            pod_mem_lim_mib += _parse_mem_to_mib(lim.get("memory", "0"))

        total_cpu_req_m   += pod_cpu_req_m
        total_cpu_lim_m   += pod_cpu_lim_m
        total_mem_req_mib += pod_mem_req_mib
        total_mem_lim_mib += pod_mem_lim_mib

        # Cleaned up the "not set" text so it fits nicely in a table column
        cpu_req_s = f"{pod_cpu_req_m}m" if pod_cpu_req_m else "0m"
        mem_req_s = f"{pod_mem_req_mib:.0f}Mi" if pod_mem_req_mib else "0Mi"
        cpu_lim_s = f"{pod_cpu_lim_m}m" if pod_cpu_lim_m else "0m"
        mem_lim_s = f"{pod_mem_lim_mib:.0f}Mi" if pod_mem_lim_mib else "0Mi"
        
        table_rows.append(f"| {pod.metadata.name} | {cpu_req_s} | {mem_req_s} | {cpu_lim_s} | {mem_lim_s} |")

    def _fmt_cpu(m: int) -> str:
        if m == 0:
            return "0m"
        return f"{m}m ({m/1000:.3f} cores)"

    def _fmt_mem(mib: float) -> str:
        if mib == 0:
            return "0Mi"
        return f"{mib:.0f}Mi ({mib/1024:.2f}Gi)"

    lines_out = [
        f"### Resource summary for namespace '{namespace}' ({len(pods.items)} pods)\n",
        f"- **TOTAL CPU REQUESTED**: {_fmt_cpu(total_cpu_req_m)}",
        f"- **TOTAL CPU LIMIT**: {_fmt_cpu(total_cpu_lim_m)}",
        f"- **TOTAL MEMORY REQUESTED**: {_fmt_mem(total_mem_req_mib)}",
        f"- **TOTAL MEMORY LIMIT**: {_fmt_mem(total_mem_lim_mib)}\n",
        f"**Per-pod breakdown:**\n",
        "| POD NAME | CPU REQ | MEM REQ | CPU LIM | MEM LIM |",
        "|---|---|---|---|---|"
    ] + table_rows

    return "\n".join(lines_out)

_ALLOW_DB_EXEC = os.getenv("ALLOW_DB_EXEC", "true").lower() in ("1", "true", "yes")

_SQL_WRITE_RE = re.compile(
    r"^\s*(insert|update|delete|drop|truncate|alter|create|replace|rename|grant|revoke"
    r"|call|exec|execute|lock|unlock|flush|reset|purge|load\s+data)\b",
    re.IGNORECASE,
)

_DB_USER_KEYS  = ("username", "user", "db-user", "db_user", "postgresql-user",
                  "mysql-user", "mariadb-user", "database-user")
_DB_PASS_KEYS  = ("password", "pass", "db-password", "db_password",
                  "postgresql-password", "mysql-password", "mariadb-password",
                  "database-password", "postgres-password")
_DB_NAME_KEYS  = ("database", "db", "db-name", "db_name", "dbname",
                  "postgresql-database", "mysql-database", "mariadb-database")
_DB_HOST_KEYS  = ("host", "db-host", "db_host", "postgresql-host", "mysql-host")
_DB_PORT_KEYS  = ("port", "db-port", "db_port", "postgresql-port", "mysql-port")

def _b64decode_safe(val: str) -> str:
    import base64 as _b64
    try:
        return _b64.b64decode(val).decode("utf-8", errors="replace").strip()
    except Exception:
        return val.strip()

def _find_db_credentials(namespace: str, pod_name: str) -> dict:
    import base64 as _b64

    creds: dict = {k: None for k in ("user", "password", "database", "host", "port")}

    def _match(key: str, patterns: tuple) -> str | None:
        kl = key.lower().replace("-", "_")
        for p in patterns:
            if p.replace("-", "_") in kl:
                return key
        return None

    def _harvest(key: str, val: str):
        if _match(key, _DB_USER_KEYS)  and not creds["user"]:
            creds["user"] = val
        elif _match(key, _DB_PASS_KEYS) and not creds["password"]:
            creds["password"] = val
        elif _match(key, _DB_NAME_KEYS) and not creds["database"]:
            creds["database"] = val
        elif _match(key, _DB_HOST_KEYS) and not creds["host"]:
            creds["host"] = val
        elif _match(key, _DB_PORT_KEYS) and not creds["port"]:
            creds["port"] = val

    try:
        pod = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
    except ApiException:
        return creds

    for container in (pod.spec.containers or []):
        for env in (container.env or []):

            if env.value:
                _harvest(env.name, env.value)
                continue

            if env.value_from:
                vf = env.value_from
                if vf.secret_key_ref:
                    try:
                        sec = _core.read_namespaced_secret(
                            name=vf.secret_key_ref.name, namespace=namespace)
                        raw = (sec.data or {}).get(vf.secret_key_ref.key, "")
                        if raw:
                            _harvest(env.name, _b64decode_safe(raw))
                    except ApiException:
                        pass
                elif vf.config_map_key_ref:
                    try:
                        cm = _core.read_namespaced_config_map(
                            name=vf.config_map_key_ref.name, namespace=namespace)
                        val = (cm.data or {}).get(vf.config_map_key_ref.key, "")
                        if val:
                            _harvest(env.name, val)
                    except ApiException:
                        pass

        for ef in (container.env_from or []):
            if ef.secret_ref:
                try:
                    sec = _core.read_namespaced_secret(
                        name=ef.secret_ref.name, namespace=namespace)
                    for k, v in (sec.data or {}).items():
                        _harvest(k, _b64decode_safe(v or ""))
                except ApiException:
                    pass
            if ef.config_map_ref:
                try:
                    cm = _core.read_namespaced_config_map(
                        name=ef.config_map_ref.name, namespace=namespace)
                    for k, v in (cm.data or {}).items():
                        _harvest(k, v or "")
                except ApiException:
                    pass

    return creds

def _detect_db_type(pod_name: str, namespace: str,
                    container_hint: str = "") -> str | None:
    try:
        pod = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
    except ApiException:
        return None

    containers = pod.spec.containers or []

    if container_hint:
        containers = sorted(containers,
                            key=lambda c: 0 if c.name == container_hint else 1)

    for c in containers:
        image = (c.image or "").lower()
        if any(x in image for x in ("mysql", "mariadb", "percona")):
            return "mysql"
        if any(x in image for x in ("postgres", "postgresql", "pg:", "/pg-", "-pg-", "pgbouncer")):
            return "postgres"

    for c in containers:
        cname = (c.name or "").lower()
        if any(x in cname for x in ("mysql", "mariadb")):
            return "mysql"
        if any(x in cname for x in ("postgres", "postgresql")):
            return "postgres"

    for c in containers:
        for env in (c.env or []):
            name_lc = env.name.lower()
            if name_lc.startswith(("postgres", "pgdata", "pguser", "pgpassword")):
                return "postgres"
            if name_lc.startswith(("mysql", "mariadb")):
                return "mysql"

    for c in containers:
        cname = (c.name or "").lower()
        if cname in ("db", "database"):
            return "postgres"

    return None

def _find_db_container(pod_name: str, namespace: str, db_type: str) -> str:
    try:
        pod = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
    except ApiException:
        return ""

    _MYSQL_HINTS    = ("mysql", "mariadb", "percona")
    _POSTGRES_HINTS = ("postgres", "postgresql", "pg:", "/pg-", "-pg-")

    hints = _MYSQL_HINTS if db_type == "mysql" else _POSTGRES_HINTS

    for c in (pod.spec.containers or []):
        if any(h in (c.image or "").lower() for h in hints):
            return c.name

    for c in (pod.spec.containers or []):
        cname = (c.name or "").lower()
        if db_type == "mysql" and any(h in cname for h in ("mysql", "mariadb", "db")):
            return c.name
        if db_type == "postgres" and any(h in cname for h in ("postgres", "pg", "db")):
            return c.name

    containers = pod.spec.containers or []
    return containers[0].name if containers else ""

def _find_db_pod(namespace: str, hint: str = "") -> tuple[str | None, str | None]:
    try:
        pods = _core.list_namespaced_pod(namespace=namespace)
    except ApiException:
        return None, None

    _DB_IMAGE_HINTS = ("mysql", "mariadb", "percona", "postgres", "postgresql")

    for pod in pods.items:
        if pod.status.phase != "Running":
            continue
        pname = pod.metadata.name
        if hint and hint.lower() not in pname.lower():
            continue
        for container in (pod.spec.containers or []):
            image = (container.image or "").lower()
            if any(h in image for h in _DB_IMAGE_HINTS):
                db_type = _detect_db_type(pname, namespace)
                if db_type:
                    return pname, db_type

    return None, None

def _exec_simple_query(pod_name: str, namespace: str, container_name: str,
                       cmd: str) -> str:
    from kubernetes.stream import stream as _k8s_stream
    try:
        kwargs = dict(stderr=False, stdin=False, stdout=True,
                      tty=False, _preload_content=True)
        if container_name:
            kwargs["container"] = container_name
        resp = _k8s_stream(
            _core.connect_get_namespaced_pod_exec,
            pod_name, namespace,
            command=["/bin/sh", "-c", cmd],
            **kwargs,
        )
        return resp.strip() if isinstance(resp, str) else ""
    except Exception:
        return ""

_PG_SYSTEM_DBS = {"postgres", "template0", "template1"}

def _discover_pg_database(pod_name: str, namespace: str,
                           container_name: str, user: str,
                           password: str) -> str:
    pg_env = ("PGPASSWORD='" + password + "' ") if password else ""
    user_flag = ("-U " + user) if user else ""
    inner_sql = "SELECT datname FROM pg_database WHERE datistemplate=false ORDER BY datname"
    cmd = (
        pg_env + "psql " + user_flag + " -d postgres --no-password -t -A "
        "-c "" + inner_sql + "" 2>/dev/null || "
        "psql -d postgres -t -A -c "" + inner_sql + """
    )
    out = _exec_simple_query(pod_name, namespace, container_name, cmd)
    for db in out.splitlines():
        db = db.strip()
        if db and db.lower() not in _PG_SYSTEM_DBS:
            return db
    return "postgres"

_MYSQL_SYSTEM_DBS = {"information_schema", "performance_schema", "mysql", "sys"}

def _discover_mysql_database(pod_name: str, namespace: str,
                              container_name: str, user: str,
                              password: str, host: str, port: str) -> str:
    pass_arg = ("-p'" + password + "'") if password else ""
    cmd = (
        "mysql -u" + user + " " + pass_arg + " -h" + host + " -P" + port + " "
        "--connect-timeout=5 --batch --silent -e 'SHOW DATABASES'"
    )
    out = _exec_simple_query(pod_name, namespace, container_name, cmd)
    for db in out.splitlines():
        db = db.strip()
        if db and db.lower() not in _MYSQL_SYSTEM_DBS:
            return db
    return ""

def exec_db_query(namespace: str, sql: str, pod_name: str = "", database: str = "", container: str = "") -> str:
    import base64, re, logging
    from kubernetes import client
    _log = logging.getLogger(__name__)
    _SQL_WRITE_RE = re.compile(r"^\s*(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b", re.IGNORECASE)
    _KUBECTL_MAX_OUT = 10000

    _core = client.CoreV1Api()

    def _default_find_db_pod(ns):
        pods = _core.list_namespaced_pod(ns).items
        for p in pods:
            if any(c in p.metadata.name.lower() for c in ("db", "mysql", "postgres")):
                return p.metadata.name, None
        return None, None

    def _default_detect_db_type(pod, ns, container_hint=""):
        try:
            pod_obj = _core.read_namespaced_pod(pod, ns)
            for c in pod_obj.spec.containers:
                cname = c.name.lower()
                if "mysql" in cname: return "mysql"
                if "postgres" in cname or "postgresql" in cname: return "postgres"
            return None
        except: return None

    def _default_find_db_container(pod, ns, db_type):
        try:
            pod_obj = _core.read_namespaced_pod(pod, ns)
            for c in pod_obj.spec.containers:
                cname = c.name.lower()
                if db_type in cname: return c.name
            return pod_obj.spec.containers[0].name
        except: return ""

    if not sql.strip():
        return "[ERROR] Empty SQL query."
    if _SQL_WRITE_RE.match(sql):
        return "[BLOCKED] Write operations are not permitted. Only SELECT/read queries allowed."

    if not pod_name:
        pod_name, _ = _default_find_db_pod(namespace)
        if not pod_name:
            return f"[ERROR] No running DB pod found in namespace '{namespace}'."
        db_type = _default_detect_db_type(pod_name, namespace)
        container = _default_find_db_container(pod_name, namespace, db_type)
    else:
        db_type = _default_detect_db_type(pod_name, namespace, container)
        if not db_type:
            return f"[ERROR] Could not detect DB type for pod '{pod_name}' in '{namespace}'."
        if not container:
            container = _default_find_db_container(pod_name, namespace, db_type)

    creds = {}
    try:
        pod = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
    except Exception:
        return f"[ERROR] Could not read pod '{pod_name}' in namespace '{namespace}'."

    for vol in pod.spec.volumes or []:
        secret_data = None
        if getattr(vol, 'secret', None):
            try:
                secret_obj = _core.read_namespaced_secret(vol.secret.secret_name, namespace)
                secret_data = secret_obj.data
            except: continue
        elif getattr(vol, 'config_map', None):
            try:
                config_obj = _core.read_namespaced_config_map(vol.config_map.name, namespace)
                secret_data = config_obj.data
            except: continue
        if secret_data:
            for k, v in secret_data.items():
                val = v
                try:
                    val = base64.b64decode(v).decode() if getattr(vol, 'secret', None) else v
                except: pass
                k_lower = k.lower()
                if "user" in k_lower: creds["user"] = val
                elif "pass" in k_lower or "pwd" in k_lower: creds["password"] = val
                elif "host" in k_lower: creds["host"] = val
                elif "port" in k_lower: creds["port"] = val
                elif "db" in k_lower or "database" in k_lower: creds["database"] = val
            if creds.get("user") and creds.get("password"): break

    if not creds.get("user") or not creds.get("password"):
        return f"[ERROR] Could not retrieve DB credentials from pod '{pod_name}'."

    db_name = database or creds.get("database") or ""
    user = creds["user"]
    password = creds["password"]
    host = creds.get("host") or "127.0.0.1"
    port = creds.get("port") or ("5432" if db_type == "postgres" else "3306")

    pg_sql = sql
    if db_type == "postgres":
        if re.match(r"^\s*SHOW\s+TABLES\s*$", pg_sql, re.IGNORECASE):
            pg_sql = "SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN ('information_schema','pg_catalog','pg_toast') ORDER BY schemaname, tablename"
        elif re.match(r"^\s*SHOW\s+DATABASES\s*$", pg_sql, re.IGNORECASE):
            pg_sql = "SELECT datname FROM pg_database ORDER BY datname"
        desc_m = re.match(r"^\s*DESCRIBE\s+(\S+)\s*$", pg_sql, re.IGNORECASE)
        if desc_m:
            tbl = desc_m.group(1).strip("`\"'")
            pg_sql = f"SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name = '{tbl}' ORDER BY ordinal_position"

    safe_sql = pg_sql.replace("'", "'\\''")

    if db_type in ("mysql", "mariadb"):
        pass_arg = f"-p'{password}'" if password else ""
        db_arg = db_name if db_name else ""
        cmd = f"mysql -u{user} {pass_arg} -h{host} -P{port} --batch --silent {db_arg} -e '{safe_sql}'"
    elif db_type == "postgres":
        pg_env = f"PGPASSWORD='{password}' " if password else ""
        db_flag = f"-d {db_name}" if db_name else ""
        user_flag = f"-U {user}" if user else ""
        if host not in ("localhost", "127.0.0.1", "::1"):
            cmd = f"{pg_env}psql {user_flag} -h {host} -p {port} {db_flag} --no-password -t -A -c '{safe_sql}'"
        else:
            cmd = f"{pg_env}psql {user_flag} {db_flag} --no-password -t -A -c '{safe_sql}'"
    else:
        return f"[ERROR] Unsupported DB type: {db_type}"

    exec_cmd = ["/bin/sh", "-c", cmd]
    stream_kwargs = dict(stderr=True, stdin=False, stdout=True, tty=False, _preload_content=True, container=container)
    try:
        resp = _core.connect_get_namespaced_pod_exec(pod_name, namespace, command=exec_cmd, **stream_kwargs)
        output = resp.strip() if isinstance(resp, str) else str(resp).strip()
    except Exception as exc:
        return f"[ERROR] Exec failed: {exc}"

    if not output: return "(Query returned no rows.)"
    if len(output) > _KUBECTL_MAX_OUT:
        output = output[:_KUBECTL_MAX_OUT] + f"\n...[output truncated at {_KUBECTL_MAX_OUT} chars]"

    m = re.match(r"^\s*SELECT\s+(.+?)\s+FROM\b", pg_sql, re.IGNORECASE | re.DOTALL)
    cols = []
    if m:
        for col in m.group(1).split(","):
            col = col.strip()
            alias_m = re.search(r"\bAS\s+(\w+)\s*$", col, re.IGNORECASE)
            if alias_m:
                cols.append(alias_m.group(1))
            else:
                token = re.split(r"[\s.(]", col)[-1].strip(")'\"")
                cols.append(token if token else col)
    if cols:
        col_header = "|".join(cols)
        col_sep = "|".join("-" * max(len(c), 4) for c in cols)
        output = col_header + "\n" + col_sep + "\n" + output

    header = f"DB query result [{db_type.upper()} · pod={pod_name} · ns={namespace}" + (f" · db={db_name}" if db_name else "") + "]\n" + "-"*60 + "\n"
    return header + output