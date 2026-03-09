import os
import re
import shlex
import logging
import json as _json
import yaml as _yaml
from pathlib import Path

from kubernetes import client as _k8s, config as _k8s_cfg
from kubernetes.client.rest import ApiException

_log = logging.getLogger("k8s")


def _load_k8s():
    kc = os.getenv("KUBECONFIG_PATH", "")
    try:
        if kc and Path(os.path.expanduser(kc)).exists():
            _k8s_cfg.load_kube_config(config_file=os.path.expanduser(kc))
            _log.info(f"Loaded kubeconfig: {kc}")
        else:
            _k8s_cfg.load_incluster_config()
            _log.info("Loaded in-cluster config")
    except Exception as e:
        _log.error(f"K8s config failed: {e}")
        raise RuntimeError(f"K8s config: {e}")


_load_k8s()

_core   = _k8s.CoreV1Api()
_apps   = _k8s.AppsV1Api()
_batch  = _k8s.BatchV1Api()
_rbac   = _k8s.RbacAuthorizationV1Api()
_net    = _k8s.NetworkingV1Api()
_autoscaling = _k8s.AutoscalingV2Api()


def reload_kubeconfig(yaml_content: str) -> dict:
    """
    Load a kubeconfig from a YAML string, write it to a temp file, re-initialise
    all Kubernetes API clients, and return {"ok": True, "server": "<host:port>"}.
    Raises ValueError with a human-readable message on failure.
    """
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


def get_pod_status(namespace: str = "all", show_all: bool = False, raw_output: bool = False) -> str:
    """
    List pods in a namespace.

    show_all=False (default, health-check mode):
      Uses a field selector to fetch ONLY non-Running pods from the API — fast
      even on large clusters because it never transfers healthy pods over the wire.

    show_all=True (listing/count mode):
      - namespace-scoped: lists every pod individually.
      - namespace="all": grouped summary (healthy count per namespace) + full
        detail for unhealthy pods. Avoids dumping thousands of lines.

    raw_output=True: kubectl-style tabular format. Only use when user explicitly
    asks to "show" or "display" pod output.
    """
    try:
        if namespace != "all":
            try:
                _core.read_namespace(name=namespace)
            except ApiException as e:
                if e.status == 404:
                    return (f"Namespace '{namespace}' does not exist in this cluster. "
                            f"Cannot report pod count for a non-existent namespace.")
                raise

        # ── Health-check mode: fetch only non-Running pods via field selector ─
        # This is the same as: kubectl get pods -A --field-selector=status.phase!=Running
        # Much faster on large clusters — avoids transferring 300+ healthy pods.
        if not show_all and not raw_output:
            non_running_phases = []
            for phase in ("Pending", "Failed", "Unknown"):
                try:
                    result = (_core.list_pod_for_all_namespaces(
                                  field_selector=f"status.phase={phase}")
                              if namespace == "all"
                              else _core.list_namespaced_pod(
                                  namespace=namespace,
                                  field_selector=f"status.phase={phase}"))
                    non_running_phases.extend(result.items)
                except ApiException:
                    pass

            # Also catch Running pods that are not fully Ready (restarts, conditions)
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
                        if ready < tot or restarts > 5:
                            non_running_phases.append(pod)
                    _cont = (page.metadata._continue
                             if page.metadata and page.metadata._continue else None)
                    if not _cont:
                        break
                except ApiException:
                    break

            if not non_running_phases:
                return f"All pods are healthy and Running in namespace '{namespace}'."

            lines = [f"Unhealthy/non-Running pods in '{namespace}':"]
            for pod in non_running_phases:
                phase    = pod.status.phase or "Unknown"
                restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
                ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                tot      = len(pod.spec.containers)
                bad      = [f"{c.type}={c.status}"
                            for c in (pod.status.conditions or []) if c.status != "True"]
                lines.append(
                    f"  {pod.metadata.namespace}/{pod.metadata.name}: {phase} "
                    f"| Ready {ready}/{tot} | Restarts:{restarts}"
                    + (f" [{', '.join(bad)}]" if bad else ""))
            return "\n".join(lines)

        # ── show_all=True all-namespace: paginated field-selector approach ────
        # Fetch Running pods in pages of 100 to avoid one giant API call.
        # Non-Running pods (Pending/Failed/Unknown) are few — fetch all at once.
        if show_all and not raw_output and namespace == "all":
            unhealthy = []
            healthy_by_ns: dict = {}

            # Non-Running phases — typically very few pods, fast
            for phase in ("Pending", "Failed", "Unknown"):
                try:
                    result = _core.list_pod_for_all_namespaces(
                        field_selector=f"status.phase={phase}")
                    for pod in result.items:
                        restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
                        ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                        tot      = len(pod.spec.containers)
                        bad      = [f"{c.type}={c.status}"
                                    for c in (pod.status.conditions or []) if c.status != "True"]
                        unhealthy.append(
                            f"  {pod.metadata.namespace}/{pod.metadata.name}: {phase} "
                            f"| Ready {ready}/{tot} | Restarts:{restarts}"
                            + (f" [{', '.join(bad)}]" if bad else ""))
                except ApiException:
                    pass

            # Running pods — paginate in batches of 100 to reduce per-call payload
            _cont = None
            while True:
                try:
                    kw = {"field_selector": "status.phase=Running", "limit": 100}
                    if _cont:
                        kw["_continue"] = _cont
                    page = _core.list_pod_for_all_namespaces(**kw)
                    for pod in page.items:
                        restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
                        ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                        tot      = len(pod.spec.containers)
                        ns_k     = pod.metadata.namespace
                        if ready < tot or restarts > 5:
                            bad = [f"{c.type}={c.status}"
                                   for c in (pod.status.conditions or []) if c.status != "True"]
                            unhealthy.append(
                                f"  {ns_k}/{pod.metadata.name}: Running "
                                f"| Ready {ready}/{tot} | Restarts:{restarts}"
                                + (f" [{', '.join(bad)}]" if bad else ""))
                        else:
                            healthy_by_ns[ns_k] = healthy_by_ns.get(ns_k, 0) + 1
                    _cont = (page.metadata._continue
                             if page.metadata and page.metadata._continue else None)
                    if not _cont:
                        break
                except ApiException:
                    break

            healthy_total = sum(healthy_by_ns.values())
            total = healthy_total + len(unhealthy)
            lines = [f"Pods across all namespaces: {total} total "
                     f"({healthy_total} healthy, {len(unhealthy)} unhealthy)."]
            if unhealthy:
                lines.append(f"\nUnhealthy pods ({len(unhealthy)}):")
                lines.extend(unhealthy)
            lines.append(f"\nHealthy Running pods by namespace ({healthy_total} total):")
            for ns_key, count in sorted(healthy_by_ns.items()):
                lines.append(f"  {ns_key}: {count} pod(s)")
            return "\n".join(lines)

        # ── Full fetch for namespace-scoped show_all or raw_output ────────────
        pods = (_core.list_pod_for_all_namespaces() if namespace == "all"
                else _core.list_namespaced_pod(namespace=namespace))

        if not pods.items:
            return f"Namespace '{namespace}' exists but has 0 pods."

        total = len(pods.items)

        if raw_output:
            if namespace == "all":
                hdr = f"{'NAMESPACE':<22} {'NAME':<55} {'READY':<7} {'STATUS':<12} {'RESTARTS':<10} {'AGE'}"
            else:
                hdr = f"{'NAME':<55} {'READY':<7} {'STATUS':<12} {'RESTARTS':<10} {'AGE'}"
            rows = [hdr, "-" * len(hdr)]
            import datetime as dt
            from datetime import timezone
            for pod in sorted(pods.items, key=lambda p: (p.metadata.namespace, p.metadata.name)):
                phase    = pod.status.phase or "Unknown"
                restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
                ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                tot      = len(pod.spec.containers)
                created  = pod.metadata.creation_timestamp
                if created:
                    age_s = int((dt.datetime.now(timezone.utc) - created).total_seconds())
                    if age_s < 3600:    age_str = f"{age_s//60}m"
                    elif age_s < 86400: age_str = f"{age_s//3600}h"
                    else:               age_str = f"{age_s//86400}d"
                else:
                    age_str = "<unknown>"
                if namespace == "all":
                    rows.append(
                        f"{pod.metadata.namespace:<22} {pod.metadata.name:<55} "
                        f"{ready}/{tot:<5} {phase:<12} {restarts:<10} {age_str}")
                else:
                    rows.append(
                        f"{pod.metadata.name:<55} {ready}/{tot:<5} {phase:<12} {restarts:<10} {age_str}")
            rows.append(f"\nTotal: {total} pod(s) in namespace '{namespace}'.")
            return "\n".join(rows)

        # Namespace-scoped listing — every pod individually
        lines = [f"Pods in '{namespace}': {total} total."]
        for pod in pods.items:
            phase    = pod.status.phase or "Unknown"
            restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
            ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
            tot      = len(pod.spec.containers)
            bad      = [f"{c.type}={c.status}"
                        for c in (pod.status.conditions or []) if c.status != "True"]
            lines.append(
                f"  {pod.metadata.namespace}/{pod.metadata.name}: {phase} "
                f"| Ready {ready}/{tot} | Restarts:{restarts}"
                + (f" [{', '.join(bad)}]" if bad else ""))
        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_pod_logs(pod_name: str, namespace: str = "default",
                 tail_lines: int = 50) -> str:
    tail_lines = min(tail_lines, 100)
    try:
        logs = _core.read_namespaced_pod_log(
            name=pod_name, namespace=namespace,
            tail_lines=tail_lines, timestamps=True)
        return (f"Last {tail_lines} lines of '{pod_name}':\n{logs}"
                if logs.strip() else f"No logs for '{pod_name}'.")
    except ApiException as e:
        return (f"Pod '{pod_name}' not found."
                if e.status == 404 else f"K8s error: {e.reason}")


def describe_pod(pod_name: str, namespace: str = "default") -> str:
    try:
        pod   = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
        lines = [
            f"Pod:       {pod.metadata.name}",
            f"Namespace: {pod.metadata.namespace}",
            f"Phase:     {pod.status.phase}",
            "Conditions:",
        ]
        for c in (pod.status.conditions or []):
            lines.append(f"  {c.type}:{c.status}"
                         + (f" — {c.message}" if c.message else ""))
        lines.append("Containers:")
        for cs in (pod.status.container_statuses or []):
            sk = list(cs.state.to_dict().keys())[0] if cs.state else "unknown"
            lines.append(f"  {cs.name}: ready={cs.ready} "
                         f"restarts={cs.restart_count} state={sk}")
            if cs.last_state and cs.last_state.terminated:
                lt = cs.last_state.terminated
                lines.append(f"    Last terminated: exit={lt.exit_code} "
                              f"reason={lt.reason}")
        for c in pod.spec.containers:
            if c.resources:
                req = c.resources.requests or {}
                lim = c.resources.limits   or {}
                lines.append(
                    f"  {c.name} resources: "
                    f"req=cpu:{req.get('cpu','none')}/mem:{req.get('memory','none')} "
                    f"lim=cpu:{lim.get('cpu','none')}/mem:{lim.get('memory','none')}")
        return "\n".join(lines)
    except ApiException as e:
        return (f"Pod '{pod_name}' not found."
                if e.status == 404 else f"K8s error: {e.reason}")


def get_node_health() -> str:
    try:
        nodes = _core.list_node()
        if not nodes.items:
            return "No nodes found."

        # Build a map of GPU requests per node from running pods.
        # This is the only reliable way to know how many GPUs are in use
        # without requiring the metrics-server (kubectl top).
        gpu_used_by_node: dict = {}
        try:
            all_pods = _core.list_pod_for_all_namespaces(
                field_selector="status.phase=Running")
            for pod in all_pods.items:
                node_name = pod.spec.node_name or ""
                if not node_name:
                    continue
                for container in (pod.spec.containers or []):
                    reqs = (container.resources.requests or {}) if container.resources else {}
                    for key, val in reqs.items():
                        if "nvidia.com/gpu" in key or "amd.com/gpu" in key:
                            try:
                                gpu_used_by_node[node_name] = (
                                    gpu_used_by_node.get(node_name, 0) + int(val))
                            except (ValueError, TypeError):
                                pass
        except Exception:
            pass  # pod scan is best-effort — degrade gracefully

        lines      = ["Node health:"]
        gpu_nodes  = 0
        for node in nodes.items:
            roles    = [k.replace("node-role.kubernetes.io/", "")
                        for k in (node.metadata.labels or {})
                        if k.startswith("node-role.kubernetes.io/")] or ["worker"]
            conds    = {c.type: c.status for c in (node.status.conditions or [])}
            pressure = [t for t in ["MemoryPressure", "DiskPressure", "PIDPressure"]
                        if conds.get(t) == "True"]
            alloc    = node.status.allocatable or {}

            gpu_allocatable = 0
            for key in alloc:
                if "nvidia.com/gpu" in key or "amd.com/gpu" in key:
                    try:
                        gpu_allocatable += int(alloc[key])
                    except (ValueError, TypeError):
                        pass
            if gpu_allocatable:
                gpu_nodes += 1

            gpu_used = gpu_used_by_node.get(node.metadata.name, 0)
            if gpu_allocatable:
                gpu_str = (f" GPU:{gpu_used}/{gpu_allocatable} in-use/allocatable"
                           + (" (all in use)" if gpu_used >= gpu_allocatable else
                              " (available)" if gpu_used == 0 else
                              f" ({gpu_allocatable - gpu_used} free)"))
            else:
                gpu_str = ""

            lines.append(
                f"  {node.metadata.name} [{','.join(roles)}]: "
                f"Ready={conds.get('Ready','?')}"
                + (f" ⚠ {','.join(pressure)}" if pressure else "")
                + f" | CPU:{alloc.get('cpu','n/a')} Mem:{alloc.get('memory','n/a')}"
                + gpu_str)

        lines.append(f"Summary: {len(nodes.items)} node(s) total, {gpu_nodes} with GPU(s).")
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
    """Return True if the event message is known background noise."""
    msg_lower = (message or "").lower()
    return any(pat in msg_lower for pat in _EVENT_NOISE_PATTERNS)


def get_events(namespace: str = "all", warning_only: bool = True) -> str:
    try:
        fs = "type=Warning" if warning_only else ""
        ev = (_core.list_event_for_all_namespaces(field_selector=fs, limit=500)
              if namespace == "all"
              else _core.list_namespaced_event(namespace=namespace,
                                               field_selector=fs, limit=500))
        if not ev.items:
            return f"No {'warning ' if warning_only else ''}events in '{namespace}'."

        sev = sorted(ev.items,
                     key=lambda e: e.last_timestamp or e.event_time or "",
                     reverse=True)

        lines      = [f"Recent events in '{namespace}':"]
        shown      = 0
        suppressed = 0
        for e in sev:
            if shown >= 20:
                break
            if _is_noisy_event(e.message):
                suppressed += 1
                continue
            lines.append(
                f"  [{e.type}] {e.involved_object.kind}/{e.involved_object.name}: "
                f"{e.reason} — {e.message} (x{e.count or 1})")
            shown += 1

        if suppressed:
            lines.append(f"  ({suppressed} environment-noise event(s) suppressed)")
        if shown == 0:
            return f"No actionable events in '{namespace}' (all were background noise)."
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_deployment_status(namespace: str = "all") -> str:
    try:
        deps = (_apps.list_deployment_for_all_namespaces()
                if namespace == "all"
                else _apps.list_namespaced_deployment(namespace=namespace))
        if not deps.items:
            return f"No deployments in '{namespace}'."
        lines = [f"Deployments in '{namespace}':"]
        for dep in deps.items:
            desired = dep.spec.replicas or 0
            ready   = dep.status.ready_replicas or 0
            avail   = dep.status.available_replicas or 0
            status  = "✓ Healthy" if ready == desired and desired > 0 else "⚠ Degraded"
            lines.append(
                f"  {dep.metadata.namespace}/{dep.metadata.name}: {status} "
                f"| Desired:{desired} Ready:{ready} Available:{avail}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_daemonset_status(namespace: str = "all") -> str:
    try:
        ds = (_apps.list_daemon_set_for_all_namespaces()
              if namespace == "all"
              else _apps.list_namespaced_daemon_set(namespace=namespace))
        if not ds.items:
            return f"No DaemonSets in '{namespace}'."
        lines = [f"DaemonSets in '{namespace}':"]
        for d in ds.items:
            desired   = d.status.desired_number_scheduled or 0
            ready     = d.status.number_ready or 0
            available = d.status.number_available or 0
            status    = "✓ Healthy" if ready == desired and desired > 0 else "⚠ Degraded"
            lines.append(
                f"  {d.metadata.namespace}/{d.metadata.name}: {status} "
                f"| Desired:{desired} Ready:{ready} Available:{available}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_statefulset_status(namespace: str = "all") -> str:
    try:
        sts = (_apps.list_stateful_set_for_all_namespaces()
               if namespace == "all"
               else _apps.list_namespaced_stateful_set(namespace=namespace))
        if not sts.items:
            return f"No StatefulSets in '{namespace}'."
        lines = [f"StatefulSets in '{namespace}':"]
        for s in sts.items:
            desired = s.spec.replicas or 0
            ready   = s.status.ready_replicas or 0
            status  = "✓ Healthy" if ready == desired and desired > 0 else "⚠ Degraded"
            lines.append(
                f"  {s.metadata.namespace}/{s.metadata.name}: {status} "
                f"| Desired:{desired} Ready:{ready}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_job_status(namespace: str = "all") -> str:
    try:
        jobs = (_batch.list_job_for_all_namespaces()
                if namespace == "all"
                else _batch.list_namespaced_job(namespace=namespace))
        if not jobs.items:
            return f"No Jobs in '{namespace}'."
        lines = [f"Jobs in '{namespace}':"]
        for j in jobs.items:
            active    = j.status.active    or 0
            succeeded = j.status.succeeded or 0
            failed    = j.status.failed    or 0
            status    = ("✓ Complete" if succeeded > 0 and active == 0
                         else "⚠ Failed" if failed > 0
                         else "⏳ Running")
            lines.append(
                f"  {j.metadata.namespace}/{j.metadata.name}: {status} "
                f"| Active:{active} Succeeded:{succeeded} Failed:{failed}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_hpa_status(namespace: str = "all") -> str:
    """Check HorizontalPodAutoscaler targets and current replica counts."""
    try:
        hpas = (_autoscaling.list_horizontal_pod_autoscaler_for_all_namespaces()
                if namespace == "all"
                else _autoscaling.list_namespaced_horizontal_pod_autoscaler(
                    namespace=namespace))
        if not hpas.items:
            return f"No HPAs in '{namespace}'."
        lines = [f"HPAs in '{namespace}':"]
        for h in hpas.items:
            cur = h.status.current_replicas or 0
            des = h.status.desired_replicas or 0
            mn  = h.spec.min_replicas or 1
            mx  = h.spec.max_replicas or "?"
            at_max = cur >= h.spec.max_replicas if h.spec.max_replicas else False
            flag = " ⚠ AT MAX" if at_max else ""
            lines.append(
                f"  {h.metadata.namespace}/{h.metadata.name}: "
                f"current={cur} desired={des} min={mn} max={mx}{flag}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_pvc_status(namespace: str = "all", detail: bool = False) -> str:
    """Check PersistentVolumeClaims.

    For a specific namespace:
      - detail=False (default): grouped summary by access mode + storage class,
        plus full detail for any non-Bound PVCs. Concise for large namespaces.
      - detail=True: every PVC listed individually (use only when asked about
        a specific workload's PVC).
    For namespace="all": summary count + access-mode breakdown + non-Bound detail.
    Access modes (RWO/RWX/ROX) are included in all output.
    """
    _AM = {"ReadWriteOnce": "RWO", "ReadWriteMany": "RWX", "ReadOnlyMany": "ROX",
           "ReadWriteOncePod": "RWOP"}

    def _access(pvc):
        modes = pvc.spec.access_modes or []
        return ",".join(_AM.get(m, m) for m in modes) or "?"

    try:
        pvcs = (_core.list_persistent_volume_claim_for_all_namespaces()
                if namespace == "all"
                else _core.list_namespaced_persistent_volume_claim(
                    namespace=namespace))
        if not pvcs.items:
            return f"No PVCs found in namespace '{namespace}'."

        total = len(pvcs.items)

        if namespace != "all":
            bound_by_am: dict = {}
            non_bound = []
            for pvc in pvcs.items:
                phase = pvc.status.phase or "Unknown"
                sc    = pvc.spec.storage_class_name or "default"
                cap   = (pvc.status.capacity or {}).get("storage", "?")
                vol   = pvc.spec.volume_name or "<unbound>"
                am    = _access(pvc)
                if phase == "Bound":
                    if detail:
                        key = f"{am} ({sc})"
                        bound_by_am.setdefault(key, []).append(
                            f"    {pvc.metadata.name}: {cap} | volume:{vol}")
                    else:
                        key = f"{am} ({sc})"
                        bound_by_am[key] = bound_by_am.get(key, 0) + 1
                else:
                    non_bound.append(
                        f"  {pvc.metadata.name}: {phase} ⚠ | "
                        f"capacity:{cap} | access:{am} | class:{sc} | volume:{vol}")

            bound_count = (sum(bound_by_am.values()) if not detail
                           else sum(len(v) for v in bound_by_am.values()))
            lines = [f"PVCs in '{namespace}': {total} total "
                     f"({bound_count} Bound, {len(non_bound)} non-Bound)."]
            if bound_by_am:
                lines.append("Bound PVCs by access mode + storage class:")
                for k, val in sorted(bound_by_am.items()):
                    if detail:
                        lines.append(f"  {k}: {len(val)} PVC(s)")
                        lines.extend(val)
                    else:
                        lines.append(f"  {k}: {val} PVC(s)")
            if non_bound:
                lines.append("Non-Bound PVCs (full detail):")
                lines.extend(non_bound)
            return "\n".join(lines)

        bound_by_am: dict = {}
        non_bound = []
        by_ns_am: dict = {}          # {namespace: {"RWO": n, "RWX": n, ...}}
        for pvc in pvcs.items:
            phase = pvc.status.phase or "Unknown"
            sc    = pvc.spec.storage_class_name or "default"
            cap   = (pvc.status.capacity or {}).get("storage", "?")
            am    = _access(pvc)
            ns_name = pvc.metadata.namespace
            if phase == "Bound":
                key = f"{am} ({sc})"
                bound_by_am[key] = bound_by_am.get(key, 0) + 1
                # Per-namespace per-access-mode count
                ns_entry = by_ns_am.setdefault(ns_name, {})
                for mode in (pvc.spec.access_modes or []):
                    short = _AM.get(mode, mode)
                    ns_entry[short] = ns_entry.get(short, 0) + 1
            else:
                non_bound.append(
                    f"  {pvc.metadata.namespace}/{pvc.metadata.name}: "
                    f"{phase} ⚠ | access:{am} | class:{sc} capacity:{cap}")

        bound = sum(bound_by_am.values())
        lines = [f"PVCs across all namespaces: {total} total ({bound} Bound, {len(non_bound)} non-Bound)."]
        if bound_by_am:
            lines.append("Bound PVCs by access mode + storage class (cluster totals):")
            for k, count in sorted(bound_by_am.items()):
                lines.append(f"  {k}: {count} PVC(s)")

        # Per-namespace breakdown — essential for "which namespace has most RWO/RWX" queries
        if by_ns_am:
            lines.append("\nBound PVCs per namespace by access mode:")
            # Sort namespaces by total PVC count descending for easy scanning
            for ns_name in sorted(by_ns_am, key=lambda n: -sum(by_ns_am[n].values())):
                counts = by_ns_am[ns_name]
                summary = "  ".join(f"{am}:{n}" for am, n in sorted(counts.items()))
                lines.append(f"  {ns_name}: {summary}  (total {sum(counts.values())})")

        if non_bound:
            lines.append("Non-Bound PVCs:")
            lines.extend(non_bound)
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error (PVC listing): {e.reason}"


def get_persistent_volumes() -> str:
    """List all PersistentVolumes with phase, capacity, access mode, reclaim policy and bound claim."""
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


def get_service_status(namespace: str = "all") -> str:
    """List Services — highlights those with no endpoints (potential misconfigs)."""
    try:
        svcs = (_core.list_service_for_all_namespaces()
                if namespace == "all"
                else _core.list_namespaced_service(namespace=namespace))
        if not svcs.items:
            return f"No services in '{namespace}'."
        lines = [f"Services in '{namespace}':"]
        for svc in svcs.items:
            stype    = svc.spec.type or "ClusterIP"
            ports    = ", ".join(
                f"{p.port}/{p.protocol}" for p in (svc.spec.ports or []))
            selector = svc.spec.selector or {}
            flag     = "" if selector else " ⚠ no selector"
            lines.append(
                f"  {svc.metadata.namespace}/{svc.metadata.name}: "
                f"{stype} ports:[{ports}]{flag}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_ingress_status(namespace: str = "all", name: str = "") -> str:
    """
    List ingresses in a namespace, or return full YAML-style detail for a
    specific ingress when name is provided.
    """
    try:
        # ── Single ingress detail ─────────────────────────────────────────────
        if name:
            ns = namespace if namespace != "all" else "default"
            try:
                ing = _net.read_namespaced_ingress(name=name, namespace=ns)
            except ApiException as e:
                if e.status == 404:
                    return f"Ingress '{name}' not found in namespace '{ns}'."
                raise
            cls   = ing.spec.ingress_class_name or "default"
            hosts = [rule.host or "*" for rule in (ing.spec.rules or [])]
            lb    = [
                addr.ip or addr.hostname
                for status in (ing.status.load_balancer.ingress or [])
                for addr in [status] if status
            ] if ing.status.load_balancer else []
            ann   = ing.metadata.annotations or {}
            lines = [
                f"Ingress: {ing.metadata.namespace}/{ing.metadata.name}",
                f"  Class:     {cls}",
                f"  Hosts:     {', '.join(hosts) or 'none'}",
                f"  LB IP:     {', '.join(lb) or 'pending'}",
            ]
            if ann:
                lines.append("  Annotations:")
                for k, v in ann.items():
                    lines.append(f"    {k}: {v}")
            for rule in (ing.spec.rules or []):
                if rule.http:
                    lines.append(f"  Rules ({rule.host or '*'}):")
                    for path in (rule.http.paths or []):
                        svc  = path.backend.service
                        lines.append(
                            f"    {path.path or '/'} → "
                            f"{svc.name}:{svc.port.number or svc.port.name}"
                            f"  [{path.path_type}]")
            tls = ing.spec.tls or []
            if tls:
                lines.append("  TLS:")
                for t in tls:
                    lines.append(f"    secret:{t.secret_name} hosts:{t.hosts}")
            return "\n".join(lines)

        # ── Namespace listing ─────────────────────────────────────────────────
        ings = (_net.list_ingress_for_all_namespaces()
                if namespace == "all"
                else _net.list_namespaced_ingress(namespace=namespace))
        if not ings.items:
            return f"No Ingresses in '{namespace}'."
        lines = [f"Ingresses in '{namespace}':"]
        for ing in ings.items:
            cls   = ing.spec.ingress_class_name or "default"
            hosts = [rule.host or "*" for rule in (ing.spec.rules or [])]
            lb    = [
                addr.ip or addr.hostname
                for status in (ing.status.load_balancer.ingress or [])
                for addr in [status]
                if status
            ] if ing.status.load_balancer else []
            lines.append(
                f"  {ing.metadata.namespace}/{ing.metadata.name}: "
                f"class:{cls} hosts:{hosts} lb:{lb or 'pending'}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_configmap_list(namespace: str = "default", filter_keys: list = None) -> str:
    """List ConfigMaps in a namespace (excludes kube-system defaults).
    Keys that look like certificates or CA bundles are flagged with [cert].
    When filter_keys is set, only configmaps containing at least one matching
    key are returned — keeps output small for targeted searches."""
    _CERT_KEY_HINTS = {"ca.crt", "tls.crt", "tls.key", "ca-bundle", "ca-certificates",
                       "ca.pem", "cert.pem", "certificate", "ssl.crt", "ssl.key"}
    try:
        cms = _core.list_namespaced_config_map(namespace=namespace)
        skip = {"kube-root-ca.crt"}
        items = [cm for cm in cms.items if cm.metadata.name not in skip]
        if not items:
            return f"No ConfigMaps in '{namespace}'."

        # ── Filtered search ───────────────────────────────────────────────────
        if filter_keys:
            _fk_lower = [f.lower() for f in filter_keys]
            matches = []
            for cm in items:
                data = cm.data or {}
                hit_keys = [k for k in data
                            if any(f in k.lower() for f in _fk_lower)]
                if hit_keys:
                    matches.append((cm.metadata.name, data, hit_keys))
            if not matches:
                return (f"No ConfigMaps in '{namespace}' contain keys matching "
                        f"{filter_keys}.\nNo credentials found in configmaps.")
            lines = [f"ConfigMaps in '{namespace}' matching keys {filter_keys} "
                     f"({len(matches)} found):"]
            for cmname, data, hit_keys in sorted(matches):
                lines.append(f"  {cmname}:")
                for k in hit_keys:
                    lines.append(f"    {k}: {data[k]}")
            return "\n".join(lines)

        # ── Full listing ──────────────────────────────────────────────────────
        lines = [f"ConfigMaps in '{namespace}':"]
        for cm in items:
            keys = list((cm.data or {}).keys())
            cert_keys = [k for k in keys
                         if k in _CERT_KEY_HINTS
                         or any(h in k.lower() for h in ("cert", "tls", "ssl", "ca.", ".crt", ".pem"))]
            tag = " [cert]" if cert_keys else ""
            lines.append(f"  {cm.metadata.name}: keys={keys}{tag}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_secrets(namespace: str = "default", name: str = "",
                decode: bool = False, filter_keys: list = None) -> str:
    """
    List secrets in a namespace, or return decoded values for a specific secret.
    When decode=True all base64 values are decoded and shown.
    When decode=False all values are hidden (keys shown only).
    When filter_keys is set, only secrets containing at least one of those keys
    are returned, with matching key values decoded — useful for credential searches
    without dumping every secret in a large namespace (avoids LLM timeout).
    """
    import base64 as _b64

    def _decode(val: str) -> str:
        try:
            return _b64.b64decode(val).decode("utf-8", errors="replace")
        except Exception:
            return "<decode error>"

    try:
        # ── Single secret detail ──────────────────────────────────────────────
        if name:
            try:
                secret = _core.read_namespaced_secret(name=name, namespace=namespace)
            except ApiException as e:
                if e.status == 404:
                    return f"Secret '{name}' not found in namespace '{namespace}'."
                raise
            data = secret.data or {}
            lines = [
                f"Secret: {namespace}/{name}",
                f"  Type: {secret.type}",
            ]
            if data:
                lines.append("  Data:")
                for k, v in data.items():
                    lines.append(f"    {k}: {_decode(v or '') if decode else '<hidden>'}")
            if not decode:
                lines.append("\n  Values hidden. Enable 'Show Secret Values' in ⚙ Settings → Security to decode.")
            ann = secret.metadata.annotations or {}
            if ann:
                lines.append("  Annotations:")
                for k, v in ann.items():
                    lines.append(f"    {k}: {v}")
            return "\n".join(lines)

        # ── Filtered search: find secrets by key name pattern ─────────────────
        # Used for credential/cert queries — only returns secrets that contain
        # at least one of the requested key patterns, with values decoded.
        # This keeps the output tiny regardless of namespace size.
        if filter_keys:
            import logging as _flog
            _flog.getLogger("tools.k8s").debug(
                f"[get_secrets] ENTRY filter_keys={filter_keys}  decode={decode}  namespace={namespace}")
            _fk_lower = [f.lower() for f in filter_keys]
            secrets = _core.list_namespaced_secret(namespace=namespace)
            # First pass: find matching secret names using key names from list response
            # (list response has key names but values are None — fetch individually if decode needed)
            candidate_names = []
            for s in secrets.items:
                data = s.data or {}
                hit_keys = [k for k in data if any(f in k.lower() for f in _fk_lower)]
                if hit_keys:
                    candidate_names.append((s.metadata.name, s.type or "Opaque", hit_keys))

            if not candidate_names:
                return (f"No secrets in '{namespace}' contain keys matching "
                        f"{filter_keys}.\nNo credentials found in secrets.")

            lines = [f"Secrets in '{namespace}' matching keys {filter_keys} "
                     f"({len(candidate_names)} found):"]
            for sname, stype, hit_keys in sorted(candidate_names):
                lines.append(f"  {sname} [type={stype}]:")
                if decode:
                    # Fetch the full secret to get actual values
                    try:
                        full = _core.read_namespaced_secret(name=sname, namespace=namespace)
                        full_data = full.data or {}
                        for k in hit_keys:
                            val = _decode(full_data.get(k) or "")
                            lines.append(f"    {k}: {val}")
                    except ApiException:
                        for k in hit_keys:
                            lines.append(f"    {k}: <fetch error>")
                else:
                    for k in hit_keys:
                        lines.append(f"    {k}: <hidden>")
                    lines.append("    ❗ Secret values are hidden — enable 'Show Secret Values' in ⚙ Settings → Security to decode.")
            return "\n".join(lines)

        # ── Namespace listing ─────────────────────────────────────────────────
        secrets = _core.list_namespaced_secret(namespace=namespace)
        if not secrets.items:
            return f"No secrets in namespace '{namespace}'."

        _CERT_TYPES = {"kubernetes.io/tls", "helm.sh/release.v1"}
        _CERT_KEY_HINTS = {"ca.crt", "tls.crt", "tls.key", "ca-bundle",
                           "ca.pem", "cert.pem", "certificate", "ssl.crt"}

        by_type: dict = {}
        for s in secrets.items:
            t = s.type or "Opaque"
            # data keys are already present on the list response — no extra API call needed
            keys = list((s.data or {}).keys())
            # Flag secrets that are certificates or contain cert-like keys
            is_cert = (t in _CERT_TYPES
                       or any(k in _CERT_KEY_HINTS for k in keys)
                       or any(h in k.lower() for k in keys
                              for h in ("cert", "tls", "ssl", "ca.", ".crt", ".pem")))
            by_type.setdefault(t, []).append((s.metadata.name, keys, is_cert))

        lines = [f"Secrets in '{namespace}' ({len(secrets.items)} total):"]
        for stype, entries in sorted(by_type.items()):
            lines.append(f"  [{stype}] ({len(entries)})")
            for n, keys, is_cert in sorted(entries):
                tag = " [cert/TLS]" if is_cert else ""
                lines.append(f"    {n}: keys={keys}{tag}")
        lines.append("\n(Ask for a specific secret by name to see its values.)")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_resource_quotas(namespace: str = "all") -> str:
    """Check ResourceQuotas — highlights namespaces near their limits."""
    try:
        quotas = (_core.list_resource_quota_for_all_namespaces()
                  if namespace == "all"
                  else _core.list_namespaced_resource_quota(namespace=namespace))
        if not quotas.items:
            return f"No ResourceQuotas in '{namespace}'."
        lines = [f"ResourceQuotas in '{namespace}':"]
        for q in quotas.items:
            hard = q.status.hard or {}
            used = q.status.used or {}
            lines.append(f"  {q.metadata.namespace}/{q.metadata.name}:")
            for resource, limit in hard.items():
                current = used.get(resource, "0")
                lines.append(f"    {resource}: {current} / {limit}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_limit_ranges(namespace: str = "all") -> str:
    try:
        lrs = (_core.list_limit_range_for_all_namespaces()
               if namespace == "all"
               else _core.list_namespaced_limit_range(namespace=namespace))
        if not lrs.items:
            return f"No LimitRanges in '{namespace}'."
        lines = [f"LimitRanges in '{namespace}':"]
        for lr in lrs.items:
            lines.append(f"  {lr.metadata.namespace}/{lr.metadata.name}:")
            for item in (lr.spec.limits or []):
                lines.append(
                    f"    type:{item.type} "
                    f"max:{item.max} min:{item.min} default:{item.default}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_service_accounts(namespace: str = "default") -> str:
    try:
        sas = _core.list_namespaced_service_account(namespace=namespace)
        if not sas.items:
            return f"No ServiceAccounts in '{namespace}'."
        lines = [f"ServiceAccounts in '{namespace}':"]
        for sa in sas.items:
            secrets = len(sa.secrets or [])
            lines.append(f"  {sa.metadata.name}: secrets={secrets}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"


def get_cluster_role_bindings() -> str:
    """List ClusterRoleBindings — useful for auditing broad permissions."""
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


def get_namespace_status() -> str:
    """List ALL namespaces — uses the Python k8s client directly against the
    remote cluster API server. No local kubectl binary required."""
    try:
        items, _cont = [], None
        while True:
            kw = {"limit": 500}
            if _cont:
                kw["_continue"] = _cont
            page = _core.list_namespace(**kw)
            items.extend(page.items)
            _cont = (page.metadata._continue
                     if page.metadata and page.metadata._continue else None)
            if not _cont:
                break
        if not items:
            return "No namespaces found."
        active = sum(1 for ns in items if (ns.status.phase or "Active") == "Active")
        lines = [f"Total namespaces: {len(items)} ({active} Active)."]
        for ns in items:
            lines.append(
                f"  {ns.metadata.name:<40} {ns.status.phase or 'Active'}"
            )
        return "\n".join(lines)
    except ApiException as e:
        return f"[ERROR] K8s API error listing namespaces: {e.reason}"


_KUBECTL_MAX_OUT  = int(os.getenv("KUBECTL_MAX_CHARS", "20000"))


def _safe_reason(e) -> str:
    """
    Return a clean, single-line error string from an ApiException.

    ApiException.reason is usually a short string like "Internal Server Error".
    str(e) / e.body can contain multi-line JSON blobs that the LLM echoes back
    verbatim, producing garbled output. We always use e.reason (or a fallback)
    and never expose e.body to the LLM.
    """
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
    """
    Return (list_all_fn, list_ns_fn, get_fn, kind_label) for a resource type.
    list_all_fn(field_selector) -> items list across all namespaces
    list_ns_fn(namespace, field_selector) -> items list in one namespace
    get_fn(name, namespace) -> single object
    """
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
    """Call list_fn with automatic pagination, return all items."""
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
    """Look up the stored version for a CRD from the API server."""
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
    """
    Parse a kubectl command string into a structured dict.
    Returns keys: verb, resource, name, namespace, all_namespaces,
                  output_format, field_selector, tail, container,
                  subcommand, args, flags
    """
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
    """Convert a kubernetes client object to a YAML string."""
    try:
        d = _k8s.ApiClient().sanitize_for_serialization(obj)
        return _yaml.dump(d, default_flow_style=False, allow_unicode=True)
    except Exception:
        return str(obj)


def _obj_to_table(items, kind: str) -> str:
    """Format a list of k8s objects into a human-readable table."""
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
    """Format a list of custom resource dicts into a table."""
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
        return f"[ERROR] Unsupported resource type: {resource!r}. Use get_pod_status, get_node_health, or other specific tools."

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


def kubectl_exec(command: str) -> str:
    """
    Execute a kubectl command against the remote cluster using the Kubernetes
    Python API client. No local kubectl binary required — all calls go directly
    to the cluster API server over HTTPS using the credentials in KUBECONFIG.

    Supports:
      kubectl get <resource> [-n ns | -A] [name] [-o yaml|json]
      kubectl describe <resource> <name> -n <ns>
      kubectl logs <pod> -n <ns> [--tail=N] [-c container]
      kubectl top nodes | top pods [-n ns | -A]
      kubectl rollout history|status deployment/<name> -n <ns>
      kubectl auth can-i <verb> <resource> [-n ns]
      kubectl api-resources
      kubectl version
      Write operations (apply/delete/patch/scale) blocked unless
        KUBECTL_ALLOW_WRITES=true (not yet implemented — raise clearly)

    Parameters
    ----------
    command : str
        Full kubectl command string, e.g. ``kubectl get pods -n vault-system``.

    Returns
    -------
    str
        Formatted output, or ``[ERROR] ...`` on failure.
    """
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

K8S_TOOLS: dict = {

    "get_pod_status": {
        "fn":          get_pod_status,
        "description": (
            "List pods in a namespace. "
            "By default only UNHEALTHY pods are returned (non-Running, not ready, or high restarts). "
            "Set show_all=true to list ALL pods including healthy ones — ALWAYS use show_all=true "
            "when the user asks 'how many pods', 'list pods', 'what pods are running', or "
            "any question that requires a complete pod count or inventory."
        ),
        "parameters":  {
            "namespace":   {"type": "string",  "default": "all"},
            "show_all":    {"type": "boolean", "default": False,
                            "description": "Set true to include healthy/running pods in the output"},
            "raw_output":  {"type": "boolean", "default": False,
                            "description": "Set true to return kubectl-style tabular output (use when user asks to 'show', 'display', or wants 'the output' of pods)"},
        },
    },
    "get_pod_logs": {
        "fn":          get_pod_logs,
        "description": "Fetch recent logs from a specific pod. For multi-container pods, specify container_name.",
        "parameters":  {
            "pod_name":       {"type": "string"},
            "namespace":      {"type": "string",  "default": "default"},
            "tail_lines":     {"type": "integer", "default": 50,
                               "description": "Number of log lines to return (max 100)"},
        },
    },
    "describe_pod": {
        "fn":          describe_pod,
        "description": "Get detailed info about a specific pod including container states and resource limits.",
        "parameters":  {
            "pod_name":  {"type": "string"},
            "namespace": {"type": "string", "default": "default"},
        },
    },

    "get_node_health": {
        "fn":          get_node_health,
        "description": "Check node health, CPU/memory/disk pressure, and allocatable resources.",
        "parameters":  {},
    },

    "get_events": {
        "fn":          get_events,
        "description": (
            "Fetch recent K8s events. Use for diagnosing issues, errors, or warnings. "
            "warning_only=true (default) returns only Warning events. "
            "Set warning_only=false to include Normal events too."
        ),
        "parameters":  {
            "namespace":    {"type": "string",  "default": "all"},
            "warning_only": {"type": "boolean", "default": True,
                             "description": "true = Warning events only; false = all events including Normal"},
        },
    },

    "get_deployment_status": {
        "fn":          get_deployment_status,
        "description": "Check Deployment replica counts and health across namespaces.",
        "parameters":  {"namespace": {"type": "string", "default": "all"}},
    },
    "get_daemonset_status": {
        "fn":          get_daemonset_status,
        "description": "Check DaemonSet scheduling health — useful for node-level agents (e.g. Longhorn, CNI).",
        "parameters":  {"namespace": {"type": "string", "default": "all"}},
    },
    "get_statefulset_status": {
        "fn":          get_statefulset_status,
        "description": "Check StatefulSet replica counts — useful for databases and Longhorn components.",
        "parameters":  {"namespace": {"type": "string", "default": "all"}},
    },
    "get_job_status": {
        "fn":          get_job_status,
        "description": "Check batch Job and CronJob run status — highlights failed jobs.",
        "parameters":  {"namespace": {"type": "string", "default": "all"}},
    },
    "get_hpa_status": {
        "fn":          get_hpa_status,
        "description": "Check HorizontalPodAutoscaler targets and whether any are pinned at max replicas.",
        "parameters":  {"namespace": {"type": "string", "default": "all"}},
    },

    "get_pvc_status": {
        "fn":          get_pvc_status,
        "description": (
            "Check PersistentVolumeClaims — access mode (RWO/RWX/ROX), capacity, storage class, bound volume. "
            "Returns a grouped summary by access mode + storage class (concise, fast). "
            "Set detail=true only when asked about a specific workload's individual PVC names. "
            "Use namespace='longhorn-system' for Longhorn storage, namespace='vault-system' for Vault."
        ),
        "parameters":  {
            "namespace": {"type": "string", "default": "all"},
            "detail":    {"type": "boolean", "default": False,
                          "description": "Set true only to list individual PVC names (slower on large namespaces)."},
        },
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

    "get_service_status": {
        "fn":          get_service_status,
        "description": "List Services and highlight those with no pod selector (potential misconfigs).",
        "parameters":  {"namespace": {"type": "string", "default": "all"}},
    },
    "get_ingress_status": {
        "fn":          get_ingress_status,
        "description": "List Ingress rules, hostnames, and load balancer addresses.",
        "parameters":  {"namespace": {"type": "string", "default": "all"}},
    },

    "get_configmap_list": {
        "fn":          get_configmap_list,
        "description": (
            "List ConfigMaps in a namespace — useful for checking configuration drift. "
            "Use filter_keys to search for configmaps containing specific key names "
            "(e.g. filter_keys=['username','password'] to find credential configmaps)."
        ),
        "parameters":  {
            "namespace":   {"type": "string", "default": "default"},
            "filter_keys": {"type": "array",  "default": None,
                            "description": "Optional list of key name substrings to filter by."},
        },
    },
    "get_secrets": {
        "fn":          get_secrets,
        "description": (
            "List or search secrets in a namespace. "
            "Use filter_keys=['username','password','user','pass'] to find secrets "
            "containing credential keys. "
            "Use filter_keys=['tls','cert','ca'] for certificate searches. "
            "If name is provided, returns all keys of that specific secret. "
            "Whether values are shown or hidden is controlled by the user's Security settings — do NOT pass a decode argument."
        ),
        "parameters": {
            "namespace":   {"type": "string", "default": "default"},
            "name":        {"type": "string", "default": ""},
            "filter_keys": {"type": "array",  "default": None,
                            "description": "Optional list of key name substrings to filter by."},
        },
    },
    "get_resource_quotas": {
        "fn":          get_resource_quotas,
        "description": "Check ResourceQuotas and current usage — useful when pods fail to schedule.",
        "parameters":  {"namespace": {"type": "string", "default": "all"}},
    },
    "get_limit_ranges": {
        "fn":          get_limit_ranges,
        "description": "List LimitRanges that enforce default CPU/memory constraints per namespace.",
        "parameters":  {"namespace": {"type": "string", "default": "all"}},
    },

    "get_service_accounts": {
        "fn":          get_service_accounts,
        "description": "List ServiceAccounts in a namespace.",
        "parameters":  {"namespace": {"type": "string", "default": "default"}},
    },
    "get_cluster_role_bindings": {
        "fn":          get_cluster_role_bindings,
        "description": "List ClusterRoleBindings — useful for auditing broad RBAC permissions.",
        "parameters":  {},
    },

    "get_namespace_status": {
        "fn":          get_namespace_status,
        "description": "List all namespaces and their phase. ALWAYS use this when the user asks 'how many namespaces', 'list namespaces', or wants a namespace count.",
        "parameters":  {},
    },
}

K8S_TOOLS["kubectl_exec"] = {
    "fn":          kubectl_exec,
    "description": (
        "Execute a kubectl-style command against the remote cluster. "
        "Uses the Kubernetes Python API client directly — no local kubectl binary needed. "
        "CRITICAL: Shell operators are NOT supported. Do NOT use: ||, &&, |, awk, grep, "
        "sed, cut, wc, 2>/dev/null, or any shell pipe/redirect. "
        "For vault/namespace pods, use get_pod_status(namespace='vault-system', show_all=True) instead. "
        "Use for: custom resources (CRDs) like Longhorn volumes/replicas/engines, "
        "rollout history/status, top nodes/pods (requires metrics-server), "
        "auth can-i, api-resources, version, and ad-hoc diagnostics. "
        "Supported verbs: get, describe, logs, top, rollout, auth, api-resources, version. "
        "Write commands (apply/delete/patch/scale) blocked unless KUBECTL_ALLOW_WRITES=true. "
        "Always prefix the command with 'kubectl'."
    ),
    "parameters": {
        "command": {
            "type": "string",
            "description": (
                "Full kubectl command starting with 'kubectl'. "
                "Examples: "
                "'kubectl get pods -n vault-system', "
                "'kubectl get volumes.longhorn.io -n longhorn-system', "
                "'kubectl describe node worker-1', "
                "'kubectl logs mypod-xyz -n default --tail=50', "
                "'kubectl rollout status deployment/myapp -n prod', "
                "'kubectl top nodes', "
                "'kubectl auth can-i list pods -n default', "
                "'kubectl get namespaces -A'"
            ),
        },
    },
}
