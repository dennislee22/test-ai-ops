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




def _is_high_restart(pod, restart_count: int) -> bool:
    """
    Return True only if the pod's restarts indicate a CURRENT problem.

    Two-stage check:
    1. Recency: if the last restart was > 1 day ago the pod has stabilised —
       do NOT flag it regardless of total count. Old history is not a current issue.
    2. Rate (for recent restarts): flag only if rate > 3 restarts/day in the
       current run window.

    Hard floor: always flag if restart_count > 100 (catastrophic history).
    """
    import datetime as _dt

    if restart_count == 0:
        return False
    if restart_count > 100:
        return True

    now = _dt.datetime.now(_dt.timezone.utc)

    # last_state.terminated.finished_at is the most reliable indicator.
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
            return False   # stable — last restart was old history, not current

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


def get_pod_status(namespace: str = "all", show_all: bool = False, raw_output: bool = False, phase_only: bool = False) -> str:
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

        # ── Health-check / phase-only mode ──────────────────────────────────
        #                    or have high recent restarts (unhealthy-but-running)
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

            if not phase_only:
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
                                non_running_phases.append(pod)
                        _cont = (page.metadata._continue
                                 if page.metadata and page.metadata._continue else None)
                        if not _cont:
                            break
                    except ApiException:
                        break

            if not non_running_phases:
                msg = "All pods are in Running phase" if phase_only else "All pods are healthy and Running"
                return f"{msg} in namespace '{namespace}'."

            label = "Non-Running pods" if phase_only else "Unhealthy/non-Running pods"
            out_lines = [f"{label} in '{namespace}':"]
            for pod in non_running_phases:
                phase    = pod.status.phase or "Unknown"
                restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
                ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                tot      = len(pod.spec.containers)
                bad      = [f"{c.type}={c.status}"
                            for c in (pod.status.conditions or []) if c.status != "True"]
                out_lines.append(
                    f"  {pod.metadata.namespace}/{pod.metadata.name}: {phase} "
                    f"| Ready {ready}/{tot} | Restarts:{restarts}"
                    + (f" [{', '.join(bad)}]" if bad else ""))
            return "\n".join(out_lines)


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
                        if ready < tot or _is_high_restart(pod, restarts):
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
    # Strip accidental "namespace/podname" format the LLM sometimes passes
    if "/" in pod_name:
        parts = pod_name.split("/", 1)
        if len(parts) == 2:
            pod_name = parts[1]
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


def get_gpu_info() -> str:
    """Return detailed GPU model/spec info from node labels and capacity."""
    try:
        nodes = _core.list_node()
        if not nodes.items:
            return "No nodes found."

        results = []
        for node in nodes.items:
            labels   = node.metadata.labels or {}
            capacity = node.status.capacity or {}
            alloc    = node.status.allocatable or {}

            # Count allocatable GPUs from capacity keys
            gpu_cap   = {}
            gpu_alloc = {}
            for key, val in capacity.items():
                if "nvidia.com/gpu" in key or "amd.com/gpu" in key:
                    gpu_cap[key] = val
            for key, val in alloc.items():
                if "nvidia.com/gpu" in key or "amd.com/gpu" in key:
                    gpu_alloc[key] = val

            if not gpu_cap and not any(
                k.startswith("nvidia.com/") or k.startswith("amd.com/")
                for k in labels
            ):
                continue  # no GPU on this node

            info = [f"Node: {node.metadata.name}"]

            # nvidia-device-plugin / gpu-feature-discovery labels
            nvidia_label_prefixes = [
                "nvidia.com/gpu.product",
                "nvidia.com/gpu.memory",
                "nvidia.com/gpu.count",
                "nvidia.com/gpu.family",
                "nvidia.com/gpu.machine",
                "nvidia.com/cuda.driver.major",
                "nvidia.com/cuda.runtime.major",
                "feature.node.kubernetes.io/pci-10de",   # NVIDIA PCI
                "nvidia.com/mig.strategy",
            ]
            for prefix in nvidia_label_prefixes:
                for k, v in labels.items():
                    if k == prefix or k.startswith(prefix):
                        info.append(f"  {k}: {v}")

            # AMD equivalents
            for k, v in labels.items():
                if k.startswith("amd.com/gpu"):
                    info.append(f"  {k}: {v}")

            # Capacity and allocatable
            for k, v in gpu_cap.items():
                info.append(f"  capacity[{k}]: {v}")
            for k, v in gpu_alloc.items():
                if k not in gpu_cap or gpu_alloc[k] != gpu_cap[k]:
                    info.append(f"  allocatable[{k}]: {v}")

            if len(info) == 1:
                info.append("  (No detailed GPU labels found — device plugin may not be running)")

            results.append("\n".join(info))

        if not results:
            return "No GPU nodes detected in the cluster."

        return "\n\n".join(results)
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




def get_ingress_status(namespace: str = "all", name: str = "",
                       port: int = 0) -> str:
    """
    List ingresses, find by hostname (FQDN), exact name, or filter by port (e.g. 443).
    When name contains dots it is treated as a hostname and ALL namespaces are searched.
    When port is set (e.g. 443), only ingresses that expose that port are returned.
    """
    def _get_ports(ing) -> list:
        """Extract all ports exposed by an ingress (from TLS and service backends)."""
        ports = set()
        # TLS presence implies 443
        if ing.spec.tls:
            ports.add(443)
        # Annotations may declare ssl-redirect or force-ssl
        ann = ing.metadata.annotations or {}
        for v in ann.values():
            if "443" in str(v):
                ports.add(443)
        # Backend service ports
        for rule in (ing.spec.rules or []):
            if rule.http:
                for path in (rule.http.paths or []):
                    svc = path.backend.service
                    p = svc.port.number if svc.port.number else None
                    if p:
                        ports.add(p)
        # If no explicit service port found, assume 80
        if not ports:
            ports.add(80)
        return sorted(ports)

    def _fmt(ing) -> str:
        cls   = ing.spec.ingress_class_name or "default"
        hosts = [rule.host or "*" for rule in (ing.spec.rules or [])]
        ports = _get_ports(ing)
        lb    = [
            addr.ip or addr.hostname
            for status in (ing.status.load_balancer.ingress or [])
            for addr in [status] if status
        ] if ing.status.load_balancer else []
        ann   = ing.metadata.annotations or {}
        out = [
            f"Ingress: {ing.metadata.namespace}/{ing.metadata.name}",
            f"  Class:     {cls}",
            f"  Hosts:     {', '.join(hosts) or 'none'}",
            f"  Ports:     {', '.join(str(p) for p in ports)}",
            f"  LB IP:     {', '.join(lb) or 'pending'}",
        ]
        if ann:
            out.append("  Annotations:")
            for k, v in ann.items():
                out.append(f"    {k}: {v}")
        for rule in (ing.spec.rules or []):
            if rule.http:
                out.append(f"  Rules ({rule.host or '*'}):")
                for path in (rule.http.paths or []):
                    svc = path.backend.service
                    out.append(
                        f"    {path.path or '/'} -> "
                        f"{svc.name}:{svc.port.number or svc.port.name}"
                        f"  [{path.path_type}]")
        tls = ing.spec.tls or []
        if tls:
            out.append("  TLS:")
            for t in tls:
                out.append(f"    secret:{t.secret_name} hosts:{t.hosts}")
        return "\n".join(out)

    try:
        all_ings = _net.list_ingress_for_all_namespaces()
        pool = all_ings.items

        # ── Port filter ───────────────────────────────────────────────────────
        if port:
            pool = [ing for ing in pool if port in _get_ports(ing)]
            if not pool:
                return f"No ingresses found exposing port {port} in any namespace."
            out = [f"Ingresses exposing port {port}:"]
            for ing in pool:
                hosts = [rule.host or "*" for rule in (ing.spec.rules or [])]
                ports = _get_ports(ing)
                out.append(
                    f"  {ing.metadata.namespace}/{ing.metadata.name}: "
                    f"hosts:{hosts} ports:{ports}")
            return "\n".join(out)

        if name:
            # ── Hostname search (name contains dots → treat as FQDN) ──────────
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
                        f"Use get_ingress_status(namespace='all') to list all ingresses."
                    )
                return "\n\n".join(_fmt(ing) for ing in matches)

            # ── Exact ingress name search ─────────────────────────────────────
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
                return "\n\n".join(_fmt(ing) for ing in matches)

        # ── List ingresses (namespace filter) ─────────────────────────────────
        if namespace != "all":
            pool = [i for i in pool if i.metadata.namespace == namespace]
        if not pool:
            return f"No Ingresses in '{namespace}'."
        out = [f"Ingresses in '{namespace}':"]
        for ing in pool:
            hosts = [rule.host or "*" for rule in (ing.spec.rules or [])]
            ports = _get_ports(ing)
            lb    = [
                addr.ip or addr.hostname
                for status in (ing.status.load_balancer.ingress or [])
                for addr in [status] if status
            ] if ing.status.load_balancer else []
            out.append(
                f"  {ing.metadata.namespace}/{ing.metadata.name}: "
                f"hosts:{hosts} ports:{ports} lb:{lb or 'pending'}")
        return "\n".join(out)
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
    """List ALL namespaces with their pod count — uses the Python k8s client directly against the
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

        # Fetch pod counts per namespace in one call per namespace
        ns_pod_counts: dict = {}
        for ns in items:
            ns_name = ns.metadata.name
            try:
                pods = _core.list_namespaced_pod(namespace=ns_name, limit=1000)
                ns_pod_counts[ns_name] = len(pods.items)
            except ApiException:
                ns_pod_counts[ns_name] = -1

        active = sum(1 for ns in items if (ns.status.phase or "Active") == "Active")
        lines = [f"Total namespaces: {len(items)} ({active} Active).",
                 f"{'NAMESPACE':<40} {'STATUS':<10} {'PODS':>5}"]
        for ns in items:
            ns_name = ns.metadata.name
            count   = ns_pod_counts.get(ns_name, 0)
            count_s = str(count) if count >= 0 else "err"
            lines.append(
                f"  {ns_name:<38} {ns.status.phase or 'Active':<10} {count_s:>5}"
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

    # ── Intercept `kubectl exec` and route to exec_pod_command ──────────────
    # Pattern: kubectl exec [-n ns] [-it|-i|-t] <pod> [--container=c] -- <shell_cmd>
    _exec_re = re.compile(
        r"^kubectl\s+exec\s+"
        r"(?:(?:-n\s+(\S+)|--namespace[= ](\S+))\s+)?"
        r"(?:-[it]{1,2}\s+)?"
        r"(\S+)"                          # pod name
        r"(?:\s+(?:--container[= ](\S+)|-c\s+(\S+)))?"
        r"\s+--\s+(.+)$",                 # shell command after --
        re.DOTALL,
    )
    _em = _exec_re.match(command)
    if _em:
        ns       = _em.group(1) or _em.group(2) or "default"
        pod      = _em.group(3)
        container = _em.group(4) or _em.group(5) or ""
        shell_cmd = _em.group(6).strip()
        # strip 'sh -c ...' wrapper — exec_pod_command wraps in sh -c itself
        _shc_sq = re.match(r"^(?:sh|bash)\s+-c\s+'(.*)'$", shell_cmd, re.DOTALL)
        _shc_dq = re.match(r'^(?:sh|bash)\s+-c\s+"(.*)"$', shell_cmd, re.DOTALL)
        if _shc_sq:
            shell_cmd = _shc_sq.group(1)
        elif _shc_dq:
            shell_cmd = _shc_dq.group(1)
        _log.info(f"[kubectl_exec] routing kubectl exec → exec_pod_command(ns={ns}, pod={pod}, container={container!r})")
        return exec_pod_command(namespace=ns, pod_name=pod, command=shell_cmd, container=container)

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

# in a single tool call — avoids calling describe_pod on every pod individually.

def _parse_cpu_to_millicores(cpu_str: str) -> int:
    """Convert a K8s CPU string to millicores (int). Returns 0 if unparseable."""
    if not cpu_str or cpu_str in ("none", "<none>", "0"):
        return 0
    cpu_str = cpu_str.strip()
    try:
        if cpu_str.endswith("m"):
            return int(cpu_str[:-1])
        # Whole cores (e.g. "1", "2.5")
        return int(float(cpu_str) * 1000)
    except (ValueError, TypeError):
        return 0


def _parse_mem_to_mib(mem_str: str) -> float:
    """Convert a K8s memory string to MiB (float). Returns 0 if unparseable."""
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
        return float(mem_str) / (1024 * 1024)  # raw bytes → MiB
    except (ValueError, TypeError):
        return 0.0



def get_namespace_resource_summary(namespace: str) -> str:
    """
    Aggregate CPU and memory requests/limits for ALL pods in a namespace.
    Includes both regular containers and init containers.
    Returns total CPU/memory requested and limited, plus per-pod breakdown.
    """
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
    pod_lines = []

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

        cpu_req_s = f"{pod_cpu_req_m}m" if pod_cpu_req_m else "0m (not set)"
        mem_req_s = f"{pod_mem_req_mib:.0f}Mi" if pod_mem_req_mib else "0Mi (not set)"
        cpu_lim_s = f"{pod_cpu_lim_m}m" if pod_cpu_lim_m else "0m (not set)"
        mem_lim_s = f"{pod_mem_lim_mib:.0f}Mi" if pod_mem_lim_mib else "0Mi (not set)"
        pod_lines.append(
            f"  {pod.metadata.name}: "
            f"cpu_req={cpu_req_s}  mem_req={mem_req_s}  "
            f"cpu_lim={cpu_lim_s}  mem_lim={mem_lim_s}"
        )

    def _fmt_cpu(m: int) -> str:
        if m == 0:
            return "0m — no CPU requests set on any pod"
        return f"{m}m ({m/1000:.3f} cores)"

    def _fmt_mem(mib: float) -> str:
        if mib == 0:
            return "0Mi — no memory requests set on any pod"
        return f"{mib:.0f}Mi ({mib/1024:.2f}Gi)"

    lines_out = [
        f"Resource summary for namespace '{namespace}' ({len(pods.items)} pods):",
        f"",
        f"  TOTAL CPU  requested : {_fmt_cpu(total_cpu_req_m)}",
        f"  TOTAL CPU  limit     : {_fmt_cpu(total_cpu_lim_m)}",
        f"  TOTAL MEM  requested : {_fmt_mem(total_mem_req_mib)}",
        f"  TOTAL MEM  limit     : {_fmt_mem(total_mem_lim_mib)}",
        f"",
        f"Per-pod breakdown (cpu_req / mem_req / cpu_lim / mem_lim):",
    ] + pod_lines

    return "\n".join(lines_out)


    def _fmt_cpu(m: int) -> str:
        if m == 0:
            return "0m (none set)"
        return f"{m}m ({m/1000:.3f} cores)"

    def _fmt_mem(mib: float) -> str:
        if mib == 0:
            return "0Mi (none set)"
        return f"{mib:.0f}Mi ({mib/1024:.2f}Gi)"

    lines = [
        f"Resource summary for namespace '{namespace}' ({len(pods.items)} pods):",
        f"",
        f"  TOTAL CPU  requested : {_fmt_cpu(total_cpu_req_m)}",
        f"  TOTAL CPU  limit     : {_fmt_cpu(total_cpu_lim_m)}",
        f"  TOTAL MEM  requested : {_fmt_mem(total_mem_req_mib)}",
        f"  TOTAL MEM  limit     : {_fmt_mem(total_mem_lim_mib)}",
        f"",
        f"Per-pod CPU/memory requests:",
    ] + pod_lines

    return "\n".join(lines)


# INSERT / UPDATE / DELETE / DROP / TRUNCATE / ALTER / CREATE are blocked.

_ALLOW_DB_EXEC = os.getenv("ALLOW_DB_EXEC", "true").lower() in ("1", "true", "yes")

_SQL_WRITE_RE = re.compile(
    r"^\s*(insert|update|delete|drop|truncate|alter|create|replace|rename|grant|revoke"
    r"|call|exec|execute|lock|unlock|flush|reset|purge|load\s+data)\b",
    re.IGNORECASE,
)

# Keys that commonly hold DB credentials in secrets / configmaps
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
    """
    Inspect the named pod's env vars, secretRef, configMapRef, and volumeMounts
    to discover DB credentials (user, password, database, host, port).
    Returns a dict with keys: user, password, database, host, port (any may be None).
    """
    import base64 as _b64

    creds: dict = {k: None for k in ("user", "password", "database", "host", "port")}

    # Helper: check a key name against known patterns
    def _match(key: str, patterns: tuple) -> str | None:
        kl = key.lower().replace("-", "_")
        for p in patterns:
            if p.replace("-", "_") in kl:
                return key
        return None

    def _harvest(key: str, val: str):
        """Slot a plain-text k/v into creds if it matches a known pattern."""
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
            # Plain value
            if env.value:
                _harvest(env.name, env.value)
                continue
            # Value from secret
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

        # envFrom — entire secret or configmap mounted as env
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
    """
    Detect whether a pod runs MySQL/MariaDB or PostgreSQL.
    Checks in order: image name → container name → env var names → 'db'-named container fallback.
    Returns 'mysql', 'postgres', or None.
    If container_hint is given, check that container first.
    """
    try:
        pod = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
    except ApiException:
        return None

    containers = pod.spec.containers or []

    # If a specific container is hinted, check it first
    if container_hint:
        containers = sorted(containers,
                            key=lambda c: 0 if c.name == container_hint else 1)

    # Pass 1: image-based detection (most reliable)
    for c in containers:
        image = (c.image or "").lower()
        if any(x in image for x in ("mysql", "mariadb", "percona")):
            return "mysql"
        if any(x in image for x in ("postgres", "postgresql", "pg:", "/pg-", "-pg-", "pgbouncer")):
            return "postgres"

    # Pass 2: container name hints
    for c in containers:
        cname = (c.name or "").lower()
        if any(x in cname for x in ("mysql", "mariadb")):
            return "mysql"
        if any(x in cname for x in ("postgres", "postgresql")):
            return "postgres"

    # Pass 3: env var name hints (POSTGRES_* / MYSQL_* / PG* are definitive)
    for c in containers:
        for env in (c.env or []):
            name_lc = env.name.lower()
            if name_lc.startswith(("postgres", "pgdata", "pguser", "pgpassword")):
                return "postgres"
            if name_lc.startswith(("mysql", "mariadb")):
                return "mysql"

    # Pass 4: container literally named "db" — default to postgres
    # (most common in Cloudera ECS and similar deployments)
    for c in containers:
        cname = (c.name or "").lower()
        if cname in ("db", "database"):
            return "postgres"

    return None


def _find_db_container(pod_name: str, namespace: str, db_type: str) -> str:
    """
    Return the name of the container inside the pod that runs the DB CLI.
    For multi-container pods, picks the container whose image or name best
    matches the db_type. Falls back to the first container.
    """
    try:
        pod = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
    except ApiException:
        return ""

    _MYSQL_HINTS    = ("mysql", "mariadb", "percona")
    _POSTGRES_HINTS = ("postgres", "postgresql", "pg:", "/pg-", "-pg-")

    hints = _MYSQL_HINTS if db_type == "mysql" else _POSTGRES_HINTS

    # 1st pass: container whose image matches
    for c in (pod.spec.containers or []):
        if any(h in (c.image or "").lower() for h in hints):
            return c.name

    # 2nd pass: container whose name matches (e.g. "db", "postgres", "mysql")
    for c in (pod.spec.containers or []):
        cname = (c.name or "").lower()
        if db_type == "mysql" and any(h in cname for h in ("mysql", "mariadb", "db")):
            return c.name
        if db_type == "postgres" and any(h in cname for h in ("postgres", "pg", "db")):
            return c.name

    # Fallback: first container
    containers = pod.spec.containers or []
    return containers[0].name if containers else ""


def _find_db_pod(namespace: str, hint: str = "") -> tuple[str | None, str | None]:
    """
    Find the first running DB pod in a namespace.
    Returns (pod_name, db_type) or (None, None).
    Optionally filters by `hint` substring in pod name.
    """
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
    """Run a one-shot shell command inside a pod and return stdout, or empty string on error."""
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
    """
    Connect to the 'postgres' maintenance DB and return the first non-system
    database found in pg_database. Falls back to 'postgres' if nothing found.
    """
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
    """
    Run SHOW DATABASES inside the pod and return the first non-system database.
    Falls back to empty string if nothing found.
    """
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


def exec_db_query(namespace: str, sql: str,
                  pod_name: str = "", database: str = "",
                  container: str = "") -> str:
    """
    Execute a read-only SQL query inside a database pod in the given namespace.

    Workflow:
      1. Locate a running DB pod (MySQL/MariaDB or PostgreSQL) — auto-detected
         from container image. Use pod_name to target a specific pod.
      2. Identify the correct container inside the pod (e.g. "db" in multi-container pods).
         Use container= to override auto-detection.
      3. Retrieve DB credentials (user, password, database name) from the pod's
         environment variables, secretRefs, and configMapRefs.
      4. Run the SQL via kubectl exec using the K8s stream API.

    Only SELECT and other read-only statements are permitted.
    INSERT / UPDATE / DELETE / DROP / ALTER / TRUNCATE are blocked.

    Parameters
    ----------
    namespace : str
        Kubernetes namespace to search for the DB pod.
    sql : str
        SQL query to run (SELECT only — write operations are blocked).
    pod_name : str, optional
        Specific pod name to target. If empty, the first running DB pod is used.
    database : str, optional
        Database/schema name to connect to. Overrides auto-detected value.
    container : str, optional
        Container name inside the pod. If empty, auto-detected from image/name.
        Use this when the pod has multiple containers and auto-detection picks wrong one.
    """
    if not _ALLOW_DB_EXEC:
        return "[ERROR] DB query execution is disabled. Set ALLOW_DB_EXEC=true to enable."

    sql = sql.strip().rstrip(";")
    if not sql:
        return "[ERROR] Empty SQL query."

    if _SQL_WRITE_RE.match(sql):
        return (
            "[BLOCKED] Write operations are not permitted. "
            "Only SELECT and read-only queries are allowed."
        )

    if pod_name:
        # Strip namespace/ prefix if LLM passes it
        if "/" in pod_name:
            pod_name = pod_name.split("/", 1)[1]
        db_type = _detect_db_type(pod_name, namespace, container_hint=container)
        if not db_type:
            # List available containers to help diagnose
            try:
                pod = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
                cnames = [c.name for c in (pod.spec.containers or [])]
                return (f"[ERROR] Could not detect DB type for pod '{pod_name}' in '{namespace}'. "
                        f"Available containers: {', '.join(cnames)}. "
                        f"Re-call with container='<name>' set to the DB container.")
            except ApiException:
                return (f"[ERROR] Pod '{pod_name}' not found in namespace '{namespace}'.")
    else:
        pod_name, db_type = _find_db_pod(namespace)
        if not pod_name:
            return (f"[ERROR] No running MySQL/MariaDB or PostgreSQL pod found "
                    f"in namespace '{namespace}'.")

    _log.info(f"[exec_db_query] pod={namespace}/{pod_name}  db_type={db_type}")

    if container:
        container_name = container  # explicit override
    else:
        container_name = _find_db_container(pod_name, namespace, db_type)
    _log.info(f"[exec_db_query] container={container_name!r}")

    creds = _find_db_credentials(namespace, pod_name)
    # Explicit caller override wins; then env-var discovery; then live DB query fallback
    db_name = database or creds.get("database") or ""

    _log.debug(f"[exec_db_query] creds found: user={creds['user']}  "
               f"db={db_name!r}  host={creds['host']}")

    from kubernetes.stream import stream as _k8s_stream

    safe_sql = sql.replace("'", "'\\''")

    if db_type == "mysql":
        user     = creds["user"] or "root"
        password = creds["password"] or ""
        host     = creds["host"] or "127.0.0.1"
        port     = creds["port"] or "3306"

        # Auto-discover the application database if not found from env vars
        if not db_name:
            db_name = _discover_mysql_database(
                pod_name, namespace, container_name, user, password, host, port)
            _log.info(f"[exec_db_query] mysql db auto-discovered: {db_name!r}")

        pass_arg = f"-p'{password}'" if password else ""
        db_arg   = db_name if db_name else ""
        cmd = (
            f"mysql -u{user} {pass_arg} -h{host} -P{port} "
            f"--connect-timeout=10 --batch --silent "
            f"{db_arg} -e '{safe_sql}'"
        )
        exec_cmd = ["/bin/sh", "-c", cmd]

    elif db_type == "postgres":
        user     = creds["user"] or "postgres"
        password = creds["password"] or ""

        # Auto-discover the application database if not found from env vars.
        # NEVER default to the username — that almost always points to the wrong db.
        if not db_name:
            db_name = _discover_pg_database(
                pod_name, namespace, container_name, user, password)
            _log.info(f"[exec_db_query] postgres db auto-discovered: {db_name!r}")

        # MySQL: table_schema=DATABASE()  →  PostgreSQL: table_schema='public'
        # (information_schema.tables covers the connected database only)
        pg_sql = re.sub(
            r"table_schema\s*=\s*DATABASE\s*\(\s*\)",
            "table_schema='public'",
            safe_sql,
            flags=re.IGNORECASE,
        )
        # MySQL: SHOW TABLES  →  list tables in connected db
        if re.match(r"^\s*SHOW\s+TABLES\s*$", pg_sql, re.IGNORECASE):
            pg_sql = (
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='public' ORDER BY table_name"
            )
        # MySQL: SHOW DATABASES  →  PostgreSQL equivalent
        if re.match(r"^\s*SHOW\s+DATABASES\s*$", pg_sql, re.IGNORECASE):
            pg_sql = "SELECT datname FROM pg_database ORDER BY datname"

        safe_sql = pg_sql  # use the rewritten SQL from here on

        # Try local Unix socket first (peer/trust auth — most common inside containers).
        # If host is explicitly set in env vars, use TCP instead.
        host = creds.get("host") or ""
        port = creds.get("port") or "5432"

        pg_env    = f"PGPASSWORD='{password}' " if password else ""
        db_flag   = f"-d {db_name}" if db_name else ""
        user_flag = f"-U {user}" if user else ""

        if host and host not in ("localhost", "127.0.0.1", "::1"):
            # Remote TCP connection
            cmd = (
                f"{pg_env}psql {user_flag} -h {host} -p {port} "
                f"{db_flag} --no-password -t -A -c '{safe_sql}'"
            )
        else:
            # Local socket — peer/trust auth; PGPASSWORD set as fallback
            cmd = (
                f"{pg_env}psql {user_flag} {db_flag} "
                f"--no-password -t -A -c '{safe_sql}' 2>&1 || "
                # fallback: drop -U (use OS user inside the container)
                f"psql {db_flag} -t -A -c '{safe_sql}'"
            )
        exec_cmd = ["/bin/sh", "-c", cmd]

    else:
        return f"[ERROR] Unsupported DB type: {db_type}"

    stream_kwargs = dict(
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=True,
    )
    # Pass container name for multi-container pods — required by the K8s API
    if container_name:
        stream_kwargs["container"] = container_name

    try:
        resp = _k8s_stream(
            _core.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            command=exec_cmd,
            **stream_kwargs,
        )
        output = resp.strip() if isinstance(resp, str) else str(resp).strip()
    except ApiException as e:
        return f"[ERROR] K8s exec failed (pod={pod_name}, container={container_name}): {_safe_reason(e)}"
    except Exception as exc:
        return f"[ERROR] Unexpected exec error: {exc}"

    if not output:
        return "(Query returned no rows.)"

    # Truncate to protect LLM context window
    if len(output) > _KUBECTL_MAX_OUT:
        output = output[:_KUBECTL_MAX_OUT] + f"\n...[output truncated at {_KUBECTL_MAX_OUT} chars]"

    # Prepend a summary header for the LLM
    header = (f"DB query result  [{db_type.upper()} · pod={pod_name} · ns={namespace}"
              + (f" · db={db_name}" if db_name else "") + "]\n"
              + "-" * 60 + "\n")
    return header + output


def _get_first_running_pod(namespace: str) -> str:
    """Return the name of the first Running pod in namespace, or '' if none found."""
    try:
        pods = _core.list_namespaced_pod(
            namespace,
            field_selector="status.phase=Running",
            limit=10,
        ).items
        for p in pods:
            if p.status and p.status.phase == "Running":
                return p.metadata.name
    except Exception:
        pass
    return ""


def _find_pod_with_binary(namespace: str, binary: str, max_pods: int = 100) -> str:
    """
    Scan running pods in namespace until one is found that has `binary` available.
    Returns pod name, or empty string if none found within max_pods attempts.
    """
    from kubernetes.stream import stream as _k8s_stream
    try:
        pods = _core.list_namespaced_pod(
            namespace,
            field_selector="status.phase=Running",
            limit=max_pods,
        ).items
    except Exception:
        return ""

    # Score pods: prefer app/service/manager pods, deprioritise DB/metrics/logging
    _prefer      = re.compile(r"(-app-|-service-|-manager-|-controller-|-worker-|-api-|-server-|-gateway-|-proxy-|-plane-app-|-platform-|-core-)", re.I)
    _deprioritise = re.compile(r"(embedded-db|postgres|mysql|mariadb|prometheus|alertmanager|fluentd|fluent-bit|elasticsearch|kafka|zookeeper|-exporter-)", re.I)

    def _pod_score(p):
        name = p.metadata.name or ""
        if _deprioritise.search(name):
            return 2   # last
        if _prefer.search(name):
            return 0   # first
        return 1       # middle

    ordered = sorted(pods, key=_pod_score)

    for p in ordered:
        if not (p.status and p.status.phase == "Running"):
            continue
        pod_name = p.metadata.name
        try:
            resp = _k8s_stream(
                _core.connect_get_namespaced_pod_exec,
                pod_name, namespace,
                command=["/bin/sh", "-c", f"which {binary} 2>/dev/null || command -v {binary} 2>/dev/null"],
                stderr=True, stdin=False, stdout=True, tty=False, _preload_content=True,
            )
            _log.debug(f"[find_pod_with_binary] pod={pod_name!r} which={resp!r:.60}")
            if resp and resp.strip():
                _log.info(f"[exec_pod_command] found {binary!r} in pod {pod_name!r}")
                return pod_name
        except Exception as _scan_err:
            _log.debug(f"[find_pod_with_binary] pod={pod_name!r} scan error: {_scan_err}")
            continue
    _log.warning(f"[exec_pod_command] {binary!r} not found in any of the scanned pods in namespace {namespace!r}")
    return ""


def exec_pod_command(namespace: str, pod_name: str, command: str, container: str = "") -> str:
    """
    Execute an arbitrary shell command inside a running pod using the K8s stream API.
    Use this for commands like openssl, base64, cat, etc. that cannot be run via kubectl_exec.

    Args:
        namespace:  Pod namespace (e.g. "cdp")
        pod_name:   Exact pod name from kubectl get pods (e.g. "cdp-embedded-db-0")
        command:    Shell command to run (e.g. "echo '...' | openssl x509 -text -noout")
        container:  Container name for multi-container pods (optional)
    Returns:
        Command stdout/stderr output, or an error string.
    """
    from kubernetes.stream import stream as _k8s_stream

    # ── Validate pod_name is an actual running pod, not a secret/cm name ────
    try:
        pod_obj = _core.read_namespaced_pod(pod_name, namespace)
        phase = (pod_obj.status.phase or "") if pod_obj.status else ""
        if phase != "Running":
            # Pod exists but not running — find one that is
            real_pod = _get_first_running_pod(namespace)
            if real_pod:
                _log.warning(f"[exec_pod_command] pod {pod_name!r} is {phase!r}, switching to {real_pod!r}")
                pod_name = real_pod
            else:
                return f"[ERROR] Pod '{pod_name}' is not Running (phase={phase}) and no Running pods found in namespace '{namespace}'."
    except ApiException as e:
        if e.status == 404:
            # pod_name doesn't exist — likely confused with a secret/cm name; auto-pick a real pod
            real_pod = _get_first_running_pod(namespace)
            if real_pod:
                _log.warning(f"[exec_pod_command] pod {pod_name!r} not found (404), auto-selecting {real_pod!r}")
                pod_name = real_pod
            else:
                return (
                    f"[ERROR] '{pod_name}' is not a valid pod name in namespace '{namespace}'. "
                    f"Use kubectl_exec('kubectl get pods -n {namespace} --field-selector=status.phase=Running "
                    f"--no-headers -o custom-columns=NAME:.metadata.name') to get a real pod name first."
                )
        else:
            return f"[ERROR] Could not verify pod '{pod_name}': {_safe_reason(e)}"

    exec_cmd = ["/bin/sh", "-c", command]
    stream_kwargs = dict(stderr=True, stdin=False, stdout=True, tty=False, _preload_content=True)
    if container:
        stream_kwargs["container"] = container

    def _run(pname: str) -> str:
        try:
            resp = _k8s_stream(
                _core.connect_get_namespaced_pod_exec,
                pname, namespace,
                command=exec_cmd,
                **stream_kwargs,
            )
            return resp.strip() if isinstance(resp, str) else str(resp).strip()
        except ApiException as e:
            return f"[ERROR] exec failed (pod={pname} ns={namespace}): {_safe_reason(e)}"
        except Exception as exc:
            return f"[ERROR] Unexpected exec error: {exc}"

    output = _run(pod_name)

    # If binary not found, try other preferred pods directly (no probing — just run and check)
    if output and ("not found" in output or "No such file" in output):
        _nf = re.search(r"(\S+):\s*not found", output)
        binary_hint = _nf.group(1) if _nf else "unknown"
        _log.warning(f"[exec_pod_command] {binary_hint!r} missing in {pod_name!r}, trying other pods...")

        try:
            all_pods = _core.list_namespaced_pod(
                namespace, field_selector="status.phase=Running", limit=200
            ).items
        except Exception:
            all_pods = []

        _prefer      = re.compile(r"(-app-|-service-|-manager-|-controller-|-worker-|-api-|-server-|-gateway-|-proxy-|-plane-app-|-platform-|-core-)", re.I)
        _deprioritise = re.compile(r"(embedded-db|postgres|mysql|mariadb|prometheus|alertmanager|fluentd|fluent-bit|elasticsearch|kafka|zookeeper|-exporter-)", re.I)

        def _pod_score(p):
            name = p.metadata.name or ""
            if _deprioritise.search(name): return 2
            if _prefer.search(name):       return 0
            return 1

        for p in sorted(all_pods, key=_pod_score):
            candidate = p.metadata.name
            if candidate == pod_name:
                continue
            _log.debug(f"[exec_pod_command] trying {candidate!r}...")
            result = _run(candidate)
            if result and "not found" not in result and "No such file" not in result and not result.startswith("[ERROR]"):
                _log.info(f"[exec_pod_command] success with pod {candidate!r}")
                return result
            _log.debug(f"[exec_pod_command] {candidate!r} failed: {result!r:.80}")

        return f"[ERROR] '{binary_hint}' not available in any running pod in namespace '{namespace}'."

    return output or "(command returned no output)"


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
            "phase_only":  {"type": "boolean", "default": False,
                            "description": (
                                "Set true when the user asks ONLY about pod phase/status "
                                "(e.g. 'which pods are not Running', 'any pod not Running'). "
                                "Returns ONLY pods whose phase is Pending/Failed/Unknown. "
                                "Does NOT include Running pods with high restarts or not-ready containers. "
                                "Use phase_only=false (default) for health/unhealthy queries."
                            )},
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
        "description": (
            "Get detailed info about a specific pod: container states, restart count, "
            "last termination reason (e.g. OOMKilled, Error), and CPU/memory requests and limits per container. "
            "Use this for: 'what are the resource limits for pod X', 'why did pod X crash', "
            "'what is the memory limit for pod X', or any OOMKilled diagnosis. "
            "This is the ONLY tool that shows per-pod resource limits and termination reasons."
        ),
        "parameters":  {
            "pod_name":  {"type": "string"},
            "namespace": {"type": "string", "default": "default"},
        },
    },

    "get_node_health": {
        "fn":          get_node_health,
        "description": (
            "Check node health, CPU/memory/disk pressure, allocatable resources, and GPU count per node. "
            "Returns GPU in-use vs allocatable counts. "
            "For GPU MODEL/SPEC (product name, memory, driver version), use get_gpu_info instead."
        ),
        "parameters":  {},
    },

    "get_gpu_info": {
        "fn":          get_gpu_info,
        "description": (
            "Get detailed GPU hardware specification for each node: GPU model/product name, VRAM, "
            "CUDA driver version, GPU family, and related node labels populated by the NVIDIA device plugin "
            "or GPU Feature Discovery (GFD). "
            "Use this for: 'what GPU model does the cluster have', 'what GPU spec', "
            "'what is the GPU type', 'what graphics card', 'NVIDIA model', 'GPU memory spec'."
        ),
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
            "namespace": {"type": "string", "default": "all",
                          "description": "Namespace to list ingresses in. Default 'all' searches every namespace."},
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
        "description": "List all namespaces with their status and pod count. ALWAYS use this when the user asks 'how many namespaces', 'list namespaces', 'namespaces with number of pods', or wants a namespace count.",
        "parameters":  {},
    },
    "get_namespace_resource_summary": {
        "fn":          get_namespace_resource_summary,
        "description": (
            "Aggregate CPU and memory requests/limits for ALL pods in a namespace in a single call. "
            "Returns total requested CPU (millicores and cores) and memory (MiB/GiB) for the namespace, "
            "plus a per-pod breakdown. "
            "Use this for: 'total cpu request for all pods in namespace X', "
            "'how much memory is requested in namespace X', "
            "'count cpu/memory requests in namespace X', "
            "'sum of cpu requests in X'. "
            "NEVER call describe_pod in a loop to aggregate resources — always use this tool instead."
        ),
        "parameters": {
            "namespace": {
                "type": "string",
                "description": "Kubernetes namespace to summarise resources for.",
            },
        },
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

K8S_TOOLS["exec_pod_command"] = {
    "fn":          exec_pod_command,
    "description": (
        "Execute an arbitrary shell command inside a running Kubernetes pod using the K8s stream API. "
        "Use this to run openssl, base64, cat, curl, or any other command inside the cluster. "
        "NOTE: kubectl_exec will automatically route 'kubectl exec ...' commands to this tool — "
        "you do not need to call this directly unless you already know the pod name. "
        "IMPORTANT: Do NOT use $() subshell substitution in command — resolve all values in prior tool calls first."
    ),
    "parameters": {
        "namespace": {"type": "string", "description": "Kubernetes namespace (e.g. 'cdp')."},
        "pod_name":  {"type": "string", "description": "Exact name of a running pod (e.g. 'cdp-embedded-db-0')."},
        "command":   {"type": "string", "description": "Shell command to run. Use literal values only — no $() subshells."},
        "container": {"type": "string", "default": "", "description": "Optional container name for multi-container pods."},
    },
}

K8S_TOOLS["exec_db_query"] = {
    "fn":          exec_db_query,
    "description": (
        "Execute a read-only SQL query inside a running database pod in a Kubernetes namespace. "
        "Supports MySQL, MariaDB, and PostgreSQL — auto-detected from the container image or name. "
        "For multi-container pods (e.g. a pod with containers: upgrade-db, k8tz, fluent-bit, db), "
        "set container='db' to target the correct database container explicitly. "
        "Credentials (username, password, database name) are automatically discovered from the "
        "pod's environment variables, Kubernetes Secrets, and ConfigMaps — no manual credential input needed. "
        "Use this tool for any query involving database contents, user accounts, passwords stored in DB, "
        "table data, or schema inspection. "
        "ONLY read-only SQL is permitted (SELECT, SHOW, DESCRIBE, EXPLAIN). "
        "INSERT / UPDATE / DELETE / DROP / ALTER / TRUNCATE are blocked. "
        "WORKFLOW for 'access db-0 of cmlwb1 and find tables in database sense': "
        "  exec_db_query(namespace='cmlwb1', pod_name='db-0', container='db', database='sense', "
        "sql=\"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'\") "
        "If the tool returns an error listing available containers, re-call with the correct container= value. "
        "PostgreSQL SQL examples (NEVER use DATABASE() — that is MySQL-only): "
        "\"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'\", "
        "\"SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name\", "
        "\"SELECT usename, passwd FROM pg_shadow WHERE usename='x'\", "
        "\"SELECT datname FROM pg_database\" "
        "MySQL/MariaDB SQL examples: "
        "\"SHOW TABLES\", \"SELECT user, host FROM mysql.user\", \"SHOW DATABASES\""
    ),
    "parameters": {
        "namespace": {
            "type": "string",
            "description": "Kubernetes namespace where the database pod runs.",
        },
        "sql": {
            "type": "string",
            "description": (
                "Read-only SQL query to execute. "
                "Examples: \"SHOW TABLES\", \"SELECT user, host FROM mysql.user\", "
                "\"SELECT usename FROM pg_catalog.pg_user\", \"DESCRIBE my_table\""
            ),
        },
        "pod_name": {
            "type": "string",
            "default": "",
            "description": (
                "Optional: specific DB pod name (e.g. 'db-0'). "
                "Leave empty to auto-detect the first running DB pod in the namespace."
            ),
        },
        "database": {
            "type": "string",
            "default": "",
            "description": (
                "Optional: database/schema name to connect to. "
                "Leave empty to use the value auto-discovered from the pod's environment."
            ),
        },
        "container": {
            "type": "string",
            "default": "",
            "description": (
                "Optional: container name inside the pod (e.g. 'db'). "
                "Required for multi-container pods where the DB container is not the first one. "
                "If the tool errors with 'available containers: ...', set this to the DB container name."
            ),
        },
    },
}
