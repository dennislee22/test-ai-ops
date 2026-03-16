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
                        raw_output: bool = False) -> str:
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

        if raw_output:
            hdr = f"{'NAMESPACE':<22} {'POD':<55} {'KEY':<35} {'OP':<10} {'VALUE':<20} {'EFFECT':<12}"
            rows = [hdr, "-" * len(hdr)]

        lines = []

        for pod in sorted(pods.items, key=lambda p: (p.metadata.namespace, p.metadata.name)):
            ns = pod.metadata.namespace
            name = pod.metadata.name

            tolerations = pod.spec.tolerations or []

            if not tolerations:
                if raw_output:
                    rows.append(f"{ns:<22} {name:<55} {'<none>':<35} {'-':<10} {'-':<20} {'-':<12}")
                else:
                    lines.append(f"{ns}/{name}: no tolerations")
                continue

            for t in tolerations:
                key = t.key or "<any>"
                op = t.operator or "Equal"
                val = t.value or "-"
                eff = t.effect or "Any"

                if raw_output:
                    rows.append(
                        f"{ns:<22} {name:<55} {key:<35} {op:<10} {val:<20} {eff:<12}"
                    )
                else:
                    lines.append(
                        f"{ns}/{name} | key:{key} op:{op} value:{val} effect:{eff}"
                    )

        if raw_output:
            rows.append(f"\nTotal pods inspected: {len(pods.items)}")
            return "\n".join(rows)

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"
        
def get_pod_resource_requests(namespace: str = "all",
                              pod_name: str | None = None,
                              raw_output: bool = False) -> str:
    try:
        if namespace != "all":
            try:
                _core.read_namespace(name=namespace)
            except ApiException as e:
                if e.status == 404:
                    return (f"Namespace '{namespace}' does not exist in this cluster. "
                            f"Cannot report pod resources for a non-existent namespace.")
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

        def _parse_cpu(v):
            if not v:
                return 0
            if v.endswith("m"):
                return float(v[:-1]) / 1000
            return float(v)

        def _parse_mem(v):
            if not v:
                return 0
            units = {"Ki": 1/1024/1024, "Mi": 1/1024, "Gi": 1, "Ti": 1024}
            for u, m in units.items():
                if v.endswith(u):
                    return float(v[:-len(u)]) * m
            return float(v)

        lines = []

        if raw_output:
            hdr = f"{'NAMESPACE':<22} {'POD':<45} {'CONTAINER':<30} {'CPU_REQ':<10} {'CPU_LIM':<10} {'MEM_REQ':<10} {'MEM_LIM':<10}"
            rows = [hdr, "-" * len(hdr)]

        for pod in sorted(pods.items, key=lambda p: (p.metadata.namespace, p.metadata.name)):
            ns = pod.metadata.namespace
            podn = pod.metadata.name

            cpu_req_total = 0.0
            cpu_lim_total = 0.0
            mem_req_total = 0.0
            mem_lim_total = 0.0

            for c in pod.spec.containers:
                req = c.resources.requests or {}
                lim = c.resources.limits or {}

                cpu_req = req.get("cpu", "0")
                cpu_lim = lim.get("cpu", "0")
                mem_req = req.get("memory", "0")
                mem_lim = lim.get("memory", "0")

                cpu_req_total += _parse_cpu(cpu_req)
                cpu_lim_total += _parse_cpu(cpu_lim)
                mem_req_total += _parse_mem(mem_req)
                mem_lim_total += _parse_mem(mem_lim)

                if raw_output:
                    rows.append(
                        f"{ns:<22} {podn:<45} {c.name:<30} "
                        f"{cpu_req:<10} {cpu_lim:<10} {mem_req:<10} {mem_lim:<10}"
                    )
                else:
                    lines.append(
                        f"{ns}/{podn} | container:{c.name} "
                        f"| cpu_req:{cpu_req} cpu_lim:{cpu_lim} "
                        f"| mem_req:{mem_req} mem_lim:{mem_lim}"
                    )

            if not raw_output:
                lines.append(
                    f"  → TOTAL | cpu_req:{cpu_req_total:.3f} cores "
                    f"| cpu_lim:{cpu_lim_total:.3f} cores "
                    f"| mem_req:{mem_req_total:.2f}Gi "
                    f"| mem_lim:{mem_lim_total:.2f}Gi"
                )

        if raw_output:
            rows.append(f"\nTotal pods inspected: {len(pods.items)}")
            return "\n".join(rows)

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"
        
def get_pod_status(namespace: str = "all", show_all: bool = False, raw_output: bool = False, phase_only: bool = False) -> str:
    try:
        if namespace != "all":
            try:
                _core.read_namespace(name=namespace)
            except ApiException as e:
                if e.status == 404:
                    return (f"Namespace '{namespace}' does not exist in this cluster. "
                            f"Cannot report pod count for a non-existent namespace.")
                raise

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

        if show_all and not raw_output and namespace == "all":
            unhealthy = []
            healthy_by_ns: dict = {}

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
                 tail_lines: int = 50, container: str = "") -> str:
    tail_lines = min(tail_lines, 100)
    try:
        # For multi-container pods the K8s API returns 400 Bad Request unless
        # a container name is specified.  Auto-detect the main app container
        # if the caller did not supply one.
        kw: dict = {"tail_lines": tail_lines, "timestamps": True}
        if container:
            kw["container"] = container
        else:
            try:
                pod = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
                containers = [c.name for c in (pod.spec.containers or [])]
                if len(containers) > 1:
                    # Prefer a container whose name matches the pod name stem,
                    # otherwise fall back to the last container (usually the app).
                    pod_stem = pod_name.rsplit("-", 2)[0] if pod_name.count("-") >= 2 else pod_name
                    preferred = next(
                        (c for c in containers if pod_stem in c or c in pod_stem),
                        containers[-1]
                    )
                    kw["container"] = preferred
            except ApiException:
                pass  # let the log call fail naturally with a clear error
        logs = _core.read_namespaced_pod_log(
            name=pod_name, namespace=namespace, **kw)
        container_label = (f" [{kw['container']}]" if "container" in kw else "")
        return (f"Last {tail_lines} lines of '{pod_name}'{container_label}:\n{logs}"
                if logs.strip() else f"No logs for '{pod_name}'{container_label}.")
    except ApiException as e:
        return (f"Pod '{pod_name}' not found."
                if e.status == 404 else f"K8s error: {e.reason}")

def describe_pod(pod_name: str, namespace: str = "default") -> str:

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

def get_unhealthy_pods_detail(namespace: str = "all") -> str:
    import datetime as _dt
    from datetime import timezone

    def _collect_unhealthy():
        found = []
        for phase in ("Pending", "Failed", "Unknown"):
            try:
                result = (_core.list_pod_for_all_namespaces(field_selector=f"status.phase={phase}")
                          if namespace == "all"
                          else _core.list_namespaced_pod(namespace=namespace, field_selector=f"status.phase={phase}"))
                found.extend(result.items)
            except ApiException:
                pass
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
            logs = _core.read_namespaced_pod_log(
                name=pod_name, namespace=ns,
                tail_lines=20, timestamps=False,
                previous=False)
            if logs and logs.strip():
                return logs.strip()
        except ApiException:
            pass
        try:
            logs = _core.read_namespaced_pod_log(
                name=pod_name, namespace=ns,
                tail_lines=20, timestamps=False,
                previous=True)
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

    out = [f"Unhealthy pods ({len(unhealthy)}) — detailed diagnosis:\n"]

    for pod in unhealthy:
        ns_name  = pod.metadata.namespace
        pod_name = pod.metadata.name
        out.append(f"{'='*70}")
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

        lines = ["Storage Classes in cluster:"]
        for sc in scs.items:
            name = sc.metadata.name
            provisioner = getattr(sc, "provisioner", "unknown")
            reclaim = getattr(sc, "reclaim_policy", "unknown")
            allow_expand = getattr(sc, "allow_volume_expansion", False)
            expand_str = "Yes" if allow_expand else "No"

            lines.append(
                f"  {name} | Provisioner: {provisioner} | ReclaimPolicy: {reclaim} | Volume Expansion: {expand_str}"
            )

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_endpoints_status(namespace: str = "all") -> str:
    try:
        eps = (_core.list_endpoints_for_all_namespaces()
               if namespace == "all"
               else _core.list_namespaced_endpoints(namespace=namespace))
        if not eps.items:
            return f"No Endpoints found in namespace '{namespace}'."
        lines = [f"Endpoints in '{namespace}':"]
        for ep in sorted(eps.items, key=lambda e: (e.metadata.namespace, e.metadata.name)):
            subsets = ep.subsets or []
            addresses_count = sum(len(s.addresses or []) for s in subsets)
            ports_count = sum(len(s.ports or []) for s in subsets)
            lines.append(f"  {ep.metadata.namespace}/{ep.metadata.name}: {addresses_count} addresses, {ports_count} ports")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"
    
def get_node_capacity() -> str:
    try:
        nodes = _core.list_node()
        if not nodes.items:
            return "No nodes found."
        lines = ["Node capacity:"]
        for node in sorted(nodes.items, key=lambda n: n.metadata.name):
            alloc = node.status.allocatable or {}
            cpu = alloc.get("cpu", "n/a")
            mem = alloc.get("memory", "n/a")
            gpu = 0
            for key in alloc:
                if "nvidia.com/gpu" in key or "amd.com/gpu" in key:
                    try:
                        gpu += int(alloc[key])
                    except (ValueError, TypeError):
                        pass
            gpu_str = f", GPU: {gpu}" if gpu else ""
            lines.append(f"  {node.metadata.name}: CPU={cpu}, Mem={mem}{gpu_str}")
        return "\n".join(lines)
    except ApiException as e:
        return f"K8s API error: {e.reason}"

def get_node_labels(node_name: str = None) -> str:
    nodes = _core.list_node().items
    results = []
    for node in nodes:
        if node_name and node.metadata.name != node_name:
            continue
        labels = node.metadata.labels or {}
        roles = [k.split("/")[-1] for k, v in labels.items() if k.startswith("node-role.kubernetes.io/")]
        results.append(f"{node.metadata.name}: roles={roles}, labels={labels}")
    return "\n".join(results)

def get_node_taints(node_name: str = None) -> str:
    nodes = _core.list_node().items
    results = []
    for node in nodes:
        if node_name and node.metadata.name != node_name:
            continue
        node_taints = node.spec.taints or []
        taints_str = ", ".join([f"{t.key}={t.value}:{t.effect}" for t in node_taints]) or "None"
        results.append(f"{node.metadata.name}: {taints_str}")
    return "\n".join(results)

def get_node_resource_requests() -> str:
    """Aggregate CPU/memory requests and limits per node from all running pods."""
    def _parse_cpu(s: str) -> float:
        """Return millicores as float."""
        s = s.strip()
        if s.endswith("m"):
            return float(s[:-1])
        try:
            return float(s) * 1000
        except ValueError:
            return 0.0

    def _parse_mem(s: str) -> float:
        """Return MiB as float."""
        s = s.strip()
        for suffix, factor in [("Ti", 1024*1024), ("Gi", 1024), ("Mi", 1),
                                ("Ki", 1/1024), ("T", 1000*1024), ("G", 1000),
                                ("M", 1), ("K", 1/1024)]:
            if s.endswith(suffix):
                try:
                    return float(s[:-len(suffix)]) * factor
                except ValueError:
                    return 0.0
        try:
            return float(s) / (1024 * 1024)
        except ValueError:
            return 0.0

    try:
        nodes = _core.list_node()
        all_pods = _core.list_pod_for_all_namespaces(field_selector="status.phase=Running")
    except ApiException as e:
        return f"K8s API error: {e.reason}"

    node_alloc = {}
    for node in nodes.items:
        alloc = node.status.allocatable or {}
        node_alloc[node.metadata.name] = {
            "cpu_alloc_m":  _parse_cpu(alloc.get("cpu", "0")),
            "mem_alloc_mi": _parse_mem(alloc.get("memory", "0")),
            "cpu_req_m": 0.0, "cpu_lim_m": 0.0,
            "mem_req_mi": 0.0, "mem_lim_mi": 0.0,
            "pod_count": 0,
        }

    for pod in all_pods.items:
        node_name = pod.spec.node_name or ""
        if node_name not in node_alloc:
            continue
        node_alloc[node_name]["pod_count"] += 1
        for c in (pod.spec.containers or []):
            res = c.resources
            if not res:
                continue
            req = res.requests or {}
            lim = res.limits   or {}
            node_alloc[node_name]["cpu_req_m"]  += _parse_cpu(req.get("cpu", "0"))
            node_alloc[node_name]["cpu_lim_m"]  += _parse_cpu(lim.get("cpu", "0"))
            node_alloc[node_name]["mem_req_mi"] += _parse_mem(req.get("memory", "0"))
            node_alloc[node_name]["mem_lim_mi"] += _parse_mem(lim.get("memory", "0"))

    lines = ["Node resource requests and limits (running pods):",
             "Note: actual CPU/memory consumption is unavailable — node-exporter is not installed.",
             "The figures below are pod requests and limits, not real-time usage."]
    for node in nodes.items:
        name = node.metadata.name
        d = node_alloc.get(name)
        if not d:
            continue
        cpu_a  = d["cpu_alloc_m"]
        mem_a  = d["mem_alloc_mi"]
        cpu_rp = round(d["cpu_req_m"]  / cpu_a  * 100, 1) if cpu_a  else 0
        cpu_lp = round(d["cpu_lim_m"]  / cpu_a  * 100, 1) if cpu_a  else 0
        mem_rp = round(d["mem_req_mi"] / mem_a  * 100, 1) if mem_a  else 0
        mem_lp = round(d["mem_lim_mi"] / mem_a  * 100, 1) if mem_a  else 0
        lines.append(
            f"  {name}  pods:{d['pod_count']}\n"
            f"    CPU  requests:{d['cpu_req_m']:.0f}m ({cpu_rp}%)  "
            f"limits:{d['cpu_lim_m']:.0f}m ({cpu_lp}%)  "
            f"allocatable:{cpu_a:.0f}m\n"
            f"    MEM  requests:{d['mem_req_mi']:.0f}Mi ({mem_rp}%)  "
            f"limits:{d['mem_lim_mi']:.0f}Mi ({mem_lp}%)  "
            f"allocatable:{mem_a:.0f}Mi"
        )
    return "\n".join(lines)

def get_node_health() -> str:
    try:
        nodes = _core.list_node()
        if not nodes.items:
            return "No nodes found."

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
            pass

        lines        = ["Node health:"]
        gpu_nodes    = 0
        tainted_nodes = 0

        for node in nodes.items:
            roles = [k.replace("node-role.kubernetes.io/", "")
                     for k in (node.metadata.labels or {})
                     if k.startswith("node-role.kubernetes.io/")] or ["worker"]

            conds = {c.type: c.status for c in (node.status.conditions or [])}

            pressure = [t for t in ["MemoryPressure", "DiskPressure", "PIDPressure"]
                        if conds.get(t) == "True"]

            alloc = node.status.allocatable or {}

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
                _gpu_free = gpu_allocatable - gpu_used
                if gpu_used == 0:
                    _gpu_status = "none in use — all free"
                elif gpu_used >= gpu_allocatable:
                    _gpu_status = "all in use — none free"
                else:
                    _gpu_status = f"{gpu_used} in use, {_gpu_free} free"
                gpu_str = f" GPU:{gpu_used}/{gpu_allocatable} ({_gpu_status})"
            else:
                gpu_str = ""

            # --- Taints ---
            taints = node.spec.taints or []
            if taints:
                tainted_nodes += 1
                taint_str = ", ".join(
                    f"{t.key}={t.value}:{t.effect}" if t.value else f"{t.key}:{t.effect}"
                    for t in taints
                )
                taint_str = f" | Taints: {taint_str}"
            else:
                taint_str = ""

            # --- Important labels ---
            labels = node.metadata.labels or {}

            sched_labels = []
            for k in (
                "topology.kubernetes.io/zone",
                "topology.kubernetes.io/region",
                "node.kubernetes.io/instance-type",
                "beta.kubernetes.io/instance-type",
                "kubernetes.io/os",
                "kubernetes.io/arch",
            ):
                if k in labels:
                    sched_labels.append(f"{k.split('/')[-1]}={labels[k]}")

            label_str = f" | Labels: {', '.join(sched_labels)}" if sched_labels else ""

            lines.append(
                f"  {node.metadata.name} [{','.join(roles)}]: "
                f"Ready={conds.get('Ready','?')}"
                + (f" ⚠ {','.join(pressure)}" if pressure else "")
                + f" | CPU:{alloc.get('cpu','n/a')} Mem:{alloc.get('memory','n/a')}"
                + gpu_str
                + taint_str
                + label_str
            )

        lines.append(
            f"Summary: {len(nodes.items)} node(s) total, "
            f"{gpu_nodes} with GPU(s), "
            f"{tainted_nodes} tainted."
        )

        return "\n".join(lines)

    except ApiException as e:
        return f"K8s API error: {e.reason}"
        
def get_gpu_info() -> str:
    try:
        nodes = _core.list_node()
        if not nodes.items:
            return "No nodes found."

        results = []
        for node in nodes.items:
            labels   = node.metadata.labels or {}
            capacity = node.status.capacity or {}
            alloc    = node.status.allocatable or {}

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
                continue

            info = [f"Node: {node.metadata.name}"]

            nvidia_label_prefixes = [
                "nvidia.com/gpu.product",
                "nvidia.com/gpu.memory",
                "nvidia.com/gpu.count",
                "nvidia.com/gpu.family",
                "nvidia.com/gpu.machine",
                "nvidia.com/cuda.driver.major",
                "nvidia.com/cuda.runtime.major",
                "feature.node.kubernetes.io/pci-10de",
                "nvidia.com/mig.strategy",
            ]
            for prefix in nvidia_label_prefixes:
                for k, v in labels.items():
                    if k == prefix or k.startswith(prefix):
                        info.append(f"  {k}: {v}")

            for k, v in labels.items():
                if k.startswith("amd.com/gpu"):
                    info.append(f"  {k}: {v}")

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

def get_job_status(namespace: str = "all",
                   show_all: bool = False,
                   raw_output: bool = False,
                   failed_only: bool = False,
                   running_only: bool = False) -> str:
    """
    List Kubernetes Jobs (and CronJobs if desired) in a namespace or across all namespaces.

    By default, only FAILED jobs are returned.
    Set show_all=True to include all jobs including complete ones.
    Set running_only=True to return only currently active (running) jobs.
    Raw output produces kubectl-style table format.
    """
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

        # RAW table output
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

def get_pvc_status(namespace: str = "all", show_all: bool = False, phase_only: bool = False) -> str:
    _ACCESS_MODES = {
        "ReadWriteOnce": "RWO",
        "ReadWriteMany": "RWX",
        "ReadOnlyMany":  "ROX",
    }

    def _access_modes_str(pvc):
        modes = pvc.spec.access_modes or []
        filtered = [_ACCESS_MODES[m] for m in modes if m in _ACCESS_MODES]
        return ",".join(filtered) if filtered else "?"

    try:
        pvcs = (
            _core.list_persistent_volume_claim_for_all_namespaces()
            if namespace == "all"
            else _core.list_namespaced_persistent_volume_claim(namespace=namespace)
        )

        if not pvcs.items:
            return f"No PVCs found in namespace '{namespace}'."

        if phase_only:
            phase_counts = {"Bound": 0, "Pending": 0, "Lost": 0, "Failed": 0, "Unknown": 0}
            for pvc in pvcs.items:
                phase = pvc.status.phase or "Unknown"
                phase_counts[phase] = phase_counts.get(phase, 0) + 1
            summary_lines = [f"PVC summary for namespace '{namespace}':"]
            for phase, count in phase_counts.items():
                summary_lines.append(f"  {phase}: {count}")
            return "\n".join(summary_lines)

        all_lines = []
        non_bound_lines = []

        for pvc in sorted(pvcs.items, key=lambda x: (x.metadata.namespace, x.metadata.name)):
            ns   = pvc.metadata.namespace
            name = pvc.metadata.name
            phase = pvc.status.phase or "Unknown"
            sc   = pvc.spec.storage_class_name or "default"
            cap  = (pvc.status.capacity or {}).get("storage", "?")
            vol  = pvc.spec.volume_name or "<unbound>"
            am   = _access_modes_str(pvc)

            line = f"{ns}/{name}: {phase} | access:{am} | class:{sc} | capacity:{cap} | volume:{vol}"
            all_lines.append(line)

            if phase != "Bound":
                non_bound_lines.append(f"{ns}/{name}: {phase} ⚠ | access:{am} | class:{sc}")

        report_lines = [
            f"PVC report ({len(pvcs.items)} total) for namespace '{namespace}':",
            "",
        ]

        if show_all or non_bound_lines:
            report_lines.append("All PVCs:")
            report_lines.extend(all_lines)
            report_lines.append("")
            report_lines.append(f"Non-Bound PVCs ({len(non_bound_lines)}):")
            report_lines.extend(non_bound_lines if non_bound_lines else ["  None"])
        else:
            report_lines.append(f"Non-Bound PVCs ({len(non_bound_lines)}):")
            report_lines.extend(non_bound_lines if non_bound_lines else ["  None"])

        return "\n".join(report_lines)

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
        """Try Longhorn CRD for usage. Returns (entry_str, is_error) tuple.
        entry_str is the formatted result or error message.
        is_error=True means it should go to errors list, False means results list."""
        try:
            custom = _k8s.CustomObjectsApi()
            lh_vol = custom.get_namespaced_custom_object(
                "longhorn.io", "v1beta2", "longhorn-system", "volumes", vol_name
            )
            actual_raw = lh_vol.get("status", {}).get("actualSize")
            total_raw  = lh_vol.get("spec", {}).get("size")
            if actual_raw is None or total_raw is None:
                return (f"  {ns}/{pvc_name}: Longhorn CRD found but actualSize/spec.size fields missing — skipped", True)
            actual = int(actual_raw or 0)
            total  = int(total_raw  or 0)
            if total == 0:
                return (f"  {ns}/{pvc_name}: Longhorn CRD found but spec.size=0 (volume not provisioned yet) — skipped", True)
            if actual == 0:
                total_gib = round(total / (1024**3), 2)
                return (f"  {ns}/{pvc_name}: Longhorn CRD spec.size={total_gib}Gi but actualSize=0 (usage unverified) — skipped", True)
            pct = round((actual / total) * 100, 1)
            used_gib  = round(actual / (1024**3), 2)
            total_gib = round(total  / (1024**3), 2)
            avail_gib = round(max(0, total - actual) / (1024**3), 2)
            # actualSize can exceed spec.size due to Longhorn replica overhead —
            # cap display pct at 100% but preserve real value in label for visibility
            display_pct = min(pct, 100.0)
            pct_label = f"{pct}% used (Longhorn replica overhead)" if pct > 100 else f"{pct}% used"
            flag = "🔴" if display_pct >= 90 else ("🟠" if display_pct >= threshold else "🟢")
            entry = (
                f"  {flag} {ns}/{pvc_name}  {pct_label}  "
                f"({used_gib}Gi used / {total_gib}Gi total, {avail_gib}Gi free)  "
                f"class:{sc}  source:longhorn-crd"
            )
            return (entry, False)
        except Exception:
            return (None, True)

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
                        if pod_hit:
                            break
                if pod_hit:
                    break

        if not pod_hit or not mount_path:
            # Fallback: try Longhorn CRD when no running pod found
            entry, is_error = _longhorn_crd_usage(vol_name, ns, pvc_name, sc, threshold)
            if entry and not is_error:
                pct_val = float(entry.split("%")[0].split()[-1])
                results.append((pct_val, entry + "  (no running pod)"))
            elif entry:
                errors.append(entry)
            else:
                errors.append(f"  {ns}/{pvc_name}: no running pod found and Longhorn CRD unavailable — skipped")
            continue

        df_out = _exec_df(pod_hit, ns, mount_path, container=pod_container)
        if not df_out:
            # df failed — try Longhorn CRD as fallback before giving up
            entry, is_error = _longhorn_crd_usage(vol_name, ns, pvc_name, sc, threshold)
            if entry and not is_error:
                pct_val = float(entry.split("%")[0].split()[-1])
                results.append((pct_val, entry + f"  (df failed on pod {pod_hit})"))
            elif entry:
                errors.append(entry)
            else:
                errors.append(f"  {ns}/{pvc_name}: df exec failed on pod {pod_hit} container {pod_container} and Longhorn CRD unavailable — skipped")
            continue

        parts = df_out.split()
        if len(parts) < 2:
            errors.append(f"  {ns}/{pvc_name}: unexpected df output: {df_out!r} — skipped")
            continue

        try:
            # df --output=used,avail gives exactly: used_kb avail_kb
            used_kb  = int(parts[0])
            avail_kb = int(parts[1])
            total_kb = used_kb + avail_kb
            pct      = round((used_kb / total_kb) * 100, 1) if total_kb > 0 else 0.0
        except (ValueError, ZeroDivisionError):
            errors.append(f"  {ns}/{pvc_name}: could not parse df numbers from: {df_out!r}")
            continue

        used_gib  = round(used_kb  / (1024 * 1024), 2)
        avail_gib = round(avail_kb / (1024 * 1024), 2)
        total_gib = round(total_kb / (1024 * 1024), 2)

        flag = "🔴" if pct >= 90 else ("🟠" if pct >= threshold else "🟢")

        results.append((pct, (
            f"  {flag} {ns}/{pvc_name}  {pct}% used  "
            f"({used_gib}Gi used / {total_gib}Gi total, {avail_gib}Gi free)  "
            f"class:{sc}  pod:{pod_hit}  mount:{mount_path}"
        )))

    results.sort(key=lambda x: x[0], reverse=True)

    lines = [f"PV Storage Usage (threshold: {threshold}%):"]

    nearing = [r for r in results if r[0] >= threshold]
    ok      = [r for r in results if r[0] < threshold]

    if nearing:
        lines.append(f"\nNearing or exceeding {threshold}% capacity ({len(nearing)} PVC(s)):")
        lines.extend(r[1] for r in nearing)
    else:
        lines.append(f"\nNo PVCs are at or above {threshold}% capacity.")

    if ok:
        lines.append(f"\nWithin capacity ({len(ok)} PVC(s)):")
        lines.extend(r[1] for r in ok)

    if errors:
        lines.append(f"\nSkipped ({len(errors)} PVC(s) — no mounted pod or df unavailable):")
        lines.extend(errors)

    return "\n".join(lines)

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

def get_service_status(namespace: str = "all") -> str:
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
                    p = svc.port.number if svc.port.number else None
                    if p:
                        ports.add(p)

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
    _CERT_KEY_HINTS = {"ca.crt", "tls.crt", "tls.key", "ca-bundle", "ca-certificates",
                       "ca.pem", "cert.pem", "certificate", "ssl.crt", "ssl.key"}
    try:
        cms = _core.list_namespaced_config_map(namespace=namespace)
        skip = {"kube-root-ca.crt"}
        items = [cm for cm in cms.items if cm.metadata.name not in skip]
        if not items:
            return f"No ConfigMaps in '{namespace}'."

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
    import base64 as _b64

    def _decode(val: str) -> str:
        try:
            return _b64.b64decode(val).decode("utf-8", errors="replace")
        except Exception:
            return "<decode error>"

    try:

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

        if filter_keys:
            import logging as _flog
            _flog.getLogger("tools.k8s").debug(
                f"[get_secrets] ENTRY filter_keys={filter_keys}  decode={decode}  namespace={namespace}")
            _fk_lower = [f.lower() for f in filter_keys]
            secrets = _core.list_namespaced_secret(namespace=namespace)

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

        secrets = _core.list_namespaced_secret(namespace=namespace)
        if not secrets.items:
            return f"No secrets in namespace '{namespace}'."

        _CERT_TYPES = {"kubernetes.io/tls", "helm.sh/release.v1"}
        _CERT_KEY_HINTS = {"ca.crt", "tls.crt", "tls.key", "ca-bundle",
                           "ca.pem", "cert.pem", "certificate", "ssl.crt"}

        by_type: dict = {}
        for s in secrets.items:
            t = s.type or "Opaque"

            keys = list((s.data or {}).keys())

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

def get_pod_images(namespace: str = "all") -> str:
    try:
        pods = (_core.list_pod_for_all_namespaces()
                if namespace == "all"
                else _core.list_namespaced_pod(namespace=namespace))
    except ApiException as e:
        return f"K8s API error: {e.reason}"

    if not pods.items:
        return f"No pods found in namespace '{namespace}'."

    image_id_map = {}
    for pod in pods.items:
        for cs in (pod.status.container_statuses or []):
            if cs.image and cs.image_id:
                image_id_map[cs.image] = cs.image_id

    rows = []
    for pod in sorted(pods.items, key=lambda p: (p.metadata.namespace, p.metadata.name)):
        ns_name  = pod.metadata.namespace
        pod_name = pod.metadata.name
        phase    = pod.status.phase or "Unknown"

        cs_by_name = {cs.name: cs for cs in (pod.status.container_statuses or [])}

        for c in (pod.spec.containers or []):
            image     = c.image or "?"
            cs        = cs_by_name.get(c.name)
            image_id  = (cs.image_id or "") if cs else ""

            digest = ""
            if "@sha256:" in image_id:
                digest = "sha256:" + image_id.split("@sha256:")[-1][:12] + "…"
            elif "@sha256:" in image:
                digest = "sha256:" + image.split("@sha256:")[-1][:12] + "…"

            tag = ""
            if ":" in image and "@" not in image:
                tag = image.split(":")[-1]
            elif "@" in image:
                tag = image.split("@")[0].split(":")[-1] if ":" in image.split("@")[0] else "latest"

            rows.append(
                f"  {ns_name}/{pod_name}  [{c.name}]  "
                f"image:{image}"
                + (f"  digest:{digest}" if digest else "")
                + f"  phase:{phase}"
            )

    if not rows:
        return f"No containers found in namespace '{namespace}'."

    header = f"Pod images in '{namespace}' ({len(pods.items)} pods):"
    return header + "\n" + "\n".join(rows)

def run_cluster_health(namespace: str = "all", show_all: bool = False, raw_output: bool = False) -> str:
    try:
        report = []

        # --- Nodes health ---
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

        # --- Namespaces and Pods ---
        ns_status = get_namespace_status(namespace=namespace, show_all=show_all, phase_only=False, raw_output=False)
        report.append("\nNamespace/Pod summary:\n" + ns_status)

        # --- PVCs ---
        pvcs = _core.list_persistent_volume_claim_for_all_namespaces().items
        unbound_pvcs = [f"{p.metadata.namespace}/{p.metadata.name}" 
                        for p in pvcs if p.status.phase != "Bound"]
        if unbound_pvcs:
            report.append(f"\nPersistentVolumeClaims not bound: {len(unbound_pvcs)}")
            if raw_output or show_all:
                report.extend(f"  {p}" for p in unbound_pvcs)

        # --- Ingresses ---
        ingresses = _ext.list_ingress_for_all_namespaces().items
        failed_ing = [f"{i.metadata.namespace}/{i.metadata.name}" for i in ingresses if not i.status.load_balancer.ingress]
        if failed_ing:
            report.append(f"\nIngresses without active load balancer: {len(failed_ing)}")
            if raw_output or show_all:
                report.extend(f"  {i}" for i in failed_ing)

        # --- Core system components ---
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
            except ApiException:
                pass

        if system_unhealthy:
            report.append(f"\nSystem components with unhealthy pods: {len(system_unhealthy)}")
            if raw_output or show_all:
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
            except ApiException:
                continue

        report.append(f"\nCluster resource requests:")
        report.append(f"  CPU requested: {cpu_total} cores")
        report.append(f"  Memory requested: {mem_total} bytes")

        return "\n".join(report)

    except ApiException as e:
        return f"[ERROR] K8s API error: {e.reason}"

def get_pod_storage(namespace: str = "all", show_all: bool = False, phase_only: bool = False) -> str:
    try:
        if namespace != "all":
            try:
                _core.read_namespace(name=namespace)
            except ApiException as e:
                if e.status == 404:
                    return f"Namespace '{namespace}' does not exist."
                raise

        pods = (_core.list_pod_for_all_namespaces() if namespace == "all"
                else _core.list_namespaced_pod(namespace=namespace))

        if not pods.items:
            return f"No pods found in namespace '{namespace}'."

        ns_storage_info = {}
        for pod in pods.items:
            ns_name = pod.metadata.namespace
            volumes = pod.spec.volumes or []

            storage_types = []
            for vol in volumes:
                pvc_name = getattr(vol.persistent_volume_claim, "claim_name", None)
                if pvc_name:
                    try:
                        pvc = _core.read_namespaced_persistent_volume_claim(
                            name=pvc_name, namespace=ns_name
                        )
                        sc = pvc.spec.storage_class_name or "default"
                        storage_types.append(sc)
                    except ApiException:
                        storage_types.append("Unknown")
                elif getattr(vol, "empty_dir", None):
                    storage_types.append("emptyDir")
                elif getattr(vol, "host_path", None):
                    storage_types.append("hostPath")
                else:
                    storage_types.append("other")

            phase = pod.status.phase or "Unknown"
            ns_storage_info.setdefault(ns_name, []).append({
                "pod_name": pod.metadata.name,
                "phase": phase,
                "storage_types": storage_types
            })

        lines = []
        for ns_name, pod_list in sorted(ns_storage_info.items()):
            total_pods = len(pod_list)
            phases_count = {"Running": 0, "Pending": 0, "Failed": 0, "Unknown": 0}
            for p in pod_list:
                if p["phase"] in phases_count:
                    phases_count[p["phase"]] += 1
                else:
                    phases_count["Unknown"] += 1

            line = (f"{ns_name}: {total_pods} pod(s) | "
                    f"Running: {phases_count['Running']}, "
                    f"Pending: {phases_count['Pending']}, "
                    f"Failed: {phases_count['Failed']}, "
                    f"Unknown: {phases_count['Unknown']}")
            lines.append(line)

            if show_all:
                for p in pod_list:
                    types = ", ".join(p["storage_types"]) if p["storage_types"] else "None"
                    lines.append(f"  {p['pod_name']}: phase={p['phase']} | storage={types}")

        if phase_only:
            return "\n".join(lines)

        if not show_all:
            lines.append("\nUse show_all=True to see per-pod storage details.")

        return "\n".join(lines)

    except ApiException as e:
        return f"[ERROR] K8s API error listing pods: {e.reason}"
    
def get_namespace_status(namespace: str = "all", show_all: bool = False) -> str:
    try:
        if namespace != "all":
            try:
                _core.read_namespace(name=namespace)
            except ApiException as e:
                if e.status == 404:
                    return f"Namespace '{namespace}' does not exist."
                raise

        ns_items = _core.list_namespace().items if namespace == "all" else [_core.read_namespace(namespace)]
        ns_pod_counts = {}

        for ns in ns_items:
            ns_name = ns.metadata.name
            pods = _core.list_namespaced_pod(ns_name, limit=1000).items
            counts = {"Running": 0, "Pending": 0, "Failed": 0, "Unknown": 0}
            unhealthy = 0

            for pod in pods:
                phase = pod.status.phase or "Unknown"
                counts[phase] = counts.get(phase, 0) + 1

                ready = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                total = len(pod.spec.containers)
                restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
                if ready < total or restarts > 5:
                    unhealthy += 1

            ns_pod_counts[ns_name] = {
                "phase": ns.status.phase or "Active",
                "counts": counts,
                "unhealthy": unhealthy,
                "total": sum(counts.values())
            }

        # Compact summary for “least pods” queries
        if not show_all:
            summary_lines = [
                f"{'NAMESPACE':<30} {'STATUS':<10} {'TOTAL':>5} {'Unhealthy':>9}"
            ]
            for ns_name, info in sorted(ns_pod_counts.items(), key=lambda x: x[1]["total"]):
                summary_lines.append(
                    f"{ns_name:<30} {info['phase']:<10} {info['total']:>5} {info['unhealthy']:>9}"
                )
            return "\n".join(summary_lines)

        # Full detail if show_all=True
        lines = [
            f"{'NAMESPACE':<30} {'STATUS':<10} {'TOTAL':>5} {'Running':>7} {'Pending':>7} "
            f"{'Failed':>7} {'Unknown':>7} {'Unhealthy':>9}"
        ]
        for ns_name, info in sorted(ns_pod_counts.items()):
            counts = info["counts"]
            lines.append(
                f"{ns_name:<30} {info['phase']:<10} {info['total']:>5} "
                f"{counts['Running']:>7} {counts['Pending']:>7} {counts['Failed']:>7} "
                f"{counts['Unknown']:>7} {info['unhealthy']:>9}"
            )
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

import json
import re
import shlex
import logging
from kubernetes import client, config
from kubernetes.client.rest import ApiException

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger(__name__)

_KUBECTL_MAX_OUT = 4000
_KUBECTL_READ_VERBS = {"get", "describe", "logs", "top", "rollout", "auth", "api-resources", "version"}

try:
    config.load_incluster_config()
except config.ConfigException:
    config.load_kubeconfig()


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

def exec_db_query(namespace: str, sql: str,
                  pod_name: str = "", database: str = "",
                  container: str = "") -> str:
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

        if "/" in pod_name:
            pod_name = pod_name.split("/", 1)[1]
        db_type = _detect_db_type(pod_name, namespace, container_hint=container)
        if not db_type:

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
        container_name = container
    else:
        container_name = _find_db_container(pod_name, namespace, db_type)
    _log.info(f"[exec_db_query] container={container_name!r}")

    creds = _find_db_credentials(namespace, pod_name)

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
        _final_sql = sql   # original SQL — used later for column-header extraction
        exec_cmd = ["/bin/sh", "-c", cmd]

    elif db_type == "postgres":
        user     = creds["user"] or "postgres"
        password = creds["password"] or ""

        if not db_name:
            db_name = _discover_pg_database(
                pod_name, namespace, container_name, user, password)
            _log.info(f"[exec_db_query] postgres db auto-discovered: {db_name!r}")

        pg_sql = re.sub(
            r"table_schema\s*=\s*DATABASE\s*\(\s*\)",
            "table_schema NOT IN ('information_schema','pg_catalog','pg_toast')",
            safe_sql,
            flags=re.IGNORECASE,
        )

        pg_sql = re.sub(
            r"'performance_schema'\s*,\s*'sys'\s*",
            "",
            pg_sql,
            flags=re.IGNORECASE,
        )

        if re.match(r"^\s*SHOW\s+TABLES\s*$", pg_sql, re.IGNORECASE):
            pg_sql = (
                "SELECT schemaname, tablename "
                "FROM pg_tables "
                "WHERE schemaname NOT IN ('information_schema','pg_catalog','pg_toast') "
                "ORDER BY schemaname, tablename"
            )

        if re.match(r"^\s*SHOW\s+DATABASES\s*$", pg_sql, re.IGNORECASE):
            pg_sql = "SELECT datname FROM pg_database ORDER BY datname"

        _desc = re.match(r"^\s*DESCRIBE\s+(\S+)\s*$", pg_sql, re.IGNORECASE)
        if _desc:
            tbl = _desc.group(1).strip("`\"'")
            pg_sql = (
                f"SELECT column_name, data_type, is_nullable, column_default "
                f"FROM information_schema.columns "
                f"WHERE table_name = '{tbl}' ORDER BY ordinal_position"
            )

        # Translate MySQL user queries to PostgreSQL equivalents.
        #
        # IMPORTANT: mysql.user has columns (user, host, password).
        # PostgreSQL equivalents:
        #   user     → pg_shadow.usename
        #   host     → pg_shadow has no per-user host restriction; use 'localhost' literal
        #   password → pg_shadow.passwd  (hashed, but present — do NOT omit)
        #
        # Always rewrite to pg_shadow (not pg_user) so the password hash column is
        # available.  Alias column names back to match the MySQL column names so the
        # LLM can read them correctly without confusion.
        if re.search(r"mysql\.user", pg_sql, re.IGNORECASE):
            # Pattern: SELECT user, host FROM mysql.user  (the exact query that broke)
            pg_sql = re.sub(
                r"SELECT\s+user\s*,\s*host\s+FROM\s+mysql\.user",
                "SELECT usename AS user, 'localhost' AS host, passwd AS password FROM pg_catalog.pg_shadow",
                pg_sql, flags=re.IGNORECASE
            )
            # Pattern: SELECT user, host, password FROM mysql.user
            pg_sql = re.sub(
                r"SELECT\s+user\s*,\s*host\s*,\s*password\s+FROM\s+mysql\.user",
                "SELECT usename AS user, 'localhost' AS host, passwd AS password FROM pg_catalog.pg_shadow",
                pg_sql, flags=re.IGNORECASE
            )
            # Fallback: any remaining FROM mysql.user → pg_shadow
            pg_sql = re.sub(
                r"FROM\s+mysql\.user",
                "FROM pg_catalog.pg_shadow",
                pg_sql, flags=re.IGNORECASE
            )
            # Rewrite bare column name references that weren't caught above
            pg_sql = re.sub(r"\buser\b(?!\s+AS)", "usename", pg_sql, flags=re.IGNORECASE)
            pg_sql = re.sub(r"\bpassword\b(?!\s+AS)", "passwd", pg_sql, flags=re.IGNORECASE)

        # Translate SELECT user() / SELECT current_user()
        if re.match(r"^\s*SELECT\s+(current_user|user)\s*\(\s*\)\s*$", pg_sql, re.IGNORECASE):
            pg_sql = "SELECT current_user"

        safe_sql = pg_sql.replace("'", "'\\''")

        host = creds.get("host") or ""
        port = creds.get("port") or "5432"

        pg_env    = f"PGPASSWORD='{password}' " if password else ""
        db_flag   = f"-d {db_name}" if db_name else ""
        user_flag = f"-U {user}" if user else ""

        if host and host not in ("localhost", "127.0.0.1", "::1"):

            cmd = (
                f"{pg_env}psql {user_flag} -h {host} -p {port} "
                f"{db_flag} --no-password -t -A -c '{safe_sql}'"
            )
        else:

            cmd = (
                f"{pg_env}psql {user_flag} {db_flag} "
                f"--no-password -t -A -c '{safe_sql}' 2>&1 || "

                f"psql {db_flag} -t -A -c '{safe_sql}' 2>&1 || "

                f"psql -U postgres {db_flag} -t -A -c '{safe_sql}'"
            )
        _final_sql = pg_sql   # translated SQL — used later for column-header extraction
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

    if len(output) > _KUBECTL_MAX_OUT:
        output = output[:_KUBECTL_MAX_OUT] + f"\n...[output truncated at {_KUBECTL_MAX_OUT} chars]"

    def _extract_columns(sql_text: str) -> list[str]:
        """Best-effort extraction of SELECT column aliases or names."""
        m = re.match(r"^\s*SELECT\s+(.+?)\s+FROM\b", sql_text, re.IGNORECASE | re.DOTALL)
        if not m:
            return []
        cols_raw = m.group(1)
        cols = []
        for col in cols_raw.split(","):
            col = col.strip()
            # Prefer AS alias
            alias_m = re.search(r"\bAS\s+(\w+)\s*$", col, re.IGNORECASE)
            if alias_m:
                cols.append(alias_m.group(1))
                continue
            # Use last token (function calls, table.col, etc.)
            token = re.split(r"[\s.(]", col)[-1].strip(")'\"")
            cols.append(token if token else col)
        return cols

    # Use the final translated SQL for column extraction
    _sql_for_cols = _final_sql
    _cols = _extract_columns(_sql_for_cols)

    if _cols:
        col_header = "|".join(_cols)
        col_sep    = "|".join("-" * max(len(c), 4) for c in _cols)
        output = col_header + "\n" + col_sep + "\n" + output

    header = (f"DB query result  [{db_type.upper()} · pod={pod_name} · ns={namespace}"
              + (f" · db={db_name}" if db_name else "") + "]\n"
              + "-" * 60 + "\n")
    return header + output
