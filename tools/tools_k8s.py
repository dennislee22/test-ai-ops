import os
import re
import ast
import json as _json
import shlex
import base64
import logging
import datetime
import tempfile
import urllib.parse
from pathlib import Path
from datetime import timezone

import yaml as _yaml
from kubernetes import client as _k8s, config as _k8s_cfg
from kubernetes.client.rest import ApiException

_KUBECTL_MAX_OUT     = 4_000
_KUBECTL_READ_VERBS  = {
    "get", "describe", "logs", "top", "rollout", "auth",
    "api-resources", "api-versions", "version", "cluster-info", "explain", "diff", "events",
}
_KUBECTL_WRITE_VERBS = {
    "apply", "create", "delete", "patch", "replace", "scale",
    "edit", "label", "annotate", "taint", "drain", "cordon",
    "uncordon", "set", "run", "expose", "autoscale",
}
_BLOCKED_VERBS = {
    "exec":         "exec is not supported; use kubectl logs or describe instead",
    "port-forward": "port-forward is not supported in unattended mode",
    "attach":       "attach is not supported",
    "proxy":        "proxy is not supported",
}
_ALLOW_WRITES  = os.getenv("KUBECTL_ALLOW_WRITES", "false").lower() in ("1", "true", "yes")
_ALLOW_DB_EXEC = os.getenv("ALLOW_DB_EXEC",        "true").lower()  in ("1", "true", "yes")

_DB_USER_KEYS  = ("username","user","db-user","db_user","postgresql-user","mysql-user","mariadb-user","database-user")
_DB_PASS_KEYS  = ("password","pass","db-password","db_password","postgresql-password","mysql-password","mariadb-password","database-password","postgres-password")
_DB_NAME_KEYS  = ("database","db","db-name","db_name","dbname","postgresql-database","mysql-database","mariadb-database")
_DB_HOST_KEYS  = ("host","db-host","db_host","postgresql-host","mysql-host")
_DB_PORT_KEYS  = ("port","db-port","db_port","postgresql-port","mysql-port")

_SQL_WRITE_RE  = re.compile(
    r"^\s*(insert|update|delete|drop|truncate|alter|create|replace|rename|grant|revoke"
    r"|call|exec|execute|lock|unlock|flush|reset|purge|load\s+data)\b",
    re.IGNORECASE,
)
_EVENT_NOISE_PATTERNS = ["cgroup", "cgroupv", "cgroup v1", "cgroup v2"]
_PG_SYSTEM_DBS        = {"postgres", "template0", "template1"}
_MYSQL_SYSTEM_DBS     = {"information_schema", "performance_schema", "mysql", "sys"}

logging.basicConfig(level=logging.INFO)
_log = logging.getLogger("k8s")

_version_api: _k8s.VersionApi
_storage:      _k8s.StorageV1Api
_core:         _k8s.CoreV1Api
_apps:         _k8s.AppsV1Api
_batch:        _k8s.BatchV1Api
_rbac:         _k8s.RbacAuthorizationV1Api
_net:          _k8s.NetworkingV1Api
_autoscaling:  _k8s.AutoscalingV2Api

def _init_api_clients() -> None:
    """(Re-)initialise all module-level API client singletons."""
    global _version_api, _storage, _core, _apps, _batch, _rbac, _net, _autoscaling
    _version_api = _k8s.VersionApi()
    _storage     = _k8s.StorageV1Api()
    _core        = _k8s.CoreV1Api()
    _apps        = _k8s.AppsV1Api()
    _batch       = _k8s.BatchV1Api()
    _rbac        = _k8s.RbacAuthorizationV1Api()
    _net         = _k8s.NetworkingV1Api()
    _autoscaling = _k8s.AutoscalingV2Api()

def _load_initial_k8s() -> None:
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
_init_api_clients()

def _api_error(e: ApiException) -> str:
    return f"[K8s API error {e.status}] {e.reason}"

def _k8s_err(e: Exception) -> str:
    """Clean one-line error string for any kubernetes-client exception.
    Handles both ApiException and the raw WebSocket/wsproto errors that
    RKE2 clusters raise (e.g. 'Handshake status 200 OK -+-+- {...}').
    """
    if isinstance(e, ApiException):
        return f"[K8s API error {e.status}] {e.reason}"
    s = str(e)
    if 'Handshake status' in s or '-+-+' in s:
        # Strip verbose HTTP header dump — keep only the status line
        first = s.split('\n')[0].split('-+-+')[0].strip().rstrip('-').strip()
        return f"[K8s API error] {first[:120] or 'WebSocket connection error'}"
    return f"[K8s API error] {s[:160]}"

def _ns_header(kind: str, namespace: str, search: str | None = None) -> str:
    """
    Returns a single plain sentence describing the query scope.

    Examples:
        _ns_header("Pods", "all")
            → "Showing all Pods across all namespaces."
        _ns_header("Pods", "production")
            → "Showing Pods in namespace `production`."
        _ns_header("Pods", "all", search="nginx")
            → "Showing all Pods across all namespaces (filter: `nginx`)."
        _ns_header("Pods", "production", search="nginx")
            → "Showing Pods in namespace `production` (filter: `nginx`)."
    """
    scope  = "all namespaces" if namespace == "all" else f"namespace `{namespace}`"
    prefix = "all " if namespace == "all" else ""
    filt   = f" (filter: `{search}`)" if search else ""
    return f"Showing {prefix}{kind} in {scope}{filt}."

def _list_pods(namespace: str = "all") -> list:
    return (_core.list_pod_for_all_namespaces().items
            if namespace == "all"
            else _core.list_namespaced_pod(namespace=namespace).items)

def _list_pods_by_node(node_name: str) -> list:
    """Return pods on a specific node. RKE2 raises a WebSocket error on
    field_selector=spec.nodeName — fall back to full list + Python filter."""
    try:
        return _core.list_pod_for_all_namespaces(
            field_selector=f"spec.nodeName={node_name}").items
    except Exception:
        return [p for p in _core.list_pod_for_all_namespaces().items
                if (p.spec.node_name or "") == node_name]

def _find_prometheus_pod() -> tuple[str | None, str | None, str]:
    """Locate the Prometheus server pod across all namespaces.
    Returns (pod_name, namespace, container_name) or (None, None, 'prometheus-server').
    Avoids field_selector which triggers WebSocket errors on RKE2.
    """
    try:
        pods = _core.list_pod_for_all_namespaces().items
    except Exception:
        return None, None, "prometheus-server"
    for p in pods:
        if p.status.phase != "Running":
            continue
        n = p.metadata.name.lower()
        if "prometheus-server" in n and "operator" not in n:
            cnames    = [c.name for c in (p.spec.containers or [])]
            container = ("prometheus-server" if "prometheus-server" in cnames
                         else (cnames[0] if cnames else "prometheus-server"))
            return p.metadata.name, p.metadata.namespace, container
    return None, None, "prometheus-server"

def _filter_pods(pods: list, search: str | None) -> list:
    if not search:
        return pods
    s = search.lower()
    filtered = [p for p in pods
                if s in p.metadata.name.lower() or s in p.metadata.namespace.lower()]
    return filtered if filtered else pods

def _get_gpu_requests(pod) -> str:
    reqs = [
        f"{c.name}:{v}"
        for c in (pod.spec.containers or [])
        for k, v in (c.resources.requests or {}).items()
        if "gpu" in k.lower()
    ]
    return ", ".join(reqs) if reqs else "-"

def _to_mebibytes(val: str) -> str:
    try:
        if val.endswith("Ki"): return f"{int(int(val[:-2]) / 1024)}Mi"
        if val.endswith("Gi"): return f"{int(float(val[:-2]) * 1024)}Mi"
        if val.endswith("Ti"): return f"{int(float(val[:-2]) * 1024 * 1024)}Mi"
        if val.endswith("Mi"): return val
        return f"{int(int(val) / 1024 / 1024)}Mi"
    except (ValueError, TypeError):
        return "0Mi"

def _as_yaml(obj) -> str:
    return f"```yaml\n{_yaml.safe_dump(obj.to_dict(), sort_keys=False)}```"

def _fmt_kv(label: str, d: dict | None, sep: str = "=", pad: int = 17) -> str:
    if not d:
        return f"{label:<{pad}} <none>"
    indent = " " * pad
    pairs  = f"\n{indent}".join(f"{k}{sep}{v}" for k, v in d.items())
    return f"{label:<{pad}} {pairs}"

def _age(ts) -> str:
    if not ts:
        return "<unknown>"
    try:
        now  = datetime.datetime.now(timezone.utc)
        s    = int((now - ts).total_seconds())
        if s < 60:    return f"{s}s"
        if s < 3600:  return f"{s // 60}m"
        if s < 86400: return f"{s // 3600}h"
        return f"{s // 86400}d"
    except Exception:
        return "<unknown>"

def _safe_reason(e) -> str:
    try:
        reason = getattr(e, "reason", None) or ""
        status = getattr(e, "status", 0)
        return f"HTTP {status} {reason}" if reason else str(e).replace("\n", " ")[:80]
    except Exception:
        return "Unknown API error"

def _b64decode_safe(val: str) -> str:
    try:
        return base64.b64decode(val).decode("utf-8", errors="replace").strip()
    except Exception:
        return val.strip()

def _is_noisy_event(message: str) -> bool:
    msg = (message or "").lower()
    return any(p in msg for p in _EVENT_NOISE_PATTERNS)

def _parse_cpu_to_millicores(v: str) -> int:
    if not v or v in ("none", "<none>", "0"): return 0
    v = v.strip()
    try:
        return int(v[:-1]) if v.endswith("m") else int(float(v) * 1000)
    except (ValueError, TypeError):
        return 0

def _parse_mem_to_mib(v: str) -> float:
    if not v or v in ("none", "<none>", "0"): return 0.0
    v = v.strip()
    units = {"Ki": 1/1024, "Mi": 1.0, "Gi": 1024.0, "Ti": 1024.0*1024,
             "K":  1/1024, "M":  1.0, "G":  1024.0}
    for suf, fac in units.items():
        if v.endswith(suf):
            try: return float(v[:-len(suf)]) * fac
            except (ValueError, TypeError): return 0.0
    try:    return float(v) / (1024 * 1024)
    except: return 0.0

def _parse_cpu_cores(val) -> float:
    if not val: return 0.0
    s = str(val)
    return float(s[:-1]) / 1000.0 if s.endswith("m") else float(s)

def _parse_mem_gib(val) -> float:
    if not val: return 0.0
    s = str(val)
    if s.endswith("Ki"): return float(s[:-2]) / (1024**2)
    if s.endswith("Mi"): return float(s[:-2]) / 1024.0
    if s.endswith("Gi"): return float(s[:-2])
    if s.endswith("Ti"): return float(s[:-2]) * 1024.0
    if s.isdigit():      return float(s) / (1024**3)
    return 0.0

def _parse_storage_to_gib(s: str) -> float:
    s = s.strip()
    try:
        if s.endswith("Ti"): return float(s[:-2]) * 1024
        if s.endswith("Gi"): return float(s[:-2])
        if s.endswith("Mi"): return float(s[:-2]) / 1024
        if s.endswith("Ki"): return float(s[:-2]) / (1024*1024)
        if s.endswith("T"):  return float(s[:-1]) * 1000
        if s.endswith("G"):  return float(s[:-1])
        if s.endswith("M"):  return float(s[:-1]) / 1000
    except ValueError: pass
    return 0.0

def _obj_to_yaml(obj) -> str:
    try:
        d = _k8s.ApiClient().sanitize_for_serialization(obj)
        return _yaml.dump(d, default_flow_style=False, allow_unicode=True)
    except Exception:
        return str(obj)

def _paginate(list_fn, *args, field_selector="", **kwargs):
    items, _cont = [], None
    while True:
        kw = {"limit": 500, **kwargs}
        if field_selector: kw["field_selector"] = field_selector
        if _cont:          kw["_continue"] = _cont
        page = list_fn(*args, **kw) if args else list_fn(**kw)
        items.extend(page.items)
        _cont = page.metadata._continue if page.metadata and page.metadata._continue else None
        if not _cont: break
    return items

def _is_high_restart(pod, restart_count: int) -> bool:
    if restart_count == 0: return False
    now = datetime.datetime.now(timezone.utc)
    last_restart_time = None
    for cs in (pod.status.container_statuses or []):
        if cs.last_state and cs.last_state.terminated:
            t = cs.last_state.terminated.finished_at
            if t and (last_restart_time is None or t > last_restart_time):
                last_restart_time = t
    if last_restart_time is not None:
        if (now - last_restart_time).total_seconds() / 3600 > 24: return False
    if restart_count > 100: return True
    run_start = None
    for cs in (pod.status.container_statuses or []):
        if cs.state and cs.state.running and cs.state.running.started_at:
            t = cs.state.running.started_at
            if run_start is None or t > run_start: run_start = t
    if run_start is None and pod.status and pod.status.start_time:
        run_start = pod.status.start_time
    if run_start is None: return restart_count > 10
    return (restart_count / max((now - run_start).total_seconds() / 86400, 0.1)) > 3.0

def _list_namespaced_or_all(list_ns_fn, list_all_fn, namespace: str) -> list:
    if namespace != "all":
        try:
            items = list_ns_fn(namespace).items
            return items if items else list_all_fn().items
        except Exception:
            return list_all_fn().items
    return list_all_fn().items

def _search_filter(items: list, search: str | None, fallback: bool = True) -> list:
    if not search: return items
    s = search.lower()
    filtered = [i for i in items
                if s in i.metadata.name.lower() or s in (i.metadata.namespace or "").lower()]
    return filtered if filtered else (items if fallback else [])

def reload_kubeconfig(yaml_content: str) -> dict:
    global _core, _apps, _batch, _rbac, _net, _autoscaling
    if not yaml_content.strip():
        raise ValueError("Empty kubeconfig.")
    m = re.search(r'server\s*:\s*(https?://[^\s\n]+)', yaml_content)
    server_url = m.group(1).strip() if m else None
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding="utf-8") as f:
        f.write(yaml_content)
        tmp_path = f.name
    try:
        _k8s_cfg.load_kube_config(config_file=tmp_path)
    except Exception as e:
        raise ValueError(f"Invalid kubeconfig: {e}")
    _init_api_clients()
    try:
        _core.list_namespace(_request_timeout=5)
    except Exception as e:
        raise ValueError(f"Kubeconfig loaded but cluster unreachable: {e}")
    _log.info(f"[kubeconfig] Reloaded — server={server_url}")
    return {"ok": True, "server": server_url or "unknown"}

def get_pod_tolerations(namespace: str = "all", pod_name: str | None = None,
                        search: str | None = None) -> str:
    try:
        if namespace != "all":
            try:
                _core.read_namespace(name=namespace)
            except ApiException as e:
                if e.status == 404:
                    return f"Namespace '{namespace}' does not exist in this cluster."
                raise
        pods = _list_pods(namespace)
        if not pods:
            return f"No pods found in namespace '{namespace}'."
        if pod_name:
            pods = [p for p in pods if pod_name in p.metadata.name]
        if pod_name and not pods:
            return f"No pods matching '{pod_name}' found in namespace '{namespace}'."

        def _build_rows(pod_list, apply_search):
            rows = []
            for pod in sorted(pod_list, key=lambda p: (p.metadata.namespace, p.metadata.name)):
                ns, name = pod.metadata.namespace, pod.metadata.name
                tolerations = pod.spec.tolerations or []
                if not tolerations:
                    rows.append((ns, name, "<none>"))
                    continue
                for t in tolerations:
                    tol_str = (f"key:{t.key or '<any>'} op:{t.operator or 'Equal'} "
                               f"value:{t.value or '-'} effect:{t.effect or 'Any'}")
                    if apply_search and search and search.lower() not in tol_str.lower():
                        continue
                    rows.append((ns, name, tol_str))
            return rows

        rows = _build_rows(pods, apply_search=True)
        if search and not rows:
            rows = _build_rows(pods, apply_search=False)
        lines = [_ns_header("Pod Tolerations", namespace, search),
                 "| NAMESPACE | POD | TOLERATION |", "|---|---|---|"]
        for ns, name, tol_str in rows:
            lines.append(f"| `{ns}` | `{name}` | {tol_str} |")
        return "\n".join(lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_pod_resource_requests(namespace: str = "all", search: str | None = None) -> str:
    try:
        pods = _list_pods(namespace)
        if not pods:
            return f"No pods found in namespace '{namespace}'."
        filtered = _filter_pods(pods, search)
        lines = [
            _ns_header("Pod Resource Requests", namespace, search),
            "| NAMESPACE | POD | CONTAINER | CPU_REQ | CPU_LIM | MEM_REQ | MEM_LIM | ATTACHED GPU |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for pod in sorted(filtered, key=lambda p: (p.metadata.namespace, p.metadata.name)):
            ns, podn = pod.metadata.namespace, pod.metadata.name
            attached_gpu = _get_gpu_requests(pod)
            cpu_req_total_m = cpu_lim_total_m = mem_req_total_mi = mem_lim_total_mi = 0
            for c in pod.spec.containers or []:
                req = c.resources.requests or {}
                lim = c.resources.limits   or {}
                cpu_req = req.get("cpu", "0")
                cpu_lim = lim.get("cpu", "0")
                cpu_req_m = cpu_req if cpu_req.endswith("m") else f"{int(float(cpu_req)*1000)}m"
                cpu_lim_m = cpu_lim if cpu_lim.endswith("m") else f"{int(float(cpu_lim)*1000)}m"
                mem_req_mi = _to_mebibytes(req.get("memory", "0"))
                mem_lim_mi = _to_mebibytes(lim.get("memory", "0"))
                cpu_req_total_m  += int(cpu_req_m.rstrip("m"))
                cpu_lim_total_m  += int(cpu_lim_m.rstrip("m"))
                mem_req_total_mi += int(mem_req_mi.rstrip("Mi"))
                mem_lim_total_mi += int(mem_lim_mi.rstrip("Mi"))
                lines.append(f"| `{ns}` | `{podn}` | `{c.name}` | {cpu_req_m} | {cpu_lim_m} "
                              f"| {mem_req_mi} | {mem_lim_mi} | {attached_gpu} |")
            lines.append(f"| `{ns}` | `{podn}` | **TOTAL** | {cpu_req_total_m}m | {cpu_lim_total_m}m "
                         f"| {mem_req_total_mi}Mi | {mem_lim_total_mi}Mi | {attached_gpu} |")
        return "\n".join(lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_pod_containers_resources(namespace: str = "all", search: str | None = None) -> str:
    try:
        pods = _list_pods(namespace)
        if not pods:
            return f"No pods found in namespace '{namespace}'."
        filtered = _filter_pods(pods, search)
        lines = [
            _ns_header("Pod Container Resources", namespace, search),
            "| NAMESPACE | POD | CONTAINER | IMAGE | CPU_REQ | CPU_LIM | MEM_REQ | MEM_LIM | ATTACHED GPU |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for pod in sorted(filtered, key=lambda p: (p.metadata.namespace, p.metadata.name)):
            ns, podn = pod.metadata.namespace, pod.metadata.name
            attached_gpu = _get_gpu_requests(pod)
            for c in pod.spec.containers or []:
                req = c.resources.requests or {}
                lim = c.resources.limits   or {}
                lines.append(
                    f"| `{ns}` | `{podn}` | `{c.name}` | `{c.image or '<none>'}` "
                    f"| {req.get('cpu', '0m')} | {lim.get('cpu', '0m')} "
                    f"| {req.get('memory', '0Mi')} | {lim.get('memory', '0Mi')} "
                    f"| {attached_gpu} |"
                )
        return "\n".join(lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

_NOT_RUNNING_PHASES = ("Pending", "Failed", "Unknown")
_NOT_RUNNING_WORDS  = {
    "notrunning", "not_running", "not-running",
    "unhealthy", "failed", "failing",
    "pending", "unknown",
    "bad", "broken", "down", "stuck", "problem", "issue",
}

def get_pod_status(namespace: str = "all", search: str | None = None,
                   phase: str | None = None) -> str:
    """
    List pods with their phase, readiness, restart count and failed conditions.

    phase parameter behaviour:
      - None / omitted   → all pods (existing behaviour)
      - "notrunning"     → only Pending, Failed, Unknown pods (uses field_selector
                           at API level — no in-memory filtering of large pod lists)
      - any specific phase string ("Pending", "Failed", "Running", …)
                         → only pods in that exact phase
    """
    try:
        phase_filter: str | None = None
        not_running_mode = False

        if phase:
            p = phase.strip().lower().replace(" ", "")
            if p in _NOT_RUNNING_WORDS:
                not_running_mode = True
            else:
                phase_filter = phase.strip().capitalize()

        if not_running_mode:
            pods = []
            for p in _NOT_RUNNING_PHASES:
                try:
                    if namespace == "all":
                        pods += _core.list_pod_for_all_namespaces(
                            field_selector=f"status.phase={p}").items
                    else:
                        pods += _core.list_namespaced_pod(
                            namespace=namespace,
                            field_selector=f"status.phase={p}").items
                except ApiException:
                    pass
        elif phase_filter:
            fs = f"status.phase={phase_filter}"
            if namespace == "all":
                pods = _core.list_pod_for_all_namespaces(field_selector=fs).items
            else:
                pods = _core.list_namespaced_pod(namespace=namespace, field_selector=fs).items
        else:
            pods = _list_pods(namespace)

        if not pods:
            if not_running_mode:
                return _ns_header("Pods", namespace) + "\n✅ All pods are Running — no Pending/Failed/Unknown pods found."
            if phase_filter:
                return _ns_header("Pods", namespace) + f"\nNo pods in phase `{phase_filter}` found."
            return f"No pods found in '{namespace}'."

        filtered = _filter_pods(pods, search)

        rows = []
        for pod in sorted(filtered, key=lambda p: (p.metadata.namespace, p.metadata.name)):
            ph       = pod.status.phase or "Unknown"
            restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
            ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
            total    = len(pod.spec.containers)
            conds    = [f"{c.type}={c.status}" for c in (pod.status.conditions or []) if c.status != "True"]

            reason = ""
            if not_running_mode or (phase_filter and phase_filter != "Running"):
                for cs in (pod.status.container_statuses or []):
                    if cs.state and cs.state.waiting and cs.state.waiting.reason:
                        reason = cs.state.waiting.reason
                        break
                    if cs.state and cs.state.terminated and cs.state.terminated.reason:
                        reason = cs.state.terminated.reason
                        break

            rows.append((pod.metadata.namespace, pod.metadata.name, ph,
                         f"{ready}/{total}", restarts,
                         ", ".join(conds) if conds else "-",
                         reason))

        if not_running_mode:
            kind_label = "Non-Running Pods (Pending / Failed / Unknown)"
        elif phase_filter:
            kind_label = f"Pods (phase={phase_filter})"
        else:
            kind_label = "Pods"

        lines = [_ns_header(kind_label, namespace, search)]

        show_reason = not_running_mode or (phase_filter and phase_filter != "Running")

        if namespace == "all":
            if show_reason:
                lines += ["| NAMESPACE | NAME | STATUS | READY | RESTARTS | REASON | CONDITIONS |",
                          "|---|---|---|---|---|---|---|"]
                for ns, nm, ph, rd, rs, cd, rsn in rows:
                    lines.append(f"| `{ns}` | `{nm}` | {ph} | {rd} | {rs} | {rsn or '-'} | {cd} |")
            else:
                lines += ["| NAMESPACE | NAME | STATUS | READY | RESTARTS | CONDITIONS |",
                          "|---|---|---|---|---|---|"]
                for ns, nm, ph, rd, rs, cd, _ in rows:
                    lines.append(f"| `{ns}` | `{nm}` | {ph} | {rd} | {rs} | {cd} |")
        else:
            if show_reason:
                lines += ["| NAME | STATUS | READY | RESTARTS | REASON | CONDITIONS |",
                          "|---|---|---|---|---|---|"]
                for _, nm, ph, rd, rs, cd, rsn in rows:
                    lines.append(f"| `{nm}` | {ph} | {rd} | {rs} | {rsn or '-'} | {cd} |")
            else:
                lines += ["| NAME | STATUS | READY | RESTARTS | CONDITIONS |",
                          "|---|---|---|---|---|"]
                for _, nm, ph, rd, rs, cd, _ in rows:
                    lines.append(f"| `{nm}` | {ph} | {rd} | {rs} | {cd} |")

        if not_running_mode and rows:
            lines.append(
                f"\n_{len(rows)} non-running pod(s) found. "
                "Ask for details on a specific pod to see logs and events._"
            )

        return "\n".join(lines)

    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_pod_logs(namespace: str = "all", search: str | None = None,
                 tail_lines: int = 50, container: str = "") -> str:
    from kubernetes.stream import stream as _k8s_stream

    def _fetch_logs_stream(pod_name, ns_name, container_name, tail):
        """Fallback: fetch logs via WebSocket exec when REST log endpoint fails."""
        try:
            cmd = ["tail", f"-n{tail}", f"/proc/1/fd/1"]
            # Use kubectl-equivalent: exec cat of the log via shell
            cmd = ["/bin/sh", "-c",
                   f"tail -n {tail} /proc/1/fd/1 2>/dev/null || "
                   f"kubectl logs --tail={tail} {pod_name} -n {ns_name} 2>/dev/null || "
                   f"echo '[No log output available]'"]
            kw = dict(command=cmd, stderr=True, stdin=False,
                      stdout=True, tty=False, _preload_content=True)
            if container_name:
                kw["container"] = container_name
            resp = _k8s_stream(_core.connect_get_namespaced_pod_exec,
                               pod_name, ns_name, **kw)
            return resp.strip() if isinstance(resp, str) else ""
        except Exception:
            return ""

    tail_lines = min(tail_lines, 100)
    try:
        pods = _list_pods(namespace)
        if not pods:
            return f"No pods found in namespace '{namespace}'."
        matching = _filter_pods(pods, search) if search else pods
        if search and not matching:
            return f"No pods matching '{search}' found in namespace '{namespace}'."
        log_entries = []
        for pod in sorted(matching, key=lambda p: (p.metadata.namespace, p.metadata.name)):
            pod_name, ns_name = pod.metadata.name, pod.metadata.namespace
            kw: dict = {"tail_lines": tail_lines, "timestamps": True}
            if container:
                kw["container"] = container
            else:
                containers = [c.name for c in (pod.spec.containers or [])]
                if containers:
                    if len(containers) > 1:
                        stem = pod_name.rsplit("-", 2)[0] if pod_name.count("-") >= 2 else pod_name
                        kw["container"] = next(
                            (c for c in containers if stem in c or c in stem), containers[-1])
                    else:
                        kw["container"] = containers[0]
            clabel = f" [{kw['container']}]" if "container" in kw else ""
            try:
                logs = _core.read_namespaced_pod_log(name=pod_name, namespace=ns_name, **kw)
                entry = (f"### `{ns_name}/{pod_name}`{clabel}\n```\n{logs}\n```" if logs.strip()
                         else f"### `{ns_name}/{pod_name}`{clabel}\n_No logs available._")
            except (ApiException, Exception) as e:
                # ApiException(status=0) = RKE2 WebSocket handshake error on REST log endpoint
                # Fall back to streaming exec
                is_ws_err = (isinstance(e, ApiException) and e.status == 0) or \
                            ('Handshake' in str(e) or '-+-+' in str(e))
                if is_ws_err:
                    logs = _fetch_logs_stream(pod_name, ns_name, kw.get("container", ""), tail_lines)
                    entry = (f"### `{ns_name}/{pod_name}`{clabel}\n```\n{logs}\n```" if logs
                             else f"### `{ns_name}/{pod_name}`{clabel}\n_No logs available (WebSocket fallback also failed)._")
                else:
                    reason = e.reason if isinstance(e, ApiException) else _k8s_err(e)
                    entry = f"### `{ns_name}/{pod_name}`{clabel}\n_Error fetching logs: {reason}_"
            log_entries.append(entry)
        header = (_ns_header("Pod Logs", namespace, search) + "\n\n"
                  if namespace == "all" else "")
        return header + "\n\n".join(log_entries)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def describe_pod(pod_name: str, namespace: str = "all", search: str | None = None,
                 show_yaml: bool = False) -> str:
    try:
        pods = _list_pods(namespace)
        if not pods:
            return f"`No pods found in namespace '{namespace}'.`"
        matching = _filter_pods(pods, search) if search else [p for p in pods if p.metadata.name == pod_name]
        if not matching:
            return f"`No pods matching '{search or pod_name}' found in namespace '{namespace}'.`"
        pod = matching[0]
        if show_yaml:
            return _as_yaml(pod)
        lines = ["```",
                 f"Name:             {pod.metadata.name}",
                 f"Namespace:        {pod.metadata.namespace}",
                 f"Priority:         {pod.spec.priority or 0}",
                 f"Service Account:  {pod.spec.service_account_name or pod.spec.service_account}",
                 f"Node:             {pod.spec.node_name or '<none>'}",
                 f"Start Time:       {pod.status.start_time}",
                 _fmt_kv("Labels:",      pod.metadata.labels),
                 _fmt_kv("Annotations:", pod.metadata.annotations, sep=": "),
                 f"Status:           {pod.status.phase}",
                 f"IP:               {pod.status.pod_ip}"]
        pod_ips = getattr(pod.status, "pod_ips", None)
        if pod_ips:
            lines += ["IPs:"] + [f"  IP:           {ip.ip}" for ip in pod_ips]
        if pod.metadata.owner_references:
            owner = pod.metadata.owner_references[0]
            lines.append(f"Controlled By:  {owner.kind}/{owner.name}")

        def _cstate(status, indent="    "):
            if not status: return
            if status.state.running:
                lines.append(f"{indent}State:        Running (Started: {status.state.running.started_at})")
            elif status.state.terminated:
                lines.append(f"{indent}State:        Terminated (Exit: {status.state.terminated.exit_code}, "
                              f"Reason: {status.state.terminated.reason})")
            elif status.state.waiting:
                lines.append(f"{indent}State:        Waiting ({status.state.waiting.reason})")
            lines.append(f"{indent}Ready:        {status.ready}")
            lines.append(f"{indent}Restart Count: {status.restart_count}")

        if pod.spec.init_containers:
            lines.append("Init Containers:")
            for c in pod.spec.init_containers:
                lines += [f"  {c.name}:", f"    Image:        {c.image}",
                          f"    Resources:    limits={c.resources.limits}, requests={c.resources.requests}",
                          f"    Mounts:       " + ", ".join(m.mount_path for m in c.volume_mounts or [])]
                _cstate(next((s for s in pod.status.init_container_statuses or [] if s.name == c.name), None))
        if pod.spec.containers:
            lines.append("Containers:")
            for c in pod.spec.containers:
                lines += [f"  {c.name}:", f"    Image:        {c.image}"]
                _cstate(next((s for s in pod.status.container_statuses or [] if s.name == c.name), None))
                lines += [f"    Requests:     {c.resources.requests or {}}",
                          f"    Limits:       {c.resources.limits or {}}",
                          f"    Mounts:       " + ", ".join(m.mount_path for m in c.volume_mounts or [])]
        if pod.status.conditions:
            lines.append("Conditions:")
            for cond in pod.status.conditions:
                lines.append(f"  Type: {cond.type:25} Status: {cond.status}")
        if pod.spec.volumes:
            lines.append("Volumes:")
            for v in pod.spec.volumes:
                vtype = next((k for k in v.to_dict() if k != "name"), "unknown")
                lines.append(f"  {v.name}: {vtype}")
        lines.append(f"QoS Class:       {pod.status.qos_class or 'None'}")
        if pod.spec.node_selector:
            lines.append("Node-Selectors:  " + ", ".join(f"{k}={v}" for k, v in pod.spec.node_selector.items()))
        if pod.spec.tolerations:
            lines.append("Tolerations:     " + ", ".join(str(t) for t in pod.spec.tolerations))
        try:
            evs = (_core.list_event_for_all_namespaces(limit=500)
                   if namespace == "all"
                   else _core.list_namespaced_event(namespace=namespace, limit=500))
            pod_events = [e for e in (evs.items or [])
                          if e.involved_object.name == pod.metadata.name
                          and e.involved_object.kind == "Pod"]
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
        return f"`Error fetching pod: {e}`"

def get_pod_images(namespace: str = "all", search: str | None = None) -> str:
    try:
        pods = (_core.list_pod_for_all_namespaces()
                if namespace == "all"
                else _core.list_namespaced_pod(namespace=namespace))
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)
    if not pods.items:
        return f"No pods found in namespace '{namespace}'."
    rows = [f"- `{p.metadata.namespace}/{p.metadata.name}` [{c.name}]: `{c.image or '?'}`"
            for p in sorted(pods.items, key=lambda p: (p.metadata.namespace, p.metadata.name))
            for c in (p.spec.containers or [])
            if not search or search.lower() in p.metadata.name.lower() or search.lower() in (c.image or "").lower()]
    if search and not rows:
        rows = [f"- `{p.metadata.namespace}/{p.metadata.name}` [{c.name}]: `{c.image or '?'}`"
                for p in sorted(pods.items, key=lambda p: (p.metadata.namespace, p.metadata.name))
                for c in (p.spec.containers or [])]
    if not rows:
        return f"No containers found in namespace '{namespace}'."
    lines = [_ns_header("Pod Images", namespace, search)]
    lines.extend(rows)
    return "\n".join(lines)

def get_pod_storage(namespace: str = "all", search: str | None = None) -> str:
    try:
        pods = _list_pods(namespace)
        if not pods:
            return f"No pods found in namespace '{namespace}'."
        pvc_pods = []
        for pod in pods:
            entries = []
            for vol in pod.spec.volumes or []:
                if vol.persistent_volume_claim:
                    pvc_name = vol.persistent_volume_claim.claim_name
                    pvc = _core.read_namespaced_persistent_volume_claim(pvc_name, pod.metadata.namespace)
                    modes = pvc.spec.access_modes or []
                    mode_str = (",".join(m for m in modes if m in ["ReadWriteOnce","ReadWriteMany"]) or "Unknown")
                    entries.append((pvc_name, mode_str, pvc.spec.storage_class_name or "Unknown"))
            if entries:
                pvc_pods.append((pod, entries))
        if not pvc_pods:
            return f"No pods with PVCs found in namespace '{namespace}'."
        all_pods = [p for p, _ in pvc_pods]
        filtered_names = {p.metadata.name for p in _filter_pods(all_pods, search)}
        pvc_pods = [(p, e) for p, e in pvc_pods if p.metadata.name in filtered_names]
        lines = [_ns_header("Pod Storage", namespace, search),
                 "| NAMESPACE | POD | PVC | ACCESS MODE | STORAGE CLASS |", "|---|---|---|---|---|"]
        for pod, entries in sorted(pvc_pods, key=lambda x: (x[0].metadata.namespace, x[0].metadata.name)):
            for pvc_name, mode_str, sc in entries:
                lines.append(f"| `{pod.metadata.namespace}` | `{pod.metadata.name}` "
                             f"| `{pvc_name}` | {mode_str} | {sc} |")
        return "\n".join(lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_unhealthy_pods_detail(namespace: str = "all") -> str:
    def _collect():
        found = []
        for phase in ("Pending","Failed","Unknown"):
            try:
                r = (_core.list_pod_for_all_namespaces(field_selector=f"status.phase={phase}")
                     if namespace == "all"
                     else _core.list_namespaced_pod(namespace=namespace, field_selector=f"status.phase={phase}"))
                found.extend(r.items)
            except ApiException:
                pass
        _cont = None
        while True:
            try:
                kw = {"field_selector": "status.phase=Running", "limit": 100}
                if namespace != "all": kw["namespace"] = namespace
                if _cont: kw["_continue"] = _cont
                page = (_core.list_pod_for_all_namespaces(**kw) if namespace == "all"
                        else _core.list_namespaced_pod(**kw))
                for pod in page.items:
                    restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
                    ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                    if ready < len(pod.spec.containers) or _is_high_restart(pod, restarts):
                        found.append(pod)
                _cont = page.metadata._continue if page.metadata and page.metadata._continue else None
                if not _cont: break
            except ApiException:
                break
        return found

    def _detail(pod):
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
            exit_code  = state_val.get("exit_code", "")
            lines.append(f"    [{cs.name}] ready={cs.ready} state={state_key} restarts={cs.restart_count}"
                         + (f" reason={reason}" if reason else "")
                         + (f" exit_code={exit_code}" if exit_code != "" else ""))
            msg = state_val.get("message", "")
            if msg:
                for mline in msg.strip().splitlines()[:5]:
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
        try:
            ev = _core.list_namespaced_event(
                namespace=pod.metadata.namespace,
                field_selector=f"involvedObject.name={pod.metadata.name}")
            for e in sorted([e for e in ev.items if e.type == "Warning"],
                            key=lambda e: (e.last_timestamp or e.event_time
                                           or datetime.datetime.min.replace(tzinfo=timezone.utc)),
                            reverse=True)[:5]:
                lines.append(f"    [{e.reason}] {e.message}")
        except ApiException:
            pass
        return lines

    def _get_logs(pod_name, ns):
        from kubernetes.stream import stream as _k8s_stream
        for prev in (False, True):
            try:
                kw = {"tail_lines": 20, "timestamps": False}
                if prev: kw["previous"] = True
                logs = _core.read_namespaced_pod_log(name=pod_name, namespace=ns, **kw)
                if logs and logs.strip():
                    return ("(previous container)\n" if prev else "") + logs.strip()
            except (ApiException, Exception) as e:
                is_ws = (isinstance(e, ApiException) and e.status == 0) or \
                        ('Handshake' in str(e) or '-+-+' in str(e))
                if is_ws and not prev:
                    # Try WebSocket exec fallback
                    try:
                        resp = _k8s_stream(
                            _core.connect_get_namespaced_pod_exec,
                            pod_name, ns,
                            command=["/bin/sh", "-c", "tail -n 20 /proc/1/fd/1 2>/dev/null || echo ''"],
                            stderr=True, stdin=False, stdout=True, tty=False, _preload_content=True)
                        if resp and resp.strip():
                            return resp.strip()
                    except Exception:
                        pass
        return ""

    try:
        unhealthy = _collect()
    except Exception as e:
        return f"[ERROR] Failed to collect unhealthy pods: {e}"

    if not unhealthy:
        scope = f"namespace '{namespace}'" if namespace != "all" else "all namespaces"
        return f"No unhealthy pods found in {scope}."

    out = [_ns_header("Unhealthy Pods", namespace), "",
           "| Namespace | Pod | Phase | Ready | Restarts |", "|---|---|---|---|---|"]
    for pod in unhealthy:
        phase    = pod.status.phase or "Unknown"
        restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
        ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
        out.append(f"| `{pod.metadata.namespace}` | `{pod.metadata.name}` "
                   f"| {phase} | {ready}/{len(pod.spec.containers)} | {restarts} |")
    for pod in unhealthy:
        out.extend(["", "=" * 70,
                    f"POD: {pod.metadata.namespace}/{pod.metadata.name}"])
        out.extend(_detail(pod))
        logs = _get_logs(pod.metadata.name, pod.metadata.namespace)
        if logs:
            out.append("  Recent logs (last 20 lines):")
            out.extend(f"    {line}" for line in logs.splitlines()[-20:])
        else:
            out.append("  Logs: none available")
        out.append("")
    return "\n".join(out)

def describe_pv(name: str, show_yaml: bool = False) -> str:
    try:
        pvs = _core.list_persistent_volume().items
        if not pvs:
            return "`No PVs found.`"
        matching = [p for p in pvs if name.lower() in p.metadata.name.lower()]
        if not matching:
            return f"`No PV matching '{name}' found.`"
        pv = matching[0]
        if show_yaml:
            return _as_yaml(pv)
        claim_ref = (f"{pv.spec.claim_ref.namespace}/{pv.spec.claim_ref.name}"
                     if pv.spec.claim_ref else "<none>")
        lines = ["```",
                 f"Name:            {pv.metadata.name}",
                 _fmt_kv("Labels:         ", pv.metadata.labels),
                 _fmt_kv("Annotations:    ", pv.metadata.annotations, sep=": "),
                 f"Finalizers:      {pv.metadata.finalizers or []}",
                 f"StorageClass:    {pv.spec.storage_class_name or '<none>'}",
                 f"Status:          {pv.status.phase}",
                 f"Claim:           {claim_ref}",
                 f"Reclaim Policy:  {pv.spec.persistent_volume_reclaim_policy}",
                 f"Access Modes:    {', '.join(pv.spec.access_modes or []) or '<none>'}",
                 f"VolumeMode:      {pv.spec.volume_mode or '<none>'}",
                 f"Capacity:        {pv.spec.capacity.get('storage') if pv.spec.capacity else '<none>'}",
                 f"Node Affinity:   {pv.spec.node_affinity or '<none>'}",
                 f"Message:         {pv.status.message or ''}"]
        if pv.spec.csi:
            csi = pv.spec.csi
            lines += ["Source:",
                      "    Type:              CSI",
                      f"    Driver:            {csi.driver}",
                      f"    FSType:            {csi.fs_type or '<none>'}",
                      f"    VolumeHandle:      {csi.volume_handle}",
                      f"    ReadOnly:          {csi.read_only}"]
            if csi.volume_attributes:
                lines.append("    VolumeAttributes:  " +
                              "\n                       ".join(f"{k}={v}" for k, v in csi.volume_attributes.items()))
        else:
            lines.append("Source:          <unknown>")
        try:
            evs = _core.list_event_for_all_namespaces(limit=500).items
            pv_events = [e for e in evs
                         if getattr(e.involved_object, "name", "") == pv.metadata.name
                         and e.involved_object.kind == "PersistentVolume"]
            if pv_events:
                lines.append("Events:")
                for e in sorted(pv_events, key=lambda x: x.last_timestamp or x.event_time or ""):
                    lines.append(f"  {e.last_timestamp} {e.type} {e.reason} — {e.message} (x{e.count or 1})")
            else:
                lines.append("Events:          <none>")
        except Exception:
            lines.append("Events:          <error fetching events>")
        lines.append("```")
        return "\n".join(lines)
    except Exception as e:
        return f"`Error fetching PV: {e}`"

def describe_pvc(name: str, namespace: str = "all", show_yaml: bool = False) -> str:
    try:
        pvcs = (_core.list_persistent_volume_claim_for_all_namespaces().items
                if namespace == "all"
                else _core.list_namespaced_persistent_volume_claim(namespace=namespace).items)
        if not pvcs:
            return f"`No PVCs found in namespace '{namespace}'.`"
        matching = [p for p in pvcs if name.lower() in p.metadata.name.lower()]
        if not matching:
            return f"`No PVC matching '{name}' found in namespace '{namespace}'.`"
        pvc = matching[0]
        if show_yaml:
            return _as_yaml(pvc)
        lines = ["```",
                 f"Name:          {pvc.metadata.name}",
                 f"Namespace:     {pvc.metadata.namespace}",
                 f"StorageClass:  {pvc.spec.storage_class_name or '<none>'}",
                 f"Status:        {pvc.status.phase}",
                 f"Volume:        {pvc.spec.volume_name or '<none>'}",
                 _fmt_kv("Labels:       ", pvc.metadata.labels),
                 _fmt_kv("Annotations:  ", pvc.metadata.annotations, sep=": "),
                 f"Finalizers:    {pvc.metadata.finalizers or []}",
                 f"Capacity:      {(pvc.status.capacity or {}).get('storage', '<none>')}",
                 f"Access Modes:  {', '.join(pvc.status.access_modes or []) or '<none>'}",
                 f"VolumeMode:    {pvc.spec.volume_mode or '<none>'}"]
        try:
            pods = _list_pods(pvc.metadata.namespace)
            used_by = [pod.metadata.name for pod in pods
                       for vol in (pod.spec.volumes or [])
                       if getattr(vol, "persistent_volume_claim", None)
                          and vol.persistent_volume_claim.claim_name == pvc.metadata.name]
            lines.append(f"Used By:       {', '.join(used_by) if used_by else '<none>'}")
        except Exception:
            lines.append("Used By:       <error fetching pods>")
        try:
            evs = (_core.list_event_for_all_namespaces(limit=500).items
                   if namespace == "all"
                   else _core.list_namespaced_event(namespace=namespace, limit=500).items)
            pvc_events = [e for e in evs
                          if getattr(e.involved_object, "name", "") == pvc.metadata.name
                          and e.involved_object.kind == "PersistentVolumeClaim"]
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
        return f"`Error fetching PVC: {e}`"

def describe_sc(name: str, show_yaml: bool = False) -> str:
    try:
        matching = [sc for sc in _storage.list_storage_class().items if sc.metadata.name == name]
        if not matching:
            return f"`No StorageClass named '{name}' found.`"
        sc = matching[0]
        if show_yaml:
            return _as_yaml(sc)
        is_default = (sc.metadata.annotations or {}).get(
            "storageclass.kubernetes.io/is-default-class", "false") == "true"
        lines = ["```",
                 f"Name:                  {sc.metadata.name}",
                 f"IsDefaultClass:        {'Yes' if is_default else 'No'}",
                 _fmt_kv("Annotations:          ", sc.metadata.annotations, sep="="),
                 f"Provisioner:           {sc.provisioner}",
                 "Parameters:            " + ",".join(f"{k}={v}" for k, v in (sc.parameters or {}).items()),
                 f"AllowVolumeExpansion:  {sc.allow_volume_expansion or False}",
                 f"MountOptions:          {', '.join(sc.mount_options) if sc.mount_options else '<none>'}",
                 f"ReclaimPolicy:         {sc.reclaim_policy or '<none>'}",
                 f"VolumeBindingMode:     {sc.volume_binding_mode or '<none>'}",
                 "Events:                <none>",
                 "```"]
        return "\n".join(lines)
    except Exception as e:
        return f"`Error fetching StorageClass: {e}`"

def get_storage_classes() -> str:
    try:
        scs = _storage.list_storage_class()
        if not scs.items:
            return "No StorageClasses found in the cluster."
        lines = ["### StorageClasses\n",
                 "| NAME | PROVISIONER | RECLAIM POLICY | EXPANSION | DEFAULT |",
                 "|---|---|---|---|---|"]
        for sc in scs.items:
            is_default = (sc.metadata.annotations or {}).get(
                "storageclass.kubernetes.io/is-default-class") == "true"
            lines.append(
                f"| {sc.metadata.name} | {getattr(sc,'provisioner','unknown')} "
                f"| {getattr(sc,'reclaim_policy','Delete')} "
                f"| {'✓ Yes' if getattr(sc,'allow_volume_expansion',False) else 'No'} "
                f"| {'★ Yes' if is_default else ''} |")
        return "\n".join(lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_pvc_status(namespace: str = "all", show_all: bool = False,
                   search: str | None = None) -> str:
    _AM = {"ReadWriteOnce": "RWO", "ReadWriteMany": "RWX"}
    def _access(pvc):
        return ",".join(_AM[m] for m in (pvc.spec.access_modes or []) if m in _AM) or "?"
    try:
        pvcs = (_core.list_persistent_volume_claim_for_all_namespaces()
                if namespace == "all"
                else _core.list_namespaced_persistent_volume_claim(namespace=namespace))
        if not pvcs.items:
            return f"No PVCs found in namespace '{namespace}'."
        rows = [(pvc.metadata.namespace, pvc.metadata.name, pvc.status.phase or "Unknown",
                 _access(pvc), pvc.spec.storage_class_name or "default",
                 (pvc.status.capacity or {}).get("storage", "?"),
                 pvc.spec.volume_name or "<unbound>")
                for pvc in sorted(pvcs.items, key=lambda x: (x.metadata.namespace, x.metadata.name))
                if not search or search.lower() in pvc.metadata.name.lower()]
        if search and not rows:
            rows = [(pvc.metadata.namespace, pvc.metadata.name, pvc.status.phase or "Unknown",
                     _access(pvc), pvc.spec.storage_class_name or "default",
                     (pvc.status.capacity or {}).get("storage", "?"),
                     pvc.spec.volume_name or "<unbound>")
                    for pvc in sorted(pvcs.items, key=lambda x: (x.metadata.namespace, x.metadata.name))]
        if not rows:
            return "No PVCs to display."
        md_lines = [_ns_header("PersistentVolumeClaims", namespace, search)]
        if namespace == "all":
            md_lines += ["| NAMESPACE | PVC | PHASE | ACCESS | CLASS | CAPACITY | VOLUME |",
                         "|---|---|---|---|---|---|---|"]
            for ns, nm, ph, am, sc, cap, vol in rows:
                md_lines.append(f"| `{ns}` | `{nm}` | {ph} | {am} | {sc} | {cap} | {vol} |")
        else:
            md_lines += ["| PVC | PHASE | ACCESS | CLASS | CAPACITY | VOLUME |", "|---|---|---|---|---|---|"]
            for _, nm, ph, am, sc, cap, vol in rows:
                md_lines.append(f"| `{nm}` | {ph} | {am} | {sc} | {cap} | {vol} |")
        return "\n".join(md_lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_persistent_volumes() -> str:
    _AM = {"ReadWriteOnce":"RWO","ReadWriteMany":"RWX","ReadOnlyMany":"ROX","ReadWriteOncePod":"RWOP"}
    try:
        pvs = _core.list_persistent_volume()
        if not pvs.items:
            return "No PersistentVolumes found."
        lines = ["PersistentVolumes:"]
        for pv in pvs.items:
            am    = ",".join(_AM.get(m, m) for m in (pv.spec.access_modes or [])) or "?"
            claim = (f"{pv.spec.claim_ref.namespace}/{pv.spec.claim_ref.name}"
                     if pv.spec.claim_ref else "unbound")
            lines.append(
                f"  {pv.metadata.name}: {pv.status.phase or 'Unknown'} | "
                f"{(pv.spec.capacity or {}).get('storage', '?')} | access:{am} | "
                f"class:{pv.spec.storage_class_name or 'none'} "
                f"policy:{pv.spec.persistent_volume_reclaim_policy or '?'} claim:{claim}")
        return "\n".join(lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def _get_pv_usage_data(threshold: int = 80) -> list:
    """
    Shared PV usage data-gathering used by both get_pv_usage() and
    generate_healthcheck_report().  Returns a list of dicts:
      {"pct_val", "ns", "pvc_name", "pct_str", "used_gib", "total_gib",
       "avail_gib", "free_pct", "sc"}
    Only PVCs with measurable usage are included (errors/skips are silently dropped).
    """
    from kubernetes.stream import stream as _k8s_stream

    def _exec_df(pod_name, namespace, mount_path, container=None):
        try:
            kwargs = dict(
                command=["/bin/sh", "-c", f"df -k --output=used,avail {mount_path} 2>/dev/null | tail -1"],
                stderr=False, stdin=False, stdout=True, tty=False, _preload_content=True)
            if container: kwargs["container"] = container
            resp = _k8s_stream(_core.connect_get_namespaced_pod_exec, pod_name, namespace, **kwargs)
            return resp.strip() if isinstance(resp, str) else ""
        except Exception:
            return ""

    def _longhorn_crd_data(vol_name, ns, pvc_name, sc):
        """Returns a result dict if Longhorn CRD is available, else None."""
        try:
            custom = _k8s.CustomObjectsApi()
            lh_vol = custom.get_namespaced_custom_object(
                "longhorn.io","v1beta2","longhorn-system","volumes",vol_name)
            actual_raw  = lh_vol.get("status",{}).get("actualSize")
            nominal_raw = lh_vol.get("spec",{}).get("size")
            if actual_raw and nominal_raw:
                used  = int(actual_raw); total = int(nominal_raw)
                pct   = round((used/total)*100,1) if total>0 else 0.0
                used_gib  = round(used/(1024**3),2)
                total_gib = round(total/(1024**3),2)
                avail_gib = round(max(total_gib-used_gib,0),2)
                free_pct  = round((1 - used/total)*100,1) if total>0 else 0.0
                return {"pct_val":pct,"ns":ns,"pvc_name":pvc_name,"sc":sc,
                        "pct_str":f"{pct}%",
                        "used_gib":used_gib,"total_gib":total_gib,
                        "avail_gib":avail_gib,"free_pct":free_pct}
        except Exception:
            pass
        return None

    try:
        all_pvcs = _core.list_persistent_volume_claim_for_all_namespaces().items
    except ApiException:
        return []

    results = []
    for pvc in all_pvcs:
        ns, pvc_name = pvc.metadata.namespace, pvc.metadata.name
        vol_name = pvc.spec.volume_name or ""
        sc       = pvc.spec.storage_class_name or "none"
        pod_hit = pod_container = mount_path = None
        try:
            for pod in _core.list_namespaced_pod(namespace=ns).items:
                if pod.status.phase != "Running": continue
                for vol in (pod.spec.volumes or []):
                    if vol.persistent_volume_claim and vol.persistent_volume_claim.claim_name == pvc_name:
                        for container in (pod.spec.containers or []):
                            for vm in (container.volume_mounts or []):
                                if vm.name == vol.name:
                                    pod_hit = pod.metadata.name
                                    pod_container = container.name
                                    mount_path = vm.mount_path
                                    break
                            if pod_hit: break
                    if pod_hit: break
        except ApiException:
            pass

        if not pod_hit or not mount_path:
            d = _longhorn_crd_data(vol_name, ns, pvc_name, sc)
            if d: results.append(d)
            continue

        df_out = _exec_df(pod_hit, ns, mount_path, container=pod_container)
        if not df_out:
            d = _longhorn_crd_data(vol_name, ns, pvc_name, sc)
            if d: results.append(d)
            continue

        parts = df_out.split()
        if len(parts) < 2: continue
        try:
            used_kb = int(parts[0]); avail_kb = int(parts[1])
            total_kb = used_kb + avail_kb
            pct = round((used_kb/total_kb)*100,1) if total_kb>0 else 0.0
        except (ValueError, ZeroDivisionError):
            continue
        used_gib  = round(used_kb/(1024*1024),2)
        avail_gib = round(avail_kb/(1024*1024),2)
        total_gib = round(total_kb/(1024*1024),2)
        free_pct  = round(avail_kb/total_kb*100,1) if total_kb>0 else 0.0
        results.append({"pct_val":pct,"ns":ns,"pvc_name":pvc_name,"sc":sc,
                        "pct_str":f"{pct}%",
                        "used_gib":used_gib,"total_gib":total_gib,
                        "avail_gib":avail_gib,"free_pct":free_pct})

    results.sort(key=lambda x: x["pct_val"], reverse=True)
    return results


def get_pv_usage(threshold: int = 80) -> str:
    results = _get_pv_usage_data(threshold)
    if not results:
        return "No PV usage data available (no mounted running pods or Longhorn CRD not accessible)."

    nearing = [r for r in results if r["pct_val"] >= threshold]
    ok      = [r for r in results if r["pct_val"] <  threshold]

    def _flag(pct):
        return "🔴" if pct >= 90 else ("🟠" if pct >= threshold else "🟢")

    def _build_table(title, data):
        if not data: return ""
        lines = [f"\n### {title}\n",
                 "| Flag | Namespace / PVC | Usage | Used (GiB) | Total (GiB) | Free (GiB) |",
                 "|---|---|---|---|---|---|"]
        for d in data:
            ns_pvc = f"{d['ns']}/{d['pvc_name']}"
            lines.append(f"| {_flag(d['pct_val'])} | `{ns_pvc}` | **{d['pct_str']}** "
                         f"| {d['used_gib']}Gi ({d['pct_str']}) | {d['total_gib']}Gi "
                         f"| {d['avail_gib']}Gi ({d['free_pct']}%) |")
        return "\n".join(lines)

    output = []
    if nearing:
        output.append(_build_table(f"⚠️ Nearing or exceeding {threshold}% capacity ({len(nearing)} PVCs)", nearing))
    else:
        output.append(f"\n✅ **No PVCs are at or above {threshold}% capacity.**\n")
    if ok:
        output.append(_build_table(f"✅ Within capacity ({len(ok)} PVCs)", ok))
    return "\n".join(output)

def get_pdb_status(namespace: str = "all") -> str:
    try:
        policy_api = _k8s.PolicyV1Api()
        pdbs = (policy_api.list_pod_disruption_budget_for_all_namespaces().items
                if namespace == "all"
                else policy_api.list_namespaced_pod_disruption_budget(namespace=namespace).items)
        if not pdbs:
            return f"No PodDisruptionBudgets found in '{namespace}'."
        lines = [_ns_header("PodDisruptionBudgets", namespace),
                 "| NAMESPACE | NAME | MIN AVAILABLE | MAX UNAVAIL | ALLOWED DISRUPTIONS | HEALTHY (CUR/DES) | STATUS |",
                 "|---|---|---|---|---|---|---|"]
        for pdb in pdbs:
            allowed = pdb.status.disruptions_allowed or 0
            lines.append(
                f"| `{pdb.metadata.namespace}` | `{pdb.metadata.name}` "
                f"| {pdb.spec.min_available if pdb.spec.min_available is not None else 'N/A'} "
                f"| {pdb.spec.max_unavailable if pdb.spec.max_unavailable is not None else 'N/A'} "
                f"| **{allowed}** "
                f"| {pdb.status.current_healthy or 0}/{pdb.status.desired_healthy or 0} "
                f"| {'🔴 BLOCKED' if allowed == 0 else '🟢 OK'} |")
        return "\n".join(lines)
    except Exception as e:
        return f"K8s API error fetching PDBs: {e}"

def get_endpoints(namespace: str = "all", search: str | None = None) -> str:
    try:
        eps_list = (_core.list_endpoints_for_all_namespaces().items
                    if namespace == "all"
                    else _core.list_namespaced_endpoints(namespace=namespace).items)
        if not eps_list:
            return f"No endpoints found in '{namespace}'."
        def _ep_rows(eps, apply_search):
            rows = []
            for ep in eps:
                if apply_search and search and search.lower() not in ep.metadata.name.lower():
                    continue
                if not ep.subsets: continue
                for subset in ep.subsets:
                    for addr in (subset.addresses or []):
                        for port in (subset.ports or []):
                            rows.append((ep.metadata.namespace, ep.metadata.name, f"{addr.ip}:{port.port}"))
            return rows
        table_rows = _ep_rows(eps_list, True)
        if search and not table_rows:
            table_rows = _ep_rows(eps_list, False)
        if not table_rows:
            return "No active Endpoints (no addresses) found."
        md_lines = [_ns_header("Endpoints", namespace, search)]
        if namespace == "all":
            md_lines += ["| NAMESPACE | NAME | ADDRESS |", "|---|---|---|"]
            for ns, nm, addr in table_rows:
                md_lines.append(f"| `{ns}` | `{nm}` | `{addr}` |")
        else:
            md_lines += ["| NAME | ADDRESS |", "|---|---|"]
            for _, nm, addr in table_rows:
                md_lines.append(f"| `{nm}` | `{addr}` |")
        return "\n".join(md_lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_service(namespace: str = "all", search: str = None) -> str:
    try:
        svcs = (_core.list_service_for_all_namespaces()
                if namespace == "all"
                else _core.list_namespaced_service(namespace=namespace))
        if not svcs.items:
            return f"No services found in '{namespace}'."
        def _rows(items, apply_search):
            rows = []
            for svc in items:
                if apply_search and search and search.lower() not in svc.metadata.name.lower(): continue
                ports    = ", ".join(f"{p.port}/{p.protocol}" for p in (svc.spec.ports or []))
                selector = svc.spec.selector or {}
                rows.append((svc.metadata.namespace, svc.metadata.name,
                              svc.spec.type or "ClusterIP", ports,
                              "✓ Present" if selector else "⚠ No selector"))
            return rows
        rows = _rows(svcs.items, True)
        if search and not rows:
            rows = _rows(svcs.items, False)
        md_lines = [_ns_header("Services", namespace, search)]
        if namespace == "all":
            md_lines += ["| NAMESPACE | NAME | TYPE | PORTS | SELECTOR STATUS |", "|---|---|---|---|---|"]
            for ns, nm, st, pts, flag in rows:
                md_lines.append(f"| `{ns}` | `{nm}` | {st} | {pts} | {flag} |")
        else:
            md_lines += ["| NAME | TYPE | PORTS | SELECTOR STATUS |", "|---|---|---|---|"]
            for _, nm, st, pts, flag in rows:
                md_lines.append(f"| `{nm}` | {st} | {pts} | {flag} |")
        return "\n".join(md_lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_ingress(namespace: str = "all", name: str = "", port: int = 0) -> str:
    def _get_ports(ing) -> list:
        ports = {443} if ing.spec.tls else set()
        for rule in (ing.spec.rules or []):
            if rule.http:
                for path in (rule.http.paths or []):
                    svc = path.backend.service
                    if svc and svc.port and svc.port.number:
                        ports.add(svc.port.number)
        return sorted(ports) if ports else [80]

    def _fmt(ing) -> str:
        hosts = [rule.host or "*" for rule in (ing.spec.rules or [])]
        ports = _get_ports(ing)
        lb    = []
        if ing.status.load_balancer and ing.status.load_balancer.ingress:
            for s in ing.status.load_balancer.ingress:
                if s.ip: lb.append(s.ip)
                elif s.hostname: lb.append(s.hostname)
        out = [f"### Ingress: `{ing.metadata.namespace}/{ing.metadata.name}`",
               f"- **Class**: {ing.spec.ingress_class_name or 'default'}",
               f"- **Hosts**: {', '.join(hosts) or 'none'}",
               f"- **Ports**: {', '.join(str(p) for p in ports)}",
               f"- **LB IP**: {', '.join(lb) or 'pending'}"]
        ann = ing.metadata.annotations or {}
        if ann:
            out.append("\n**Annotations:**")
            out.extend(f"- `{k}`: {v}" for k, v in ann.items())
        for rule in (ing.spec.rules or []):
            if rule.http:
                out.append(f"\n**Rules ({rule.host or '*'}):**")
                for path in (rule.http.paths or []):
                    svc = path.backend.service
                    out.append(f"- `{path.path or '/'}` ➔ Service `{svc.name}:{svc.port.number or svc.port.name}` ({path.path_type})")
        if ing.spec.tls:
            out.append("\n**TLS:**")
            for t in ing.spec.tls:
                out.append(f"- Secret `{t.secret_name}` for hosts: {', '.join(t.hosts) if t.hosts else 'none'}")
        return "\n".join(out)

    try:
        pool = _net.list_ingress_for_all_namespaces().items
        if port:
            pool = [i for i in pool if port in _get_ports(i)]
            if not pool:
                return f"No ingresses found exposing port {port} in any namespace."
            out = [f"### Ingresses exposing port {port}\n",
                   "| NAMESPACE | NAME | HOSTS | PORTS |", "|---|---|---|---|"]
            for ing in pool:
                hosts = [r.host or "*" for r in (ing.spec.rules or [])]
                out.append(f"| {ing.metadata.namespace} | {ing.metadata.name} "
                           f"| {', '.join(hosts)} | {', '.join(str(p) for p in _get_ports(ing))} |")
            return "\n".join(out)
        if name:
            if "." in name:
                matches = [ing for ing in pool
                           for rule in (ing.spec.rules or [])
                           if rule.host and rule.host.lower() == name.lower()]
                if not matches:
                    return f"No ingress found with hostname '{name}' in any namespace."
                return "\n\n---\n\n".join(_fmt(ing) for ing in matches)
            if namespace != "all":
                try:
                    return _fmt(_net.read_namespaced_ingress(name=name, namespace=namespace))
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
        out = [_ns_header("Ingresses", namespace)]
        if namespace == "all":
            out += ["| NAMESPACE | NAME | HOSTS | PORTS | LOAD BALANCER |", "|---|---|---|---|---|"]
        else:
            out += ["| NAME | HOSTS | PORTS | LOAD BALANCER |", "|---|---|---|---|"]
        for ing in pool:
            hosts = [r.host or "*" for r in (ing.spec.rules or [])]
            ports = _get_ports(ing)
            lb    = []
            if ing.status.load_balancer and ing.status.load_balancer.ingress:
                for s in ing.status.load_balancer.ingress:
                    if s.ip: lb.append(s.ip)
                    elif s.hostname: lb.append(s.hostname)
            if namespace == "all":
                out.append(f"| {ing.metadata.namespace} | {ing.metadata.name} | {', '.join(hosts) or '*'} "
                           f"| {', '.join(str(p) for p in ports)} | {', '.join(lb) or 'pending'} |")
            else:
                out.append(f"| {ing.metadata.name} | {', '.join(hosts) or '*'} "
                           f"| {', '.join(str(p) for p in ports)} | {', '.join(lb) or 'pending'} |")
        return "\n".join(out)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_network_policy_status(namespace: str = "all") -> str:
    try:
        nps = (_net.list_network_policy_for_all_namespaces().items
               if namespace == "all"
               else _net.list_namespaced_network_policy(namespace=namespace).items)
        if not nps:
            return f"No NetworkPolicies found in '{namespace}'. ⚠️ Cluster may be completely open to lateral movement."
        lines = [_ns_header("NetworkPolicies", namespace),
                 "| NAMESPACE | NAME | POD SELECTOR | POLICY TYPES |", "|---|---|---|---|"]
        covered = set()
        for np in nps:
            covered.add(np.metadata.namespace)
            sel = np.spec.pod_selector.match_labels
            sel_str = ",".join(f"{k}={v}" for k, v in sel.items()) if sel else "ALL PODS"
            lines.append(f"| `{np.metadata.namespace}` | `{np.metadata.name}` "
                         f"| `{sel_str}` | {', '.join(np.spec.policy_types or [])} |")
        if namespace == "all":
            try:
                missing = {n.metadata.name for n in _core.list_namespace().items} - covered
                if missing:
                    lines.append("\n**⚠️ WARNING: The following namespaces have NO NetworkPolicies:**")
                    lines.append(f"> `{', '.join(sorted(missing))}`")
            except Exception:
                pass
        return "\n".join(lines)
    except Exception as e:
        return f"K8s API error fetching NetworkPolicies: {e}"

def get_node_capacity() -> str:
    try:
        nodes = _core.list_node()
        if not nodes.items:
            return "No nodes found."
        headers = ["NODE","CPU ALLOC","CPU REQ","CPU AVAIL","RAM ALLOC (Gi)","RAM REQ (Gi)","RAM AVAIL (Gi)","GPU"]
        lines   = ["Showing Node Capacity across all nodes.",
                   "| " + " | ".join(headers) + " |",
                   "|" + "|".join(["---"]*len(headers)) + "|"]
        for node in sorted(nodes.items, key=lambda n: n.metadata.name):
            alloc     = node.status.allocatable or {}
            cpu_alloc = _parse_cpu_cores(alloc.get("cpu", 0))
            mem_alloc = _parse_mem_gib(alloc.get("memory", "0Ki"))
            gpu       = sum(int(alloc[k]) for k in alloc
                            if ("nvidia.com/gpu" in k or "amd.com/gpu" in k) and str(alloc[k]).isdigit())
            pods_on_node = _list_pods_by_node(node.metadata.name)
            cpu_req  = sum(_parse_cpu_cores((c.resources.requests or {}).get("cpu", 0))
                           for pod in pods_on_node for c in (pod.spec.containers or []) if c.resources)
            mem_req  = sum(_parse_mem_gib((c.resources.requests or {}).get("memory", "0"))
                           for pod in pods_on_node for c in (pod.spec.containers or []) if c.resources)
            cpu_avail = round(cpu_alloc - cpu_req, 2)
            mem_avail = round(mem_alloc - mem_req, 2)
            pct = lambda a, b: f"({round((a/b)*100,1)}%)" if b > 0 else "(0%)"
            lines.append(
                f"| {node.metadata.name} | {round(cpu_alloc,2)} | {round(cpu_req,2)} {pct(cpu_req,cpu_alloc)} "
                f"| {cpu_avail} {pct(cpu_avail,cpu_alloc)} | {round(mem_alloc,2)} "
                f"| {round(mem_req,2)} {pct(mem_req,mem_alloc)} | {mem_avail} {pct(mem_avail,mem_alloc)} | {gpu} |")
        return "\n".join(lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_node_labels(search: str = None) -> str:
    try:
        nodes = _core.list_node().items
        if not nodes:
            return "No nodes found."
        if search and search.lower().strip() in ("*","all","label","labels","node","nodes"):
            search = None
        results = []
        for node in nodes:
            labels      = node.metadata.labels or {}
            label_lines = [f"  - {k}={v}" for k, v in labels.items()] or ["  - <none>"]
            if not search:
                results.append(f"**Node: {node.metadata.name}**\n" + "\n".join(label_lines) + "\n")
                continue
            s = search.lower()
            if s in node.metadata.name.lower():
                results.append(f"**Node: {node.metadata.name}**\n" + "\n".join(label_lines) + "\n")
                continue
            filtered = [l for l in label_lines if s in l.lower()]
            if filtered:
                results.append(f"**Node: {node.metadata.name}**\n" + "\n".join(filtered) + "\n")
        return "\n".join(results) if results else f"No nodes or labels found matching '{search}'."
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def _fmt_ts(ts: float, tz_name: str) -> str:
    """Format a Unix timestamp in the user's timezone."""
    try:
        import zoneinfo
        tz  = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz  = timezone.utc
    dt = datetime.datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz)
    return dt.strftime("%H:%M")


def get_top_pods(namespace: str = "all", limit: int = 10,
                 sort_by: str = "cpu", ascending: bool = False,
                 search: str | None = None, duration: str = "",
                 user_timezone: str = "UTC") -> str:
    """
    Show top (or bottom) pods by CPU or memory.

    When duration is empty: live snapshot from metrics-server.
    When duration is set (e.g. '1h', '6h'): average usage from Prometheus over that period.
    """
    import time as _time

    _LOWEST_WORDS = {"lowest", "least", "bottom", "smallest", "lightest", "minimum", "min", "low"}
    if sort_by.strip().lower() in _LOWEST_WORDS:
        sort_by   = "cpu"
        ascending = True

    sort_key_label = "CPU" if sort_by.lower() in ("cpu", "cpu_m") else "memory"
    direction      = "lowest" if ascending else "top"
    scope          = f"namespace `{namespace}`" if namespace != "all" else "all namespaces"
    filter_note    = f" matching `{search}`" if search else ""
    tz_label       = user_timezone if user_timezone != "UTC" else "UTC"

    if duration:
        return _get_top_pods_prometheus(
            namespace=namespace, limit=limit, sort_by=sort_by,
            ascending=ascending, search=search, duration=duration,
            user_timezone=user_timezone)

    custom = _k8s.CustomObjectsApi()
    try:
        if namespace == "all":
            items = custom.list_cluster_custom_object(
                "metrics.k8s.io", "v1beta1", "pods").get("items", [])
        else:
            items = custom.list_namespaced_custom_object(
                "metrics.k8s.io", "v1beta1", namespace, "pods").get("items", [])
    except ApiException as e:
        return f"[ERROR] Metrics not available: {e.reason}. Is metrics-server installed?"

    if not items:
        return "No pod metrics found. Is metrics-server installed and running?"

    try:
        import zoneinfo
        tz      = zoneinfo.ZoneInfo(user_timezone)
        now_str = datetime.datetime.now(tz).strftime(f"%Y-%m-%d %H:%M {tz_label}")
    except Exception:
        now_str = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    rows = []
    for item in items:
        meta  = item["metadata"]
        name  = meta["name"]
        if search and search.lower() not in name.lower() \
                  and search.lower() not in meta.get("namespace", "").lower():
            continue
        ctrs  = item.get("containers", [])
        cpu_m = sum(_parse_cpu_to_millicores(c["usage"].get("cpu",    "0")) for c in ctrs)
        mem_m = sum(_parse_mem_to_mib(        c["usage"].get("memory", "0")) for c in ctrs)
        rows.append((meta.get("namespace", "-"), name, cpu_m, mem_m))

    if not rows:
        return (f"No pod metrics found matching `{search}`."
                if search else "No pod metrics found.")

    sk = 2 if sort_by.lower() in ("cpu", "cpu_m") else 3
    rows.sort(key=lambda x: x[sk], reverse=not ascending)
    rows = rows[:max(1, limit)]

    header  = (f"{direction.title()} {len(rows)} pods by {sort_key_label} "
               f"in {scope}{filter_note} — live snapshot at {now_str}.")
    col_ns  = max(len("NAMESPACE"), max(len(r[0]) for r in rows))
    col_pod = max(len("POD"),       max(len(r[1]) for r in rows))
    hdr_row = f"{'NAMESPACE':<{col_ns}}  {'POD':<{col_pod}}  {'CPU(m)':>9}  {'MEMORY(MiB)':>12}"
    sep     = "-" * len(hdr_row)
    lines   = [header, "```", hdr_row, sep]
    for ns_v, name, cpu_m, mem_mib in rows:
        lines.append(f"{ns_v:<{col_ns}}  {name:<{col_pod}}  {cpu_m:>8}m  {mem_mib:>10.0f}Mi")
    lines.append("```")

    import time as _time
    now_ts   = int(_time.time())
    sk_label = "CPU(m)" if sort_by.lower() in ("cpu", "cpu_m") else "Memory(MiB)"
    series_out = [
        {
            "label":  f"{ns_v}/{name}" if ns_v != "-" else name,
            "values": [[now_ts, float(cpu_m if sort_by.lower() in ("cpu","cpu_m") else mem_mib)]],
        }
        for ns_v, name, cpu_m, mem_mib in rows
    ]
    graph_json = _json.dumps(
        {"title": f"{direction.title()} {len(rows)} pods — {sk_label} live",
         "unit":  "m" if sort_by.lower() in ("cpu","cpu_m") else "MiB",
         "duration": "live", "series": series_out},
        separators=(",", ":"))

    return "\n".join(lines) + f"\n§GRAPH§{graph_json}§GRAPH§"


def _get_top_pods_prometheus(namespace: str, limit: int, sort_by: str,
                              ascending: bool, search, duration: str,
                              user_timezone: str) -> str:
    import time as _time
    from kubernetes.stream import stream as _k8s_stream

    sort_key_label = "CPU" if sort_by.lower() in ("cpu", "cpu_m") else "memory"
    direction      = "lowest" if ascending else "top"
    scope          = f"namespace `{namespace}`" if namespace != "all" else "all namespaces"
    filter_note    = f" matching `{search}`" if search else ""
    tz_label       = user_timezone if user_timezone != "UTC" else "UTC"

    dur_map = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    try:
        dur_sec = int(duration[:-1]) * dur_map.get(duration[-1], 1)
    except Exception:
        dur_sec = 3600

    ns_filter = f',namespace="{namespace}"' if namespace not in ("all", "") else ""
    base_filter = f'container!="",container!="POD"{ns_filter}'

    if sort_key_label == "CPU":
        promql = (
            'avg_over_time('
            'sum by (pod, namespace) ('
            'rate(container_cpu_usage_seconds_total{' + base_filter + '}[5m])'
            ')[' + duration + ':60s]'
            ') * 1000'
        )
    else:
        promql = (
            'avg_over_time('
            'sum by (pod, namespace) ('
            'container_memory_working_set_bytes{' + base_filter + '}'
            ')[' + duration + ':60s]'
            ') / 1048576'
        )

    prom_pod, prom_ns, prom_container = _find_prometheus_pod()
    if not prom_pod:
        return "No running Prometheus server pod found."

    def _exec(cmd, large: bool = False):
        try:
            if large:
                ws = _k8s_stream(
                    _core.connect_get_namespaced_pod_exec,
                    prom_pod, prom_ns,
                    command=["/bin/sh", "-c", cmd],
                    container=prom_container,
                    stderr=False, stdin=False, stdout=True, tty=False,
                    _preload_content=False)
                chunks = []
                while ws.is_open():
                    ws.update(timeout=60)
                    if ws.peek_stdout():
                        chunks.append(ws.read_stdout())
                ws.close()
                resp = "".join(chunks)
            else:
                resp = _k8s_stream(
                    _core.connect_get_namespaced_pod_exec,
                    prom_pod, prom_ns,
                    command=["/bin/sh", "-c", cmd],
                    container=prom_container,
                    stderr=False, stdin=False, stdout=True, tty=False,
                    _preload_content=True)
            if isinstance(resp, bytes):
                resp = resp.decode("utf-8", errors="replace")
            return resp.strip() if isinstance(resp, str) else str(resp).strip()
        except Exception as exc:
            return f"[exec error: {exc}]"

    base_url = "http://localhost:9090"
    probe    = _exec(f"curl -s -o /dev/null -w '%{{http_code}}' '{base_url}/prometheus/api/v1/query?query=up'")
    api_base = f"{base_url}/prometheus/api/v1" if probe.strip() == "200" else f"{base_url}/api/v1"

    end_ts   = int(_time.time())
    start_ts = end_ts - dur_sec
    enc      = urllib.parse.quote(promql, safe="")
    url      = f"{api_base}/query_range?query={enc}&start={start_ts}&end={end_ts}&step=60s"
    raw      = _exec(f"curl -s --max-time 60 '{url}'", large=True)

    if not raw:
        return "Prometheus returned an empty response. The query may have timed out or returned no data."

    try:
        data = _json.loads(raw)
    except Exception:
        return f"Prometheus returned non-JSON: {raw[:300]}"

    if data.get("status") != "success":
        return f"Prometheus query failed: {data.get('error', data)}"

    results = data.get("data", {}).get("result", [])
    if not results:
        return f"No Prometheus data for metric '{sort_key_label}' over the last {duration}."

    def _mean(r):
        vals = [float(v[1]) for v in r.get("values", []) if v[1] != "NaN"]
        return sum(vals) / len(vals) if vals else 0.0

    ranked = []
    for r in results:
        ml    = r.get("metric", {})
        ns_v  = ml.get("namespace", "-")
        pod_v = ml.get("pod", ml.get("pod_name", "?"))
        if search and search.lower() not in pod_v.lower() \
                  and search.lower() not in ns_v.lower():
            continue
        ranked.append((ns_v, pod_v, _mean(r), r.get("values", [])))

    if not ranked:
        return (f"No Prometheus data matching `{search}` over the last {duration}."
                if search else f"No Prometheus data over the last {duration}.")

    ranked.sort(key=lambda x: x[2], reverse=not ascending)
    ranked = ranked[:max(1, limit)]

    rows = [(ns_v, pod_v, mean_v) for ns_v, pod_v, mean_v, _ in ranked]

    unit = "m" if sort_key_label == "CPU" else "Mi"
    try:
        import zoneinfo
        tz       = zoneinfo.ZoneInfo(user_timezone)
        from_str = datetime.datetime.fromtimestamp(start_ts, tz=timezone.utc).astimezone(tz).strftime("%H:%M")
        to_str   = datetime.datetime.fromtimestamp(end_ts,   tz=timezone.utc).astimezone(tz).strftime(f"%H:%M {tz_label}")
    except Exception:
        from_str = datetime.datetime.utcfromtimestamp(start_ts).strftime("%H:%M")
        to_str   = datetime.datetime.utcfromtimestamp(end_ts).strftime("%H:%M UTC")

    header  = (f"{direction.title()} {len(rows)} pods by avg {sort_key_label} "
               f"in {scope}{filter_note} — last {duration} ({from_str}–{to_str}).")
    col_ns  = max(len("NAMESPACE"), max(len(r[0]) for r in rows))
    col_pod = max(len("POD"),       max(len(r[1]) for r in rows))
    col_hdr = f"AVG {sort_key_label}({unit})"
    hdr_row = f"{'NAMESPACE':<{col_ns}}  {'POD':<{col_pod}}  {col_hdr:>14}"
    sep     = "-" * len(hdr_row)
    lines   = [header, "```", hdr_row, sep]
    for ns_v, pod_v, mean_v in rows:
        lines.append(f"{ns_v:<{col_ns}}  {pod_v:<{col_pod}}  {mean_v:>12.1f}{unit}")
    lines.append("```")

    series_out = [
        {
            "label":  f"{ns_v}/{pod_v}" if ns_v != "-" else pod_v,
            "values": [[float(ts), float(v)] for ts, v in vals if v != "NaN"],
        }
        for ns_v, pod_v, _, vals in ranked
    ]
    title      = f"Top {len(rows)} pods — avg {sort_key_label} ({from_str}–{to_str})"
    graph_json = _json.dumps(
        {"title": title, "unit": unit, "duration": duration, "series": series_out},
        separators=(",", ":"))

    return "\n".join(lines) + f"\n§GRAPH§{graph_json}§GRAPH§"


def get_top_nodes(limit: int = 0, ascending: bool = False,
                  user_timezone: str = "UTC") -> str:
    tz_label = user_timezone if user_timezone != "UTC" else "UTC"
    custom   = _k8s.CustomObjectsApi()
    try:
        items = custom.list_cluster_custom_object(
            "metrics.k8s.io", "v1beta1", "nodes").get("items", [])
    except ApiException as e:
        return f"[ERROR] Metrics not available: {e.reason}. Is metrics-server installed?"

    if not items:
        return "No node metrics found. Is metrics-server installed and running?"

    try:
        import zoneinfo
        tz      = zoneinfo.ZoneInfo(user_timezone)
        now_str = datetime.datetime.now(tz).strftime(f"%Y-%m-%d %H:%M {tz_label}")
    except Exception:
        now_str = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    rows = []
    for item in items:
        name  = item["metadata"]["name"]
        cpu_m = _parse_cpu_to_millicores(item["usage"].get("cpu",    "0"))
        mem_m = _parse_mem_to_mib(        item["usage"].get("memory", "0"))
        rows.append((name, cpu_m, mem_m))

    rows.sort(key=lambda x: x[1], reverse=not ascending)
    if limit > 0:
        rows = rows[:limit]

    direction = "lowest" if ascending else "top"
    col_node  = max(len("NODE"), max(len(r[0]) for r in rows))
    hdr_row   = f"{'NODE':<{col_node}}  {'CPU(m)':>9}  {'MEMORY(MiB)':>12}"
    sep       = "-" * len(hdr_row)
    title     = f"Node resource usage — {direction} {len(rows)} node(s) by CPU — live at {now_str}."
    lines     = [title, "```", hdr_row, sep]
    for name, cpu_m, mem_mib in rows:
        lines.append(f"{name:<{col_node}}  {cpu_m:>8}m  {mem_mib:>10.0f}Mi")
    lines.append("```")
    return "\n".join(lines)

def get_node_taints(search: str = None, tainted_only: bool = False,
                    taint_search: str = None) -> str:
    """
    List node taints.

    search       — filters by NODE NAME (partial match).
    taint_search — filters by TAINT KEY or VALUE content (partial match).
                   Use when the user asks for nodes tainted with a specific key/value
                   e.g. 'cde', 'gpu', 'dedicated'.
    tainted_only — when True, show only nodes that have at least one taint.
    """
    _TAINT_INTENT_WORDS = {
        "tainted", "taint", "any", "all", "which", "show", "list", "every", "what"
    }

    if search and search.strip().lower() in _TAINT_INTENT_WORDS:
        tainted_only = True
        search = None

    try:
        nodes = _core.list_node().items
        if not nodes:
            return "No nodes found."

        if search:
            matched = [n for n in nodes if search.lower() in n.metadata.name.lower()]
            if matched:
                nodes = matched

        rows = []
        for node in nodes:
            taints = node.spec.taints or []
            if tainted_only and not taints:
                continue
            if not taints:
                if not taint_search:
                    rows.append((node.metadata.name, "<none>", "-", "-"))
            else:
                for t in taints:
                    key    = t.key    or "<any>"
                    val    = t.value  or "-"
                    effect = t.effect or "-"
                    if taint_search:
                        ts = taint_search.lower()
                        if ts not in key.lower() and ts not in val.lower():
                            continue
                    rows.append((node.metadata.name, key, val, effect))

        if not rows:
            if taint_search:
                return f"No nodes found with a taint matching `{taint_search}`."
            return "No tainted nodes found." if tainted_only else "No nodes found."

        scope = "tainted nodes" if tainted_only else "nodes"
        filter_note = taint_search or search
        lines = [_ns_header(f"Node Taints ({scope})", "all", filter_note),
                 "| NODE | KEY | VALUE | EFFECT |", "|---|---|---|---|"]
        for node_name, key, val, effect in rows:
            lines.append(f"| `{node_name}` | {key} | {val} | {effect} |")
        return "\n".join(lines)

    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_node_info(node_name: str = None) -> str:
    headers = ["NODE","ROLES","STATUS","CPU","MEMORY","GPU"]
    def _build_row(node) -> str:
        roles = (",".join(k.split("/")[-1] for k in (node.metadata.labels or {})
                          if "node-role.kubernetes.io" in k) or "worker")
        conds  = {c.type: c.status for c in (node.status.conditions or [])}
        alloc  = node.status.allocatable or {}
        gpu    = next((alloc[k] for k in alloc if "nvidia.com/gpu" in k or "amd.com/gpu" in k), "0")
        return (f"| {node.metadata.name} | {roles} "
                f"| {'Ready' if conds.get('Ready')=='True' else 'NotReady'} "
                f"| {alloc.get('cpu','?')} | {alloc.get('memory','?')} | {gpu} |")
    try:
        nodes = _core.list_node().items
        if not nodes:
            return "No nodes found."
        filtered = [n for n in nodes if node_name and node_name.lower() in n.metadata.name.lower()]
        if node_name and not filtered:
            out = [f"### No matches for '{node_name}'. Showing all nodes:\n"]
            target_nodes = nodes
        else:
            out = ["### Node Info" + (f" (matching '{node_name}')" if node_name else "") + "\n"]
            target_nodes = filtered if node_name else nodes
        out += ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"]*len(headers)) + "|"]
        out.extend(_build_row(n) for n in target_nodes)
        return "\n".join(out)
    except Exception as e:
        return f"Unexpected error: {e}"

def _workload_table(namespace, kind, items, desired_fn, ready_fn, avail_fn, search=None):
    if not items:
        return f"No {kind}s in '{namespace}'."
    filtered = [i for i in items if not search or search.lower() in i.metadata.name.lower()]
    if search and not filtered:
        return f"No {kind}s matching '{search}' found in '{namespace}'."
    lines = [_ns_header(f"{kind}s", namespace, search),
             "| NAMESPACE | NAME | STATUS | DESIRED | READY | AVAILABLE |",
             "|---|---|---|---|---|---|"]
    for item in filtered:
        desired   = desired_fn(item)
        ready     = ready_fn(item)
        available = avail_fn(item)
        status    = ("✓ Scaled to 0" if desired == 0 and ready == 0 else
                     "✓ Healthy" if ready == desired else "⚠ Degraded")
        lines.append(f"| {item.metadata.namespace} | {item.metadata.name} "
                     f"| {status} | {desired} | {ready} | {available} |")
    return "\n".join(lines)

def get_deployment(namespace: str = "all", search: str = None) -> str:
    try:
        items = (_apps.list_deployment_for_all_namespaces() if namespace == "all"
                 else _apps.list_namespaced_deployment(namespace=namespace)).items
        return _workload_table(namespace, "Deployment", items,
                               lambda d: d.spec.replicas or 0,
                               lambda d: d.status.ready_replicas or 0,
                               lambda d: d.status.available_replicas or 0, search)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_daemonset(namespace: str = "all") -> str:
    try:
        items = (_apps.list_daemon_set_for_all_namespaces() if namespace == "all"
                 else _apps.list_namespaced_daemon_set(namespace=namespace)).items
        return _workload_table(namespace, "DaemonSet", items,
                               lambda d: d.status.desired_number_scheduled or 0,
                               lambda d: d.status.number_ready or 0,
                               lambda d: d.status.number_available or 0)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_replicaset(namespace: str = "all", search: str = None) -> str:
    try:
        items = (_apps.list_replica_set_for_all_namespaces() if namespace == "all"
                 else _apps.list_namespaced_replica_set(namespace=namespace)).items
        return _workload_table(namespace, "ReplicaSet", items,
                               lambda r: r.status.replicas or 0,
                               lambda r: r.status.ready_replicas or 0,
                               lambda r: r.status.available_replicas or 0, search)
    except Exception as e:
        return f"Unexpected error: {e}"

def get_statefulset(namespace: str = "all", search: str = None) -> str:
    try:
        items = (_apps.list_stateful_set_for_all_namespaces() if namespace == "all"
                 else _apps.list_namespaced_stateful_set(namespace=namespace)).items
        return _workload_table(namespace, "StatefulSet", items,
                               lambda s: s.spec.replicas or 0,
                               lambda s: s.status.ready_replicas or 0,
                               lambda s: getattr(s.status, "available_replicas", None) or 0, search)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_hpa_status(namespace: str = "all") -> str:
    try:
        hpas = (_autoscaling.list_horizontal_pod_autoscaler_for_all_namespaces()
                if namespace == "all"
                else _autoscaling.list_namespaced_horizontal_pod_autoscaler(namespace=namespace))
        if not hpas.items:
            return f"No HPAs in '{namespace}'."
        lines = [_ns_header("HorizontalPodAutoscalers", namespace),
                 "| Namespace | HPA Name | Current | Desired | Min | Max | Status |",
                 "|-----------|----------|---------|---------|-----|-----|--------|"]
        for h in hpas.items:
            cur    = h.status.current_replicas or 0
            at_max = cur >= h.spec.max_replicas if h.spec.max_replicas else False
            lines.append(f"| {h.metadata.namespace} | {h.metadata.name} | {cur} "
                         f"| {h.status.desired_replicas or 0} | {h.spec.min_replicas or 1} "
                         f"| {h.spec.max_replicas or '?'} | {'⚠ AT MAX' if at_max else 'OK'} |")
        return "\n".join(lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_adhoc_job_status(namespace: str = "all", show_all: bool = False,
                         raw_output: bool = False, failed_only: bool = False,
                         running_only: bool = False, exclude_cronjobs: bool = True) -> str:
    try:
        jobs = (_batch.list_job_for_all_namespaces()
                if namespace == "all"
                else _batch.list_namespaced_job(namespace=namespace))
        if not jobs.items:
            return f"No Jobs in '{namespace}'."

        def _is_cj_spawn(job) -> bool:
            return any(o.kind == "CronJob" for o in (job.metadata.owner_references or []))

        filtered = [j for j in jobs.items if not (exclude_cronjobs and _is_cj_spawn(j))]
        if not filtered:
            return f"No standalone Jobs found in '{namespace}' (CronJob spawns are hidden)."

        def _classify(j):
            a, s, f = j.status.active or 0, j.status.succeeded or 0, j.status.failed or 0
            if s > 0 and a == 0: return "✓ Complete", a, s, f
            if f > 0:            return "⚠ Failed",   a, s, f
            return "⏳ Running", a, s, f

        if raw_output:
            hdr  = f"{'NAMESPACE':<22} {'NAME':<55} {'STATUS':<12} {'ACTIVE':<7} {'SUCCEEDED':<9} {'FAILED':<7}"
            rows = [_ns_header("Jobs", namespace), hdr, "-"*len(hdr)]
            for j in sorted(filtered, key=lambda j: (j.metadata.namespace, j.metadata.name)):
                status, active, succeeded, failed = _classify(j)
                if running_only and active == 0: continue
                rows.append(f"{j.metadata.namespace:<22} {j.metadata.name:<55} "
                             f"{status.split()[-1]:<12} {active:<7} {succeeded:<9} {failed:<7}")
            return "\n".join(rows) if len(rows) > 2 else "No Jobs are currently running."

        running_jobs, failed_jobs, healthy_jobs = [], [], []
        for j in sorted(filtered, key=lambda j: (j.metadata.namespace, j.metadata.name)):
            status, active, succeeded, failed = _classify(j)
            entry = (f"{j.metadata.namespace}/{j.metadata.name}: {status} | "
                     f"Active:{active} Succeeded:{succeeded} Failed:{failed}")
            if running_only:
                if active > 0: running_jobs.append(entry)
            elif failed_only:
                if failed > 0: failed_jobs.append(entry)
            else:
                (failed_jobs if status == "⚠ Failed" else healthy_jobs).append(entry)

        header = (f"{_ns_header('Jobs', namespace)}\n"
                  f"Standalone Jobs: {len(filtered)} total"
                  + (" (CronJob spawns hidden)." if exclude_cronjobs else "."))
        lines = [header]
        if running_only:
            lines += [f"\nRunning jobs ({len(running_jobs)}):"] + running_jobs or ["  None"]
        else:
            lines += [f"\nFailed/Unhealthy jobs ({len(failed_jobs)}):"] + (failed_jobs or ["  None"])
            if show_all and healthy_jobs:
                lines += [f"\nHealthy/Complete jobs ({len(healthy_jobs)}):"] + healthy_jobs
        return "\n".join(lines)
    except Exception as e:
        return f"K8s API error: {e}"

def get_cronjob_status(namespace: str = "all", search: str = None) -> str:
    try:
        cjs = (_batch.list_cron_job_for_all_namespaces().items
               if namespace == "all"
               else _batch.list_namespaced_cron_job(namespace=namespace).items)
        if not cjs:
            return f"No CronJobs found in '{namespace}'."
        lines = [_ns_header("CronJobs", namespace, search),
                 "| NAMESPACE | NAME | SCHEDULE | SUSPENDED | ACTIVE JOBS | LAST RUN |",
                 "|---|---|---|---|---|---|"]
        for cj in cjs:
            if search and search.lower() not in cj.metadata.name.lower():
                continue
            last_sched = cj.status.last_schedule_time
            if last_sched:
                diff = datetime.datetime.now(timezone.utc) - last_sched
                d, s = diff.days, diff.seconds
                last_sched_str = (f"{d}d {s//3600}h {(s%3600)//60}m ago"
                                  if d > 0 else f"{s//3600}h {(s%3600)//60}m ago")
            else:
                last_sched_str = "Never"
            lines.append(f"| `{cj.metadata.namespace}` | `{cj.metadata.name}` "
                         f"| `{cj.spec.schedule}` | {'⏸️ Yes' if cj.spec.suspend else '▶️ No'} "
                         f"| {len(cj.status.active) if cj.status.active else 0} | {last_sched_str} |")
        return "\n".join(lines)
    except Exception as e:
        return f"K8s API error fetching CronJobs: {e}"

def get_cluster_version() -> str:
    try:
        v = _version_api.get_code()
        return (f"Major: {v.major}, Minor: {v.minor}, GitVersion: {v.git_version}, "
                f"GitCommit: {v.git_commit}, BuildDate: {v.build_date}")
    except Exception as e:
        return f"[ERROR] Failed to get cluster version: {e}"

def get_namespace_status(namespace: str = "all", show_all: bool = False,
                         sort_by: str | None = None, limit: int | None = None) -> str:
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
            pods    = _core.list_namespaced_pod(ns.metadata.name, limit=1000).items
            running = 0
            other   = {}
            for pod in pods:
                phase = pod.status.phase or "Unknown"
                if phase == "Running":
                    running += 1
                else:
                    other[phase] = other.get(phase, 0) + 1
            ns_pod_info[ns.metadata.name] = {"running": running, "other": other}
        sort_map = {
            "pods_asc":  lambda x:  x[1]["running"] + sum(x[1]["other"].values()),
            "pods_desc": lambda x: -(x[1]["running"] + sum(x[1]["other"].values())),
            "name_asc":  lambda x:  x[0],
            "name_desc": lambda x:  x[0][::-1],
        }
        sorted_ns = sorted(ns_pod_info.items(), key=sort_map.get(sort_by, lambda x: x[0]))
        if limit:
            sorted_ns = sorted_ns[:limit]
        lines = [_ns_header("Namespace Status", namespace),
                 "| Namespace | Total Pods | Running | Other State |",
                 "|-----------|------------|---------|--------------|"]
        pod_totals = {}
        for ns_name, info in sorted_ns:
            total = info["running"] + sum(info["other"].values())
            pod_totals[ns_name] = total
            other_str = ", ".join(f"{k}: {v}" for k, v in info["other"].items()) or "-"
            lines.append(f"| {ns_name} | {total} | {info['running']} | {other_str} |")
        if pod_totals:
            lines += ["",
                      f"_Namespace with the least pods: {min(pod_totals, key=pod_totals.get)} ({min(pod_totals.values())} pods)_",
                      f"_Namespace with the most pods: {max(pod_totals, key=pod_totals.get)} ({max(pod_totals.values())} pods)_"]
        return "\n".join(lines)
    except ApiException as e:
        return f"[ERROR] K8s API error listing namespaces: {e.reason}"

def get_events(namespace: str = "all", search: str | None = None, type: str = "All") -> str:
    try:
        type_upper = type.capitalize()
        fs     = f"type={type_upper}" if type_upper in ("Warning","Normal") else ""
        events = (_core.list_event_for_all_namespaces(field_selector=fs, limit=500).items
                  if namespace == "all"
                  else _core.list_namespaced_event(namespace=namespace, field_selector=fs, limit=500).items)
        if not events:
            return f"`No such events in '{namespace}'.`"
        s = search.lower() if search else None
        matching = [e for e in events
                    if not s or s in (e.message or "").lower()
                               or s in (e.metadata.namespace or "").lower()
                               or s in getattr(e.involved_object, "name", "").lower()]
        if not matching:
            return f"`No such events matching '{search}' found in namespace '{namespace}'.`"
        sorted_evs = sorted(matching, key=lambda e: e.last_timestamp or e.event_time or "", reverse=True)
        lines = [_ns_header("Events", namespace, search), "```"]
        shown = suppressed = 0
        for e in sorted_evs:
            if shown >= 20: break
            if _is_noisy_event(e.message):
                suppressed += 1
                continue
            lines.append(f"{e.last_timestamp or e.event_time or 'N/A'}  "
                         f"{e.type:<7}  {e.reason:<18}  "
                         f"{namespace}/{getattr(e.involved_object,'kind','Unknown')}/"
                         f"{getattr(e.involved_object,'name','Unknown')}  "
                         f"{e.message} (x{e.count or 1})")
            shown += 1
        if suppressed:
            lines.append(f"_({suppressed} noisy/background event(s) suppressed)_")
        if shown == 0:
            return f"`No such events in '{namespace}' (all were background noise).`"
        lines.append("```")
        return "\n".join(lines)
    except ApiException as e:
        return f"`{_api_error(e)}`"
    except Exception as e:
        return f"`Error fetching events: {e}`"

def get_gpu_info() -> str:
    try:
        nodes = _core.list_node().items
        if not nodes:
            return "No nodes found."
        pods  = _core.list_pod_for_all_namespaces().items
        lines = ["Showing all GPU Nodes across all namespaces.",
                 "| NODE | PRODUCT | COUNT | VRAM/GRAM | ALLOCATABLE | ATTACHED PODS |",
                 "|---|---|---|---|---|---|"]
        has_gpu = False
        for node in sorted(nodes, key=lambda n: n.metadata.name):
            labels   = node.metadata.labels or {}
            alloc    = node.status.allocatable or {}
            gpu_keys = [k for k in alloc if "gpu" in k.lower()]
            if not gpu_keys: continue
            gpu_key  = gpu_keys[0]
            gpu_alloc = alloc.get(gpu_key, "0")
            product  = labels.get(f"{gpu_key}.product") or labels.get("gpu.product") or "Unknown"
            count    = labels.get(f"{gpu_key}.count") or labels.get("gpu.count") or gpu_alloc
            memory   = labels.get(f"{gpu_key}.memory") or labels.get("gpu.memory") or "n/a"
            if gpu_alloc == "0" and product == "Unknown": continue
            pod_list = [f"{pod.metadata.namespace}/{pod.metadata.name}"
                        for pod in pods
                        if pod.status.phase in ("Running","Pending")
                           and pod.spec.node_name == node.metadata.name
                           and any("gpu" in k.lower() and str(v).isdigit() and int(v) > 0
                                   for c in (pod.spec.containers or [])
                                   for k, v in {**(c.resources.limits or {}), **(c.resources.requests or {})}.items())]
            lines.append(f"| {node.metadata.name} | {product} | {count} | {memory}Mi "
                         f"| {gpu_alloc} | {', '.join(pod_list) if pod_list else '-'} |")
            has_gpu = True
        return "\n".join(lines) if has_gpu else "No GPU nodes detected in the cluster."
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def find_resource(name_substring: str, resource_type: str = None, namespace: str = None) -> str:
    try:
        _INTENT_WORDS = {"all", "any", "everything", "anything", "every", "show", "list"}

        if name_substring and name_substring.strip().lower() in _INTENT_WORDS:
            name_substring = ""

        if namespace and namespace.strip().lower() in ("all", "any", ""):
            namespace = None

        _VALID_TYPES = {
            "pod", "svc", "service", "ingress", "pvc",
            "deployment", "deploy",
            "replicaset", "rs",
            "daemonset", "ds",
            "statefulset", "sts",
            "configmap", "cm",
            "secret",
        }
        if resource_type:
            rt = resource_type.strip().lower()
            resource_type = rt if rt in _VALID_TYPES else None
        else:
            resource_type = None

        def _build_resources(rt_filter):
            r = []

            def _status(desired, ready):
                if desired == 0 and ready == 0: return "Scaled to 0"
                return f"{ready}/{desired} ready" + (" ✅" if ready == desired else " ⚠️")

            if not rt_filter or rt_filter == "pod":
                pods = (_core.list_namespaced_pod(namespace).items if namespace
                        else _core.list_pod_for_all_namespaces().items)
                r.append(("Pod", pods,
                           lambda p: f"{p.status.phase or 'Unknown'} on {p.spec.node_name or 'n/a'}"))

            if not rt_filter or rt_filter in ("deployment", "deploy"):
                deps = (_apps.list_namespaced_deployment(namespace).items if namespace
                        else _apps.list_deployment_for_all_namespaces().items)
                r.append(("Deployment", deps,
                           lambda d, _s=_status: _s(d.spec.replicas or 0, d.status.ready_replicas or 0)))

            if not rt_filter or rt_filter in ("daemonset", "ds"):
                dss = (_apps.list_namespaced_daemon_set(namespace).items if namespace
                       else _apps.list_daemon_set_for_all_namespaces().items)
                r.append(("DaemonSet", dss,
                           lambda d, _s=_status: _s(d.status.desired_number_scheduled or 0,
                                                     d.status.number_ready or 0)))

            if not rt_filter or rt_filter in ("statefulset", "sts"):
                stss = (_apps.list_namespaced_stateful_set(namespace).items if namespace
                        else _apps.list_stateful_set_for_all_namespaces().items)
                r.append(("StatefulSet", stss,
                           lambda s, _s=_status: _s(s.spec.replicas or 0, s.status.ready_replicas or 0)))

            if not rt_filter or rt_filter in ("replicaset", "rs"):
                rss = (_apps.list_namespaced_replica_set(namespace).items if namespace
                       else _apps.list_replica_set_for_all_namespaces().items)
                r.append(("ReplicaSet", rss,
                           lambda rs, _s=_status: _s(rs.spec.replicas or 0, rs.status.ready_replicas or 0)))

            if not rt_filter or rt_filter in ("svc", "service"):
                svcs = (_core.list_namespaced_service(namespace).items if namespace
                        else _core.list_service_for_all_namespaces().items)
                r.append(("Service", svcs,
                           lambda svc: f"{svc.spec.type} {svc.spec.cluster_ip}"))

            if not rt_filter or rt_filter == "ingress":
                ings = (_net.list_namespaced_ingress(namespace).items if namespace
                        else _net.list_ingress_for_all_namespaces().items)
                r.append(("Ingress", ings,
                           lambda ing: ", ".join(h.host for h in (ing.spec.rules or []) if h.host) or "-"))

            if not rt_filter or rt_filter == "pvc":
                pvcs = (_core.list_namespaced_persistent_volume_claim(namespace).items if namespace
                        else _core.list_persistent_volume_claim_for_all_namespaces().items)
                r.append(("PVC", pvcs,
                           lambda pvc: (f"{pvc.status.phase or 'Unknown'} "
                                        f"{(pvc.spec.resources.requests or {}).get('storage', 'n/a') if pvc.spec.resources else 'n/a'}")))

            if not rt_filter or rt_filter in ("configmap", "cm"):
                cms = (_core.list_namespaced_config_map(namespace).items if namespace
                       else _core.list_config_map_for_all_namespaces().items)
                r.append(("ConfigMap", cms,
                           lambda cm: f"{len(cm.data or {})} key(s)"))

            if not rt_filter or rt_filter == "secret":
                secs = (_core.list_namespaced_secret(namespace).items if namespace
                        else _core.list_secret_for_all_namespaces().items)
                r.append(("Secret", secs,
                           lambda sec: sec.type or "Opaque"))

            return r

        def _search(resources, force: bool = False) -> list[str]:
            results = []
            for kind, items, details_fn in resources:
                for item in items:
                    if force or not name_substring or name_substring.lower() in item.metadata.name.lower():
                        results.append(
                            f"| {kind} | {item.metadata.namespace or '-'} "
                            f"| {item.metadata.name} | {details_fn(item)} |"
                        )
            return results

        resources = _build_resources(resource_type)
        results   = _search(resources)

        if not results and name_substring and resource_type:
            all_resources = _build_resources(None)
            results       = _search(all_resources)
            if results:
                note = (f"Searched all resource types for `{name_substring}`:")
                lines = [note,
                         "| Resource Type | Namespace | Name | Status/Details |",
                         "|---|---|---|---|"]
                return "\n".join(lines + results)

        if not results and name_substring:
            all_resources = _build_resources(None)
            results       = _search(all_resources, force=True)
            fallback_note = (f"No resources found matching `{name_substring}`. "
                             f"Showing all resources instead.")
            lines = [fallback_note,
                     "| Resource Type | Namespace | Name | Status/Details |",
                     "|---|---|---|---|"]
            return "\n".join(lines + results)

        scope       = f"namespace `{namespace}`" if namespace else "all namespaces"
        filter_note = f" matching `{name_substring}`" if name_substring else ""
        header      = f"Showing resources{filter_note} in {scope}."
        lines       = [header,
                       "| Resource Type | Namespace | Name | Status/Details |",
                       "|---|---|---|---|"]
        return "\n".join(lines + results)

    except Exception as e:
        return f"Unexpected error: {e}"

_SYSTEM_NAMESPACES = ("kube-system", "coredns", "longhorn-system", "ingress-nginx")
_MAX_DETAIL_ITEMS  = 5

def _cap(items: list, max_n: int = _MAX_DETAIL_ITEMS) -> str:
    """Render up to max_n items inline; suffix with (+N more) if list is longer."""
    shown = items[:max_n]
    rest  = len(items) - max_n
    s     = ", ".join(f"`{x}`" for x in shown)
    return s + (f" (+{rest} more)" if rest > 0 else "")

def run_cluster_health() -> str:
    """
    Scorecard-style cluster health report. One line per check — only failures
    get extra detail. Designed to be token-efficient while still giving Claude
    enough signal to answer 'is my cluster ok?' in one sentence.
    """
    from kubernetes.stream import stream as _k8s_stream

    now_str  = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out      = ["```", f"Cluster Health Report — {now_str}", ""]
    criticals: list[str] = []
    warnings:  list[str] = []
    healthy:   list[str] = []

    out.append("── Nodes " + "─" * 52)
    try:
        nodes = _core.list_node().items
        node_issues = False
        for node in sorted(nodes, key=lambda n: n.metadata.name):
            conds  = {c.type: c.status for c in (node.status.conditions or [])}
            alloc  = node.status.allocatable or {}
            cap    = node.status.capacity    or {}

            ready  = conds.get("Ready") == "True"
            status = "✅ Ready" if ready else "🔴 NotReady"

            cpu_alloc     = _parse_cpu_cores(alloc.get("cpu", 0))
            pods_on_node  = _list_pods_by_node(node.metadata.name)
            cpu_req       = sum(
                _parse_cpu_cores((c.resources.requests or {}).get("cpu", 0))
                for p in pods_on_node
                for c in (p.spec.containers or [])
                if c.resources
            )
            cpu_used_pct  = round((cpu_req / cpu_alloc) * 100) if cpu_alloc > 0 else 0
            cpu_str       = (f"{round(cpu_req, 2)}C / {round(cpu_alloc, 2)}C"
                             f" ({cpu_used_pct}% used)")

            mem_alloc_gib = _parse_mem_gib(alloc.get("memory", "0Ki"))
            mem_req_gib   = sum(
                _parse_mem_gib((c.resources.requests or {}).get("memory", "0"))
                for p in pods_on_node
                for c in (p.spec.containers or [])
                if c.resources
            )
            mem_used_pct  = round((mem_req_gib / mem_alloc_gib) * 100) if mem_alloc_gib > 0 else 0
            mem_str       = (f"{round(mem_req_gib, 1)}Gi / {round(mem_alloc_gib, 1)}Gi"
                             f" ({mem_used_pct}% used)")

            pressure = []
            if conds.get("MemoryPressure") == "True": pressure.append("MemPressure")
            if conds.get("DiskPressure")   == "True": pressure.append("DiskPressure")
            if conds.get("PIDPressure")    == "True": pressure.append("PIDPressure")
            pressure_str = f" ⚠️  {', '.join(pressure)}" if pressure else ""

            taints    = node.spec.taints or []
            taint_str = ", ".join(
                f"{t.key}{'=' + t.value if t.value else ''}:{t.effect}" for t in taints
            ) if taints else "<none>"

            out.append(
                f"  🖥️  {node.metadata.name}"
                f" | {status}{pressure_str}"
                f" | CPU: {cpu_str}"
                f" | RAM: {mem_str}"
                f" | Taints: {taint_str}"
            )

            if not ready:
                criticals.append(f"Node `{node.metadata.name}` is NotReady")
                node_issues = True
            if pressure:
                warnings.append(f"Node `{node.metadata.name}` has {', '.join(pressure)}")
                node_issues = True

        if not node_issues:
            healthy.append("Nodes")

    except Exception as e:
        out.append(f"⚠️  Could not check nodes: {e}")
        warnings.append("Node check failed")

    out.append("\n── System Pods " + "─" * 45)
    try:
        sys_pod_issues = False
        for ns in _SYSTEM_NAMESPACES:
            try:
                pods = _core.list_namespaced_pod(namespace=ns, limit=500).items
                if not pods:
                    continue
                unhealthy_pods = [
                    p for p in pods
                    if sum(1 for cs in (p.status.container_statuses or []) if cs.ready)
                    < len(p.spec.containers or [])
                ]
                total = len(pods)
                if unhealthy_pods:
                    out.append(f"⚠️  {ns}: {len(unhealthy_pods)}/{total} pods not ready")
                    for p in unhealthy_pods:
                        ready   = sum(1 for cs in (p.status.container_statuses or []) if cs.ready)
                        tot_c   = len(p.spec.containers or [])
                        restarts = sum(cs.restart_count for cs in (p.status.container_statuses or []))
                        reason  = ""
                        for cs in (p.status.container_statuses or []):
                            if cs.state and cs.state.waiting and cs.state.waiting.reason:
                                reason = cs.state.waiting.reason; break
                            if cs.state and cs.state.terminated and cs.state.terminated.reason:
                                reason = cs.state.terminated.reason; break
                        reason_str = f" ({reason})" if reason else ""
                        out.append(
                            f"    🔴 Namespace: {ns} | Pod: {p.metadata.name}"
                            f" | Ready: {ready}/{tot_c}"
                            f" | Restarts: {restarts}{reason_str}"
                        )
                    warnings.append(f"{ns}: {len(unhealthy_pods)} pod(s) not ready")
                    sys_pod_issues = True
                else:
                    out.append(f"✅ {ns}: {total}/{total} pods ready")
            except Exception:
                pass
        if not sys_pod_issues:
            healthy.append("System Pods")
    except Exception as e:
        out.append(f"⚠️  Could not check system pods: {e}")
        warnings.append("System pod check failed")

    out.append("\n── Workloads " + "─" * 47)
    try:
        workload_issues = False

        deps    = _apps.list_deployment_for_all_namespaces().items
        dep_bad = [f"{d.metadata.namespace}/{d.metadata.name}" for d in deps
                   if (d.spec.replicas or 0) > 0
                   and (d.status.ready_replicas or 0) < (d.spec.replicas or 0)]
        if dep_bad:
            out.append(f"⚠️  {len(dep_bad)}/{len(deps)} Deployments degraded — {_cap(dep_bad)}")
            warnings.append(f"{len(dep_bad)} Deployment(s) degraded: {', '.join(dep_bad)}")
            workload_issues = True
        else:
            out.append(f"✅ {len(deps)} Deployments healthy")

        dss    = _apps.list_daemon_set_for_all_namespaces().items
        ds_bad = [f"{d.metadata.namespace}/{d.metadata.name}" for d in dss
                  if (d.status.desired_number_scheduled or 0) > 0
                  and (d.status.number_ready or 0) < (d.status.desired_number_scheduled or 0)]
        if ds_bad:
            out.append(f"⚠️  {len(ds_bad)}/{len(dss)} DaemonSets degraded — {_cap(ds_bad)}")
            warnings.append(f"{len(ds_bad)} DaemonSet(s) degraded: {', '.join(ds_bad)}")
            workload_issues = True
        else:
            out.append(f"✅ {len(dss)} DaemonSets healthy")

        stss    = _apps.list_stateful_set_for_all_namespaces().items
        sts_bad = [f"{s.metadata.namespace}/{s.metadata.name}" for s in stss
                   if (s.spec.replicas or 0) > 0
                   and (s.status.ready_replicas or 0) < (s.spec.replicas or 0)]
        if sts_bad:
            out.append(f"⚠️  {len(sts_bad)}/{len(stss)} StatefulSets degraded — {_cap(sts_bad)}")
            warnings.append(f"{len(sts_bad)} StatefulSet(s) degraded: {', '.join(sts_bad)}")
            workload_issues = True
        else:
            out.append(f"✅ {len(stss)} StatefulSets healthy")

        failed_pods = []
        for phase in _NOT_RUNNING_PHASES:
            try:
                result = _core.list_pod_for_all_namespaces(
                    field_selector=f"status.phase={phase}").items
                failed_pods.extend(result)
            except Exception:
                pass
        if failed_pods:
            out.append(f"🔴 {len(failed_pods)} pod(s) not running:")
            for p in failed_pods:
                phase    = p.status.phase or "Unknown"
                restarts = sum(cs.restart_count for cs in (p.status.container_statuses or []))
                reason   = ""
                for cs in (p.status.container_statuses or []):
                    if cs.state and cs.state.waiting and cs.state.waiting.reason:
                        reason = cs.state.waiting.reason; break
                    if cs.state and cs.state.terminated and cs.state.terminated.reason:
                        reason = cs.state.terminated.reason; break
                reason_str = f" ({reason})" if reason else ""
                out.append(
                    f"    🔴 Namespace: {p.metadata.namespace} | Pod: {p.metadata.name}"
                    f" | Phase: {phase} | Restarts: {restarts}{reason_str}"
                )
            pod_labels = [f"{p.metadata.namespace}/{p.metadata.name}" for p in failed_pods]
            criticals.append(f"{len(failed_pods)} pod(s) not running: {', '.join(pod_labels[:5])}"
                             + (f" (+{len(failed_pods)-5} more)" if len(failed_pods) > 5 else ""))
            workload_issues = True
        else:
            out.append("✅ No failed/pending pods")

        if not workload_issues:
            healthy.append("Workloads")

    except Exception as e:
        out.append(f"⚠️  Could not check workloads: {e}")
        warnings.append("Workload check failed")

    out.append("\n── Storage " + "─" * 49)
    storage_issues = False
    try:
        pvcs    = _core.list_persistent_volume_claim_for_all_namespaces().items
        bound   = [p for p in pvcs if p.status.phase == "Bound"]
        unbound = [f"{p.metadata.namespace}/{p.metadata.name}"
                   for p in pvcs if p.status.phase != "Bound"]
        if unbound:
            out.append(f"🔴 {len(unbound)}/{len(pvcs)} PVCs not Bound — {_cap(unbound)}")
            criticals.append(f"{len(unbound)} PVC(s) not Bound: {', '.join(unbound[:5])}"
                             + (f" (+{len(unbound)-5} more)" if len(unbound) > 5 else ""))
            storage_issues = True
        else:
            out.append(f"✅ {len(bound)}/{len(pvcs)} PVCs Bound")
    except Exception as e:
        out.append(f"⚠️  Could not check PVCs: {e}")
        warnings.append("PVC check failed")
        storage_issues = True

    try:
        def _exec_df_quick(pod_name, ns, mount_path, container=None):
            try:
                kwargs = dict(
                    command=["/bin/sh", "-c",
                             f"df -k --output=used,avail {mount_path} 2>/dev/null | tail -1"],
                    stderr=False, stdin=False, stdout=True, tty=False, _preload_content=True)
                if container:
                    kwargs["container"] = container
                resp = _k8s_stream(
                    _core.connect_get_namespaced_pod_exec, pod_name, ns, **kwargs)
                return resp.strip() if isinstance(resp, str) else ""
            except Exception:
                return ""

        pv_over = []
        all_pvcs = _core.list_persistent_volume_claim_for_all_namespaces().items
        for pvc in all_pvcs:
            ns, pvc_name = pvc.metadata.namespace, pvc.metadata.name
            pod_hit = pod_container = mount_path = None
            try:
                for pod in _core.list_namespaced_pod(namespace=ns).items:
                    if pod.status.phase != "Running":
                        continue
                    for vol in (pod.spec.volumes or []):
                        if (vol.persistent_volume_claim and
                                vol.persistent_volume_claim.claim_name == pvc_name):
                            for container in (pod.spec.containers or []):
                                for vm in (container.volume_mounts or []):
                                    if vm.name == vol.name:
                                        pod_hit       = pod.metadata.name
                                        pod_container = container.name
                                        mount_path    = vm.mount_path
                                        break
                                if pod_hit: break
                        if pod_hit: break
            except Exception:
                pass

            if not pod_hit:
                continue

            df_out = _exec_df_quick(pod_hit, ns, mount_path, container=pod_container)
            if not df_out:
                continue
            parts = df_out.split()
            if len(parts) < 2:
                continue
            try:
                used_kb  = int(parts[0])
                avail_kb = int(parts[1])
                total_kb = used_kb + avail_kb
                pct      = round(used_kb / total_kb * 100, 1) if total_kb > 0 else 0.0
            except (ValueError, ZeroDivisionError):
                continue

            if pct >= 80:
                used_gib  = round(used_kb  / (1024 * 1024), 2)
                total_gib = round(total_kb / (1024 * 1024), 2)
                flag      = "🔴" if pct >= 90 else "🟠"
                pv_over.append((pct, flag, ns, pvc_name, used_gib, total_gib))

        if pv_over:
            pv_over.sort(key=lambda x: x[0], reverse=True)
            out.append(f"{'🔴' if any(p[0] >= 90 for p in pv_over) else '🟠'} "
                       f"{len(pv_over)} PV(s) above 80% capacity:")
            for pct, flag, ns, pvc_name, used_gib, total_gib in pv_over:
                out.append(
                    f"    {flag} Namespace: {ns} | PVC: {pvc_name}"
                    f" | Usage: {pct}% ({used_gib}Gi / {total_gib}Gi)"
                )
            if any(p[0] >= 90 for p in pv_over):
                criticals.append(
                    f"{sum(1 for p in pv_over if p[0] >= 90)} PV(s) above 90% capacity")
            warnings.append(f"{len(pv_over)} PV(s) above 80% capacity")
            storage_issues = True
        else:
            out.append("✅ All PV disk usage within capacity (< 80%)")

    except Exception as e:
        out.append(f"⚠️  Could not check PV usage: {e}")
        warnings.append("PV usage check failed")
        storage_issues = True

    if not storage_issues:
        healthy.append("Storage")

    out.append("\n── Recent Warning Events " + "─" * 36)
    try:
        events = _core.list_event_for_all_namespaces(
            field_selector="type=Warning", limit=200).items
        seen: dict = {}
        for e in events:
            if _is_noisy_event(e.message or ""):
                continue
            key  = (e.reason or "", getattr(e.involved_object, "name", ""))
            prev = seen.get(key)
            if prev is None or (e.count or 1) > (prev[0] or 1):
                seen[key] = (e.count or 1, e.metadata.namespace,
                             e.reason, key[1], e.message or "")
        top = sorted(seen.values(), key=lambda x: x[0], reverse=True)[:8]
        if top:
            for count, ns, reason, obj, msg in top:
                short_msg = msg[:60] + "…" if len(msg) > 60 else msg
                out.append(f"⚠️  {reason:<22} {ns}/{obj}  {short_msg}  (x{count})")
            warnings.append(f"{len(top)} recent warning event type(s)")
        else:
            out.append("✅ No recent warning events")
            healthy.append("Events")
    except Exception as e:
        out.append(f"⚠️  Could not fetch events: {e}")
        warnings.append("Event check failed")

    out.append("\n── Summary " + "─" * 49)
    if criticals:
        out.append(f"🔴 {len(criticals)} critical issue(s):")
        for item in criticals:
            out.append(f"   • {item}")
    if warnings:
        out.append(f"🟠 {len(warnings)} warning(s):")
        for item in warnings:
            out.append(f"   • {item}")
    if healthy:
        out.append(f"✅ Healthy: {', '.join(healthy)}")
    if not criticals and not warnings:
        out.append("✅ Cluster is healthy — all checks passed")

    out.append("")
    out.append("── Next Action Plan " + "─" * 41)
    out.append("💬 Continue asking the chatbot to investigate any specific area reported above.")
    out.append("📋 For a complete and comprehensive health check report, click `Healthcheck Report` via ⚙ Settings.")
    out.append("🤖 Check out ECS Knowledge Bot to find out about known issues, Dos and Don'ts, and best practices.")
    out.append("```")

    return "\n".join(out)

def _report_err(e) -> str:
    """Concise, single-line error string safe to embed in HTML report.
    Strips the verbose HTTP headers/body that kubernetes-client WebSocket
    errors include in str(e) (e.g. 'Handshake status 200 OK -+-+- {...}').
    """
    s = str(e)
    if 'Handshake status' in s or '-+-+-' in s or len(s) > 160:
        first = s.split('\n')[0].split('-+-+')[0].strip().rstrip('-').strip()
        return first[:120] or 'Kubernetes API error (connection issue)'
    return s[:160]


def generate_healthcheck_report() -> str:
    """Full, multi-section cluster health report — emits pure HTML for weasyprint."""
    import html as _html

    now_str  = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    _MAX_ROWS = 20

    def esc(s):
        return _html.escape(str(s)) if s is not None else ""

    # SVG icon helpers — zero font dependency, renders perfectly in weasyprint air-gapped
    _SVG_OK   = ('<svg width="11" height="11" viewBox="0 0 11 11" xmlns="http://www.w3.org/2000/svg" style="vertical-align:middle;margin-right:3px">'
                 '<circle cx="5.5" cy="5.5" r="5" fill="#16a34a"/>'
                 '<polyline points="2.5,5.5 4.5,7.5 8.5,3.5" fill="none" stroke="#fff" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>'
                 '</svg>')
    _SVG_WARN = ('<svg width="11" height="11" viewBox="0 0 11 11" xmlns="http://www.w3.org/2000/svg" style="vertical-align:middle;margin-right:3px">'
                 '<polygon points="5.5,1 10.5,10 0.5,10" fill="#d97706"/>'
                 '<rect x="5" y="4" width="1" height="3.5" fill="#fff"/>'
                 '<circle cx="5.5" cy="8.8" r="0.7" fill="#fff"/>'
                 '</svg>')
    _SVG_ERR  = ('<svg width="11" height="11" viewBox="0 0 11 11" xmlns="http://www.w3.org/2000/svg" style="vertical-align:middle;margin-right:3px">'
                 '<circle cx="5.5" cy="5.5" r="5" fill="#dc2626"/>'
                 '<line x1="3" y1="3" x2="8" y2="8" stroke="#fff" stroke-width="1.5" stroke-linecap="round"/>'
                 '<line x1="8" y1="3" x2="3" y2="8" stroke="#fff" stroke-width="1.5" stroke-linecap="round"/>'
                 '</svg>')

    def section(title):
        return f'<h2>{esc(title)}</h2>'

    def ok(msg):
        return f'<p class="ok">{_SVG_OK}{esc(msg)}</p>'

    def warn(msg):
        return f'<p class="warn">{_SVG_WARN}{esc(msg)}</p>'

    def err(msg):
        return f'<p class="err">{_SVG_ERR}{esc(msg)}</p>'

    def info(msg):
        return f'<p>{esc(msg)}</p>'

    def table(headers, rows, cap=_MAX_ROWS):
        if not rows:
            return ""
        ths = "".join(f"<th>{esc(h)}</th>" for h in headers)
        trs = ""
        for row in rows[:cap]:
            trs += "<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>"
        overflow = len(rows) - cap
        t = f'<table><thead><tr>{ths}</tr></thead><tbody>{trs}</tbody></table>'
        if overflow > 0:
            t += f'<p class="note"><em>… and {overflow} more row(s) not shown.</em></p>'
        return t

    def subsection(title):
        return f'<h3>{esc(title)}</h3>'

    R = []   # HTML fragments

    # ── Header ────────────────────────────────────────────────────────────
    R.append(f'<p class="note">Generated: {esc(now_str)}</p>')

    # Get k8s version for use in node section
    k8s_version = "unknown"
    try:
        k8s_version = _version_api.get_code().git_version
    except Exception:
        pass

    # ── 1. Node Infrastructure ────────────────────────────────────────────
    R.append(section("1. Node Infrastructure"))
    try:
        nodes = _core.list_node().items
        if not nodes:
            R.append(warn("No nodes found."))
        else:
            R.append(subsection(f"Nodes ({len(nodes)} total)"))
            R.append(f'<p><strong>Kubernetes Version:</strong> {esc(k8s_version)}</p>')
            rows = []
            for node in sorted(nodes, key=lambda n: n.metadata.name):
                roles  = (",".join(k.split("/")[-1] for k in (node.metadata.labels or {})
                                   if "node-role.kubernetes.io" in k) or "worker")
                conds  = {c.type: c.status for c in (node.status.conditions or [])}
                status = ('<span style="color:#16a34a;font-weight:600">&#10003; Ready</span>'
                          if conds.get("Ready") == "True" else
                          '<span style="color:#dc2626;font-weight:600">&#10007; NotReady</span>')
                alloc  = node.status.allocatable or {}
                cpu    = alloc.get("cpu", "?")
                mem_ki = alloc.get("memory", "0Ki")
                try:
                    mem_gib = f"{round(int(mem_ki.rstrip('Ki')) / (1024*1024), 1)} GiB"
                except Exception:
                    mem_gib = mem_ki
                gpu    = next((alloc[k] for k in alloc
                               if "nvidia.com/gpu" in k or "amd.com/gpu" in k), "-")
                flags  = []
                if conds.get("MemoryPressure") == "True": flags.append("MemPressure")
                if conds.get("DiskPressure")   == "True": flags.append("DiskPressure")
                status_str = status + (f' <span style="color:#d97706">[{",".join(flags)}]</span>' if flags else "")
                rows.append([esc(node.metadata.name), esc(roles),
                              status_str, esc(cpu), esc(mem_gib), esc(gpu)])
            R.append(table(["NAME","ROLES","STATUS","CPU","MEMORY","GPU"], rows))

            R.append(subsection("Node Capacity (Allocatable vs Requested)"))
            # Pre-fetch all pods once — avoids per-node field_selector calls that
            # trigger WebSocket errors on RKE2 clusters.
            try:
                _all_pods_for_capacity = _core.list_pod_for_all_namespaces().items
            except Exception:
                _all_pods_for_capacity = []
            rows2 = []
            for node in sorted(nodes, key=lambda n: n.metadata.name):
                alloc     = node.status.allocatable or {}
                cpu_alloc = _parse_cpu_cores(alloc.get("cpu", 0))
                mem_alloc = _parse_mem_gib(alloc.get("memory", "0Ki"))
                pods_on   = [p for p in _all_pods_for_capacity
                             if (p.spec.node_name or "") == node.metadata.name]
                cpu_req   = sum(_parse_cpu_cores((c.resources.requests or {}).get("cpu", 0))
                                for p in pods_on for c in (p.spec.containers or []) if c.resources)
                mem_req   = sum(_parse_mem_gib((c.resources.requests or {}).get("memory", "0"))
                                for p in pods_on for c in (p.spec.containers or []) if c.resources)
                pct = lambda a, b: f"({round(a/b*100,1)}%)" if b > 0 else "(0%)"
                rows2.append([
                    esc(node.metadata.name),
                    str(round(cpu_alloc,2)),
                    f"{round(cpu_req,2)} {pct(cpu_req,cpu_alloc)}",
                    f"{round(cpu_alloc-cpu_req,2)} {pct(cpu_alloc-cpu_req,cpu_alloc)}",
                    str(round(mem_alloc,2)),
                    f"{round(mem_req,2)} {pct(mem_req,mem_alloc)}",
                    f"{round(mem_alloc-mem_req,2)} {pct(mem_alloc-mem_req,mem_alloc)}",
                ])
            R.append(table(["NODE","CPU ALLOC","CPU REQ","CPU AVAIL",
                             "RAM ALLOC (Gi)","RAM REQ (Gi)","RAM AVAIL (Gi)"], rows2))

            tainted = [(n.metadata.name, t) for n in nodes for t in (n.spec.taints or [])]
            if tainted:
                R.append(subsection("Node Taints"))
                R.append(table(["NODE","KEY","VALUE","EFFECT"],
                    [[esc(nm), esc(t.key or "<any>"), esc(t.value or "-"), esc(t.effect or "-")]
                     for nm, t in tainted]))
            else:
                R.append(ok("No node taints defined."))

            gpu_nodes = [n for n in nodes if any("gpu" in k.lower()
                         for k in (n.status.allocatable or {}))]
            if gpu_nodes:
                R.append(subsection("GPU Nodes"))
                rows4 = []
                for node in gpu_nodes:
                    labels  = node.metadata.labels or {}
                    alloc   = node.status.allocatable or {}
                    gpu_key = next((k for k in alloc if "gpu" in k.lower()), None)
                    gpu_alloc = alloc.get(gpu_key, "0") if gpu_key else "0"
                    product = labels.get("gpu.product", "Unknown")
                    count   = labels.get("gpu.count", gpu_alloc)
                    memory  = labels.get("gpu.memory", "n/a")
                    rows4.append([esc(node.metadata.name), esc(product),
                                  esc(count), f"{esc(memory)}Mi", esc(gpu_alloc)])
                R.append(table(["NODE","PRODUCT","COUNT","VRAM","ALLOCATABLE"], rows4))
            else:
                R.append(ok("No GPU nodes detected."))
    except Exception as e:
        R.append(warn(f"Could not gather node data: {_report_err(e)}"))

    # ── 3. Resource Capacity ──────────────────────────────────────────────
    R.append(section("2. Resource Capacity"))
    try:
        all_ns = [ns.metadata.name for ns in _core.list_namespace().items]
        ns_data = []
        for ns in all_ns:
            try:
                pods = _core.list_namespaced_pod(namespace=ns, limit=1000).items
                cpu_req = mem_req = cpu_lim = mem_lim = 0
                for pod in pods:
                    for c in list(pod.spec.containers or []) + list(pod.spec.init_containers or []):
                        req = (c.resources.requests or {}) if c.resources else {}
                        lim = (c.resources.limits   or {}) if c.resources else {}
                        cpu_req += _parse_cpu_to_millicores(req.get("cpu",    "0"))
                        mem_req += _parse_mem_to_mib(req.get("memory", "0"))
                        cpu_lim += _parse_cpu_to_millicores(lim.get("cpu",    "0"))
                        mem_lim += _parse_mem_to_mib(lim.get("memory", "0"))
                if pods:
                    ns_data.append((ns, len(pods), cpu_req, mem_req, cpu_lim, mem_lim))
            except Exception:
                pass
        ns_data.sort(key=lambda x: x[2], reverse=True)
        R.append(subsection("Namespace Resource Requests vs Limits (top by CPU request)"))
        rows = []
        for ns, pod_count, cpu_req, mem_req, cpu_lim, mem_lim in ns_data:
            rows.append([esc(ns), str(pod_count),
                         f"{cpu_req}m", f"{mem_req:.0f}Mi",
                         f"{cpu_lim}m" if cpu_lim else "none",
                         f"{mem_lim:.0f}Mi"])
        R.append(table(["NAMESPACE","PODS","CPU REQ","MEM REQ","CPU LIM","MEM LIM"], rows))
    except Exception as e:
        R.append(warn(f"Could not gather resource data: {_report_err(e)}"))

    try:
        q_items = _core.list_resource_quota_for_all_namespaces().items
        if q_items:
            R.append(subsection("Resource Quotas"))
            rows = []
            for q in sorted(q_items, key=lambda x: (x.metadata.namespace, x.metadata.name)):
                hard = q.status.hard or {}
                used = q.status.used or {}
                for res in sorted(hard.keys()):
                    hard_val = hard.get(res, "0")
                    used_val = used.get(res, "0")
                    try:
                        if "cpu" in res:
                            pct = round(_parse_cpu_to_millicores(str(used_val)) /
                                        max(_parse_cpu_to_millicores(str(hard_val)), 1) * 100, 1)
                        elif "memory" in res:
                            pct = round(_parse_mem_to_mib(str(used_val)) /
                                        max(_parse_mem_to_mib(str(hard_val)), 1) * 100, 1)
                        else:
                            pct = round(int(str(used_val).split(".")[0]) /
                                        max(int(str(hard_val).split(".")[0]), 1) * 100, 1)
                        icon = ('<span style="color:#dc2626;font-weight:700">CRIT</span>' if pct >= 90
                                else ('<span style="color:#d97706;font-weight:700">WARN</span>' if pct >= 80
                                      else '<span style="color:#16a34a">OK</span>'))
                        pct_str = f"{icon} {pct}%"
                    except Exception:
                        pct_str = "-"
                    rows.append([esc(q.metadata.namespace), esc(q.metadata.name),
                                  esc(res), esc(str(used_val)), esc(str(hard_val)), pct_str])
            R.append(table(["NAMESPACE","QUOTA","RESOURCE","USED","HARD","%"], rows))
        else:
            R.append(ok("No ResourceQuotas defined."))
    except Exception as e:
        R.append(warn(f"Could not gather quota data: {_report_err(e)}"))

    try:
        lr_items = _core.list_limit_range_for_all_namespaces().items
        if lr_items:
            R.append(subsection("LimitRanges"))
            rows = []
            def _g(v, key): return (v or {}).get(key, "-")
            for lr in sorted(lr_items, key=lambda x: (x.metadata.namespace, x.metadata.name)):
                for item in (lr.spec.limits or []):
                    rows.append([esc(lr.metadata.namespace), esc(lr.metadata.name),
                                  esc(item.type), esc(_g(item.default,"cpu")),
                                  esc(_g(item.default,"memory")),
                                  esc(_g(item.max,"cpu")), esc(_g(item.max,"memory"))])
            R.append(table(["NAMESPACE","LIMITRANGE","TYPE","CPU DEFAULT",
                             "MEM DEFAULT","CPU MAX","MEM MAX"], rows))
        else:
            R.append(ok("No LimitRanges defined."))
    except Exception as e:
        R.append(warn(f"Could not gather LimitRange data: {_report_err(e)}"))

    # ── 4. Storage Health ─────────────────────────────────────────────────
    R.append(section("3. Storage Health"))
    try:
        scs = _storage.list_storage_class().items
        if scs:
            R.append(subsection("Storage Classes"))
            rows = []
            for sc in scs:
                is_default = (sc.metadata.annotations or {}).get(
                    "storageclass.kubernetes.io/is-default-class") == "true"
                rows.append([esc(sc.metadata.name), esc(sc.provisioner),
                              esc(sc.reclaim_policy or "Delete"),
                              "Yes" if sc.allow_volume_expansion else "No",
                              "★ Default" if is_default else "-"])
            R.append(table(["NAME","PROVISIONER","RECLAIM POLICY","EXPANSION","DEFAULT"], rows))
        else:
            R.append(ok("No StorageClasses defined."))
    except Exception as e:
        R.append(warn(f"Could not gather StorageClass data: {_report_err(e)}"))

    try:
        pvcs    = _core.list_persistent_volume_claim_for_all_namespaces().items
        bound   = [p for p in pvcs if p.status.phase == "Bound"]
        unbound = [p for p in pvcs if p.status.phase != "Bound"]
        if unbound:
            R.append(warn(f"{len(unbound)} PVC(s) not Bound / {len(bound)} Bound / {len(pvcs)} total"))
            rows = []
            for p in unbound:
                rows.append([esc(p.metadata.namespace), esc(p.metadata.name),
                              esc(p.status.phase or "Unknown"),
                              esc(p.spec.storage_class_name or "-"),
                              esc((p.status.capacity or {}).get("storage","?"))])
            R.append(table(["NAMESPACE","PVC","PHASE","CLASS","CAPACITY"], rows))
        else:
            R.append(ok(f"All {len(pvcs)} PVCs Bound — 0 not Bound."))
    except Exception as e:
        R.append(warn(f"Could not gather PVC data: {_report_err(e)}"))

    try:
        R.append(subsection("PV Disk Usage"))
        pv_data = _get_pv_usage_data(threshold=80)
        if pv_data:
            def _circle(color):
                return (f'<svg width="14" height="14" viewBox="0 0 14 14" xmlns="http://www.w3.org/2000/svg">' +
                        f'<circle cx="7" cy="7" r="6" fill="{color}"/></svg>')

            def _pv_table(rows, heading, hdr_cls):
                _warn_icon = ('<svg width="12" height="12" viewBox="0 0 12 12" xmlns="http://www.w3.org/2000/svg" style="vertical-align:middle;flex-shrink:0">'
                              '<polygon points="6,1 11.5,11 0.5,11" fill="#d97706"/>'
                              '<rect x="5.4" y="4.5" width="1.2" height="3.5" fill="#fff"/>'
                              '<circle cx="6" cy="9.5" r="0.8" fill="#fff"/>'
                              '</svg>')
                _ok_icon   = ('<svg width="12" height="12" viewBox="0 0 12 12" xmlns="http://www.w3.org/2000/svg" style="vertical-align:middle;flex-shrink:0">'
                              '<circle cx="6" cy="6" r="5.5" fill="#16a34a"/>'
                              '<polyline points="3,6 5,8 9,4" fill="none" stroke="#fff" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>'
                              '</svg>')
                icon = _warn_icon if hdr_cls == "pv-hdr-warn" else _ok_icon
                trs = ""
                for d in rows:
                    pct = d["pct_val"]
                    if pct >= 90:   fc = "#dc2626"
                    elif pct >= 80: fc = "#d97706"
                    else:           fc = "#16a34a"
                    usage_color = "#dc2626" if pct >= 90 else ("#d97706" if pct >= 80 else "#0369a1")
                    trs += (f'<tr>'
                            f'<td class="flag-cell">{_circle(fc)}</td>'
                            f'<td><code>{esc(d["ns"])}/{esc(d["pvc_name"])}</code></td>'
                            f'<td style="color:{usage_color};font-weight:700;white-space:nowrap">{pct}%</td>'
                            f'<td style="white-space:nowrap">{d["used_gib"]}Gi ({pct}%)</td>'
                            f'<td style="white-space:nowrap">{d["total_gib"]}Gi</td>'
                            f'<td style="white-space:nowrap">{d["avail_gib"]}Gi ({d["free_pct"]}%)</td>'
                            f'</tr>')
                col_hdr = ('<tr><th style="width:28px"></th>'
                           '<th>Namespace / PVC</th><th>Usage</th>'
                           '<th>Used (GiB)</th><th>Total (GiB)</th><th>Free (GiB)</th></tr>')
                return (f'<div class="pv-hdr {hdr_cls}">{icon}{esc(heading)} ({len(rows)} PVC{"s" if len(rows)!=1 else ""})</div>'
                        f'<div class="pv-table">'
                        f'<table><thead>{col_hdr}</thead><tbody>{trs}</tbody></table>'
                        f'</div>')

            critical = [d for d in pv_data if d["pct_val"] >= 80]
            healthy  = [d for d in pv_data if d["pct_val"] <  80]
            if critical:
                R.append(_pv_table(critical, "Nearing or exceeding 80% capacity", "pv-hdr-warn"))
            if healthy:
                R.append(_pv_table(healthy[:5], "Within capacity", "pv-hdr-ok"))
                if len(healthy) > 5:
                    R.append(f'<p class="note"><em>({len(healthy)-5} more healthy volumes not shown)</em></p>')
        else:
            R.append(ok("No volume usage data available."))
    except Exception as e:
        R.append(warn(f"Could not gather PV usage data: {_report_err(e)}"))

    # ── 5. Workload Health ────────────────────────────────────────────────
    R.append(section("4. Workload Health"))

    def _workload_section(kind, items, desired_fn, ready_fn, avail_fn):
        if not items:
            R.append(ok(f"No {kind}s found."))
            return
        degraded = [i for i in items
                    if (desired_fn(i) or 0) > 0 and (ready_fn(i) or 0) < (desired_fn(i) or 0)]
        if not degraded:
            R.append(ok(f"{len(items)} {kind}(s) — all healthy."))
            return
        R.append(warn(f"{len(degraded)}/{len(items)} {kind}(s) degraded:"))
        rows = [[esc(i.metadata.namespace), esc(i.metadata.name),
                 str(desired_fn(i) or 0), str(ready_fn(i) or 0), str(avail_fn(i) or 0)]
                for i in degraded]
        R.append(table(["NAMESPACE","NAME","DESIRED","READY","AVAILABLE"], rows))

    try:
        _workload_section("Deployment", _apps.list_deployment_for_all_namespaces().items,
                          lambda d: d.spec.replicas or 0,
                          lambda d: d.status.ready_replicas or 0,
                          lambda d: d.status.available_replicas or 0)
    except Exception as e:
        R.append(warn(f"Deployments: {_report_err(e)}"))

    try:
        _workload_section("DaemonSet", _apps.list_daemon_set_for_all_namespaces().items,
                          lambda d: d.status.desired_number_scheduled or 0,
                          lambda d: d.status.number_ready or 0,
                          lambda d: d.status.number_available or 0)
    except Exception as e:
        R.append(warn(f"DaemonSets: {_report_err(e)}"))

    try:
        _workload_section("StatefulSet", _apps.list_stateful_set_for_all_namespaces().items,
                          lambda s: s.spec.replicas or 0,
                          lambda s: s.status.ready_replicas or 0,
                          lambda s: getattr(s.status,"available_replicas",None) or 0)
    except Exception as e:
        R.append(warn(f"StatefulSets: {_report_err(e)}"))

    try:
        rss = _apps.list_replica_set_for_all_namespaces().items
        orphaned = [r for r in rss
                    if (r.spec.replicas or 0) > 0
                    and (r.status.ready_replicas or 0) < (r.spec.replicas or 0)
                    and not r.metadata.owner_references]
        if orphaned:
            R.append(warn(f"{len(orphaned)} orphaned/degraded ReplicaSet(s):"))
            rows = [[esc(r.metadata.namespace), esc(r.metadata.name),
                     str(r.spec.replicas or 0), str(r.status.ready_replicas or 0)]
                    for r in orphaned]
            R.append(table(["NAMESPACE","NAME","DESIRED","READY"], rows))
        else:
            R.append(ok("No orphaned/degraded ReplicaSets."))
    except Exception as e:
        R.append(warn(f"ReplicaSets: {_report_err(e)}"))

    try:
        jobs = _batch.list_job_for_all_namespaces().items
        failed_jobs = [j for j in jobs
                       if j.status.failed and (j.status.failed or 0) > 0
                       and not (j.status.completion_time)]
        if failed_jobs:
            R.append(warn(f"{len(failed_jobs)} failed Job(s):"))
            rows = [[esc(j.metadata.namespace), esc(j.metadata.name),
                     str(j.status.failed or 0)]
                    for j in failed_jobs]
            R.append(table(["NAMESPACE","NAME","FAILURES"], rows))
        else:
            R.append(ok("No failed Jobs."))
    except Exception as e:
        R.append(warn(f"Jobs: {_report_err(e)}"))

    try:
        cronjobs = _batch.list_cron_job_for_all_namespaces().items
        if cronjobs:
            R.append(subsection(f"CronJobs ({len(cronjobs)} total)"))
            rows = []
            for cj in sorted(cronjobs, key=lambda x: (x.metadata.namespace, x.metadata.name)):
                last = cj.status.last_schedule_time
                if last:
                    delta = datetime.datetime.now(timezone.utc) - last.replace(tzinfo=timezone.utc)
                    h, m = divmod(int(delta.total_seconds()) // 60, 60)
                    last_str = f"{h}h {m}m ago"
                else:
                    last_str = "Never"
                suspended = "⏸ Yes" if cj.spec.suspend else "▶ No"
                rows.append([esc(cj.metadata.namespace), esc(cj.metadata.name),
                              esc(cj.spec.schedule), suspended, last_str])
            R.append(table(["NAMESPACE","NAME","SCHEDULE","SUSPENDED","LAST RUN"], rows))
    except Exception as e:
        R.append(warn(f"CronJobs: {_report_err(e)}"))

    try:
        all_pods = _core.list_pod_for_all_namespaces().items
        non_running = [p for p in all_pods
                       if p.status.phase not in ("Running","Succeeded")
                       and not any(c.type == "Ready" and c.status == "True"
                                   for c in (p.status.conditions or []))]
        if non_running:
            R.append(warn(f"Non-Running Pods ({len(non_running)} total)"))
            rows = []
            for p in non_running[:_MAX_ROWS]:
                cs     = p.status.container_statuses or []
                ready  = sum(1 for c in cs if c.ready)
                restarts = sum(c.restart_count or 0 for c in cs)
                reason = (p.status.reason or
                          next((s.state.waiting.reason for c in cs
                                if (s := c) and c.state and c.state.waiting
                                and c.state.waiting.reason), "") or
                          p.status.phase or "")
                rows.append([esc(p.metadata.namespace), esc(p.metadata.name),
                              esc(p.status.phase or "Unknown"),
                              f"{ready}/{len(p.spec.containers)}",
                              str(restarts), esc(reason)])
            R.append(table(["NAMESPACE","NAME","PHASE","READY","RESTARTS","REASON"], rows))
            if len(non_running) > _MAX_ROWS:
                R.append(f'<p class="note"><em>({len(non_running)-_MAX_ROWS} more not shown)</em></p>')
        else:
            R.append(ok("All pods running."))
    except Exception as e:
        R.append(warn(f"Pod status: {_report_err(e)}"))

    # ── 6. Networking & DNS ───────────────────────────────────────────────
    R.append(section("5. Networking & DNS"))
    try:
        DNS_NS       = "kube-system"
        DNS_PATTERNS = ["coredns","core-dns","kube-dns"]
        dns_pods = [p for p in _core.list_namespaced_pod(namespace=DNS_NS).items
                    if any(pat in p.metadata.name.lower() for pat in DNS_PATTERNS)
                    and "autoscaler" not in p.metadata.name.lower()
                    and (p.status.phase or "").lower() != "succeeded"]
        if dns_pods:
            not_ready_dns = [p for p in dns_pods
                             if sum(1 for cs in (p.status.container_statuses or []) if cs.ready)
                             < len(p.spec.containers or [])]
            if not_ready_dns:
                R.append(warn(f"CoreDNS: {len(not_ready_dns)}/{len(dns_pods)} pods not ready — "
                               f"{_cap([p.metadata.name for p in not_ready_dns])}"))
            else:
                R.append(ok(f"CoreDNS: {len(dns_pods)}/{len(dns_pods)} pods ready"))

            dns_test_pod = next((p for p in dns_pods if p.status.phase == "Running"), None)
            if dns_test_pod:
                from kubernetes.stream import stream as _k8s_stream
                test_targets = ["kubernetes.default.svc.cluster.local",
                                "kube-dns.kube-system.svc.cluster.local"]
                R.append(subsection("CoreDNS Resolution Tests"))
                res_rows = []
                for target in test_targets:
                    try:
                        resp = _k8s_stream(
                            _core.connect_get_namespaced_pod_exec,
                            dns_test_pod.metadata.name, DNS_NS,
                            command=["/bin/sh", "-c", f"nslookup {target} 2>&1 | head -5"],
                            stderr=False, stdin=False, stdout=True, tty=False, _preload_content=True)
                        out = resp.strip() if isinstance(resp, str) else ""
                        resolved = "Address" in out or "answer" in out.lower()
                        flag = '<span style="color:#16a34a;font-weight:700">OK</span>' if resolved else '<span style="color:#dc2626;font-weight:700">FAIL</span>'
                        res_rows.append([flag,
                                          f"<code>{esc(target)}</code>",
                                          "Resolved" if resolved else
                                          f'<span style="color:#dc2626">FAILED</span>'])
                    except Exception as ex:
                        res_rows.append(['<span style="color:#d97706">ERR</span>', f"<code>{esc(target)}</code>",
                                          f"exec error: {esc(str(ex))}"])
                R.append(table(["", "Target", "Result"], res_rows))
        else:
            R.append(warn("CoreDNS: no DNS pods found in kube-system"))
    except Exception as e:
        R.append(warn(f"CoreDNS check failed: {_report_err(e)}"))

    # NetworkPolicy check removed — too noisy for report

    # ── 7. Certificates & RBAC ────────────────────────────────────────────
    R.append(section("6. Certificates & RBAC"))
    try:
        custom = _k8s.CustomObjectsApi()
        certs  = custom.list_cluster_custom_object(
            "cert-manager.io","v1","certificates").get("items",[])
        if certs:
            not_ready_certs = []
            expiring_soon   = []
            for cert in certs:
                meta   = cert.get("metadata",{})
                status = cert.get("status",{})
                ready  = next((c for c in status.get("conditions",[])
                               if c.get("type") == "Ready"), None)
                is_ready = ready and ready.get("status") == "True"
                not_after = status.get("notAfter")
                if not is_ready:
                    not_ready_certs.append(f"{meta.get('namespace')}/{meta.get('name')}")
                elif not_after:
                    try:
                        exp = datetime.datetime.fromisoformat(not_after.replace("Z","+00:00"))
                        days_left = (exp - datetime.datetime.now(timezone.utc)).days
                        if days_left <= 30:
                            expiring_soon.append(
                                f"{meta.get('namespace')}/{meta.get('name')} ({days_left}d)")
                    except Exception:
                        pass
            if not_ready_certs:
                R.append(err(f"{len(not_ready_certs)} cert(s) not Ready: "
                              f"{_cap(not_ready_certs)}"))
            if expiring_soon:
                R.append(warn(f"{len(expiring_soon)} cert(s) expiring within 30 days: "
                               f"{_cap(expiring_soon)}"))
            if not not_ready_certs and not expiring_soon:
                R.append(ok(f"{len(certs)} cert-manager certificate(s) — all Ready and not expiring soon"))
        else:
            R.append(ok("No cert-manager Certificates found (cert-manager may not be installed)"))
    except Exception as e:
        if "404" in str(e):
            R.append(ok("cert-manager not installed — skipping certificate check"))
        else:
            R.append(warn(f"Certificates: {_report_err(e)}"))

    # Webhook FailurePolicy check removed — too noisy for report

    # ── 8. Kubernetes Control Plane & Recent Diagnostics ──────────────────
    R.append(section("7. Kubernetes Control Plane & Recent Diagnostics"))

    # Component statuses
    try:
        cs_items = _core.list_component_status().items
        if cs_items:
            R.append(subsection("Component Statuses"))
            rows = []
            for c in cs_items:
                cond   = c.conditions[0] if c.conditions else None
                status = cond.status if cond else "Unknown"
                flag   = '<span style="color:#16a34a;font-weight:700">&#10003;</span>' if status == "True" else '<span style="color:#dc2626;font-weight:700">&#10007;</span>'
                rows.append([f"{flag} {esc(c.metadata.name)}",
                              esc(cond.message if cond else "-"),
                              esc(cond.error if cond else "-")])
            R.append(table(["COMPONENT", "MESSAGE", "ERROR"], rows))
        else:
            R.append(ok("Component statuses not available (normal on managed clusters)"))
    except Exception:
        R.append(ok("Component statuses not available (normal on managed clusters)"))

    # Control plane pods
    try:
        pods = _core.list_namespaced_pod(namespace="kube-system").items
        core_components = ["kube-apiserver", "etcd", "kube-controller-manager", "kube-scheduler"]
        cp_pods = [p for p in pods if any(comp in p.metadata.name for comp in core_components)]
        R.append(subsection("Control Plane Pods (kube-system)"))
        if cp_pods:
            rows = []
            for pod in cp_pods:
                phase    = pod.status.phase or "Unknown"
                ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                total    = len(pod.spec.containers)
                restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
                flag     = '<span style="color:#16a34a;font-weight:700">&#10003;</span>' if phase == "Running" and ready == total else '<span style="color:#dc2626;font-weight:700">&#10007;</span>'
                comp     = next((c for c in core_components if c in pod.metadata.name),
                                pod.metadata.name.split("-")[0])
                rows.append([esc(comp), esc(pod.metadata.name),
                              f"{flag} {esc(phase)}", f"{ready}/{total}", str(restarts)])
            R.append(table(["COMPONENT", "POD NAME", "STATUS", "READY", "RESTARTS"], rows))
        else:
            R.append(ok("No control plane pods visible (managed cluster)"))
    except Exception as e:
        R.append(warn(f"Control plane pods: {_report_err(e)}"))

    try:
        events = _core.list_event_for_all_namespaces(
            field_selector="type=Warning", limit=500).items
        seen: dict = {}
        for e in events:
            if _is_noisy_event(e.message or ""):
                continue
            key  = (e.reason or "", getattr(e.involved_object,"name",""))
            prev = seen.get(key)
            if prev is None or (e.count or 1) > (prev[0] or 1):
                seen[key] = (e.count or 1, e.metadata.namespace,
                             e.reason or "-", key[1], e.message or "")
        top = sorted(seen.values(), key=lambda x: x[0], reverse=True)[:15]
        if top:
            R.append(subsection(f"Top Warning Events ({len(top)} unique)"))
            rows = []
            for count, ns, reason, obj, msg in top:
                short_msg = msg[:80] + "…" if len(msg) > 80 else msg
                rows.append([f"x{count}", esc(reason),
                              f"<code>{esc(ns)}/{esc(obj)}</code>", esc(short_msg)])
            R.append(table(["COUNT","REASON","NAMESPACE / OBJECT","MESSAGE"], rows))
        else:
            R.append(ok("No recent warning events"))
    except Exception as e:
        R.append(warn(f"Warning events: {_report_err(e)}"))

    R.append('<hr/>')
    R.append('<p>This is your complete health check report. '
             'Ask the chatbot to drill into any flagged area for deeper diagnostics.</p>')

    return "\n".join(R)


def get_control_plane_status() -> str:
    try:
        lines = ["### Control Plane Health\n"]
        try:
            cs = _core.list_component_status().items
            if cs:
                lines += ["**Component Statuses:**", "| NAME | STATUS | MESSAGE | ERROR |", "|---|---|---|---|"]
                for c in cs:
                    cond   = c.conditions[0] if c.conditions else None
                    status = cond.status if cond else "Unknown"
                    lines.append(f"| {c.metadata.name} | {'🟢' if status=='True' else '🔴'} {status} "
                                 f"| {cond.message if cond else '-'} | {cond.error if cond else '-'} |")
                lines.append("")
        except Exception:
            pass
        pods = _core.list_namespaced_pod(namespace="kube-system").items
        core_components = ["kube-apiserver","etcd","kube-controller-manager","kube-scheduler"]
        lines += ["**Control Plane Pods (`kube-system`):**",
                  "| COMPONENT | POD NAME | STATUS | READY | RESTARTS |", "|---|---|---|---|---|"]
        found = False
        for pod in pods:
            if any(comp in pod.metadata.name for comp in core_components):
                found    = True
                phase    = pod.status.phase or "Unknown"
                ready    = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
                total    = len(pod.spec.containers)
                restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
                flag     = "🟢" if phase == "Running" and ready == total else "🔴"
                lines.append(f"| {pod.metadata.name.split('-')[0]} | `{pod.metadata.name}` "
                             f"| {flag} {phase} | {ready}/{total} | {restarts} |")
        if not found:
            lines += ["| _N/A_ | _No managed control plane pods visible_ | - | - | - |",
                      "\n_(Note: managed services like EKS/GKE/AKS abstract control plane pods.)_"]
        return "\n".join(lines)
    except Exception as e:
        return f"K8s API error fetching Control Plane status: {e}"

def get_webhook_health() -> str:
    try:
        adm_api   = _k8s.AdmissionregistrationV1Api()
        mutating  = adm_api.list_mutating_webhook_configuration().items
        validating = adm_api.list_validating_webhook_configuration().items
        if not mutating and not validating:
            return "No Mutating or Validating Webhook Configurations found."
        lines = ["### Admission Webhooks\n",
                 "| TYPE | NAME | WEBHOOK | FAILURE POLICY | TARGET SERVICE/URL |",
                 "|---|---|---|---|---|"]
        for config_list, wh_type in ((mutating,"Mutating"),(validating,"Validating")):
            for config in config_list:
                for wh in (config.webhooks or []):
                    if wh.client_config.service:
                        svc = wh.client_config.service
                        target = f"svc: {svc.namespace}/{svc.name}:{svc.port or 443}"
                    elif wh.client_config.url:
                        target = f"url: {wh.client_config.url}"
                    else:
                        target = "Unknown"
                    flag = "⚠️ FAIL" if wh.failure_policy == "Fail" else "IGNORE"
                    lines.append(f"| {wh_type} | `{config.metadata.name}` | `{wh.name}` "
                                 f"| **{flag}** | `{target}` |")
        return "\n".join(lines)
    except Exception as e:
        return f"K8s API error fetching Webhooks: {e}"

def get_certificate_status(namespace: str = "all") -> str:
    try:
        custom = _k8s.CustomObjectsApi()
        try:
            certs = (custom.list_cluster_custom_object("cert-manager.io","v1","certificates").get("items",[])
                     if namespace == "all"
                     else custom.list_namespaced_custom_object(
                         "cert-manager.io","v1",namespace,"certificates").get("items",[]))
        except _k8s.rest.ApiException as e:
            if e.status == 404:
                return "cert-manager CRDs (certificates.cert-manager.io) are not installed on this cluster."
            raise
        if not certs:
            return f"No cert-manager Certificates found in '{namespace}'."
        lines = [_ns_header("cert-manager Certificates", namespace),
                 "| NAMESPACE | NAME | READY | SECRET NAME | EXPIRATION (NOT AFTER) |",
                 "|---|---|---|---|---|"]
        for cert in certs:
            meta    = cert.get("metadata", {})
            status_d = cert.get("status", {})
            flag, ready_status = next(
                (("🟢",c["status"]) if c["status"]=="True" else ("🔴",c["status"])
                 for c in status_d.get("conditions",[]) if c.get("type")=="Ready"),
                ("❓","Unknown"),
            )
            lines.append(f"| `{meta.get('namespace','unknown')}` | `{meta.get('name','unknown')}` "
                         f"| {flag} {ready_status} | `{cert.get('spec',{}).get('secretName','unknown')}` "
                         f"| {status_d.get('notAfter','Unknown')} |")
        return "\n".join(lines)
    except Exception as e:
        return f"K8s API error fetching Certificates: {e}"

def _list_namespaced_or_all(list_ns_fn, list_all_fn, namespace: str) -> list:
    if namespace != "all":
        try:
            items = list_ns_fn(namespace).items
            return items if items else list_all_fn().items
        except Exception:
            return list_all_fn().items
    return list_all_fn().items

def _search_filter(items: list, search: str | None, fallback: bool = True) -> list:
    if not search: return items
    s = search.lower()
    filtered = [i for i in items
                if s in i.metadata.name.lower() or s in (i.metadata.namespace or "").lower()]
    return filtered if filtered else (items if fallback else [])

def get_configmap_list(namespace: str = "all", search: str | None = None,
                       filter_keys: list = None) -> str:
    _CERT_KEY_HINTS = {"ca.crt","tls.crt","tls.key","ca-bundle","ca-certificates",
                       "ca.pem","cert.pem","certificate","ssl.crt","ssl.key"}
    try:
        items = _list_namespaced_or_all(
            _core.list_namespaced_config_map,
            _core.list_config_map_for_all_namespaces, namespace)
        items = [cm for cm in items if cm.metadata.name != "kube-root-ca.crt"]
        if not items:
            return "No ConfigMaps found."
        filtered = _search_filter(items, search)
        lines = [_ns_header("ConfigMaps", namespace, search),
                 "| NAMESPACE | CONFIGMAP | KEYS | TYPE |", "|---|---|---|---|"]
        for cm in sorted(filtered, key=lambda x: (x.metadata.namespace, x.metadata.name)):
            data = cm.data or {}
            keys = list(data.keys())
            if filter_keys:
                fk   = [f.lower() for f in filter_keys]
                keys = [k for k in keys if any(f in k.lower() for f in fk)]
                if not keys: continue
            cert_keys = [k for k in keys
                         if k in _CERT_KEY_HINTS
                         or any(h in k.lower() for h in ("cert","tls","ssl","ca.",".crt",".pem"))]
            lines.append(f"| `{cm.metadata.namespace}` | `{cm.metadata.name}` "
                         f"| {keys if keys else '-'} | {'cert' if cert_keys else '-'} |")
        return "\n".join(lines) if len(lines) > 2 else "No matching ConfigMaps found."
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_secret_list(namespace: str = "all", name: str = "", pod_name: str = None,
                    filter_keys: list = None, decode: bool = False) -> str:
    def _decode(val: str) -> str:
        try: return base64.b64decode(val).decode("utf-8", errors="replace")
        except Exception: return "<decode error>"

    hidden_msg = "<hidden> — enable 'Show Secret Values' in ⚙ Settings → Security to decode."
    CERT_HINTS = {"tls","cert","certs","certificate","crt","pem","key","ca","ssl","x509"}

    try:
        if pod_name:
            pod = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
            secrets    = {vol.secret.secret_name for vol in (pod.spec.volumes or []) if vol.secret}
            configmaps = {vol.config_map.name      for vol in (pod.spec.volumes or []) if vol.config_map}
            lines = [f"Secrets and ConfigMaps attached to pod '{namespace}/{pod_name}':"]
            if secrets:
                lines.append("  Secrets:")
                for sname in sorted(secrets):
                    try:
                        s    = _core.read_namespaced_secret(name=sname, namespace=namespace)
                        data = s.data or {}
                        lines.append(f"    {sname} [type={s.type}]")
                        for k, v in data.items():
                            is_cert = any(h in k.lower() for h in CERT_HINTS)
                            lines.append(f"      {k}: {_decode(v) if (decode or is_cert) else hidden_msg}")
                    except ApiException:
                        lines.append(f"    {sname}: <fetch error>")
            else:
                lines.append("  Secrets: None")
            if configmaps:
                lines.append("  ConfigMaps:")
                for cm_name in sorted(configmaps):
                    try:
                        cm   = _core.read_namespaced_config_map(name=cm_name, namespace=namespace)
                        keys = list(cm.data.keys() if cm.data else [])
                        lines.append(f"    {cm_name}: keys={keys if keys else 'None'}")
                    except ApiException:
                        lines.append(f"    {cm_name}: <fetch error>")
            else:
                lines.append("  ConfigMaps: None")
            return "\n".join(lines)

        if name:
            secret = _core.read_namespaced_secret(name=name, namespace=namespace)
            data   = secret.data or {}
            lines  = [f"Secret: {namespace}/{name}", f"  Type: {secret.type}"]
            if data:
                lines.append("  Data:")
                for k, v in data.items():
                    is_cert = any(h in k.lower() for h in CERT_HINTS)
                    lines.append(f"    {k}: {_decode(v) if (decode or is_cert or any(h in name.lower() for h in CERT_HINTS)) else hidden_msg}")
            else:
                lines.append("  Data: None")
            return "\n".join(lines)

        secrets = _core.list_namespaced_secret(namespace=namespace)
        if not secrets.items:
            return f"No secrets in namespace '{namespace}'."
        _CERT_TYPES = {"kubernetes.io/tls","helm.sh/release.v1"}
        by_type: dict = {}
        for s in secrets.items:
            t    = s.type or "Opaque"
            keys = list((s.data or {}).keys())
            is_cert = t in _CERT_TYPES or any(any(h in k.lower() for h in CERT_HINTS) for k in keys)
            by_type.setdefault(t, []).append((s.metadata.name, keys, is_cert))
        lines = [f"Secrets in '{namespace}' ({len(secrets.items)} total):"]
        for stype, entries in sorted(by_type.items()):
            lines.append(f"  [{stype}] ({len(entries)})")
            for n, keys, is_cert in sorted(entries):
                lines.append(f"    {n}: keys={keys}{' [cert/TLS]' if is_cert else ''}")
        if filter_keys:
            fk = [f.lower() for f in filter_keys]
            filt = [l for l in lines if any(f in l.lower() for f in fk)]
            lines = ["Filtered secrets:"] + filt if filt else [f"No secrets match keys {filter_keys}."]
        return "\n".join(lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_resource_quotas(namespace: str = "all", search: str | None = None) -> str:
    try:
        items = _list_namespaced_or_all(
            _core.list_namespaced_resource_quota,
            _core.list_resource_quota_for_all_namespaces, namespace)
        if not items:
            return "No ResourceQuotas found."
        filtered = _search_filter(items, search)
        lines = [_ns_header("ResourceQuotas", namespace, search),
                 "| NAMESPACE | QUOTA | RESOURCE | USED | HARD |", "|---|---|---|---|---|"]
        for q in sorted(filtered, key=lambda x: (x.metadata.namespace, x.metadata.name)):
            hard = q.status.hard or {}
            used = q.status.used or {}
            for res in sorted(hard.keys()):
                lines.append(f"| `{q.metadata.namespace}` | `{q.metadata.name}` "
                             f"| {res} | {used.get(res,'0')} | {hard.get(res)} |")
        return "\n".join(lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_limit_ranges(namespace: str = "all", search: str | None = None) -> str:
    try:
        items = _list_namespaced_or_all(
            _core.list_namespaced_limit_range,
            _core.list_limit_range_for_all_namespaces, namespace)
        if not items:
            return "No LimitRanges found."
        filtered = _search_filter(items, search)
        def _get(v, key):
            return (v or {}).get(key, "-")
        lines = [_ns_header("LimitRanges", namespace, search),
                 "| NAMESPACE | LIMITRANGE | TYPE | CPU_MAX | CPU_MIN | CPU_DEFAULT | MEM_MAX | MEM_MIN | MEM_DEFAULT |",
                 "|---|---|---|---|---|---|---|---|---|"]
        for lr in sorted(filtered, key=lambda x: (x.metadata.namespace, x.metadata.name)):
            for item in (lr.spec.limits or []):
                lines.append(
                    f"| `{lr.metadata.namespace}` | `{lr.metadata.name}` | {item.type} | "
                    f"{_get(item.max,'cpu')} | {_get(item.min,'cpu')} | {_get(item.default,'cpu')} | "
                    f"{_get(item.max,'memory')} | {_get(item.min,'memory')} | {_get(item.default,'memory')} |")
        return "\n".join(lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_serviceaccounts(namespace: str = "all", search: str | None = None) -> str:
    try:
        items = _list_namespaced_or_all(
            _core.list_namespaced_service_account,
            _core.list_service_account_for_all_namespaces, namespace)
        if not items:
            return "No ServiceAccounts found."
        filtered  = _search_filter(items, search)
        rb_items  = _rbac.list_role_binding_for_all_namespaces().items
        crb_items = _rbac.list_cluster_role_binding().items
        lines = [_ns_header("ServiceAccounts", namespace, search),
                 "| NAMESPACE | SERVICEACCOUNT | ROLES | CLUSTER ROLES |", "|---|---|---|---|"]
        for sa in sorted(filtered, key=lambda x: (x.metadata.namespace, x.metadata.name)):
            ns   = sa.metadata.namespace
            name = sa.metadata.name
            roles = sorted({rb.role_ref.name for rb in rb_items
                            if rb.metadata.namespace == ns
                            and any(s.kind == "ServiceAccount" and s.name == name and s.namespace == ns
                                    for s in (rb.subjects or []))})
            croles = sorted({crb.role_ref.name for crb in crb_items
                             if any(s.kind == "ServiceAccount" and s.name == name and s.namespace == ns
                                    for s in (crb.subjects or []))})
            lines.append(f"| `{ns}` | `{name}` "
                         f"| {', '.join(roles) or '-'} | {', '.join(croles) or '-'} |")
        return "\n".join(lines)
    except Exception as e:
        return f"K8s error: {e}"

def get_cluster_role_bindings() -> str:
    try:
        crbs = _rbac.list_cluster_role_binding()
        if not crbs.items:
            return "No ClusterRoleBindings found."
        lines = ["ClusterRoleBindings:"]
        for crb in crbs.items:
            subjects = [f"{s.kind}/{s.name}" for s in (crb.subjects or [])]
            lines.append(f"  {crb.metadata.name}: role={crb.role_ref.name} → {subjects}")
        return "\n".join(lines)
    except ApiException as e:
        return _api_error(e)
    except Exception as e:
        return _k8s_err(e)

def get_namespace_resource_summary(namespace: str) -> str:
    try:
        pods = _core.list_namespaced_pod(namespace=namespace, limit=1000)
    except ApiException as e:
        return f"[ERROR] {_safe_reason(e)}"
    if not pods.items:
        return f"No pods found in namespace '{namespace}'."
    total_cpu_req = total_cpu_lim = 0
    total_mem_req = total_mem_lim = 0.0
    table_rows    = []
    for pod in pods.items:
        cpu_req = cpu_lim = 0
        mem_req = mem_lim = 0.0
        for c in list(pod.spec.containers or []) + list(pod.spec.init_containers or []):
            req = (c.resources.requests or {}) if c.resources else {}
            lim = (c.resources.limits   or {}) if c.resources else {}
            cpu_req += _parse_cpu_to_millicores(req.get("cpu",    "0"))
            cpu_lim += _parse_cpu_to_millicores(lim.get("cpu",    "0"))
            mem_req += _parse_mem_to_mib(req.get("memory", "0"))
            mem_lim += _parse_mem_to_mib(lim.get("memory", "0"))
        total_cpu_req += cpu_req; total_cpu_lim += cpu_lim
        total_mem_req += mem_req; total_mem_lim += mem_lim
        table_rows.append(f"| {pod.metadata.name} | {cpu_req or 0}m | {mem_req:.0f}Mi "
                          f"| {cpu_lim or 0}m | {mem_lim:.0f}Mi |")

    def _fmt_cpu(m): return "0m" if m == 0 else f"{m}m ({m/1000:.3f} cores)"
    def _fmt_mem(mib): return "0Mi" if mib == 0 else f"{mib:.0f}Mi ({mib/1024:.2f}Gi)"

    return "\n".join([
        f"### Resource summary for namespace '{namespace}' ({len(pods.items)} pods)\n",
        f"- **TOTAL CPU REQUESTED**: {_fmt_cpu(total_cpu_req)}",
        f"- **TOTAL CPU LIMIT**: {_fmt_cpu(total_cpu_lim)}",
        f"- **TOTAL MEMORY REQUESTED**: {_fmt_mem(total_mem_req)}",
        f"- **TOTAL MEMORY LIMIT**: {_fmt_mem(total_mem_lim)}\n",
        "**Per-pod breakdown:**\n",
        "| POD NAME | CPU REQ | MEM REQ | CPU LIM | MEM LIM |",
        "|---|---|---|---|---|",
    ] + table_rows)

def get_coredns_health() -> str:
    from kubernetes.stream import stream as _k8s_stream

    DNS_NS       = "kube-system"
    DNS_PATTERNS = ["coredns", "core-dns", "kube-dns"]

    def _is_dns_pod(p):
        n = p.metadata.name.lower()
        return (any(pat in n for pat in DNS_PATTERNS)
                and not n.startswith("helm-install-")
                and (p.status.phase or "").lower() != "succeeded")

    def _exec_in_pod(pod_name: str, ns: str, cmd: str) -> str:
        try:
            resp = _k8s_stream(
                _core.connect_get_namespaced_pod_exec, pod_name, ns,
                command=["/bin/sh", "-c", cmd],
                stderr=True, stdin=False, stdout=True, tty=False, _preload_content=True)
            return resp.strip() if isinstance(resp, str) else ""
        except Exception as exc:
            return f"[exec failed: {exc}]"

    lines = ["CoreDNS Health Check:"]
    try:
        all_pods = _core.list_namespaced_pod(namespace=DNS_NS)
    except ApiException as e:
        return f"K8s API error reading kube-system pods: {e.reason}"

    dns_pods        = [p for p in all_pods.items if _is_dns_pod(p) and "autoscaler" not in p.metadata.name.lower()]
    autoscaler_pods = [p for p in all_pods.items if _is_dns_pod(p) and "autoscaler" in p.metadata.name.lower()]

    if not dns_pods:
        return "\n".join(lines + [f"\n  No CoreDNS pods found in '{DNS_NS}'."])

    def _pod_line(pod):
        phase    = pod.status.phase or "Unknown"
        ready_cs = sum(1 for cs in (pod.status.container_statuses or []) if cs.ready)
        total_cs = len(pod.status.container_statuses or [])
        restarts = sum(cs.restart_count for cs in (pod.status.container_statuses or []))
        flag     = "✅" if phase == "Running" and ready_cs == total_cs else "❌"
        return (f"    {flag} {pod.metadata.name}  phase:{phase}  "
                f"ready:{ready_cs}/{total_cs}  restarts:{restarts}  node:{pod.spec.node_name or '?'}")

    lines.append(f"\n  CoreDNS pods in '{DNS_NS}':")
    running_pods = []
    for pod in dns_pods:
        lines.append(_pod_line(pod))
        if pod.status.phase == "Running":
            running_pods.append(pod)

    if autoscaler_pods:
        lines.append("\n  CoreDNS autoscaler:")
        for pod in autoscaler_pods:
            lines.append(_pod_line(pod))

    try:
        dns_svcs = [s for s in _core.list_namespaced_service(namespace=DNS_NS).items
                    if any(pat in s.metadata.name.lower() for pat in DNS_PATTERNS)]
        if dns_svcs:
            lines.append("\n  CoreDNS service(s):")
            for svc in dns_svcs:
                ports = ", ".join(f"{p.port}/{p.protocol}" for p in (svc.spec.ports or []))
                lines.append(f"    {svc.metadata.name}  clusterIP:{svc.spec.cluster_ip or '?'}  ports:[{ports}]")
        else:
            lines.append("\n  ⚠ No CoreDNS service found in kube-system.")
    except ApiException:
        lines.append("\n  ⚠ Could not retrieve CoreDNS service.")

    coredns_test_pods = [p for p in running_pods
                         if "coredns" in p.metadata.name.lower()
                         and "autoscaler" not in p.metadata.name.lower()]
    if not coredns_test_pods:
        lines.append("\n  ⚠ DNS resolution test skipped — no running CoreDNS pod available.")
        return "\n".join(lines)

    test_pod = coredns_test_pods[0].metadata.name
    lines.append(f"\n  DNS resolution test (running from pod: {test_pod}):")

    resolv = _exec_in_pod(test_pod, DNS_NS, "cat /etc/resolv.conf 2>/dev/null")
    nameserver = search_domain = ""
    for rline in resolv.splitlines():
        rline = rline.strip()
        if rline.startswith("nameserver") and len(rline.split()) >= 2:
            nameserver = rline.split()[1]
        if rline.startswith("search") and len(rline.split()) >= 2:
            search_domain = rline.split()[1]

    test_targets = ([f"console-cdp.apps.{search_domain}"] if search_domain else [])
    try:
        for ing in (_net.list_ingress_for_all_namespaces().items or []):
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

    lines.append(f"\n  nslookup tests (nameserver: {nameserver or 'default'}, search: {search_domain or 'none'}):")
    for target in test_targets[:3]:
        output    = _exec_in_pod(test_pod, DNS_NS, f"nslookup {target} 2>&1")
        out_lower = output.lower()
        has_addr  = bool(re.search(r'address[\s\d:]+\d+\.\d+\.\d+\.\d+', out_lower))
        is_fail   = (not output or "[exec failed" in output
                     or any(tok in out_lower for tok in
                            ("bad address","can't resolve","nxdomain","server can't find",
                             "no answer","timed out","connection refused"))
                     or bool(re.search(r'\bfailed\b', out_lower)))
        flag = "✅" if has_addr and not is_fail else "❌"
        lines.append(f"    {flag} nslookup {target}")
        lines.extend(f"       {oline}" for oline in output.splitlines())

    return "\n".join(lines)

def query_prometheus_metrics(metric: str = "cpu", duration: str = "1h",
                              step: str = "60s", namespace: str = "",
                              user_timezone: str = "UTC") -> str:
    import time as _time
    from kubernetes.stream import stream as _k8s_stream

    METRIC_MAP = {
        "cpu":            ("Pod CPU Usage (millicores)",
                           'sum by (pod, namespace) (rate(container_cpu_usage_seconds_total{container!="",container!="POD"}[5m])) * 1000',
                           "m"),
        "node_cpu":       ("Pod CPU Usage (millicores)",
                           'sum by (pod, namespace) (rate(container_cpu_usage_seconds_total{container!="",container!="POD"}[5m])) * 1000',
                           "m"),
        "pod_cpu":        ("Pod CPU Usage (millicores)",
                           'sum by (pod, namespace) (rate(container_cpu_usage_seconds_total{container!="",container!="POD"}[5m])) * 1000',
                           "m"),
        "memory":         ("Pod Memory Usage (MiB)",
                           'sum by (pod, namespace) (container_memory_working_set_bytes{container!="",container!="POD"}) / 1048576',
                           "MiB"),
        "node_memory":    ("Pod Memory Usage (MiB)",
                           'sum by (pod, namespace) (container_memory_working_set_bytes{container!="",container!="POD"}) / 1048576',
                           "MiB"),
        "pod_memory":     ("Pod Memory Usage (MiB)",
                           'sum by (pod, namespace) (container_memory_working_set_bytes{container!="",container!="POD"}) / 1048576',
                           "MiB"),
        "cluster_cpu":    ("Cluster CPU Usage (millicores)",
                           'sum(rate(container_cpu_usage_seconds_total{container!="",container!="POD"}[5m])) * 1000',
                           "m"),
        "cluster_memory": ("Cluster Memory Usage (MiB)",
                           'sum(container_memory_working_set_bytes{container!="",container!="POD"}) / 1048576',
                           "MiB"),
        "disk_io":        None, "pvc_io": None, "network_in": None, "network_out": None,
    }
    FALLBACK_PROMQL = {
        "cpu":         ['sum by (pod, namespace) (rate(container_cpu_usage_seconds_total{container!="POD"}[5m])) * 1000',
                        'sum by (pod, namespace) (container_cpu_usage)'],
        "node_cpu":    ['sum by (pod, namespace) (rate(container_cpu_usage_seconds_total{container!="POD"}[5m])) * 1000',
                        'sum by (pod, namespace) (container_cpu_usage)'],
        "pod_cpu":     ['sum by (pod, namespace) (rate(container_cpu_usage_seconds_total{container!="POD"}[5m])) * 1000',
                        'sum by (pod, namespace) (container_cpu_usage)'],
        "memory":      ['sum by (pod, namespace) (container_memory_usage_bytes{container!="POD"}) / 1048576'],
        "node_memory": ['sum by (pod, namespace) (container_memory_usage_bytes{container!="POD"}) / 1048576'],
        "pod_memory":  ['sum by (pod, namespace) (container_memory_usage_bytes{container!="POD"}) / 1048576'],
    }

    key = metric.lower().replace(" ", "_").replace("-", "_")
    entry = METRIC_MAP.get(key)
    if entry is None:
        return (f"The '{metric}' metric is not available on this cluster. "
                f"Available metrics: cpu, memory, pod_cpu, pod_memory, cluster_cpu, cluster_memory.")
    title, promql, unit = entry

    _NS_IGNORE = {"all","any","every","cluster","cluster-wide","clusterwide","none","global","*"}
    ns = namespace.strip().lower()
    if ns in _NS_IGNORE: ns = ""

    def _inject_ns(pql, ns_val):
        ns_filter = f'namespace="{ns_val}"'
        def _add(m):
            inner = m.group(1).strip()
            if ns_filter in inner:
                return '{' + inner + '}'
            return '{' + (inner + ',' if inner else '') + ns_filter + '}'
        return re.sub(r'\{([^}]*)\}', _add, pql)

    if ns:
        promql = _inject_ns(promql, ns)
        title  = f"{title} — ns:{ns}"

    dur_map = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    try:
        dur_sec = int(duration[:-1]) * dur_map.get(duration[-1], 1)
    except Exception:
        dur_sec = 3600
    end_ts   = int(_time.time())
    start_ts = end_ts - dur_sec

    prom_pod, prom_ns, prom_container = _find_prometheus_pod()
    if not prom_pod:
        return "No running Prometheus server pod found."

    def _exec(cmd, large: bool = False):
        try:
            if large:
                ws = _k8s_stream(
                    _core.connect_get_namespaced_pod_exec,
                    prom_pod, prom_ns,
                    command=["/bin/sh", "-c", cmd],
                    container=prom_container,
                    stderr=False, stdin=False, stdout=True, tty=False,
                    _preload_content=False)
                chunks = []
                while ws.is_open():
                    ws.update(timeout=60)
                    if ws.peek_stdout():
                        chunks.append(ws.read_stdout())
                ws.close()
                resp = "".join(chunks)
            else:
                resp = _k8s_stream(
                    _core.connect_get_namespaced_pod_exec,
                    prom_pod, prom_ns,
                    command=["/bin/sh", "-c", cmd],
                    container=prom_container,
                    stderr=False, stdin=False, stdout=True, tty=False,
                    _preload_content=True)
            if isinstance(resp, bytes): resp = resp.decode("utf-8", errors="replace")
            elif not isinstance(resp, str):
                try: resp = _json.dumps(resp) if hasattr(resp, "__iter__") else str(resp)
                except Exception: resp = str(resp)
            return resp.strip()
        except Exception as exc:
            return f"[exec error: {exc}]"

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
                    f"HTTP probes: /prometheus → {probe}, / → {probe2}. Pod: {prom_pod} in {prom_ns}.")

    label_raw = _exec(f"curl -s --max-time 10 '{api_base}/labels?match[]=container_cpu_usage_seconds_total'", large=True)
    try:
        available_labels = _json.loads(label_raw).get("data", [])
    except Exception:
        available_labels = []
    has_node_label = "node" in available_labels

    if has_node_label and not ns:
        if key in ("cpu", "node_cpu", "pod_cpu"):
            promql = 'sum by (node) (rate(container_cpu_usage_seconds_total{container!="",container!="POD"}[5m])) * 1000'
            title  = "Node CPU Usage (millicores)"
        elif key in ("memory", "node_memory", "pod_memory"):
            promql = 'sum by (node) (container_memory_working_set_bytes{container!="",container!="POD"}) / 1048576'
            title  = "Node Memory Usage (MiB)"

    def _query(pql):
        enc = urllib.parse.quote(pql, safe="")
        url = f"{api_base}/query_range?query={enc}&start={start_ts}&end={end_ts}&step={step}"
        raw = _exec(f"curl -s --max-time 60 '{url}'", large=True)
        if not raw or raw.startswith("[exec error"): return None, raw or "Empty response"
        try:
            return _json.loads(raw), None
        except Exception:
            try:
                return ast.literal_eval(raw), None
            except Exception:
                return None, f"Prometheus returned non-JSON response:\n{raw[:500]}"

    data, err = _query(promql)
    if err: return err
    if data.get("status") != "success":
        return f"Prometheus query failed: {data.get('error', data)}"

    results = data.get("data", {}).get("result", [])
    if not results:
        for fallback_pql in FALLBACK_PROMQL.get(key, []):
            if ns: fallback_pql = _inject_ns(fallback_pql, ns)
            fb_data, fb_err = _query(fallback_pql)
            if not fb_err and fb_data and fb_data.get("status") == "success":
                fb_results = fb_data.get("data", {}).get("result", [])
                if fb_results:
                    results = fb_results; promql = fallback_pql; break

    if not results:
        disc_raw = _exec(f"curl -s --max-time 10 '{api_base}/label/__name__/values'")
        hint = ""
        try:
            all_metrics = _json.loads(disc_raw).get("data", [])
            relevant = [m for m in all_metrics
                        if any(kw in m for kw in ["cpu","memory","mem","node_","container_"])][:15]
            if relevant: hint = f"\nAvailable metrics include: {', '.join(relevant)}"
        except Exception:
            pass
        return f"No data returned for metric '{metric}' over the last {duration}.\nPromQL tried: {promql}{hint}"

    def _last_val(r):
        vals = r.get("values", [])
        try: return float(vals[-1][1]) if vals else 0.0
        except (ValueError, IndexError): return 0.0

    results = sorted(results, key=_last_val, reverse=True)
    CHART_CAP = 8

    def _fmt(v, u):
        if u == "%":     return f"{v:.1f}%"
        if u == "cores": return f"{v:.3f}"
        if u == "m":
            if v == 0: return "0m"
            return f"{v:.4f}m" if v < 0.01 else f"{v:.3f}m" if v < 0.1 else f"{v:.2f}m" if v < 1 else f"{v:.1f}m"
        if u == "MiB": return f"{v:.0f}Mi"
        if u in ("bytes","bytes/s"):
            return f"{v/1e9:.2f}G" if v >= 1e9 else f"{v/1e6:.1f}M" if v >= 1e6 else f"{v/1e3:.0f}k" if v >= 1e3 else f"{v:.0f}"
        return f"{v:.2f}"

    series_out = []
    for r in results[:CHART_CAP]:
        ml  = r.get("metric", {})
        ns_val  = ml.get("namespace") or ml.get("pod_namespace") or ""
        pod_val = ml.get("pod") or ml.get("pod_name") or ""
        if ns_val and pod_val: label = f"{ns_val}/{pod_val}"
        elif pod_val:           label = pod_val
        elif ns_val:            label = ns_val
        else:
            inst  = ml.get("instance", "")
            label = inst.split(".")[0] if inst else next(iter(ml.values()), "unknown")
        series_out.append({"label": label,
                           "values": [[float(ts), float(v)] for ts, v in r.get("values", [])]})

    cap_note    = f" (top {CHART_CAP} of {len(results)})" if len(results) > CHART_CAP else ""
    node_caveat = ""
    if not ns and not has_node_label:
        node_caveat = (f"\nNote: per-node metrics are unavailable (container_cpu_usage has no 'node' label "
                       f"on this cluster). Showing pod-level data.")

    tz_label = user_timezone if user_timezone not in ("", "UTC") else "UTC"
    try:
        import zoneinfo
        _tz     = zoneinfo.ZoneInfo(user_timezone)
        _now_str = datetime.datetime.now(_tz).strftime(f"%Y-%m-%d %H:%M {tz_label}")
    except Exception:
        _now_str = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    summary_lines = [f"📊 {title} — last {duration} (as of {_now_str}){cap_note}{node_caveat}", "```"]
    summary_lines.extend(
        f"  {s['label']}: {_fmt(s['values'][-1][1], unit)}"
        for s in series_out if s["values"])
    summary_lines.append("```")

    graph_payload = _json.dumps(
        {"title": title, "unit": unit, "duration": duration, "series": series_out},
        separators=(",", ":"))
    return "\n".join(summary_lines) + f"\n§GRAPH§{graph_payload}§GRAPH§"

def _find_db_credentials(namespace: str, pod_name: str) -> dict:
    creds: dict = {k: None for k in ("user","password","database","host","port")}

    def _match(key: str, patterns: tuple) -> bool:
        kl = key.lower().replace("-", "_")
        return any(p.replace("-", "_") in kl for p in patterns)

    def _harvest(key: str, val: str):
        if   _match(key, _DB_USER_KEYS)  and not creds["user"]:     creds["user"]     = val
        elif _match(key, _DB_PASS_KEYS)  and not creds["password"]: creds["password"] = val
        elif _match(key, _DB_NAME_KEYS)  and not creds["database"]: creds["database"] = val
        elif _match(key, _DB_HOST_KEYS)  and not creds["host"]:     creds["host"]     = val
        elif _match(key, _DB_PORT_KEYS)  and not creds["port"]:     creds["port"]     = val

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
                        sec = _core.read_namespaced_secret(name=vf.secret_key_ref.name, namespace=namespace)
                        raw = (sec.data or {}).get(vf.secret_key_ref.key, "")
                        if raw: _harvest(env.name, _b64decode_safe(raw))
                    except ApiException:
                        pass
                elif vf.config_map_key_ref:
                    try:
                        cm  = _core.read_namespaced_config_map(name=vf.config_map_key_ref.name, namespace=namespace)
                        val = (cm.data or {}).get(vf.config_map_key_ref.key, "")
                        if val: _harvest(env.name, val)
                    except ApiException:
                        pass
        for ef in (container.env_from or []):
            if ef.secret_ref:
                try:
                    sec = _core.read_namespaced_secret(name=ef.secret_ref.name, namespace=namespace)
                    for k, v in (sec.data or {}).items():
                        _harvest(k, _b64decode_safe(v or ""))
                except ApiException:
                    pass
            if ef.config_map_ref:
                try:
                    cm = _core.read_namespaced_config_map(name=ef.config_map_ref.name, namespace=namespace)
                    for k, v in (cm.data or {}).items():
                        _harvest(k, v or "")
                except ApiException:
                    pass
    return creds

def _detect_db_type(pod_name: str, namespace: str, container_hint: str = "") -> str | None:
    try:
        pod = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
    except ApiException:
        return None
    containers = pod.spec.containers or []
    if container_hint:
        containers = sorted(containers, key=lambda c: 0 if c.name == container_hint else 1)
    for c in containers:
        image = (c.image or "").lower()
        if any(x in image for x in ("mysql","mariadb","percona")):                         return "mysql"
        if any(x in image for x in ("postgres","postgresql","pg:","pg-","pgbouncer")): return "postgres"
    for c in containers:
        cname = (c.name or "").lower()
        if any(x in cname for x in ("mysql","mariadb")): return "mysql"
        if any(x in cname for x in ("postgres","postgresql")): return "postgres"
    for c in containers:
        for env in (c.env or []):
            n = env.name.lower()
            if n.startswith(("postgres","pgdata","pguser","pgpassword")): return "postgres"
            if n.startswith(("mysql","mariadb")): return "mysql"
    return None

def _find_db_container(pod_name: str, namespace: str, db_type: str) -> str:
    try:
        pod = _core.read_namespaced_pod(name=pod_name, namespace=namespace)
    except ApiException:
        return ""
    hints = ("mysql","mariadb","percona") if db_type == "mysql" else ("postgres","postgresql","pg:","pg-")
    for c in (pod.spec.containers or []):
        if any(h in (c.image or "").lower() for h in hints): return c.name
    for c in (pod.spec.containers or []):
        cname = (c.name or "").lower()
        if db_type == "mysql"    and any(h in cname for h in ("mysql","mariadb","db")): return c.name
        if db_type == "postgres" and any(h in cname for h in ("postgres","pg","db")):   return c.name
    return (pod.spec.containers or [{}])[0].name if pod.spec.containers else ""

def _find_db_pod(namespace: str, hint: str = "") -> tuple[str | None, str | None]:
    try:
        pods = _core.list_namespaced_pod(namespace=namespace)
    except ApiException:
        return None, None
    _DB_IMAGE_HINTS = ("mysql","mariadb","percona","postgres","postgresql")
    for pod in pods.items:
        if pod.status.phase != "Running": continue
        pname = pod.metadata.name
        if hint and hint.lower() not in pname.lower(): continue
        for container in (pod.spec.containers or []):
            if any(h in (container.image or "").lower() for h in _DB_IMAGE_HINTS):
                db_type = _detect_db_type(pname, namespace)
                if db_type: return pname, db_type
    return None, None

def _exec_simple_query(pod_name: str, namespace: str, container_name: str, cmd: str) -> str:
    from kubernetes.stream import stream as _k8s_stream
    try:
        kwargs = dict(stderr=False, stdin=False, stdout=True, tty=False, _preload_content=True)
        if container_name: kwargs["container"] = container_name
        resp = _k8s_stream(
            _core.connect_get_namespaced_pod_exec, pod_name, namespace,
            command=["/bin/sh", "-c", cmd], **kwargs)
        return resp.strip() if isinstance(resp, str) else ""
    except Exception:
        return ""

def _discover_pg_database(pod_name: str, namespace: str, container_name: str,
                           user: str, password: str) -> str:
    pg_env    = f"PGPASSWORD='{password}' " if password else ""
    user_flag = f"-U {user}" if user else ""
    inner_sql = "SELECT datname FROM pg_database WHERE datistemplate=false ORDER BY datname"
    cmd = (f"{pg_env}psql {user_flag} -d postgres --no-password -t -A -c \"{inner_sql}\" 2>/dev/null || "
           f"psql -d postgres -t -A -c \"{inner_sql}\"")
    out = _exec_simple_query(pod_name, namespace, container_name, cmd)
    for db in out.splitlines():
        db = db.strip()
        if db and db.lower() not in _PG_SYSTEM_DBS: return db
    return "postgres"

def _discover_mysql_database(pod_name: str, namespace: str, container_name: str,
                              user: str, password: str, host: str, port: str) -> str:
    pass_arg = f"-p'{password}'" if password else ""
    cmd = (f"mysql -u{user} {pass_arg} -h{host} -P{port} "
           f"--connect-timeout=5 --batch --silent -e 'SHOW DATABASES'")
    out = _exec_simple_query(pod_name, namespace, container_name, cmd)
    for db in out.splitlines():
        db = db.strip()
        if db and db.lower() not in _MYSQL_SYSTEM_DBS: return db
    return ""

def exec_db_query(namespace: str, sql: str, pod_name: str = "", database: str = "",
                  container: str = "") -> str:
    if not _ALLOW_DB_EXEC:
        return "[BLOCKED] DB exec is disabled (ALLOW_DB_EXEC=false)."
    if not sql.strip():
        return "[ERROR] Empty SQL query."
    if _SQL_WRITE_RE.match(sql):
        return "[BLOCKED] Write operations are not permitted. Only SELECT/read queries allowed."

    if not pod_name:
        pod_name, db_type = _find_db_pod(namespace)
        if not pod_name:
            return f"[ERROR] No running DB pod found in namespace '{namespace}'."
        if not container:
            container = _find_db_container(pod_name, namespace, db_type)
    else:
        db_type = _detect_db_type(pod_name, namespace, container)
        if not db_type:
            return f"[ERROR] Could not detect DB type for pod '{pod_name}' in '{namespace}'."
        if not container:
            container = _find_db_container(pod_name, namespace, db_type)

    creds = _find_db_credentials(namespace, pod_name)
    if not creds.get("user") or not creds.get("password"):
        return f"[ERROR] Could not retrieve DB credentials from pod '{pod_name}'."

    db_name  = database or creds.get("database") or ""
    user     = creds["user"]
    password = creds["password"]
    host     = creds.get("host") or "127.0.0.1"
    port     = creds.get("port") or ("5432" if db_type == "postgres" else "3306")

    pg_sql = sql
    if db_type == "postgres":
        if re.match(r"^\s*SHOW\s+TABLES\s*$", pg_sql, re.IGNORECASE):
            pg_sql = "SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN ('information_schema','pg_catalog','pg_toast') ORDER BY schemaname, tablename"
        elif re.match(r"^\s*SHOW\s+DATABASES\s*$", pg_sql, re.IGNORECASE):
            pg_sql = "SELECT datname FROM pg_database ORDER BY datname"
        desc_m = re.match(r"^\s*DESCRIBE\s+(\S+)\s*$", pg_sql, re.IGNORECASE)
        if desc_m:
            tbl    = desc_m.group(1).strip('`"\'')
            pg_sql = (f"SELECT column_name, data_type, is_nullable, column_default "
                      f"FROM information_schema.columns WHERE table_name = '{tbl}' ORDER BY ordinal_position")

    safe_sql = pg_sql.replace("'", "'\\''")

    if db_type in ("mysql","mariadb"):
        pass_flag = f"-p'{password}'" if password else ""
        cmd = f"mysql -u{user} {pass_flag} -h{host} -P{port} --batch --silent {db_name} -e '{safe_sql}'"
    elif db_type == "postgres":
        pg_env    = f"PGPASSWORD='{password}' " if password else ""
        db_flag   = f"-d {db_name}" if db_name else ""
        user_flag = f"-U {user}" if user else ""
        if host not in ("localhost","127.0.0.1","::1"):
            cmd = f"{pg_env}psql {user_flag} -h {host} -p {port} {db_flag} --no-password -t -A -c '{safe_sql}'"
        else:
            cmd = f"{pg_env}psql {user_flag} {db_flag} --no-password -t -A -c '{safe_sql}'"
    else:
        return f"[ERROR] Unsupported DB type: {db_type}"

    from kubernetes.stream import stream as _k8s_stream
    try:
        resp = _k8s_stream(
            _core.connect_get_namespaced_pod_exec, pod_name, namespace,
            command=["/bin/sh", "-c", cmd],
            container=container,
            stderr=True, stdin=False, stdout=True, tty=False, _preload_content=True)
        output = resp.strip() if isinstance(resp, str) else str(resp).strip()
    except Exception as exc:
        return f"[ERROR] Exec failed: {exc}"

    if not output:
        return "(Query returned no rows.)"
    if len(output) > _KUBECTL_MAX_OUT:
        output = output[:_KUBECTL_MAX_OUT] + f"\n...[output truncated at {_KUBECTL_MAX_OUT} chars]"

    m = re.match(r"^\s*SELECT\s+(.+?)\s+FROM\b", pg_sql, re.IGNORECASE | re.DOTALL)
    if m:
        cols = []
        for col in m.group(1).split(","):
            col = col.strip()
            alias = re.search(r"\bAS\s+(\w+)\s*$", col, re.IGNORECASE)
            if alias:
                cols.append(alias.group(1))
            else:
                token = re.split(r"[\s.(]", col)[-1].strip(")'\"")
                cols.append(token if token else col)
        if cols:
            output = "|".join(cols) + "\n" + "|".join("-" * max(len(c),4) for c in cols) + "\n" + output

    header = (f"DB query result [{db_type.upper()} · pod={pod_name} · ns={namespace}"
              + (f" · db={db_name}" if db_name else "") + "]\n" + "-" * 60 + "\n")
    return header + output

def _resolve_crd_version(group: str, plural: str) -> str:
    try:
        ext = _k8s.ApiextensionsV1Api()
        crd = ext.read_custom_resource_definition(f"{plural}.{group}")
        for v in crd.spec.versions:
            if v.storage: return v.name
        return crd.spec.versions[0].name
    except Exception:
        return "v1"

def _list_custom_all(plural: str, group: str, version: str) -> list:
    try:
        return _k8s.CustomObjectsApi().list_cluster_custom_object(group, version, plural).get("items", [])
    except Exception:
        return []

def _list_custom_ns(ns: str, plural: str, group: str, version: str) -> list:
    try:
        return _k8s.CustomObjectsApi().list_namespaced_custom_object(group, version, ns, plural).get("items", [])
    except Exception:
        return []

def _get_custom(name: str, ns: str, plural: str, group: str, version: str) -> dict:
    custom = _k8s.CustomObjectsApi()
    if ns: return custom.get_namespaced_custom_object(group, version, ns, plural, name)
    return custom.get_cluster_custom_object(group, version, plural, name)

def _get_resource_fns(resource: str):
    r = resource.lower()
    _POLICY = _k8s.PolicyV1Api()
    _TABLE = [
        (("pod","pods","po"),                             (_core.list_pod_for_all_namespaces, _core.list_namespaced_pod, _core.read_namespaced_pod, "Pod")),
        (("deployment","deployments","deploy"),           (_apps.list_deployment_for_all_namespaces, _apps.list_namespaced_deployment, _apps.read_namespaced_deployment, "Deployment")),
        (("replicaset","replicasets","rs"),               (_apps.list_replica_set_for_all_namespaces, _apps.list_namespaced_replica_set, _apps.read_namespaced_replica_set, "ReplicaSet")),
        (("statefulset","statefulsets","sts"),            (_apps.list_stateful_set_for_all_namespaces, _apps.list_namespaced_stateful_set, _apps.read_namespaced_stateful_set, "StatefulSet")),
        (("daemonset","daemonsets","ds"),                 (_apps.list_daemon_set_for_all_namespaces, _apps.list_namespaced_daemon_set, _apps.read_namespaced_daemon_set, "DaemonSet")),
        (("service","services","svc"),                   (_core.list_service_for_all_namespaces, _core.list_namespaced_service, _core.read_namespaced_service, "Service")),
        (("configmap","configmaps","cm"),                (_core.list_config_map_for_all_namespaces, _core.list_namespaced_config_map, _core.read_namespaced_config_map, "ConfigMap")),
        (("secret","secrets"),                           (_core.list_secret_for_all_namespaces, _core.list_namespaced_secret, _core.read_namespaced_secret, "Secret")),
        (("persistentvolumeclaim","persistentvolumeclaims","pvc","pvcs"), (_core.list_persistent_volume_claim_for_all_namespaces, _core.list_namespaced_persistent_volume_claim, _core.read_namespaced_persistent_volume_claim, "PersistentVolumeClaim")),
        (("persistentvolume","persistentvolumes","pv","pvs"), (_core.list_persistent_volume, None, lambda n, ns: _core.read_persistent_volume(n), "PersistentVolume")),
        (("node","nodes","no"),                          (_core.list_node, None, lambda n, ns: _core.read_node(n), "Node")),
        (("namespace","namespaces","ns"),                (_core.list_namespace, None, lambda n, ns: _core.read_namespace(n), "Namespace")),
        (("job","jobs"),                                 (_batch.list_job_for_all_namespaces, _batch.list_namespaced_job, _batch.read_namespaced_job, "Job")),
        (("cronjob","cronjobs","cj"),                    (_batch.list_cron_job_for_all_namespaces, _batch.list_namespaced_cron_job, _batch.read_namespaced_cron_job, "CronJob")),
        (("ingress","ingresses","ing"),                  (_net.list_ingress_for_all_namespaces, _net.list_namespaced_ingress, _net.read_namespaced_ingress, "Ingress")),
        (("horizontalpodautoscaler","horizontalpodautoscalers","hpa","hpas"), (_autoscaling.list_horizontal_pod_autoscaler_for_all_namespaces, _autoscaling.list_namespaced_horizontal_pod_autoscaler, _autoscaling.read_namespaced_horizontal_pod_autoscaler, "HorizontalPodAutoscaler")),
        (("event","events","ev"),                        (_core.list_event_for_all_namespaces, _core.list_namespaced_event, _core.read_namespaced_event, "Event")),
        (("role","roles"),                               (_rbac.list_role_for_all_namespaces, _rbac.list_namespaced_role, _rbac.read_namespaced_role, "Role")),
        (("clusterrole","clusterroles"),                 (_rbac.list_cluster_role, None, lambda n, ns: _rbac.read_cluster_role(n), "ClusterRole")),
        (("rolebinding","rolebindings"),                 (_rbac.list_role_binding_for_all_namespaces, _rbac.list_namespaced_role_binding, _rbac.read_namespaced_role_binding, "RoleBinding")),
        (("clusterrolebinding","clusterrolebindings"),   (_rbac.list_cluster_role_binding, None, lambda n, ns: _rbac.read_cluster_role_binding(n), "ClusterRoleBinding")),
        (("serviceaccount","serviceaccounts","sa"),      (_core.list_service_account_for_all_namespaces, _core.list_namespaced_service_account, _core.read_namespaced_service_account, "ServiceAccount")),
        (("storageclass","storageclasses","sc"),         (_storage.list_storage_class, None, lambda n, ns: _storage.read_storage_class(n), "StorageClass")),
        (("endpoints","endpoint","ep"),                  (_core.list_endpoints_for_all_namespaces, _core.list_namespaced_endpoints, _core.read_namespaced_endpoints, "Endpoints")),
        (("networkpolicy","networkpolicies","np"),        (_net.list_network_policy_for_all_namespaces, _net.list_namespaced_network_policy, _net.read_namespaced_network_policy, "NetworkPolicy")),
        (("poddisruptionbudget","poddisruptionbudgets","pdb"), (_POLICY.list_pod_disruption_budget_for_all_namespaces, _POLICY.list_namespaced_pod_disruption_budget, _POLICY.read_namespaced_pod_disruption_budget, "PodDisruptionBudget")),
    ]
    for aliases, row in _TABLE:
        if r in aliases:
            list_all_fn, list_ns_fn, get_one_fn, kind = row
            list_all = lambda fs="", _f=list_all_fn: _paginate(_f, field_selector=fs)
            list_ns  = (lambda ns, fs="", _f=list_ns_fn: _paginate(_f, ns, field_selector=fs)) if list_ns_fn else None
            return list_all, list_ns, get_one_fn, kind
    if "." in resource:
        plural, group = resource.split(".", 1)
        version = _resolve_crd_version(group, plural)
        return (
            lambda fs="", _p=plural, _g=group, _v=version: _list_custom_all(_p, _g, _v),
            lambda ns, fs="", _p=plural, _g=group, _v=version: _list_custom_ns(ns, _p, _g, _v),
            lambda name, ns, _p=plural, _g=group, _v=version: _get_custom(name, ns, _p, _g, _v),
            resource,
        )
    return None

def _fmt_pod(p) -> str:
    ns, name  = p.metadata.namespace or "", p.metadata.name
    ready_cs  = p.status.container_statuses or []
    ready     = sum(1 for c in ready_cs if c.ready)
    total     = len(ready_cs) or len(p.spec.containers or [])
    restarts  = sum(c.restart_count or 0 for c in ready_cs)
    age       = _age(p.metadata.creation_timestamp)
    row = f"{name:<50} {ready}/{total}  {restarts:<6} {p.status.phase or 'Unknown':<12} {p.spec.node_name or '<none>':<30} {age}"
    return (f"{ns:<30} " + row) if ns else row

def _fmt_node(n) -> str:
    role  = (",".join(k.split("/")[-1] for k in (n.metadata.labels or {})
                      if "node-role.kubernetes.io" in k) or "worker")
    conds = {c.type: c.status for c in (n.status.conditions or [])}
    ver   = n.status.node_info.kubelet_version if n.status and n.status.node_info else ""
    return f"{n.metadata.name:<40} {role:<20} {'Ready' if conds.get('Ready')=='True' else 'NotReady':<10} {_age(n.metadata.creation_timestamp):<10} {ver}"

def _fmt_deployment(d) -> str:
    ns, name  = d.metadata.namespace or "", d.metadata.name
    ready     = d.status.ready_replicas or 0
    desired   = d.spec.replicas or 0
    available = d.status.available_replicas or 0
    age       = _age(d.metadata.creation_timestamp)
    row = f"{name:<50} {ready}/{desired}  available={available}  {age}"
    return (f"{ns:<30} " + row) if ns else row

def _obj_to_table(items, kind: str) -> str:
    if not items:
        return f"No {kind} resources found."
    lines = []
    k = kind.lower()
    if k == "pod":
        ns_col = any(getattr(p.metadata, "namespace", None) for p in items)
        lines.append((f"{'NAMESPACE':<30} " if ns_col else "") +
                     f"{'NAME':<50} {'READY':<8} {'RESTARTS':<8} {'STATUS':<12} {'NODE':<30} {'AGE'}")
        for p in items: lines.append(_fmt_pod(p))
    elif k == "deployment":
        lines.append(f"{'NAMESPACE':<30} {'NAME':<50} {'READY':<8} {'AVAILABLE':<12} {'AGE'}")
        for d in items: lines.append(_fmt_deployment(d))
    elif k == "node":
        lines.append(f"{'NAME':<40} {'ROLES':<20} {'STATUS':<10} {'AGE':<10} {'VERSION'}")
        for n in items: lines.append(_fmt_node(n))
    elif k == "namespace":
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
                f"{ev.type or '':<10} {ev.reason or '':<25} {obj_ref:<40} {(ev.message or '')[:80]}")
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
            try:
                ts_str = meta.get("creationTimestamp")
                age    = _age(datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))) if ts_str else "<unknown>"
            except Exception:
                age = "<unknown>"
            status = item.get("status", {})
            state  = status.get("state", status.get("phase", status.get("robustness", "")))
            lines.append(f"  {meta.get('namespace',''):<30} {meta.get('name',''):<50} {age}"
                         + (f"  state={state}" if state else ""))
    else:
        lines.append(f"{'NAME':<50} {'AGE'}")
        for item in items:
            lines.append(f"  {item.get('metadata',{}).get('name',''):<50} <n/a>")
    return "\n".join(lines)

def _handle_get(p: dict) -> str:
    fns = _get_resource_fns(p["resource"])
    if fns is None:
        return f"[ERROR] Unsupported resource type: {p['resource']!r}."
    list_all, list_ns, get_one, kind = fns
    try:
        if p["name"]:
            obj = get_one(p["name"], p["namespace"])
            if p["output_format"] in ("json","-ojson"):
                return _json.dumps(_k8s.ApiClient().sanitize_for_serialization(obj), indent=2)
            return _obj_to_yaml(obj)
        items = list_all(p["field_selector"]) if p["all_namespaces"] or list_ns is None else list_ns(p["namespace"], p["field_selector"])
        if p["output_format"] in ("yaml","-oyaml"):
            return _yaml.dump([_k8s.ApiClient().sanitize_for_serialization(o) for o in items],
                              default_flow_style=False, allow_unicode=True)
        if p["output_format"] in ("json","-ojson"):
            return _json.dumps([_k8s.ApiClient().sanitize_for_serialization(o) for o in items], indent=2)
        if items and isinstance(items[0], dict):
            return _custom_to_table(items, kind)
        return _obj_to_table(items, kind)
    except ApiException as e:
        return f"[ERROR] API error getting {p['resource']}: {_safe_reason(e)}"


def _handle_describe(p: dict) -> str:
    fns = _get_resource_fns(p["resource"])
    if fns is None:
        return f"[ERROR] Unsupported resource type: {p['resource']!r}"
    _, _, get_one, _ = fns
    try:
        return _obj_to_yaml(get_one(p["name"] or "", p["namespace"]))
    except ApiException as e:
        return f"[ERROR] API error describing {p['resource']}/{p['name']}: {e.reason}"


def _handle_top(p: dict) -> str:
    if p["resource"] in ("node", "nodes", "no"):
        return get_top_nodes()
    else:
        ns = "all" if p["all_namespaces"] else p["namespace"]
        return get_top_pods(namespace=ns)


def _handle_rollout(p: dict) -> str:
    subverb = p["args"][1] if len(p["args"]) > 1 else p["subcommand"]
    ref     = p["args"][2] if len(p["args"]) > 2 else ""
    name    = ref.split("/", 1)[-1] if "/" in ref else (ref or p["name"] or "")
    try:
        d = _apps.read_namespaced_deployment(name, p["namespace"])
        if subverb == "status":
            ready   = d.status.ready_replicas or 0
            desired = d.spec.replicas or 0
            if ready == desired == (d.status.available_replicas or 0) == (d.status.updated_replicas or 0):
                return f"deployment.apps/{name} successfully rolled out"
            return f"Waiting: {ready}/{desired} replicas ready"
        if subverb == "history":
            return f"deployment.apps/{name}\nREVISION  CHANGE-CAUSE\n(Revision history requires kubectl or annotations)"
        return f"[ERROR] Unsupported rollout subcommand: {subverb!r}"
    except ApiException as e:
        return f"[ERROR] {e.reason}"


def _handle_api_resources() -> str:
    lines = ["Available API resources (group/version | kind | namespaced):"]
    try:
        core = _k8s.CoreV1Api()
        for r in core.get_api_resources().resources:
            if "/" not in r.name:
                lines.append(f"  v1 | {r.kind} ({r.name}) | {'true' if r.namespaced else 'false'}")
    except Exception as e:
        lines.append(f"  [ERROR] Core resources: {e}")
    try:
        apiclient = _k8s.ApisApi()
        for grp in apiclient.get_api_versions().groups:
            version = grp.preferred_version.group_version
            try:
                resources = _k8s.ApiClient().call_api(
                    f"/apis/{version}", "GET",
                    response_type="object", _return_http_data_only=True)
                for r in (resources.get("resources") or []):
                    if "/" not in r.get("name", ""):
                        lines.append(
                            f"  {version} | {r.get('kind', '?')} ({r['name']}) | {r.get('namespaced', '?')}"
                        )
            except Exception:
                pass
    except Exception as e:
        lines.append(f"  [ERROR] API groups: {e}")
    try:
        ext  = _k8s.ApiextensionsV1Api()
        crds = ext.list_custom_resource_definition()
        if crds.items:
            lines.append("\nCustom Resource Definitions:")
            for crd in crds.items:
                lines.append(f"  {crd.metadata.name}")
    except Exception as e:
        lines.append(f"  [ERROR] CRDs: {e}")
    return "\n".join(lines)


def kubectl_exec(command: str) -> str:
    command = command.strip()
    _log.info(f"[kubectl_exec] {command!r}")
    if not re.match(r"^kubectl(\s|$)", command):
        return "[ERROR] Command must start with 'kubectl'."
    _SHELL_OPS = re.compile(r'(\|\||&&|(?<!<)>(?!>)|\bawk\b|\bgrep\b|\bsed\b|\bcut\b|\bwc\b|2>/dev/null)')
    if _SHELL_OPS.search(command):
        return ("[ERROR] Shell operators and pipes are not supported. "
                "Use a dedicated tool like get_pod_status(), get_events(), etc.")
    p    = _parse_kubectl(command)
    verb = p["verb"]
    if verb in _BLOCKED_VERBS:
        return f"[ERROR] {_BLOCKED_VERBS[verb]}"
    if verb in _KUBECTL_WRITE_VERBS and not _ALLOW_WRITES:
        return (f"[ERROR] Write operation '{verb}' is disabled. "
                "Set KUBECTL_ALLOW_WRITES=true to enable writes.")
    if verb == "logs":
        return "[ERROR] Use get_pod_logs() tool for log retrieval — it supports auto-container selection and fallback."
    if verb == "version":
        return get_cluster_version()
    if verb == "auth":
        return "[ERROR] kubectl auth can-i is not implemented in API mode."
    _DISPATCH = {
        "get":           _handle_get,
        "describe":      _handle_describe,
        "top":           _handle_top,
        "rollout":       _handle_rollout,
        "api-resources": lambda _: _handle_api_resources(),
    }
    try:
        handler = _DISPATCH.get(verb)
        if handler:
            out = handler(p)
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
    tokens = shlex.split(command.strip())
    if tokens and tokens[0] == "kubectl":
        tokens = tokens[1:]
    result = {"verb":"","resource":"","name":"","namespace":"default",
              "all_namespaces":False,"output_format":"","field_selector":"",
              "tail":100,"container":"","subcommand":"","args":[],"flags":{}}
    if not tokens: return result
    result["verb"] = tokens[0]; tokens = tokens[1:]
    i, positional = 0, []
    while i < len(tokens):
        t = tokens[i]
        if   t in ("-n","--namespace") and i+1<len(tokens):     result["namespace"] = tokens[i+1]; i+=2
        elif t.startswith("--namespace="):                        result["namespace"] = t.split("=",1)[1]; i+=1
        elif t.startswith("-n") and len(t)>2:                    result["namespace"] = t[2:]; i+=1
        elif t in ("-A","--all-namespaces"):                      result["all_namespaces"] = True; i+=1
        elif t in ("-o","--output") and i+1<len(tokens):         result["output_format"] = tokens[i+1]; i+=2
        elif t.startswith("--output=") or t.startswith("-o"):    result["output_format"] = t.split("=",1)[-1].lstrip("-o"); i+=1
        elif t.startswith("--field-selector="):                  result["field_selector"] = t.split("=",1)[1]; i+=1
        elif t == "--field-selector" and i+1<len(tokens):        result["field_selector"] = tokens[i+1]; i+=2
        elif t.startswith("--tail="):
            try: result["tail"] = int(t.split("=")[1])
            except ValueError: pass
            i+=1
        elif t in ("-c","--container") and i+1<len(tokens):     result["container"] = tokens[i+1]; i+=2
        elif t.startswith("--container="):                        result["container"] = t.split("=",1)[1]; i+=1
        elif t in ("--no-headers","--show-kind","--show-labels"): i+=1
        elif t.startswith("-"):
            if i+1<len(tokens) and not tokens[i+1].startswith("-"): result["flags"][t] = tokens[i+1]; i+=2
            else: result["flags"][t] = True; i+=1
        else:
            positional.append(t); i+=1
    result["args"] = positional
    if positional:
        result["resource"] = positional[0]
        if len(positional) >= 2: result["name"]       = positional[1]
        if len(positional) >= 3: result["subcommand"] = positional[2]
    return result

def kubectl_exec(command: str) -> str:
    command = command.strip()
    _log.info(f"[kubectl_exec] {command!r}")
    if not re.match(r"^kubectl(\s|$)", command):
        return "[ERROR] Command must start with 'kubectl'."
    _SHELL_OPS = re.compile(r'(\|\||&&|(?<!<)>(?!>)|\bawk\b|\bgrep\b|\bsed\b|\bcut\b|\bwc\b|2>/dev/null)')
    if _SHELL_OPS.search(command):
        return ("[ERROR] Shell operators and pipes are not supported. "
                "Use a dedicated tool like get_pod_status(), get_events(), etc.")
    p    = _parse_kubectl(command)
    verb = p["verb"]
    if verb in _BLOCKED_VERBS:
        return f"[ERROR] {_BLOCKED_VERBS[verb]}"
    if verb in _KUBECTL_WRITE_VERBS and not _ALLOW_WRITES:
        return (f"[ERROR] Write operation '{verb}' is disabled. "
                "Set KUBECTL_ALLOW_WRITES=true to enable writes.")
    _DISPATCH = {
        "get":           _handle_get,
        "describe":      _handle_describe,
        "logs":          _handle_logs,
        "top":           _handle_top,
        "rollout":       _handle_rollout,
        "auth":          _handle_auth_cani,
        "api-resources": lambda _: _handle_api_resources(),
        "version":       lambda _: _handle_version(),
    }
    try:
        handler = _DISPATCH.get(verb)
        if handler:
            out = handler(p)
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
