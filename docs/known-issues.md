# Known Issues — Kubernetes Cluster

## KI-001: OOMKilled Pods Due to Memory Leak in Connection Pool

**Severity**: High  
**Affected Components**: Any service using database connection pooling  
**Symptoms**:
- Pod phase shows `OOMKilled`
- Container restart count increases rapidly
- Logs show `connection pool exhausted` or `too many clients`
- Memory usage climbs steadily before crash

**Root Cause**: Connection pool not releasing connections on timeout. Each idle connection holds ~5MB RAM.

**Diagnosis Steps**:
1. Check pod restart count: `kubectl get pods -n <namespace>`
2. Check logs: `kubectl logs <pod> --previous`
3. Look for `FATAL: remaining connection slots are reserved`

**Resolution**:
1. Restart the affected pod to clear connections (temporary fix)
2. Set `max_connections` in pool config to match DB server limit / number of replicas
3. Enable connection timeout: `pool_timeout=30, pool_recycle=1800`
4. Long term: upgrade to PgBouncer for connection pooling

---

## KI-002: CrashLoopBackOff After ConfigMap Update

**Severity**: High  
**Affected Components**: Any deployment using ConfigMaps  
**Symptoms**:
- Pod enters `CrashLoopBackOff` shortly after a ConfigMap change
- Events show `Back-off restarting failed container`
- Logs show config parsing errors or missing environment variables

**Root Cause**: Pods do not automatically reload ConfigMaps. Old pods continue with cached config until restarted, new pods may fail if ConfigMap has errors.

**Resolution**:
1. Validate ConfigMap before applying: `kubectl diff -f configmap.yaml`
2. If ConfigMap is invalid, roll back: `kubectl rollout undo deployment/<name>`
3. Use `kubectl rollout restart deployment/<name>` after a valid ConfigMap update

---

## KI-003: Node NotReady Due to Disk Pressure

**Severity**: Critical  
**Affected Components**: Worker nodes  
**Symptoms**:
- Node condition shows `DiskPressure=True`
- Node status transitions to `NotReady`
- Pods on affected node show `Evicted` status
- Events: `Evicted pod due to disk pressure`

**Root Cause**: `/var/lib/docker` or `/var/log` fills up due to accumulated container logs or image layers.

**Resolution**:
1. Check disk usage: `df -h` on the node
2. Clean unused images: `docker image prune -a`
3. Clean logs: `journalctl --vacuum-size=500M`
4. Increase node disk size if recurring

---

## KI-004: Pending Pods Due to Insufficient Resources

**Severity**: Medium  
**Affected Components**: Scheduler  
**Symptoms**:
- Pods stuck in `Pending` state
- Events show `Insufficient cpu` or `Insufficient memory`
- `kubectl describe pod` shows `0/N nodes are available`

**Root Cause**: Resource requests exceed available allocatable capacity on all nodes.

**Resolution**:
1. Check node capacity: `kubectl describe nodes | grep -A5 "Allocated resources"`
2. Review pod resource requests — reduce if over-specified
3. Add nodes to the cluster if requests are legitimate
4. Check for resource quotas: `kubectl describe resourcequota -n <namespace>`

---

## KI-005: Intermittent 503 Errors from Ingress

**Severity**: Medium  
**Affected Components**: Ingress controller, Services  
**Symptoms**:
- Sporadic HTTP 503 responses
- Pod logs show no errors
- Ingress controller logs show `upstream connect error`

**Root Cause**: Readiness probe not configured — pods receiving traffic before they are ready.

**Resolution**:
1. Add readiness probe to deployment spec
2. Ensure `initialDelaySeconds` is long enough for app startup
3. Check service selector matches pod labels exactly
