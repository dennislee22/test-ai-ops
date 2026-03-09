# Longhorn Distributed Storage — Operations Guide

## What is Longhorn

Longhorn is a cloud-native distributed block storage system for Kubernetes built by Rancher Labs (SUSE).
It provides persistent volumes backed by replicated block storage across nodes. It is the default
storage class in this cluster (`storageClassName: longhorn`).

## Namespace and Key Components

All Longhorn components run in the `longhorn-system` namespace.

| Component | Pod prefix | Role |
|---|---|---|
| longhorn-manager | longhorn-manager-* | Main controller — one per node |
| longhorn-driver-deployer | longhorn-driver-deployer-* | Deploys CSI driver |
| longhorn-ui | longhorn-ui-* | Web dashboard |
| engine image | engine-image-ei-* | Volume engine binary — one per node |
| instance manager | instance-manager-* | Manages engine/replica processes |
| CSI plugin | csi-* | Kubernetes CSI interface |

## How Volumes Work

- Each PersistentVolume has a **frontend** (what the pod mounts) and an **engine** (handles I/O).
- Data is replicated across **replica** processes, each on a different node for HA.
- Default replica count: 3. A volume needs >=2 replicas healthy to be writable.
- Volumes can be: Healthy, Degraded (missing replicas), Faulted (unwritable).

## Common Issues and Diagnosis

### Volume Degraded
**Symptoms:** Pod running but writes are slow; Longhorn UI shows "Degraded".
**Cause:** One or more replicas offline — usually because a node went down or disk filled.
**Check:** `get_pod_status namespace=longhorn-system` — look for instance-manager pods in Error/CrashLoop.
**Fix:** If node is back online, Longhorn will auto-rebuild. If disk full, expand or clean the disk.

### Volume Faulted / Pod Stuck in Pending
**Symptoms:** Pod stuck `ContainerCreating` or `Pending`; PVC not bound.
**Cause:** All replicas offline, volume engine crashed, or node taints blocking scheduling.
**Check:** `get_events namespace=longhorn-system`, `describe_pod` on the stuck pod.
**Fix:** Check if nodes hosting replicas are Ready. Longhorn may need manual replica deletion and rebuild.

### CrashLoopBackOff on instance-manager
**Symptoms:** `instance-manager-*` pod restarting.
**Cause:** Disk I/O error, kernel module missing, or iSCSI target issue.
**Check:** `get_pod_logs pod_name=<instance-manager-pod> namespace=longhorn-system`
**Fix:** Check node disk health (`smartctl`). Ensure `iscsid` service is running on all nodes.

### Node Not Schedulable for Replicas
**Symptoms:** Longhorn reports node as disabled or unschedulable.
**Cause:** Node has NoSchedule taint, Longhorn disk tag mismatch, or node not Ready.
**Check:** `get_node_health` — look for pressure conditions. Check node labels for disk tags.

### Engine Image Not Deployed
**Symptoms:** `engine-image-ei-*` pod in Error on some nodes.
**Cause:** Image pull failure (air-gapped) or container runtime issue.
**Check:** `get_pod_status namespace=longhorn-system`, `get_events namespace=longhorn-system`.
**Fix:** Ensure engine image is pre-loaded on all nodes: `ctr images import longhorn-engine.tar`.

## Key Metrics to Check

When diagnosing Longhorn issues always check these in order:
1. `get_node_health` — are all nodes Ready without disk/memory pressure?
2. `get_pod_status namespace=longhorn-system` — are all Longhorn pods Running?
3. `get_events namespace=longhorn-system` — any warnings about volumes or replicas?
4. `get_pod_logs` on any failing Longhorn pod — look for I/O errors or connection refused.

## Longhorn and cgroupv1

This cluster uses **cgroupv1**. Longhorn fully supports cgroupv1. Do NOT attempt to migrate
to cgroupv2 as it is not supported in this environment.

## Storage Classes

| Class | Provisioner | Notes |
|---|---|---|
| longhorn | driver.longhorn.io | Default — 3 replicas, RWO |
| longhorn-static | driver.longhorn.io | For pre-provisioned volumes |

## Useful Longhorn Namespaced Resources

```bash
# Check all Longhorn pods
kubectl get pods -n longhorn-system

# Check volumes
kubectl get volumes.longhorn.io -n longhorn-system

# Check replicas
kubectl get replicas.longhorn.io -n longhorn-system

# Check nodes
kubectl get nodes.longhorn.io -n longhorn-system
```
