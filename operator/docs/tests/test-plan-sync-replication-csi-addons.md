# Test Plan: Synchronous Replication through csi-addons

Related design: [`designs/design-sync-replication-csi-addons.md`](../designs/design-sync-replication-csi-addons.md)

Scenario IDs are permanent. `Type` is one of `Positive`, `Negative`, `Boundary`, `Regression`. The `Test` column holds the verbatim test function, or `—` when nothing covers the scenario yet, in which case the ID reappears in [What Is Not Yet Covered](#what-is-not-yet-covered). Nothing in this design is implemented, so every row is `—` today and this plan is the coverage the implementation must deliver.

Class prefixes: `U-` unit (mock backend HTTP and fake Kubernetes objects, no cluster), `I-` integration (`envtest` plus the csi-addons sidecar plus a mock backend), `E-` end-to-end (a live two-site synchronous cluster, the switch driven by pod scheduling), `M-` manual (needs orchestration not yet automated). The group scenarios (`G-`, Phase 2) are a planned block and carry no `Type` until the work is scoped (design §14).

The control model is scheduling-driven: the node plugin reads the pod's site, signals it onto the `VolumeReplication`, the replication controller promotes there, and `NodeStageVolume` gates the connection on the promote (design §6). There is no RamenDR for sync. The per-RPC behavior (dispatch, verbs, status) is tested on its own, and the node-stage logic and the gate are tested as their own group, because the gate is where a wrong-site attach would happen and it is the property the design exists to guarantee.

---

## 1. Unit Tests

Drive each replication RPC handler against an `httptest` backend, and the node-stage logic against fake Kubernetes objects, asserting which route is called, with which query parameters, how the backend's status code maps to the gRPC code, and when the gate releases. No cluster, because the dispatch, the classification, the correlation, and the gate decision are pure logic.

### Method dispatch (§5)

File: `csi-driver/internal/csi/controller/replication_method_test.go`

| #    | Scenario                                                                  | Type     | Test |
|------|---------------------------------------------------------------------------|----------|------|
| U-01 | No `method` parameter: the RPC takes the async path unchanged             | Negative | —    |
| U-02 | `method: async` explicit: the async path                                  | Positive | —    |
| U-03 | `method: sync`: the sync route is called                                  | Positive | —    |
| U-04 | An unrecognized `method` value is rejected, not silently treated as async | Boundary | —    |

### Site resolution (§5.1, §9.2)

File: `csi-driver/internal/csi/controller/replication_method_test.go`

| #    | Scenario                                                          | Type     | Test |
|------|-------------------------------------------------------------------|----------|------|
| U-05 | `method: sync` with `site`: the value is passed as `?site=`       | Positive | —    |
| U-06 | `method: sync` with no `site`: `InvalidArgument`, no backend call | Negative | —    |

### Promote (§7.1, §12)

File: `csi-driver/internal/csi/controller/replication_sync_promote_test.go`

| #    | Scenario                                                                                                                       | Type     | Test |
|------|--------------------------------------------------------------------------------------------------------------------------------|----------|------|
| U-07 | Unforced `PromoteVolume` maps to `planned=true`                                                                                | Positive | —    |
| U-08 | Forced `PromoteVolume` maps to `planned=false`                                                                                 | Positive | —    |
| U-09 | `409` "in progress" returns a retryable code and does not escalate                                                             | Negative | —    |
| U-10 | `200` with a `SyncPromoteResultDTO` returns success                                                                            | Positive | —    |
| U-11 | `412` peer-site-offline on an unforced promote makes the controller auto-escalate to a forced promote (`planned=false`) (§7.1) | Boundary | —    |
| U-12 | `409` gate refusal returns retryable and is not the escalation code                                                            | Boundary | —    |
| U-13 | `409` forced-while-peer-live returns retryable, force does not act on a live site                                              | Negative | —    |

### Demote (§7.2)

File: `csi-driver/internal/csi/controller/replication_sync_demote_test.go`

| #    | Scenario                                                          | Type     | Test |
|------|-------------------------------------------------------------------|----------|------|
| U-14 | `204` fenced on this site returns success                         | Positive | —    |
| U-15 | `204` not-served-here (no-op) returns success, so a retry is safe | Boundary | —    |
| U-16 | `409` gate refusal returns retryable                              | Negative | —    |
| U-17 | `500` ANA RPC failed returns retryable                            | Negative | —    |

### No-op verbs (§7.3)

File: `csi-driver/internal/csi/controller/replication_sync_noop_test.go`

| #    | Scenario                                                                        | Type     | Test |
|------|---------------------------------------------------------------------------------|----------|------|
| U-18 | `EnableVolumeReplication` on a sync class returns success with no backend call  | Negative | —    |
| U-19 | `DisableVolumeReplication` on a sync class returns success with no backend call | Negative | —    |
| U-20 | `ResyncVolume` on a sync class returns success with no backend call             | Negative | —    |

### Status mapping (§8)

File: `csi-driver/internal/csi/controller/replication_sync_status_test.go`

| #    | Scenario                                                                                                                                                       | Type     | Test |
|------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|------|
| U-21 | Healthy sync status maps to `Completed` true, `Degraded` and `Resyncing` false                                                                                 | Positive | —    |
| U-22 | A running catch-up maps to `Resyncing` true, `Completed` false                                                                                                 | Positive | —    |
| U-23 | A behind or unknown zone maps to `Degraded` true, `Completed` false                                                                                            | Negative | —    |
| U-24 | `role` primary maps to `source`, secondary to `secondary`                                                                                                      | Positive | —    |
| U-25 | In-sync reports `lastSyncTime` as the read time, behind reports the real older instant; `lag_seconds`/`bytes_behind` map to `lastSyncDuration`/`lastSyncBytes` | Boundary | —    |
| U-26 | `GetVolumeReplicationInfo` reads `replication/status` regardless of method (§5.3)                                                                              | Positive | —    |

### Node-stage site signaling and the gate (§6)

File: `csi-driver/internal/csi/node/stage_sync_test.go`

| #    | Scenario                                                                                                             | Type       | Test |
|------|----------------------------------------------------------------------------------------------------------------------|------------|------|
| U-27 | The node's site is read from its `topology.kubernetes.io/zone` label (§6.1)                                          | Positive   | —    |
| U-28 | The volume handle correlates to the `VolumeReplication` via PV `claimRef` to PVC to the referencing VR (§6.2)        | Positive   | —    |
| U-29 | The class whose `site` parameter equals the node's site is selected; the VR is patched to it and `primary` (§6.2)    | Positive   | —    |
| U-30 | A non-sync volume (no `method: sync` class, or no VR at all) stages with no site read, no patch, and no gate (§6.6)  | Regression | —    |
| U-31 | The patch is idempotent: a VR already pointed at this site's class and primary is not re-patched, staging proceeds   | Boundary   | —    |
| U-32 | The gate releases only when the VR reports `state: Primary` with `Completed` true and the class site matching (§6.4) | Positive   | —    |
| U-33 | The gate holds with a retryable error while the promote is in progress, and the re-stage re-checks status (§6.4)     | Boundary   | —    |
| U-34 | A `409` gate refusal or a `412` lost-peer surfaces as a staging error carrying the backend message (§6.4, §12)       | Negative   | —    |
| U-35 | A sync volume with no `VolumeReplication` fails staging with `FailedPrecondition`, nothing patched (§12)             | Negative   | —    |
| U-36 | No sync class exists for the node's site: staging fails with `FailedPrecondition` naming the site (§6.2, §12)        | Negative   | —    |
| U-37 | On `NodeUnstageVolume` the old site is demoted, and a demote of a not-served volume is a `204` no-op (§6.5)          | Positive   | —    |

### Misconfiguration and transport (§11, §12)

File: `csi-driver/internal/csi/controller/replication_sync_errors_test.go`

| #    | Scenario                                                                                              | Type     | Test |
|------|-------------------------------------------------------------------------------------------------------|----------|------|
| U-38 | A sync class against a non-sync cluster (`400`) returns `FailedPrecondition` with the backend message | Negative | —    |
| U-39 | A backend transport error returns a retryable code                                                    | Negative | —    |
| U-40 | The node plugin cannot reach the Kubernetes API to patch the VR: staging holds with a retryable error | Negative | —    |

---

## 2. Integration Tests

Run the full staging path and the `VolumeReplication` reconcile against the kubernetes-csi-addons sidecar and a mock backend over `envtest`, proving the node-plugin-driven switch and the condition surface.

File: `csi-driver/e2e/` (csi-addons integration), mock backend in-process

| #    | Scenario                                                                                                                                    | Type       | Test |
|------|---------------------------------------------------------------------------------------------------------------------------------------------|------------|------|
| I-01 | A patched-to-primary sync `VolumeReplication` drives `Enable` then `Promote` and serves the volume on its site                              | Positive   | —    |
| I-02 | The promote poll advances across reconciles (`409` in progress to `200`)                                                                    | Positive   | —    |
| I-03 | `Demote` on the secondary side fences the volume                                                                                            | Positive   | —    |
| I-04 | The `VolumeReplication` conditions (`Completed`, `Degraded`, `Resyncing`) reflect the sync status and `lastSyncTime` is fresh while in sync | Positive   | —    |
| I-05 | An async-class `VolumeReplication` is unaffected by the sync dispatch and is never patched by the node plugin                               | Regression | —    |
| I-06 | A sync class missing `site` surfaces an error condition and a `SyncClassMisconfigured` event                                                | Negative   | —    |
| I-07 | `NodeStageVolume` reads the node site, patches the VR to that site's class, waits out the promote poll, and attaches only after `Completed` | Positive   | —    |
| I-08 | A pod re-staged on the other site re-points the VR and moves leadership: the volume follows the pod                                         | Positive   | —    |
| I-09 | `NodeStageVolume` holds (pod stays `ContainerCreating`) on a `409` gate refusal and on a `412`, with the matching event                     | Negative   | —    |
| I-10 | The node `DaemonSet` service account can read node, PV, PVC, and classes and patch the VR, and is denied everything else (§4, RBAC)         | Positive   | —    |

### Operator provisioning (§9.5, §9.6)

Run the `ClusterDeploymentConfig` expansion, the `StorageCluster` and `StorageNode` reconcilers, and the `spec.config` webhook against `envtest` with a mock control plane, asserting the create and add bodies and the admission rules.

File: `operator/internal/controller/*_unit_test.go` and the envtest suites

| #    | Scenario                                                                                                                                          | Type     | Test |
|------|---------------------------------------------------------------------------------------------------------------------------------------------------|----------|------|
| I-11 | `ClusterTemplate.enableSyncReplication=true` expands to the `StorageCluster` toggle and sends `sync_replication: true` in the cluster-create body | Positive | —    |
| I-12 | `enableSyncReplication` is immutable: the webhook rejects a change on a live `StorageCluster`                                                     | Negative | —    |
| I-13 | A `NodeGroup.site` expands to `StorageNode.spec.config.site` and is sent as `site` in the node-add body                                           | Positive | —    |
| I-14 | On a sync cluster, a node with no `site` is rejected before add, mirroring the failure-domain check                                               | Negative | —    |
| I-15 | `StorageNode.spec.config.site` is immutable: the `spec.config` webhook rejects a change once set                                                  | Negative | —    |

---

## 3. End-to-End Tests

A live two-site synchronous cluster, the switch driven by pod scheduling, with data verified on the target site after each move. The only class that proves the gate actually prevents a wrong-site attach and that the connection entries serve the volume.

File: live-cluster recipe (see M-01 for the orchestration this shares)

| #    | Scenario                                                                                                                                                                                               | Type     | Test |
|------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|------|
| E-01 | A pod scheduled to site A comes up with its volume promoted on A, data written and read back                                                                                                           | Positive | —    |
| E-02 | The pod is rescheduled to site B (node drain or affinity change): leadership follows, data verified on B                                                                                               | Positive | —    |
| E-03 | Disaster: site A taken down, the pod is rescheduled onto B, the stage's planned promote gets `412` and the controller auto-escalates to a forced promote with no human in the loop, data verified on B | Boundary | —    |
| E-04 | Recovery: site A returns and resyncs as secondary while the pod on B keeps serving, data on B unchanged throughout                                                                                     | Positive | —    |
| E-05 | The gate blocks the attach until the promote completes: a pod never comes up connected to a site the volume is not led from                                                                            | Boundary | —    |

---

## 4. Group Tests — Phase 2 (Planned)

The consistency-group variant (design §14). A planned block: the type of each scenario is decided when the group RPC handlers are scoped.

| #    | Scenario                                                                                                                                 | Type |
|------|------------------------------------------------------------------------------------------------------------------------------------------|------|
| G-01 | One pod mounting several members of a group patches the one `VolumeGroupReplication` and promotes the group as a unit on the node's site | —    |
| G-02 | A group demote fences members in order, and a retry is a no-op for those already fenced                                                  | —    |
| G-03 | The group status reads cluster-wide plus `member_count`, never a per-member sum                                                          | —    |
| G-04 | A group with an unresolvable or foreign member refuses the whole request before fencing anything                                         | —    |
| G-05 | Every member's stage gates on the one group promote, so no pod comes up with members split across sites                                  | —    |

---

## 5. Manual Scenarios

### M-01: a store shared across the site boundary blocks a switchover

**Design reference:** §12.

**What to verify:** a promote on the target site answers `409` naming the still-served volumes (`volumes`) when the store carries a volume outside the switching set that was never demoted, and succeeds once that volume is demoted too.

**Test concept:**
1. On a two-site sync cluster, create two volumes on one store, one whose pod is moving sites and one pinned to the source site.
2. Move the first volume's pod to the target site so its stage promotes there, while the second volume stays served on the source.
3. Confirm the promote answers `409` with the undemoted volume in `volumes`, and the moving pod holds in `ContainerCreating`.
4. Move or stop the second volume too, confirm the promote then reaches `200` and the pod comes up.

**Open question:** whether the operator should detect a shared store at provision time rather than at switchover (design §12 treats it as operational).

---

## Coverage Summary

| Class                 | Scenarios | Covered | Not covered |
|-----------------------|-----------|---------|-------------|
| Unit (`U-`)           | 40        | 0       | 40          |
| Integration (`I-`)    | 15        | 0       | 15          |
| End-to-end (`E-`)     | 5         | 0       | 5           |
| Group, Phase 2 (`G-`) | 5         | 0       | 5           |
| Manual (`M-`)         | 1         | 0       | 1           |
| **Total**             | **66**    | **0**   | **66**      |

---

## What Is Not Yet Covered

Nothing is implemented: this plan defines the coverage the implementation delivers. The gaps that matter most, in the order they should land:

| #                 | Gap                                                                                                           | Reason                                                                                                                                                         |
|-------------------|---------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| U-32 through U-37 | The gate and the node-stage signal: release only on promoted-here, hold otherwise, correlate and patch the VR | The highest risk (design §6.4, §15). A gate released before the promote completes is a wrong-site attach, the one outcome the design exists to prevent.        |
| U-09, U-11, U-12  | The `409` against `412` protocol: retryable gate refusal, the escalation trigger, in-progress poll            | Second-highest risk (design §12, §15). A `409` miscoded as escalation forces a promote that should wait; a `412` miscoded as a retry hangs a disaster promote. |
| U-01 through U-08 | Method dispatch, site resolution, and `force` to `planned` inversion                                          | The feature does nothing correctly until the dispatch and the inversion are right.                                                                             |
| U-14 through U-40 | Demote, the no-op verbs, status mapping, unstage demote, and misconfiguration                                 | The rest of the per-RPC and node-stage behavior.                                                                                                               |
| I-01 through I-10 | The reconcile loop and the staging path against the sidecar, plus the node plugin RBAC                        | Proves the node-plugin-driven switch, the condition surface, and the privilege surface, which the unit tests cannot.                                           |
| I-11 through I-15 | The operator provisioning CR fields                                                                           | The cluster toggle and node site must reach the backend create and add bodies and be enforced by admission (design §9.5, §9.6).                                |
| E-01 through E-05 | Pod placement and movement, disaster, recovery resync, and the gate's wrong-site guard                        | Needs a two-site cluster; the only proof the gate prevents a wrong-site attach and the connection entries serve the volume.                                    |
| G-01 through G-05 | The group variant                                                                                             | Phase 2; testable once the group RPC handlers land (design §14).                                                                                               |
| M-01              | Shared-store blocking                                                                                         | Needs a store deliberately shared across the site boundary.                                                                                                    |
