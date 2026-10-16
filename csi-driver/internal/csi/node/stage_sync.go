// Scheduling-driven sync replication at the node: deciding, per PVC at stage
// time, whether a volume is managed by a synchronous VolumeReplication and must
// follow its pod, and (later) performing that switchover
// (design-sync-replication-csi-addons.md §6). The detection here is the gate in
// front of the ordinary staging path; the switchover itself lands in a later
// step.
package node

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/types"

	"github.com/simplyblock/atlas/errs"
	"github.com/simplyblock/atlas/kube"

	csicommon "github.com/simplyblock/csi-driver/internal/csi/common"
)

// syncReplicationResolver is the narrow slice of the atlas-lib resolver the node
// plugin needs: the per-PVC lookup of the synchronous VolumeReplication, and of
// the synchronous VolumeGroupReplication, that protects a claim. The site is
// carried on the object itself (its SyncSiteAnnotation), not selected from a
// class, so no class lookup is needed (design §6.2, §14). *kube.InformerResolver
// satisfies it; the group lookup returns errs.ErrUnsupported until the group
// informers are enabled, which the node treats as "not a group member."
type syncReplicationResolver interface {
	SyncVolumeReplicationForClaim(ctx context.Context, claim types.NamespacedName) (kube.VolumeReplicationRef, error)
	SyncVolumeGroupReplicationForClaim(
		ctx context.Context, claim types.NamespacedName, claimLabels map[string]string,
	) (kube.VolumeGroupReplicationRef, error)
}

// The VolumeReplication spec.replicationState and status.state values the
// switchover sets and the gate reads (csi-addons: lowercase spec verb, titled
// status).
const (
	replicationStatePrimary   = "primary"
	replicationStateSecondary = "secondary"
	observedStatePrimary      = "Primary"
	observedStateSecondary    = "Secondary"
)

// detectSyncReplication decides, per PVC at stage time, whether vc's volume is
// managed by a synchronous VolumeReplication and must follow its pod (design
// §6.6). It returns (ref, true, nil) for a sync-managed volume, (zero, false,
// nil) for an ordinary one, and a retryable error when the determination cannot
// be made, so a sync volume is never staged on the ordinary path on a guess.
//
// The determination is per PVC, from the informer cache, and trusts a synced
// cache: a not-found read on synced caches is an ordinary volume. It fails
// closed only while the caches have not synced, which is the one window a
// not-found could be a sync volume the cache has not seen yet (design §6.6).
func (ns *Server) detectSyncReplication(
	ctx context.Context, vc map[string]string,
) (kube.VolumeReplicationRef, bool, error) {
	if ns.syncResolver == nil {
		// Sync replication is not configured on this node, so every volume stages
		// as it does today: no lookup, no gate.
		return kube.VolumeReplicationRef{}, false, nil
	}
	if ns.syncCachesSynced == nil || !ns.syncCachesSynced() {
		return kube.VolumeReplicationRef{}, false, status.Error(codes.Unavailable,
			"sync-replication informer caches are not synced yet; retry staging")
	}
	claimNS, claimName := vc[csicommon.CSIStorageNamespaceKey], vc[csicommon.CSIStorageNameKey]
	if claimNS == "" || claimName == "" {
		return kube.VolumeReplicationRef{}, false, status.Error(codes.FailedPrecondition,
			"cannot determine sync-replication membership: the volume carries no PVC identity "+
				"(external-provisioner --extra-create-metadata is required on a sync cluster)")
	}

	ref, err := ns.syncResolver.SyncVolumeReplicationForClaim(ctx,
		types.NamespacedName{Namespace: claimNS, Name: claimName})
	switch {
	case errors.Is(err, errs.ErrNotFound):
		// No synchronous VolumeReplication protects this PVC, so it is an ordinary
		// volume. The synced caches make this answer trustworthy.
		return kube.VolumeReplicationRef{}, false, nil
	case err != nil:
		return kube.VolumeReplicationRef{}, false, status.Error(codes.Unavailable, err.Error())
	default:
		return ref, true, nil
	}
}

// ensureSyncPromoted drives the switchover for a sync-managed volume so it is
// served on this node's site before the ordinary attach runs (design §6.2-§6.4).
// It sets the VolumeReplication's SyncSiteAnnotation to this node's site, as
// primary, and then gates: it returns nil only once the controller reports the
// promote complete, at which point staging falls through to the ordinary attach,
// which connects to the now-led (this) site. Every other outcome is a retryable
// error so the kubelet re-stages. It never blocks: one step per call, no inline
// poll (design §6.4, return-and-retry).
func (ns *Server) ensureSyncPromoted(ctx context.Context, vr kube.VolumeReplicationRef) error {
	if ns.syncNodeSite == nil || ns.syncPatchVR == nil {
		return status.Error(codes.Internal,
			"sync replication is half-configured: the node-site reader or the VolumeReplication patch is unset")
	}
	site, err := ns.syncNodeSite(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "read this node's site: %v", err)
	}

	// A move forces the promote to re-run, the way RamenDR drives a failover: the
	// site rides on the mutable sync-site annotation, but an annotation-only change
	// leaves the VolumeReplication already `primary`/Completed, so the csi-addons
	// controller never re-issues PromoteVolume and the backend's leadership never
	// moves. Driving replicationState primary -> secondary -> primary is a real
	// spec transition the controller acts on, re-issuing DemoteVolume then
	// PromoteVolume (design §6.3, §6.4). The gate below can only release once that
	// fresh promote completes, because the status it reads is site-agnostic and
	// would otherwise be stale.
	//
	// Step 1 -- demote the OLD site: the volume is promoted (Primary, Completed) on
	// the site the annotation currently names. Set replicationState: secondary while
	// KEEPING that annotation, so DemoteVolume targets the old site. The backend's
	// planned promote to this site is refused ("409 not demoted on site <old>")
	// until the old site is demoted -- the failover does not fence it for us -- so
	// the demote has to land on the old site, not this one.
	if vr.Site != site && vr.ReplicationState == replicationStatePrimary &&
		vr.ObservedState == observedStatePrimary && vr.Completed {
		if err := ns.syncPatchVR(ctx, vr, vr.Site, replicationStateSecondary); err != nil {
			return status.Errorf(codes.Unavailable,
				"demote old site %q of %s/%s before switchover: %v", vr.Site, vr.Namespace, vr.Name, err)
		}
		return status.Errorf(codes.Unavailable,
			"demoting old site %q of %s/%s before switchover to %q; retrying", vr.Site, vr.Namespace, vr.Name, site)
	}

	// Step 2 -- promote THIS site: the old site's demote is in flight. Once the
	// controller has observed it (status Secondary), move the annotation here and
	// set primary, which it turns into PromoteVolume -> failover?site=<this site>;
	// the old site is demoted now, so the planned promote is accepted. Waiting for
	// the demote to be observed is what makes secondary -> primary a transition the
	// controller sees, rather than a no-op that collapses back to primary.
	if vr.ReplicationState == replicationStateSecondary {
		if vr.ObservedState != observedStateSecondary {
			return status.Errorf(codes.Unavailable,
				"demote before switchover to site %q of %s/%s is converging; retrying", site, vr.Namespace, vr.Name)
		}
		if err := ns.syncPatchVR(ctx, vr, site, replicationStatePrimary); err != nil {
			return status.Errorf(codes.Unavailable,
				"promote site %q on %s/%s: %v", site, vr.Namespace, vr.Name, err)
		}
		return status.Errorf(codes.Unavailable,
			"promote to site %q started on %s/%s; retrying", site, vr.Namespace, vr.Name)
	}

	// First promote (or the annotation/state not yet set to this site): set the
	// sync-site annotation and replicationState: primary. On a fresh
	// VolumeReplication the controller promotes naturally, so there is nothing to
	// re-trigger and no demote step. A single mutable patch, so the class never
	// changes.
	if vr.Site != site || vr.ReplicationState != replicationStatePrimary {
		if err := ns.syncPatchVR(ctx, vr, site, replicationStatePrimary); err != nil {
			return status.Errorf(codes.Unavailable,
				"signal site %q onto VolumeReplication %s/%s: %v", site, vr.Namespace, vr.Name, err)
		}
		return status.Errorf(codes.Unavailable,
			"switchover to site %q requested for %s/%s; retrying", site, vr.Namespace, vr.Name)
	}

	// Promoted and complete on this site: the Primary state with the Completed
	// condition set, reached after the promote above rather than inherited from a
	// prior site (design §6.4, §8).
	if vr.ObservedState == observedStatePrimary && vr.Completed {
		return nil
	}
	return status.Errorf(codes.Unavailable,
		"promote of %s/%s on site %q is converging; retrying", vr.Namespace, vr.Name, site)
}

// detectSyncGroup decides, per PVC at stage time, whether vc's volume is a member
// of a synchronous VolumeGroupReplication and must follow its pod as part of the
// group (design §14). It returns (ref, true, nil) for a group member, (_, false,
// nil) for a volume that is not one (it stages ordinarily, or via the per-volume
// path), and a retryable error while the caches are not synced.
//
// A VGR names members by a label selector, so this needs the PVC's labels, which
// the volume context does not carry; syncClaimLabels reads them (one PVC read).
// It is wired only when the group informers are enabled, so on a cluster without
// the group path configured this is a no-op with no PVC read.
func (ns *Server) detectSyncGroup(
	ctx context.Context, vc map[string]string,
) (kube.VolumeGroupReplicationRef, bool, error) {
	if ns.syncResolver == nil || ns.syncClaimLabels == nil {
		// The group path is not configured on this node.
		return kube.VolumeGroupReplicationRef{}, false, nil
	}
	if ns.syncCachesSynced == nil || !ns.syncCachesSynced() {
		return kube.VolumeGroupReplicationRef{}, false, status.Error(codes.Unavailable,
			"sync-replication informer caches are not synced yet; retry staging")
	}
	claimNS, claimName := vc[csicommon.CSIStorageNamespaceKey], vc[csicommon.CSIStorageNameKey]
	if claimNS == "" || claimName == "" {
		return kube.VolumeGroupReplicationRef{}, false, status.Error(codes.FailedPrecondition,
			"cannot determine sync-replication membership: the volume carries no PVC identity "+
				"(external-provisioner --extra-create-metadata is required on a sync cluster)")
	}
	claim := types.NamespacedName{Namespace: claimNS, Name: claimName}
	labels, err := ns.syncClaimLabels(ctx, claim)
	if err != nil {
		return kube.VolumeGroupReplicationRef{}, false, status.Errorf(codes.Unavailable,
			"read labels of claim %s to resolve its consistency group: %v", claim, err)
	}
	ref, err := ns.syncResolver.SyncVolumeGroupReplicationForClaim(ctx, claim, labels)
	switch {
	case errors.Is(err, errs.ErrNotFound) || errors.Is(err, errs.ErrUnsupported):
		// Not a synchronous-group member, or the group path is not wired: the
		// volume takes the per-volume path or stages ordinarily.
		return kube.VolumeGroupReplicationRef{}, false, nil
	case err != nil:
		return kube.VolumeGroupReplicationRef{}, false, status.Error(codes.Unavailable, err.Error())
	default:
		return ref, true, nil
	}
}

// ensureGroupPromoted is the group analog of ensureSyncPromoted: it holds
// staging until this node's site is the consistency group's active site, driving
// the switchover through the one VolumeGroupReplication the way RamenDR drives a
// failover (design §14, §6.3, §6.4). The site rides on the VGR's sync-site
// annotation, and a move is forced to re-run by transitioning the VGR's
// replicationState primary -> secondary -> primary, so the csi-addons controller
// re-issues the group demote then the group promote (the backend failover to this
// site). The whole group promotes as a unit, so a pod never comes up with some
// members led from one site and some from the other.
func (ns *Server) ensureGroupPromoted(ctx context.Context, vgr kube.VolumeGroupReplicationRef) error {
	if ns.syncNodeSite == nil || ns.syncGroupPatchVGR == nil {
		return status.Error(codes.Internal,
			"sync replication is half-configured: the node-site reader or the VolumeGroupReplication patch is unset")
	}
	site, err := ns.syncNodeSite(ctx)
	if err != nil {
		return status.Errorf(codes.Unavailable, "read this node's site: %v", err)
	}

	// Step 1 -- demote the OLD site: the group is promoted (Primary, Completed) on
	// the site its annotation currently names. Set replicationState: secondary while
	// KEEPING that annotation, so the group demote targets the old site, which the
	// planned group promote to this site requires demoted first (§14).
	if vgr.Site != site && vgr.ReplicationState == replicationStatePrimary &&
		vgr.ObservedState == observedStatePrimary && vgr.Completed {
		if err := ns.syncGroupPatchVGR(ctx, vgr, vgr.Site, replicationStateSecondary); err != nil {
			return status.Errorf(codes.Unavailable,
				"demote old site %q of group %s/%s before switchover: %v", vgr.Site, vgr.Namespace, vgr.Name, err)
		}
		return status.Errorf(codes.Unavailable,
			"demoting old site %q of group %s/%s before switchover to %q; retrying", vgr.Site, vgr.Namespace, vgr.Name, site)
	}

	// Step 2 -- promote THIS site: once the old site's group demote is observed
	// (status Secondary), move the annotation here and set primary, which the
	// controller turns into the group failover to this site.
	if vgr.ReplicationState == replicationStateSecondary {
		if vgr.ObservedState != observedStateSecondary {
			return status.Errorf(codes.Unavailable,
				"group demote before switchover to site %q of %s/%s is converging; retrying", site, vgr.Namespace, vgr.Name)
		}
		if err := ns.syncGroupPatchVGR(ctx, vgr, site, replicationStatePrimary); err != nil {
			return status.Errorf(codes.Unavailable,
				"promote group %s/%s on site %q: %v", vgr.Namespace, vgr.Name, site, err)
		}
		return status.Errorf(codes.Unavailable,
			"promote of group %s/%s to site %q started; retrying", vgr.Namespace, vgr.Name, site)
	}

	// First promote (or the annotation/state not yet set to this site): set the
	// sync-site annotation and replicationState: primary. A fresh VGR promotes
	// naturally, so there is nothing to re-trigger and no demote step.
	if vgr.Site != site || vgr.ReplicationState != replicationStatePrimary {
		if err := ns.syncGroupPatchVGR(ctx, vgr, site, replicationStatePrimary); err != nil {
			return status.Errorf(codes.Unavailable,
				"signal site %q onto VolumeGroupReplication %s/%s: %v", site, vgr.Namespace, vgr.Name, err)
		}
		return status.Errorf(codes.Unavailable,
			"switchover of group %s/%s to site %q requested; retrying", vgr.Namespace, vgr.Name, site)
	}

	// Promoted and complete on this site.
	if vgr.ObservedState == observedStatePrimary && vgr.Completed {
		return nil
	}
	return status.Errorf(codes.Unavailable,
		"promote of group %s/%s on site %q is converging; retrying", vgr.Namespace, vgr.Name, site)
}
