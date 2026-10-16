package node

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/types"

	"github.com/simplyblock/atlas/errs"
	"github.com/simplyblock/atlas/kube"

	csicommon "github.com/simplyblock/csi-driver/internal/csi/common"
)

type fakeSyncResolver struct {
	ref  kube.VolumeReplicationRef
	err  error
	last types.NamespacedName

	// The group half: the VGR this fake returns for a claim, and the error (an
	// ErrNotFound means "not a group member", an ErrUnsupported means the group
	// path is not wired), plus the labels the detect path passed through.
	groupRef    kube.VolumeGroupReplicationRef
	groupErr    error
	groupLabels map[string]string
}

func (f *fakeSyncResolver) SyncVolumeReplicationForClaim(
	_ context.Context, claim types.NamespacedName,
) (kube.VolumeReplicationRef, error) {
	f.last = claim
	return f.ref, f.err
}

func (f *fakeSyncResolver) SyncVolumeGroupReplicationForClaim(
	_ context.Context, claim types.NamespacedName, claimLabels map[string]string,
) (kube.VolumeGroupReplicationRef, error) {
	f.last = claim
	f.groupLabels = claimLabels
	return f.groupRef, f.groupErr
}

// testSiteA is the node's site in these cases, reused across the sync fixtures.
const testSiteA = "site-a"

// claimCtx is the volume context external-provisioner stamps with a PVC's
// identity; namespace and name are fixed because the cases vary the VR, not the
// claim they correlate through.
func claimCtx() map[string]string {
	return map[string]string{
		csicommon.CSIStorageNamespaceKey: "app",
		csicommon.CSIStorageNameKey:      "claim1",
	}
}

func TestDetectSyncReplication(t *testing.T) {
	synced := func() bool { return true }

	t.Run("sync not configured: ordinary volume, no lookup", func(t *testing.T) {
		ns := &Server{} // syncResolver nil
		_, isSync, err := ns.detectSyncReplication(context.Background(), claimCtx())
		if err != nil || isSync {
			t.Fatalf("isSync=%v err=%v, want false/nil", isSync, err)
		}
	})

	t.Run("sync-managed volume is detected", func(t *testing.T) {
		fr := &fakeSyncResolver{ref: kube.VolumeReplicationRef{Name: "vr1", Namespace: "app", ClassName: "sb-sync-a"}}
		ns := &Server{syncResolver: fr, syncCachesSynced: synced}
		ref, isSync, err := ns.detectSyncReplication(context.Background(), claimCtx())
		if err != nil || !isSync {
			t.Fatalf("isSync=%v err=%v, want true/nil", isSync, err)
		}
		if ref.Name != "vr1" {
			t.Errorf("ref = %+v", ref)
		}
		if fr.last != (types.NamespacedName{Namespace: "app", Name: "claim1"}) {
			t.Errorf("looked up %v, want app/claim1", fr.last)
		}
	})

	t.Run("ordinary volume: not-found on synced caches is trusted", func(t *testing.T) {
		fr := &fakeSyncResolver{err: fmt.Errorf("none: %w", errs.ErrNotFound)}
		ns := &Server{syncResolver: fr, syncCachesSynced: synced}
		_, isSync, err := ns.detectSyncReplication(context.Background(), claimCtx())
		if err != nil || isSync {
			t.Fatalf("isSync=%v err=%v, want false/nil (ordinary volume)", isSync, err)
		}
	})

	t.Run("caches not synced: fail closed, retryable", func(t *testing.T) {
		fr := &fakeSyncResolver{ref: kube.VolumeReplicationRef{Name: "vr1"}}
		ns := &Server{syncResolver: fr, syncCachesSynced: func() bool { return false }}
		_, _, err := ns.detectSyncReplication(context.Background(), claimCtx())
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable (fail closed until synced)", status.Code(err))
		}
	})

	t.Run("missing PVC identity: fail closed", func(t *testing.T) {
		ns := &Server{syncResolver: &fakeSyncResolver{}, syncCachesSynced: synced}
		_, _, err := ns.detectSyncReplication(context.Background(), map[string]string{})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("code = %v, want FailedPrecondition", status.Code(err))
		}
	})

	t.Run("resolver infra error is retryable", func(t *testing.T) {
		fr := &fakeSyncResolver{err: fmt.Errorf("api down")}
		ns := &Server{syncResolver: fr, syncCachesSynced: synced}
		_, _, err := ns.detectSyncReplication(context.Background(), claimCtx())
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable", status.Code(err))
		}
	})
}

func TestEnsureSyncPromoted(t *testing.T) {
	// serverFor builds a node whose site is site-a, recording each VR patch the
	// node makes as "<site>/<replicationState>".
	serverFor := func(patched *[]string) *Server {
		return &Server{
			syncCachesSynced: func() bool { return true },
			syncNodeSite:     func(context.Context) (string, error) { return testSiteA, nil },
			syncPatchVR: func(_ context.Context, _ kube.VolumeReplicationRef, site, state string) error {
				if patched != nil {
					*patched = append(*patched, site+"/"+state)
				}
				return nil
			},
		}
	}

	t.Run("first promote: not yet on the site, not Completed -> annotation + primary", func(t *testing.T) {
		var patched []string
		ns := serverFor(&patched)
		// A fresh VR as the administrator creates it: primary, no site annotation yet,
		// not promoted anywhere.
		vr := kube.VolumeReplicationRef{Namespace: "app", Name: "vr1", Site: "", ReplicationState: "primary"}
		err := ns.ensureSyncPromoted(context.Background(), vr)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable (switch requested)", status.Code(err))
		}
		if len(patched) != 1 || patched[0] != testSiteA+"/primary" {
			t.Errorf("patched = %v, want [site-a/primary]", patched)
		}
	})

	// Regression: 2026-10-15-sync-switchover-no-op -- a move patched the sync-site
	// annotation but left replicationState `primary` unchanged, so the csi-addons
	// controller never re-issued PromoteVolume, the backend's failover never fired,
	// and leadership never moved (sbcli sync_active_site stayed put). The gate then
	// released on the stale, site-agnostic Primary/Completed status. A move must
	// force the promote to re-run RamenDR-style (primary -> secondary -> primary),
	// and the secondary step must demote the OLD site, not this one, or the backend
	// refuses the planned promote ("409 not demoted on site <old>").
	t.Run("move: promoted on another site -> demote the OLD site (secondary)", func(t *testing.T) {
		var patched []string
		ns := serverFor(&patched)
		// Promoted and Completed on site-b; this node (site-a) is the move target.
		vr := kube.VolumeReplicationRef{
			Namespace: "app", Name: "vr1", Site: "site-b", ReplicationState: "primary",
			ObservedState: "Primary", Completed: true,
		}
		err := ns.ensureSyncPromoted(context.Background(), vr)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable (switch started)", status.Code(err))
		}
		// The demote keeps the old site's annotation (site-b), so DemoteVolume
		// targets the old site; this site is promoted only after it is demoted.
		if len(patched) != 1 || patched[0] != "site-b/secondary" {
			t.Errorf("patched = %v, want [site-b/secondary] (demote the old site first)", patched)
		}
	})

	t.Run("mid-switch: on the site, secondary, demote observed -> re-promote (primary)", func(t *testing.T) {
		var patched []string
		ns := serverFor(&patched)
		vr := kube.VolumeReplicationRef{
			Namespace: "app", Name: "vr1", Site: testSiteA, ReplicationState: "secondary",
			ObservedState: "Secondary", Completed: true,
		}
		err := ns.ensureSyncPromoted(context.Background(), vr)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable (re-promote started)", status.Code(err))
		}
		if len(patched) != 1 || patched[0] != testSiteA+"/primary" {
			t.Errorf("patched = %v, want [site-a/primary]", patched)
		}
	})

	t.Run("mid-switch: secondary but demote not yet observed -> wait, no patch", func(t *testing.T) {
		var patched []string
		ns := serverFor(&patched)
		vr := kube.VolumeReplicationRef{
			Namespace: "app", Name: "vr1", Site: testSiteA, ReplicationState: "secondary",
			ObservedState: "Primary", Completed: true,
		}
		err := ns.ensureSyncPromoted(context.Background(), vr)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable (waiting for demote)", status.Code(err))
		}
		if len(patched) != 0 {
			t.Errorf("must not promote before the demote is observed, patched = %v", patched)
		}
	})

	t.Run("on the site, primary, promote still converging: retry, no patch", func(t *testing.T) {
		var patched []string
		ns := serverFor(&patched)
		vr := kube.VolumeReplicationRef{
			Namespace: "app", Name: "vr1", Site: testSiteA, ReplicationState: "primary",
			ObservedState: "Unknown", Completed: false,
		}
		err := ns.ensureSyncPromoted(context.Background(), vr)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable (converging)", status.Code(err))
		}
		if len(patched) != 0 {
			t.Errorf("a converging promote must not be re-patched, patched = %v", patched)
		}
	})

	t.Run("Primary but Completed not yet set: still converging, retry", func(t *testing.T) {
		ns := serverFor(nil)
		vr := kube.VolumeReplicationRef{
			Namespace: "app", Name: "vr1", Site: testSiteA, ReplicationState: "primary",
			ObservedState: "Primary", Completed: false,
		}
		if status.Code(ns.ensureSyncPromoted(context.Background(), vr)) != codes.Unavailable {
			t.Fatal("Primary without Completed must not proceed: the promote has not confirmed")
		}
	})

	t.Run("promoted and complete on this site: proceed", func(t *testing.T) {
		ns := serverFor(nil)
		vr := kube.VolumeReplicationRef{
			Namespace: "app", Name: "vr1", Site: testSiteA, ReplicationState: "primary",
			ObservedState: "Primary", Completed: true,
		}
		if err := ns.ensureSyncPromoted(context.Background(), vr); err != nil {
			t.Fatalf("err = %v, want nil (proceed to attach)", err)
		}
	})

	t.Run("node site unreadable: retryable", func(t *testing.T) {
		ns := serverFor(nil)
		ns.syncNodeSite = func(context.Context) (string, error) { return "", fmt.Errorf("node gone") }
		err := ns.ensureSyncPromoted(context.Background(), kube.VolumeReplicationRef{Namespace: "app", Name: "vr1"})
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable", status.Code(err))
		}
	})

	t.Run("half-configured seams: internal error", func(t *testing.T) {
		ns := &Server{syncCachesSynced: func() bool { return true }}
		err := ns.ensureSyncPromoted(context.Background(), kube.VolumeReplicationRef{Namespace: "app", Name: "vr1"})
		if status.Code(err) != codes.Internal {
			t.Fatalf("code = %v, want Internal", status.Code(err))
		}
	})
}

func TestDetectSyncGroup(t *testing.T) {
	synced := func() bool { return true }
	// groupLabels is the label the fake PVC carries; a VGR selects its members by
	// such a label, and detectSyncGroup must pass what it read to the resolver.
	labels := func() map[string]string {
		return map[string]string{"storage.simplyblock.io/consistency-group": "g1"}
	}

	t.Run("group path not configured: no lookup, not a member", func(t *testing.T) {
		// syncResolver set but syncClaimLabels nil: the group path is off.
		ns := &Server{syncResolver: &fakeSyncResolver{}, syncCachesSynced: synced}
		_, isGroup, err := ns.detectSyncGroup(context.Background(), claimCtx())
		if err != nil || isGroup {
			t.Fatalf("isGroup=%v err=%v, want false/nil", isGroup, err)
		}
	})

	t.Run("group member is detected, labels are passed through", func(t *testing.T) {
		fr := &fakeSyncResolver{groupRef: kube.VolumeGroupReplicationRef{Name: "vgr1", Namespace: "app"}}
		ns := &Server{
			syncResolver:     fr,
			syncCachesSynced: synced,
			syncClaimLabels:  func(context.Context, types.NamespacedName) (map[string]string, error) { return labels(), nil },
		}
		ref, isGroup, err := ns.detectSyncGroup(context.Background(), claimCtx())
		if err != nil || !isGroup {
			t.Fatalf("isGroup=%v err=%v, want true/nil", isGroup, err)
		}
		if ref.Name != "vgr1" {
			t.Errorf("ref = %+v", ref)
		}
		if fr.groupLabels["storage.simplyblock.io/consistency-group"] != "g1" {
			t.Errorf("labels passed = %v, want the claim's consistency-group label", fr.groupLabels)
		}
	})

	t.Run("not a group member: not-found is trusted as ordinary", func(t *testing.T) {
		fr := &fakeSyncResolver{groupErr: fmt.Errorf("none: %w", errs.ErrNotFound)}
		ns := &Server{
			syncResolver:     fr,
			syncCachesSynced: synced,
			syncClaimLabels:  func(context.Context, types.NamespacedName) (map[string]string, error) { return labels(), nil },
		}
		_, isGroup, err := ns.detectSyncGroup(context.Background(), claimCtx())
		if err != nil || isGroup {
			t.Fatalf("isGroup=%v err=%v, want false/nil (ordinary volume)", isGroup, err)
		}
	})

	t.Run("group path unsupported by the resolver: not a member", func(t *testing.T) {
		fr := &fakeSyncResolver{groupErr: fmt.Errorf("no vgr informer: %w", errs.ErrUnsupported)}
		ns := &Server{
			syncResolver:     fr,
			syncCachesSynced: synced,
			syncClaimLabels:  func(context.Context, types.NamespacedName) (map[string]string, error) { return labels(), nil },
		}
		_, isGroup, err := ns.detectSyncGroup(context.Background(), claimCtx())
		if err != nil || isGroup {
			t.Fatalf("isGroup=%v err=%v, want false/nil (group path unsupported)", isGroup, err)
		}
	})

	t.Run("caches not synced: fail closed, retryable", func(t *testing.T) {
		ns := &Server{
			syncResolver:     &fakeSyncResolver{},
			syncCachesSynced: func() bool { return false },
			syncClaimLabels:  func(context.Context, types.NamespacedName) (map[string]string, error) { return labels(), nil },
		}
		_, _, err := ns.detectSyncGroup(context.Background(), claimCtx())
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable (fail closed until synced)", status.Code(err))
		}
	})

	t.Run("missing PVC identity: fail closed", func(t *testing.T) {
		ns := &Server{
			syncResolver:     &fakeSyncResolver{},
			syncCachesSynced: synced,
			syncClaimLabels:  func(context.Context, types.NamespacedName) (map[string]string, error) { return labels(), nil },
		}
		_, _, err := ns.detectSyncGroup(context.Background(), map[string]string{})
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("code = %v, want FailedPrecondition", status.Code(err))
		}
	})

	t.Run("label read fails: retryable", func(t *testing.T) {
		ns := &Server{
			syncResolver:     &fakeSyncResolver{},
			syncCachesSynced: synced,
			syncClaimLabels: func(context.Context, types.NamespacedName) (map[string]string, error) {
				return nil, fmt.Errorf("api down")
			},
		}
		_, _, err := ns.detectSyncGroup(context.Background(), claimCtx())
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable", status.Code(err))
		}
	})
}

func TestEnsureGroupPromoted(t *testing.T) {
	// serverFor builds a node whose site is site-a, recording each VGR patch the
	// node makes as "<site>/<replicationState>".
	serverFor := func(patched *[]string) *Server {
		return &Server{
			syncNodeSite: func(context.Context) (string, error) { return testSiteA, nil },
			syncGroupPatchVGR: func(_ context.Context, _ kube.VolumeGroupReplicationRef, site, state string) error {
				if patched != nil {
					*patched = append(*patched, site+"/"+state)
				}
				return nil
			},
		}
	}

	t.Run("first promote: fresh group -> annotation + primary", func(t *testing.T) {
		var patched []string
		ns := serverFor(&patched)
		vgr := kube.VolumeGroupReplicationRef{Namespace: "app", Name: "vgr1", Site: "", ReplicationState: "primary"}
		if status.Code(ns.ensureGroupPromoted(context.Background(), vgr)) != codes.Unavailable {
			t.Fatal("want Unavailable (switch requested)")
		}
		if len(patched) != 1 || patched[0] != testSiteA+"/primary" {
			t.Errorf("patched = %v, want [site-a/primary]", patched)
		}
	})

	t.Run("move: group promoted on another site -> demote the OLD site (secondary)", func(t *testing.T) {
		var patched []string
		ns := serverFor(&patched)
		vgr := kube.VolumeGroupReplicationRef{
			Namespace: "app", Name: "vgr1", Site: "site-b", ReplicationState: "primary",
			ObservedState: "Primary", Completed: true,
		}
		if status.Code(ns.ensureGroupPromoted(context.Background(), vgr)) != codes.Unavailable {
			t.Fatal("want Unavailable (switch started)")
		}
		if len(patched) != 1 || patched[0] != "site-b/secondary" {
			t.Errorf("patched = %v, want [site-b/secondary] (demote the old site first)", patched)
		}
	})

	t.Run("mid-switch: secondary, demote observed -> re-promote (primary)", func(t *testing.T) {
		var patched []string
		ns := serverFor(&patched)
		vgr := kube.VolumeGroupReplicationRef{
			Namespace: "app", Name: "vgr1", Site: testSiteA, ReplicationState: "secondary",
			ObservedState: "Secondary", Completed: true,
		}
		if status.Code(ns.ensureGroupPromoted(context.Background(), vgr)) != codes.Unavailable {
			t.Fatal("want Unavailable (re-promote started)")
		}
		if len(patched) != 1 || patched[0] != testSiteA+"/primary" {
			t.Errorf("patched = %v, want [site-a/primary]", patched)
		}
	})

	t.Run("mid-switch: secondary but demote not yet observed -> wait, no patch", func(t *testing.T) {
		var patched []string
		ns := serverFor(&patched)
		vgr := kube.VolumeGroupReplicationRef{
			Namespace: "app", Name: "vgr1", Site: testSiteA, ReplicationState: "secondary",
			ObservedState: "Primary", Completed: true,
		}
		if status.Code(ns.ensureGroupPromoted(context.Background(), vgr)) != codes.Unavailable {
			t.Fatal("want Unavailable (waiting for demote)")
		}
		if len(patched) != 0 {
			t.Errorf("must not promote before the group demote is observed, patched = %v", patched)
		}
	})

	t.Run("promoted and complete on this site: proceed", func(t *testing.T) {
		ns := serverFor(nil)
		vgr := kube.VolumeGroupReplicationRef{
			Namespace: "app", Name: "vgr1", Site: testSiteA, ReplicationState: "primary",
			ObservedState: "Primary", Completed: true,
		}
		if err := ns.ensureGroupPromoted(context.Background(), vgr); err != nil {
			t.Fatalf("err = %v, want nil (proceed to attach)", err)
		}
	})

	t.Run("half-configured seams: internal error", func(t *testing.T) {
		ns := &Server{}
		err := ns.ensureGroupPromoted(context.Background(), kube.VolumeGroupReplicationRef{Namespace: "app", Name: "vgr1"})
		if status.Code(err) != codes.Internal {
			t.Fatalf("code = %v, want Internal", status.Code(err))
		}
	})
}
