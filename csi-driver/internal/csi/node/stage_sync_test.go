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
}

func (f *fakeSyncResolver) SyncVolumeReplicationForClaim(
	_ context.Context, claim types.NamespacedName,
) (kube.VolumeReplicationRef, error) {
	f.last = claim
	return f.ref, f.err
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
	// serverFor builds a node whose site is site-a, recording the site of any VR
	// patch the node makes.
	serverFor := func(patched *[]string) *Server {
		return &Server{
			syncCachesSynced: func() bool { return true },
			syncNodeSite:     func(context.Context) (string, error) { return testSiteA, nil },
			syncPatchVR: func(_ context.Context, _ kube.VolumeReplicationRef, site string) error {
				if patched != nil {
					*patched = append(*patched, site)
				}
				return nil
			},
		}
	}

	t.Run("not yet on the site: patches the annotation and asks for a retry", func(t *testing.T) {
		var patched []string
		ns := serverFor(&patched)
		// A VR currently annotated for the other site.
		vr := kube.VolumeReplicationRef{Namespace: "app", Name: "vr1", Site: "site-b", ReplicationState: "secondary"}
		err := ns.ensureSyncPromoted(context.Background(), vr)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable (switch requested)", status.Code(err))
		}
		if len(patched) != 1 || patched[0] != testSiteA {
			t.Errorf("patched = %v, want [site-a]", patched)
		}
	})

	t.Run("on the site but promote still converging: retry, no patch", func(t *testing.T) {
		var patched []string
		ns := serverFor(&patched)
		vr := kube.VolumeReplicationRef{
			Namespace: "app", Name: "vr1", Site: testSiteA, ReplicationState: "primary",
			ObservedState: "Secondary", Completed: false,
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
