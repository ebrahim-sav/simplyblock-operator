// Ordering contract for sync-managed staging: the connection to a sync volume is
// fetched only after its switchover gate, because a connect on the stale led
// site clears the volume's birth state and deadlocks the first-placement promote.
package node

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	k8smount "k8s.io/mount-utils"

	"github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/simplyblock/atlas/kube"

	csicommon "github.com/simplyblock/csi-driver/internal/csi/common"
	"github.com/simplyblock/csi-driver/internal/mount"
)

// TestNodeStageVolumeDefersConnectForSyncVolume pins the stage ordering for a
// sync-managed volume: its connection must be fetched only after the switchover
// gate has run.
//
// Regression: 2026-10-17-sync-pre-gate-connect-clears-birth-state. NodeStageVolume
// refreshed the volume context (a GET .../connect) before the gate. On a
// first placement whose pod lands on a site the volume's LVS is not yet led from,
// that connect resolves to the current (stale) led site and clears
// sync_never_connected there. The backend keys its first-placement auto-demote on
// the volume never having been connected, so once the flag was cleared the planned
// promote to this node's site was refused "not demoted on site <old>" forever and
// the pod never started.
func TestNodeStageVolumeDefersConnectForSyncVolume(t *testing.T) {
	gateCalls := 0
	connectsBeforeGate := 0

	fe, _ := scriptedExec(nil)
	ns := &Server{
		mounter:          mount.NewWith(k8smount.NewFakeMounter(nil), fe),
		volumeLocks:      csicommon.NewVolumeLocks(),
		syncResolver:     &fakeSyncResolver{ref: kube.VolumeReplicationRef{Namespace: "app", Name: "vr1"}},
		syncCachesSynced: func() bool { return true },
		syncNodeSite:     func(context.Context) (string, error) { return testSiteA, nil },
		syncPatchVR: func(context.Context, kube.VolumeReplicationRef, string, string) error {
			gateCalls++
			return nil
		},
	}
	// A fresh VR (no site, no state) drives ensureSyncPromoted's first-promote
	// branch: it patches the VR and returns a retryable error, so staging stops at
	// the gate -- exactly where a sync volume's connect must not yet have run.
	ns.refreshConn = func(context.Context, string, map[string]string) {
		if gateCalls == 0 {
			connectsBeforeGate++
		}
	}

	req := &csi.NodeStageVolumeRequest{
		VolumeId:          pvcTestHandle,
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  mountCapability(),
		VolumeContext:     claimCtx(),
	}
	_, err := ns.NodeStageVolume(context.Background(), req)

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("want a retryable Unavailable from the switchover gate, got %v", err)
	}
	if gateCalls == 0 {
		t.Fatal("the switchover gate never ran; the test did not reach the path under test")
	}
	if connectsBeforeGate != 0 {
		t.Fatalf("the volume was connected %d time(s) before the switchover gate; a sync "+
			"volume's connect must be deferred until after the promote", connectsBeforeGate)
	}
}
