// The reconciler guard for a group restore. The group-restore drive (fan out N
// member recoveries, wait for all, bind one claim per member) is not yet wired,
// so an operation that carries spec.groupBackupID must fail fast with a clear
// reason rather than enter the single-backup path, whose BackupRef is empty for a
// group operation.

package backup

import (
	"context"
	"strings"
	"testing"

	simplyblockv1alpha2 "github.com/simplyblock/simplyblock-operator/api/v1alpha2"
)

func TestGroupRestoreFailsFastUntilTheDriveLands(t *testing.T) {
	r := &StorageBackupOpsReconciler{}
	ops := &simplyblockv1alpha2.StorageBackupOps{
		Spec: simplyblockv1alpha2.StorageBackupOpsSpec{
			GroupBackupID: "cluster-1/gb-1",
			Action:        simplyblockv1alpha2.StorageBackupOpsActionRestore,
		},
	}

	done, err := r.validate(context.Background(), ops)
	if done {
		t.Fatal("the group restore reported progress; it should have failed fast")
	}
	if err == nil {
		t.Fatal("a group restore was accepted; want a fatal error until the drive lands")
	}
	if !strings.Contains(err.Error(), "group restore") || !strings.Contains(err.Error(), "cluster-1/gb-1") {
		t.Errorf("the error is %q, want it to say group restore is unsupported and name the group backup id", err.Error())
	}
}
