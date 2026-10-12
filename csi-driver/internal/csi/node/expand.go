// Growing an already-staged volume onto the capacity it gained. The controller
// side of the same operation, growing the volume itself, is in the controller
// service and has already run by the time this does.
package node

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
)

func (ns *Server) NodeExpandVolume(
	ctx context.Context,
	req *csi.NodeExpandVolumeRequest,
) (*csi.NodeExpandVolumeResponse, error) {
	klog.Infof("NodeExpandVolume: called with args %+v", req)

	volumeID := req.GetVolumeId()
	unlock := ns.volumeLocks.Lock(volumeID)
	defer unlock()

	stagingParentPath := req.GetStagingTargetPath()
	volumeContext, err := lookupVolumeContext(stagingParentPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to retrieve volume context for volume %s: %v", volumeID, err)
	}

	// A pNFS client grows nothing and re-primes instead.
	//
	// Nothing to grow: the metadata server grew the filesystem on its own host,
	// and this node sees the new size through NFS. The plan here could not
	// answer anyway, since the filesystem it names is pnfs rather than anything
	// with a resize tool.
	//
	// The prime is the part that matters. Growing the namespace invalidates the
	// client's cached block device, and whoever causes the next I/O resolves it
	// again -- in their own mount namespace. A pod gets kubelet's minimal /dev
	// with no disk/, so the resolve fails, the fail bit is set, and every write
	// after it routes through the metadata server. Silently: the writes
	// succeed. This container has the host's /dev, and kubelet calls this RPC
	// on every node holding the volume, so it is where the race is won.
	//
	// The controller only recorded the new size, and the metadata server grows
	// the filesystem afterward, on its own time. A prime taken before that
	// resolves the device ID the grow is about to replace, and the pods then
	// resolve the new one themselves. So the prime waits until this mount shows
	// the size asked for, and until then the expansion is not done here: an
	// Unavailable error, which kubelet retries.
	if isPNFSVolume(volumeContext) {
		stagingTargetPath := getStagingTargetPath(req)
		if required := req.GetCapacityRange().GetRequiredBytes(); required > 0 {
			have, err := mountSize(stagingTargetPath)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to read the size of volume %s's mount: %v", volumeID, err)
			}
			if !pnfsGrowArrived(have, required) {
				return nil, status.Errorf(codes.Unavailable,
					"volume %s's mount shows %d bytes, not yet the %d asked for: the export has not grown yet",
					volumeID, have, required)
			}
		}
		if err := primeLayout(ctx, stagingTargetPath); err != nil {
			return nil, status.Errorf(codes.Internal,
				"failed to re-prime the layout of volume %s after its expansion: %v", volumeID, err)
		}
		klog.Infof("volume %s is served by an export: grew nothing, and re-took its layout", volumeID)
		return &csi.NodeExpandVolumeResponse{}, nil
	}

	plan, err := ns.attachPlan(ctx, volumeID, getStagingTargetPath(req), volumeContext, req.GetVolumeCapability())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to build the stack plan for volume %s: %v", volumeID, err)
	}

	// Bottom to top, skipping the layers that cannot grow. A raw block volume's
	// plan has none of those, so its expansion is a walk that changes nothing:
	// the block device was already resized at the storage layer, and neither
	// resize tool has anything to do with a device carrying no filesystem.
	//
	// Convergent, because kubelet reissues this after one that already
	// succeeded: both resize tools take the whole of what is now underneath them
	// and report success when that is where they already are.
	if err := ns.stack.runner.Grow(ctx, plan); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to grow volume %s: %v", volumeID, err)
	}

	klog.Infof("grew the stack of volume %s onto its new capacity", volumeID)
	return &csi.NodeExpandVolumeResponse{}, nil
}

// mountSize is the size a mounted filesystem reports, from statfs.
func mountSize(path string) (int64, error) {
	var s unix.Statfs_t
	if err := unix.Statfs(path, &s); err != nil {
		return 0, err
	}
	// In uint64 and converted once: Bsize is int64 on Linux and uint32 on darwin.
	return int64(s.Blocks * uint64(s.Bsize)), nil
}

// pnfsGrowArrived reports whether a pNFS mount showing have bytes has reached
// required. statfs reports an XFS filesystem's size without its internal log,
// so a grown export reads a little short; the tolerance, 1/64 of the size
// clamped to between 64 MiB and 2 GiB, covers the largest default log and stays
// below any grow worth waiting for. A smaller grow is taken as arrived, which is
// what every expansion did before this check.
func pnfsGrowArrived(have, required int64) bool {
	const minTolerance, maxTolerance = int64(64) << 20, int64(2) << 30
	tolerance := min(max(required/64, minTolerance), maxTolerance)
	return have >= required-tolerance
}
