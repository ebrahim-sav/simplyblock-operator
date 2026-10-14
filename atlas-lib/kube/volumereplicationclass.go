// The csi-addons VolumeReplicationClass, read as unstructured, so the node plugin
// can learn whether a VolumeReplication is synchronous (its class's method
// parameter) and, for the switchover, which site a class names
// (design-sync-replication-csi-addons.md §5.1, §6.2). The parameter keys are the
// cross-component contract between the driver's class dispatch and this lookup.
package kube

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/simplyblock/atlas/errs"
)

const (
	// ReplicationMethodParam selects the replication backend a class drives, and
	// ReplicationSiteParam is the site a sync switchover targets. They are read
	// from a VolumeReplicationClass's spec.parameters.
	ReplicationMethodParam = "replication.storage.simplyblock.io/method"
	ReplicationSiteParam   = "replication.storage.simplyblock.io/site"
	// ReplicationMethodSync is the method value that marks a synchronous class.
	ReplicationMethodSync = "sync"
)

// VolumeReplicationClassGVR is the GroupVersionResource of the csi-addons
// VolumeReplicationClass CRD (cluster-scoped).
var VolumeReplicationClassGVR = schema.GroupVersionResource{
	Group:    "replication.storage.openshift.io",
	Version:  "v1alpha1",
	Resource: "volumereplicationclasses",
}

// VolumeReplicationClassRef is the slice of a VolumeReplicationClass the node
// plugin reads: its provisioner, and the method and site its parameters carry.
type VolumeReplicationClassRef struct {
	Name        string
	Provisioner string
	Method      string // parameters[ReplicationMethodParam]
	Site        string // parameters[ReplicationSiteParam]
}

// IsSync reports whether the class drives the synchronous backend.
func (c VolumeReplicationClassRef) IsSync() bool { return c.Method == ReplicationMethodSync }

// VolumeReplicationClassRefFrom extracts the fields of interest from a
// VolumeReplicationClass unstructured object.
func VolumeReplicationClassRefFrom(u *unstructured.Unstructured) VolumeReplicationClassRef {
	if u == nil {
		return VolumeReplicationClassRef{}
	}
	prov, _, _ := unstructured.NestedString(u.Object, "spec", "provisioner")
	params, _, _ := unstructured.NestedStringMap(u.Object, "spec", "parameters")
	return VolumeReplicationClassRef{
		Name:        u.GetName(),
		Provisioner: prov,
		Method:      params[ReplicationMethodParam],
		Site:        params[ReplicationSiteParam],
	}
}

// VolumeReplicationClassNameKeys returns the index key for a
// VolumeReplicationClass: its name, when the class is provisioned by this driver.
// A class with a foreign provisioner gets no key, so an informer resolver using
// this index never resolves one, mirroring StorageClassNameKeys.
func VolumeReplicationClassNameKeys(u *unstructured.Unstructured) []string {
	if u == nil {
		return nil
	}
	prov, _, _ := unstructured.NestedString(u.Object, "spec", "provisioner")
	if prov != DriverName {
		return nil
	}
	return []string{u.GetName()}
}

// indexVRClassByName adapts VolumeReplicationClassNameKeys to cache.IndexFunc.
func indexVRClassByName(obj interface{}) ([]string, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, nil
	}
	return VolumeReplicationClassNameKeys(u), nil
}

// VolumeReplicationClassSiteKeys returns the index key for a
// VolumeReplicationClass: the site it names, when the class is this driver's and
// synchronous and carries a site. This is what lets the node plugin find the
// class that names its node's site to re-point a VolumeReplication at
// (design §6.2). An async or foreign class gets no key.
func VolumeReplicationClassSiteKeys(u *unstructured.Unstructured) []string {
	ref := VolumeReplicationClassRefFrom(u)
	if ref.Provisioner != DriverName || !ref.IsSync() || ref.Site == "" {
		return nil
	}
	return []string{ref.Site}
}

// indexVRClassBySite adapts VolumeReplicationClassSiteKeys to cache.IndexFunc.
func indexVRClassBySite(obj interface{}) ([]string, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, nil
	}
	return VolumeReplicationClassSiteKeys(u), nil
}

// VolumeReplicationClassForSite returns the synchronous VolumeReplicationClass
// that names site, which the node plugin re-points a VolumeReplication at so the
// switchover targets the pod's site (design §6.2). It returns errs.ErrNotFound
// when no sync class names the site, and errs.ErrUnsupported when the resolver
// has no VolumeReplicationClass informer.
func (r *InformerResolver) VolumeReplicationClassForSite(ctx context.Context, site string) (VolumeReplicationClassRef, error) {
	if r.vrClass == nil {
		return VolumeReplicationClassRef{}, fmt.Errorf("kube: no VolumeReplicationClass informer configured: %w", errs.ErrUnsupported)
	}
	objs, err := r.vrClass.GetIndexer().ByIndex(IndexVRClassBySite, site)
	if err != nil {
		return VolumeReplicationClassRef{}, fmt.Errorf("kube: index VolumeReplicationClass by site %q: %w", site, err)
	}
	for _, o := range objs {
		if u, ok := o.(*unstructured.Unstructured); ok {
			return VolumeReplicationClassRefFrom(u), nil
		}
	}
	return VolumeReplicationClassRef{}, fmt.Errorf("kube: no sync VolumeReplicationClass names site %q: %w", site, errs.ErrNotFound)
}

// VolumeReplicationClassByName returns the VolumeReplicationClass named name, or
// errs.ErrNotFound if none exists (or the one that does is not this driver's),
// and errs.ErrUnsupported when the resolver has no VolumeReplicationClass informer.
func (r *InformerResolver) VolumeReplicationClassByName(ctx context.Context, name string) (VolumeReplicationClassRef, error) {
	if r.vrClass == nil {
		return VolumeReplicationClassRef{}, fmt.Errorf("kube: no VolumeReplicationClass informer configured: %w", errs.ErrUnsupported)
	}
	objs, err := r.vrClass.GetIndexer().ByIndex(IndexVRClassByName, name)
	if err != nil {
		return VolumeReplicationClassRef{}, fmt.Errorf("kube: index VolumeReplicationClass by name %q: %w", name, err)
	}
	for _, o := range objs {
		if u, ok := o.(*unstructured.Unstructured); ok {
			return VolumeReplicationClassRefFrom(u), nil
		}
	}
	return VolumeReplicationClassRef{}, fmt.Errorf("kube: no VolumeReplicationClass %q: %w", name, errs.ErrNotFound)
}
