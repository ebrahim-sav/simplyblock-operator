// The csi-addons VolumeGroupReplication (VGR) and its class, read as unstructured
// so this package takes no dependency on the csi-addons Go types. The group path
// mirrors the single-volume one (volumereplication.go): the active site rides on
// the VGR's SyncSiteAnnotation (not its immutable class), one site-agnostic
// VolumeGroupReplicationClass marks the group synchronous by its method
// parameter, and the whole consistency group follows one object
// (design-sync-replication-csi-addons.md §14).
//
// A VGR names its members by a label selector (spec.source.selector), not by a
// per-claim reference, so correlating a PVC to its VGR is a selector match over
// the VGRs in the claim's namespace against the claim's labels -- unlike a
// VolumeReplication, which is found by the claim it names in spec.dataSource.
package kube

import (
	"context"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/simplyblock/atlas/errs"
	"github.com/simplyblock/atlas/lvol"
)

// VolumeGroupReplicationGVR and VolumeGroupReplicationClassGVR are the csi-addons
// group CRDs (VGR is namespaced, the class cluster-scoped).
var (
	VolumeGroupReplicationGVR = schema.GroupVersionResource{
		Group:    "replication.storage.openshift.io",
		Version:  "v1alpha1",
		Resource: "volumegroupreplications",
	}
	VolumeGroupReplicationClassGVR = schema.GroupVersionResource{
		Group:    "replication.storage.openshift.io",
		Version:  "v1alpha1",
		Resource: "volumegroupreplicationclasses",
	}
	// VolumeGroupReplicationContentGVR is the cluster-scoped content object that
	// binds a VolumeGroupReplication to its backend group: it carries the group
	// handle the driver returned (spec.volumeGroupReplicationHandle) and a
	// reference back to the VGR (spec.volumeGroupReplicationRef). It is the bridge
	// the controller needs, because a group replication request names the group by
	// that handle and carries no claim, so the VGR (and its site annotation) is
	// reached handle -> content -> VGR.
	VolumeGroupReplicationContentGVR = schema.GroupVersionResource{
		Group:    "replication.storage.openshift.io",
		Version:  "v1alpha1",
		Resource: "volumegroupreplicationcontents",
	}
)

// VolumeGroupReplicationClassRef is the slice of a VolumeGroupReplicationClass the
// plugin reads: its provisioner and the method its parameters carry. The site is
// not a class parameter (it rides on the VGR's annotation), so one class serves
// both sites, exactly as the per-volume class does (§5.1, §9.1).
type VolumeGroupReplicationClassRef struct {
	Name        string
	Provisioner string
	Method      string // parameters[ReplicationMethodParam]
}

// IsSync reports whether the class drives the synchronous backend.
func (c VolumeGroupReplicationClassRef) IsSync() bool { return c.Method == ReplicationMethodSync }

// VolumeGroupReplicationClassRefFrom extracts the fields of interest from a
// VolumeGroupReplicationClass unstructured object.
func VolumeGroupReplicationClassRefFrom(u *unstructured.Unstructured) VolumeGroupReplicationClassRef {
	if u == nil {
		return VolumeGroupReplicationClassRef{}
	}
	prov, _, _ := unstructured.NestedString(u.Object, "spec", "provisioner")
	params, _, _ := unstructured.NestedStringMap(u.Object, "spec", "parameters")
	return VolumeGroupReplicationClassRef{
		Name:        u.GetName(),
		Provisioner: prov,
		Method:      params[ReplicationMethodParam],
	}
}

// VolumeGroupReplicationClassNameKeys returns the index key for a
// VolumeGroupReplicationClass: its name, when provisioned by this driver. A
// foreign class gets no key, mirroring VolumeReplicationClassNameKeys.
func VolumeGroupReplicationClassNameKeys(u *unstructured.Unstructured) []string {
	if u == nil {
		return nil
	}
	prov, _, _ := unstructured.NestedString(u.Object, "spec", "provisioner")
	if prov != DriverName {
		return nil
	}
	return []string{u.GetName()}
}

func indexVGRClassByName(obj interface{}) ([]string, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, nil
	}
	return VolumeGroupReplicationClassNameKeys(u), nil
}

// indexVGRContentByGroupHandle keys a VolumeGroupReplicationContent by the backend
// group handle it carries, so the controller can resolve a group replication
// request (which names the group by that handle) back to its VGR.
func indexVGRContentByGroupHandle(obj interface{}) ([]string, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, nil
	}
	h, _, _ := unstructured.NestedString(u.Object, "spec", "volumeGroupReplicationHandle")
	if h == "" {
		return nil, nil
	}
	return []string{h}, nil
}

// VolumeGroupReplicationRef is the slice of a VolumeGroupReplication the plugin
// acts on: its class, replicationState, status, its member selector, and the
// SyncSiteAnnotation that names the site the whole group is served from.
type VolumeGroupReplicationRef struct {
	Name             string
	Namespace        string
	ClassName        string // spec.volumeGroupReplicationClassName
	ReplicationState string // spec.replicationState (primary|secondary|resync)
	ObservedState    string // status.state (Primary|Secondary|Unknown)
	Completed        bool   // status.conditions[type=Completed].status == True
	// Site is the value of the SyncSiteAnnotation: the site the node set the group
	// to be served from, which the group promote and demote address (§14, §6.2).
	Site string
	// Selector is spec.source.selector compiled: the members of the group are the
	// PVCs it matches. Nil when the selector is absent or unparseable, in which
	// case the VGR matches nothing (a VGR with no selector names no members).
	Selector labels.Selector
}

// VolumeGroupReplicationRefFrom extracts the fields of interest, spec and status,
// from a VolumeGroupReplication unstructured object.
func VolumeGroupReplicationRefFrom(u *unstructured.Unstructured) VolumeGroupReplicationRef {
	if u == nil {
		return VolumeGroupReplicationRef{}
	}
	class, _, _ := unstructured.NestedString(u.Object, "spec", "volumeGroupReplicationClassName")
	state, _, _ := unstructured.NestedString(u.Object, "spec", "replicationState")
	observed, _, _ := unstructured.NestedString(u.Object, "status", "state")
	var selector labels.Selector
	if raw, found, _ := unstructured.NestedMap(u.Object, "spec", "source", "selector"); found {
		ls := &metav1.LabelSelector{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, ls); err == nil {
			if s, err := metav1.LabelSelectorAsSelector(ls); err == nil {
				selector = s
			}
		}
	}
	return VolumeGroupReplicationRef{
		Name:             u.GetName(),
		Namespace:        u.GetNamespace(),
		ClassName:        class,
		ReplicationState: state,
		ObservedState:    observed,
		Completed:        conditionTrue(u, "Completed"),
		Site:             u.GetAnnotations()[SyncSiteAnnotation],
		Selector:         selector,
	}
}

// VolumeGroupReplicationClassByName returns the VolumeGroupReplicationClass named
// name, or errs.ErrNotFound (or the one that does is not this driver's), and
// errs.ErrUnsupported when no VGRClass informer is configured.
func (r *InformerResolver) VolumeGroupReplicationClassByName(ctx context.Context, name string) (VolumeGroupReplicationClassRef, error) {
	if r.vgrClass == nil {
		return VolumeGroupReplicationClassRef{}, fmt.Errorf("kube: no VolumeGroupReplicationClass informer configured: %w", errs.ErrUnsupported)
	}
	objs, err := r.vgrClass.GetIndexer().ByIndex(IndexVGRClassByName, name)
	if err != nil {
		return VolumeGroupReplicationClassRef{}, fmt.Errorf("kube: index VolumeGroupReplicationClass by name %q: %w", name, err)
	}
	for _, o := range objs {
		if u, ok := o.(*unstructured.Unstructured); ok {
			return VolumeGroupReplicationClassRefFrom(u), nil
		}
	}
	return VolumeGroupReplicationClassRef{}, fmt.Errorf("kube: no VolumeGroupReplicationClass %q: %w", name, errs.ErrNotFound)
}

// VolumeGroupReplicationForClaim returns the VolumeGroupReplication in claim's
// namespace whose member selector matches claimLabels. A VGR with an empty or
// absent selector is skipped rather than treated as matching every claim, so a
// misconfigured VGR never captures a volume by accident. Returns errs.ErrNotFound
// when none matches, errs.ErrUnsupported when no VGR informer is configured.
//
// The claim's labels are an argument rather than read here because the node-side
// resolver carries no PVC informer (it reads the one PVC directly); the
// controller-side VolumeGroupReplicationForVolumeHandle supplies them from its PVC
// informer.
func (r *InformerResolver) VolumeGroupReplicationForClaim(
	ctx context.Context, claim types.NamespacedName, claimLabels map[string]string,
) (VolumeGroupReplicationRef, error) {
	if r.vgr == nil {
		return VolumeGroupReplicationRef{}, fmt.Errorf("kube: no VolumeGroupReplication informer configured: %w", errs.ErrUnsupported)
	}
	set := labels.Set(claimLabels)
	for _, o := range r.vgr.GetStore().List() {
		u, ok := o.(*unstructured.Unstructured)
		if !ok || u.GetNamespace() != claim.Namespace {
			continue
		}
		ref := VolumeGroupReplicationRefFrom(u)
		if ref.Selector == nil || ref.Selector.Empty() {
			continue
		}
		if ref.Selector.Matches(set) {
			return ref, nil
		}
	}
	return VolumeGroupReplicationRef{}, fmt.Errorf("kube: no VolumeGroupReplication selects claim %s: %w", claim, errs.ErrNotFound)
}

// SyncVolumeGroupReplicationForClaim returns the synchronous VolumeGroupReplication
// that protects the claim: the VGR whose selector matches claimLabels and whose
// class is sync. It returns errs.ErrNotFound when the claim is not a sync-group
// member (so the volume stages ordinarily), mirroring SyncVolumeReplicationForClaim.
func (r *InformerResolver) SyncVolumeGroupReplicationForClaim(
	ctx context.Context, claim types.NamespacedName, claimLabels map[string]string,
) (VolumeGroupReplicationRef, error) {
	vgr, err := r.VolumeGroupReplicationForClaim(ctx, claim, claimLabels)
	if err != nil {
		return VolumeGroupReplicationRef{}, err
	}
	class, err := r.VolumeGroupReplicationClassByName(ctx, vgr.ClassName)
	if err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return VolumeGroupReplicationRef{}, fmt.Errorf(
				"kube: VolumeGroupReplicationClass %q for claim %s does not resolve: %w",
				vgr.ClassName, claim, errs.ErrNotFound)
		}
		return VolumeGroupReplicationRef{}, err
	}
	if !class.IsSync() {
		return VolumeGroupReplicationRef{}, fmt.Errorf(
			"kube: VolumeGroupReplication for claim %s is not synchronous: %w", claim, errs.ErrNotFound)
	}
	return vgr, nil
}

// VolumeGroupReplicationForVolumeHandle resolves a CSI volume handle to the
// VolumeGroupReplication protecting it: PV by handle, to its claim, to the claim's
// labels (from the PVC informer), to the VGR whose selector matches. The
// controller uses it to read the group's active site from the VGR annotation.
func (r *InformerResolver) VolumeGroupReplicationForVolumeHandle(ctx context.Context, h lvol.VolumeHandle) (VolumeGroupReplicationRef, error) {
	if r.pvc == nil {
		return VolumeGroupReplicationRef{}, fmt.Errorf("kube: no PersistentVolumeClaim informer configured: %w", errs.ErrUnsupported)
	}
	pv, err := r.PVByVolumeHandle(ctx, h)
	if err != nil {
		return VolumeGroupReplicationRef{}, err
	}
	ref := pv.Spec.ClaimRef
	if ref == nil || ref.Namespace == "" || ref.Name == "" {
		return VolumeGroupReplicationRef{}, fmt.Errorf("kube: pv %s for volume %q names no claim: %w", pv.GetName(), h, errs.ErrNotFound)
	}
	claim := types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}
	obj, exists, err := r.pvc.GetIndexer().GetByKey(claim.String())
	if err != nil {
		return VolumeGroupReplicationRef{}, fmt.Errorf("kube: get claim %s: %w", claim, err)
	}
	if !exists {
		return VolumeGroupReplicationRef{}, fmt.Errorf("kube: claim %s not found: %w", claim, errs.ErrNotFound)
	}
	pvc, ok := obj.(interface{ GetLabels() map[string]string })
	if !ok {
		return VolumeGroupReplicationRef{}, fmt.Errorf("kube: claim %s is not a PVC object: %w", claim, errs.ErrUnsupported)
	}
	return r.VolumeGroupReplicationForClaim(ctx, claim, pvc.GetLabels())
}

// VolumeGroupReplicationForGroupHandle resolves a backend consistency-group handle
// (cg:cluster:group) to the VolumeGroupReplication protecting it, so the controller
// can read the group's active site from the VGR's SyncSiteAnnotation. A group
// replication request names the group by this handle and carries no claim, so the
// correlation runs through the content object: handle -> VolumeGroupReplicationContent
// (indexed by spec.volumeGroupReplicationHandle) -> spec.volumeGroupReplicationRef ->
// the VGR. It returns errs.ErrUnsupported when the content informer is not
// configured (the node path does not use it), and errs.ErrNotFound when no content
// or VGR matches.
func (r *InformerResolver) VolumeGroupReplicationForGroupHandle(ctx context.Context, h lvol.VolumeHandle) (VolumeGroupReplicationRef, error) {
	if r.vgrContent == nil || r.vgr == nil {
		return VolumeGroupReplicationRef{}, fmt.Errorf("kube: no VolumeGroupReplicationContent informer configured: %w", errs.ErrUnsupported)
	}
	objs, err := r.vgrContent.GetIndexer().ByIndex(IndexVGRContentByHandle, string(h))
	if err != nil {
		return VolumeGroupReplicationRef{}, fmt.Errorf("kube: index VolumeGroupReplicationContent by handle %q: %w", h, err)
	}
	for _, o := range objs {
		content, ok := o.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(content.Object, "spec", "volumeGroupReplicationRef", "name")
		namespace, _, _ := unstructured.NestedString(content.Object, "spec", "volumeGroupReplicationRef", "namespace")
		if name == "" || namespace == "" {
			continue
		}
		key := types.NamespacedName{Namespace: namespace, Name: name}.String()
		vgrObj, exists, err := r.vgr.GetIndexer().GetByKey(key)
		if err != nil {
			return VolumeGroupReplicationRef{}, fmt.Errorf("kube: get VolumeGroupReplication %s: %w", key, err)
		}
		if !exists {
			continue
		}
		if u, ok := vgrObj.(*unstructured.Unstructured); ok {
			return VolumeGroupReplicationRefFrom(u), nil
		}
	}
	return VolumeGroupReplicationRef{}, fmt.Errorf("kube: no VolumeGroupReplication for group handle %q: %w", h, errs.ErrNotFound)
}

// EnableGroupContentInformer adds the VolumeGroupReplicationContent informer (and
// its handle indexer) to a resolver, so VolumeGroupReplicationForGroupHandle
// resolves. The controller needs it to correlate a group replication request to a
// VGR; the node does not (it correlates by claim), so this is a separate enabler
// from EnableGroupInformers. Call it before the informer starts.
func (r *InformerResolver) EnableGroupContentInformer(volumeGroupReplicationContents cache.SharedIndexInformer) error {
	if volumeGroupReplicationContents == nil {
		return fmt.Errorf("kube: EnableGroupContentInformer requires the VGRContent informer: %w", errs.ErrUnsupported)
	}
	if err := volumeGroupReplicationContents.AddIndexers(cache.Indexers{
		IndexVGRContentByHandle: indexVGRContentByGroupHandle,
	}); err != nil {
		return fmt.Errorf("kube: add VolumeGroupReplicationContent handle indexer: %w", err)
	}
	r.vgrContent = volumeGroupReplicationContents
	return nil
}

// EnableGroupInformers adds the VolumeGroupReplication and
// VolumeGroupReplicationClass informers (and their indexers) to a resolver, so the
// group lookups resolve. It is additive: a resolver built by NewSyncResolver or
// NewResolver gains the group path by calling this before the informers start.
// Indexers cannot be added to a running informer, so call it before Start.
func (r *InformerResolver) EnableGroupInformers(volumeGroupReplications, volumeGroupReplicationClasses cache.SharedIndexInformer) error {
	if volumeGroupReplications == nil || volumeGroupReplicationClasses == nil {
		return fmt.Errorf("kube: EnableGroupInformers requires the VGR and VGRClass informers: %w", errs.ErrUnsupported)
	}
	if err := volumeGroupReplicationClasses.AddIndexers(cache.Indexers{
		IndexVGRClassByName: indexVGRClassByName,
	}); err != nil {
		return fmt.Errorf("kube: add VolumeGroupReplicationClass indexer: %w", err)
	}
	r.vgr = volumeGroupReplications
	r.vgrClass = volumeGroupReplicationClasses
	return nil
}
