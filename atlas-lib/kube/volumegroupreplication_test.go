package kube

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	kfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/simplyblock/atlas/errs"
)

func vgrUnstructured(name, ns, class, state string, matchLabels map[string]string) *unstructured.Unstructured {
	ml := map[string]interface{}{}
	for k, v := range matchLabels {
		ml[k] = v
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "replication.storage.openshift.io/v1alpha1",
		"kind":       "VolumeGroupReplication",
		"metadata":   map[string]interface{}{"name": name, "namespace": ns},
		"spec": map[string]interface{}{
			"volumeGroupReplicationClassName": class,
			"replicationState":                state,
			"source":                          map[string]interface{}{"selector": map[string]interface{}{"matchLabels": ml}},
		},
	}}
}

func vgrClassUnstructured(name, provisioner, method string) *unstructured.Unstructured {
	params := map[string]interface{}{}
	if method != "" {
		params[ReplicationMethodParam] = method
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "replication.storage.openshift.io/v1alpha1",
		"kind":       "VolumeGroupReplicationClass",
		"metadata":   map[string]interface{}{"name": name},
		"spec":       map[string]interface{}{"provisioner": provisioner, "parameters": params},
	}}
}

// newVGRResolver builds a resolver with the group informers enabled (and throwaway
// PV/PVC informers NewResolver requires). The handle test builds its own so it can
// seed the PV and PVC.
func newVGRResolver(t *testing.T, vgrInf, vgrClassInf cache.SharedIndexInformer) *InformerResolver {
	t.Helper()
	kf := informers.NewSharedInformerFactory(kfake.NewSimpleClientset(), 0)
	r, err := NewResolver(ResolverConfig{
		PersistentVolumes:      kf.Core().V1().PersistentVolumes().Informer(),
		PersistentVolumeClaims: kf.Core().V1().PersistentVolumeClaims().Informer(),
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	if err := r.EnableGroupInformers(vgrInf, vgrClassInf); err != nil {
		t.Fatalf("EnableGroupInformers: %v", err)
	}
	return r
}

func TestVolumeGroupReplicationClassRefFrom(t *testing.T) {
	got := VolumeGroupReplicationClassRefFrom(vgrClassUnstructured("sb-sync-group", DriverName, ReplicationMethodSync))
	want := VolumeGroupReplicationClassRef{Name: "sb-sync-group", Provisioner: DriverName, Method: ReplicationMethodSync}
	if got != want {
		t.Errorf("ref = %+v, want %+v", got, want)
	}
	if !got.IsSync() {
		t.Error("IsSync() = false, want true for method sync")
	}
	if VolumeGroupReplicationClassRefFrom(vgrClassUnstructured("a", DriverName, "async")).IsSync() {
		t.Error("IsSync() = true for method async")
	}
}

func TestVolumeGroupReplicationRefFrom(t *testing.T) {
	u := vgrUnstructured("vgr1", "app", "sb-sync-group", "primary", map[string]string{"cg": "g1"})
	u.Object["status"] = map[string]interface{}{"state": "Primary"}
	u = withVRSite(u, "site-b") // the SyncSiteAnnotation is shared with the VR path
	got := VolumeGroupReplicationRefFrom(u)
	if got.Name != "vgr1" || got.Namespace != "app" || got.ClassName != "sb-sync-group" ||
		got.ReplicationState != "primary" || got.ObservedState != "Primary" || got.Site != "site-b" {
		t.Errorf("ref = %+v", got)
	}
	if got.Selector == nil || !got.Selector.Matches(labels.Set{"cg": "g1"}) {
		t.Errorf("selector did not match its matchLabels: %v", got.Selector)
	}
	if got.Selector.Matches(labels.Set{"cg": "other"}) {
		t.Error("selector matched a non-member label set")
	}
}

func TestInformerResolver_SyncVolumeGroupReplicationForClaim(t *testing.T) {
	vgrInf, vgrClassInf := newUnstructuredInformer(), newUnstructuredInformer()
	r := newVGRResolver(t, vgrInf, vgrClassInf)
	add := func(o interface{}) {
		t.Helper()
		if err := vgrInf.GetStore().Add(o); err != nil {
			t.Fatal(err)
		}
	}
	// A sync VGR selecting members by the consistency-group label.
	add(vgrUnstructured("vgr-sync", "app", "sb-sync-group", "primary", map[string]string{"cg": "g1"}))
	// An async VGR, and one in another namespace, which must never match.
	add(vgrUnstructured("vgr-async", "app", "sb-async-group", "primary", map[string]string{"cg": "g2"}))
	add(vgrUnstructured("vgr-other-ns", "elsewhere", "sb-sync-group", "primary", map[string]string{"cg": "g1"}))
	// A VGR with an empty selector must not capture every claim.
	add(vgrUnstructured("vgr-empty-sel", "app", "sb-sync-group", "primary", map[string]string{}))
	if err := vgrClassInf.GetStore().Add(vgrClassUnstructured("sb-sync-group", DriverName, ReplicationMethodSync)); err != nil {
		t.Fatal(err)
	}
	if err := vgrClassInf.GetStore().Add(vgrClassUnstructured("sb-async-group", DriverName, "async")); err != nil {
		t.Fatal(err)
	}

	claim := types.NamespacedName{Namespace: "app", Name: "pvc-1"}
	got, err := r.SyncVolumeGroupReplicationForClaim(context.Background(), claim, map[string]string{"cg": "g1"})
	if err != nil {
		t.Fatalf("sync group member: %v", err)
	}
	if got.Name != "vgr-sync" {
		t.Errorf("resolved to %q, want vgr-sync", got.Name)
	}

	// A claim an async VGR selects stages ordinarily (ErrNotFound).
	if _, err := r.SyncVolumeGroupReplicationForClaim(context.Background(), claim, map[string]string{"cg": "g2"}); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("async group: err = %v, want ErrNotFound", err)
	}
	// A claim no VGR selects: ErrNotFound.
	if _, err := r.SyncVolumeGroupReplicationForClaim(context.Background(), claim, map[string]string{"cg": "none"}); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("unselected claim: err = %v, want ErrNotFound", err)
	}
	// An empty-selector VGR never captures a claim with no labels.
	if _, err := r.SyncVolumeGroupReplicationForClaim(context.Background(), claim, map[string]string{}); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("empty selector: err = %v, want ErrNotFound", err)
	}
}

func TestInformerResolver_VolumeGroupReplicationForVolumeHandle(t *testing.T) {
	kf := informers.NewSharedInformerFactory(kfake.NewSimpleClientset(), 0)
	pvInf := kf.Core().V1().PersistentVolumes().Informer()
	pvcInf := kf.Core().V1().PersistentVolumeClaims().Informer()
	vgrInf, vgrClassInf := newUnstructuredInformer(), newUnstructuredInformer()
	r, err := NewResolver(ResolverConfig{PersistentVolumes: pvInf, PersistentVolumeClaims: pvcInf})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	if err := r.EnableGroupInformers(vgrInf, vgrClassInf); err != nil {
		t.Fatalf("EnableGroupInformers: %v", err)
	}

	pv := managedPV("c:p:v", "sb")
	pv.Name = "pv1"
	pv.Spec.ClaimRef = &corev1.ObjectReference{Namespace: "app", Name: "pvc-1"}
	if err := pvInf.GetStore().Add(pv); err != nil {
		t.Fatal(err)
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "pvc-1", Labels: map[string]string{"cg": "g1"}},
	}
	if err := pvcInf.GetStore().Add(pvc); err != nil {
		t.Fatal(err)
	}
	if err := vgrInf.GetStore().Add(
		withVRSite(vgrUnstructured("vgr1", "app", "sb-sync-group", "primary", map[string]string{"cg": "g1"}), "site-b")); err != nil {
		t.Fatal(err)
	}

	got, err := r.VolumeGroupReplicationForVolumeHandle(context.Background(), "c:p:v")
	if err != nil {
		t.Fatalf("VolumeGroupReplicationForVolumeHandle: %v", err)
	}
	if got.Name != "vgr1" || got.Site != "site-b" {
		t.Errorf("ref = %+v, want Name=vgr1 Site=site-b", got)
	}

	// A handle no PV carries resolves to ErrNotFound.
	if _, err := r.VolumeGroupReplicationForVolumeHandle(context.Background(), "no:such:handle"); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("missing handle: err = %v, want ErrNotFound", err)
	}
}

// vgrContentUnstructured builds a VolumeGroupReplicationContent binding a backend
// group handle to a VGR by name and namespace.
func vgrContentUnstructured(name, handle, vgrName, vgrNamespace string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "replication.storage.openshift.io/v1alpha1",
		"kind":       "VolumeGroupReplicationContent",
		"metadata":   map[string]interface{}{"name": name},
		"spec": map[string]interface{}{
			"volumeGroupReplicationHandle": handle,
			"volumeGroupReplicationRef": map[string]interface{}{
				"name":      vgrName,
				"namespace": vgrNamespace,
			},
		},
	}}
}

func TestInformerResolver_VolumeGroupReplicationForGroupHandle(t *testing.T) {
	vgrInf := newUnstructuredInformer()
	vgrClassInf := newUnstructuredInformer()
	contentInf := newUnstructuredInformer()
	r := newVGRResolver(t, vgrInf, vgrClassInf)
	if err := r.EnableGroupContentInformer(contentInf); err != nil {
		t.Fatalf("EnableGroupContentInformer: %v", err)
	}

	// The VGR, promoted on site-b, and the content binding the group handle to it.
	if err := vgrInf.GetStore().Add(
		withVRSite(vgrUnstructured("vgr1", "app", "sb-sync-group", "primary", map[string]string{"cg": "g1"}), "site-b")); err != nil {
		t.Fatal(err)
	}
	const groupHandle = "cg:cluster-uuid:group-uuid"
	if err := contentInf.GetStore().Add(vgrContentUnstructured("content1", groupHandle, "vgr1", "app")); err != nil {
		t.Fatal(err)
	}

	got, err := r.VolumeGroupReplicationForGroupHandle(context.Background(), groupHandle)
	if err != nil {
		t.Fatalf("VolumeGroupReplicationForGroupHandle: %v", err)
	}
	if got.Name != "vgr1" || got.Site != "site-b" {
		t.Errorf("ref = %+v, want Name=vgr1 Site=site-b", got)
	}

	// A handle no content carries resolves to ErrNotFound.
	if _, err := r.VolumeGroupReplicationForGroupHandle(context.Background(), "cg:no:such"); !errors.Is(err, errs.ErrNotFound) {
		t.Errorf("missing handle: err = %v, want ErrNotFound", err)
	}

	// Without the content informer, the lookup is unsupported (the node path).
	rNoContent := newVGRResolver(t, newUnstructuredInformer(), newUnstructuredInformer())
	if _, err := rNoContent.VolumeGroupReplicationForGroupHandle(context.Background(), groupHandle); !errors.Is(err, errs.ErrUnsupported) {
		t.Errorf("no content informer: err = %v, want ErrUnsupported", err)
	}
}
