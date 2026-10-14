package node

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/simplyblock/atlas/kube"
)

func TestReadNodeSite(t *testing.T) {
	// labeledNode carries zone-a, and topologyServer names the driver after it.
	ns := topologyServer(t, labeledNode(nil))
	site, err := ns.readNodeSite(context.Background())
	if err != nil || site != "zone-a" {
		t.Fatalf("site=%q err=%v, want zone-a/nil", site, err)
	}

	// A node with no zone label cannot name its site: error, never a guess.
	bare := topologyServer(t, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}})
	if _, err := bare.readNodeSite(context.Background()); err == nil {
		t.Error("a node with no zone label must error, not resolve a site")
	}
}

func TestPatchVolumeReplicationToSite(t *testing.T) {
	vr := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "replication.storage.openshift.io/v1alpha1",
		"kind":       "VolumeReplication",
		"metadata":   map[string]interface{}{"name": "vr1", "namespace": "app"},
		"spec": map[string]interface{}{
			"volumeReplicationClass": "sb-sync-b",
			"replicationState":           "secondary",
			"dataSource":                 map[string]interface{}{"kind": "PersistentVolumeClaim", "name": "claim1"},
		},
	}}
	dc := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{kube.VolumeReplicationGVR: "VolumeReplicationList"}, vr)

	err := patchVolumeReplicationToSite(context.Background(), dc,
		kube.VolumeReplicationRef{Namespace: "app", Name: "vr1"}, "sb-sync-a")
	if err != nil {
		t.Fatalf("patch: %v", err)
	}

	got, err := dc.Resource(kube.VolumeReplicationGVR).Namespace("app").
		Get(context.Background(), "vr1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	class, _, _ := unstructured.NestedString(got.Object, "spec", "volumeReplicationClass")
	state, _, _ := unstructured.NestedString(got.Object, "spec", "replicationState")
	if class != "sb-sync-a" || state != "primary" {
		t.Errorf("after patch: class=%q state=%q, want sb-sync-a/primary", class, state)
	}
	// The merge touches only the two fields; the dataSource the admin set stays.
	ds, _, _ := unstructured.NestedString(got.Object, "spec", "dataSource", "name")
	if ds != "claim1" {
		t.Errorf("dataSource was disturbed: %q, want claim1", ds)
	}
}
