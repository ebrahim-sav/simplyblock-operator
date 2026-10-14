// Resolving the effective client set, and the address a client mounts at.
//
// The set is what goes into the exports(5) entry, so it is the only thing
// standing between a shared filesystem and every host that can reach the
// metadata server. It is asserted with its negative: what it lets in, and what
// it keeps out.

package controller

import (
	"context"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// testNodeIP is the address the baseline Kubernetes node publishes, and so the
// one the client set resolves to and the one an export is reachable at.
const testNodeIP = "192.168.10.81"

// kubeNode is a cluster node a client could mount an export from.
func kubeNode(name, internalIP string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Status: corev1.NodeStatus{
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: internalIP},
			},
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

// The set has to be safe by construction: nothing publishes the filesystem to
// the whole network. Every node that can run a pod is in it, which is wider
// than the nodes currently running one and narrower than everything -- the
// narrowing to actual placement is the refinement noted in the design, and
// until it exists this is the floor.
func TestClientSetIsClusterNodesAndNothingElse(t *testing.T) {
	r, _ := newExportReconciler(t, &fakeAssembler{}, readyExport(),
		kubeNode("vm01", testNodeIP), kubeNode("vm02", "192.168.10.82"))

	got, err := r.clusterNodeAddresses(context.Background())
	if err != nil {
		t.Fatalf("clusterNodeAddresses: %v", err)
	}
	slices.Sort(got)
	want := []string{testNodeIP, "192.168.10.82"}
	if !slices.Equal(got, want) {
		t.Errorf("clients = %v, want %v", got, want)
	}
	if slices.Contains(got, "*") {
		t.Error("the client set resolved to a wildcard")
	}
}

// The reconciler records the resolved set, because the assembler reads it back
// from status and an export with an empty set publishes to nobody.
func TestBindingRecordsTheResolvedClientSet(t *testing.T) {
	r, cl, _ := newPodHostedReconciler(t, testExport(nil), mdsDriver(), boundMDSPod(readyPod),
		kubeNode("vm01", testNodeIP))

	reconcileExport(t, r)

	if got := loadExport(t, cl).Status.AllowedClients; len(got) == 0 {
		t.Error("binding an export left its client set empty, so it publishes to nobody")
	}
}

// Regression: pnfs-1791981266 on OpenShift. OVN-Kubernetes leaves spec.podCIDRs
// empty and records each node's pod subnet in its k8s.ovn.org/node-subnets
// annotation, and a node reaches the metadata server from its address there.
// With the InternalIPs alone, the server refused every mount with an access
// denial. The subnet in the annotation is admitted as spec.podCIDRs would be.
func TestClientSetAdmitsTheOVNKubernetesNodeSubnet(t *testing.T) {
	ovn := kubeNode("worker-4", "192.168.10.247")
	ovn.Annotations = map[string]string{"k8s.ovn.org/node-subnets": `{"default":["10.129.2.0/23"]}`}
	r, _ := newExportReconciler(t, &fakeAssembler{}, readyExport(), ovn)

	got, err := r.clusterNodeAddresses(context.Background())
	if err != nil {
		t.Fatalf("clusterNodeAddresses: %v", err)
	}
	slices.Sort(got)
	if want := []string{"10.129.2.0/23", "192.168.10.247"}; !slices.Equal(got, want) {
		t.Errorf("clients = %v, want %v", got, want)
	}
}

// spec.podCIDRs, where the CNI fills it, stays the source: the annotation is
// read only on a node that has none.
func TestClientSetPrefersSpecPodCIDRsOverTheOVNAnnotation(t *testing.T) {
	node := kubeNode("vm01", testNodeIP)
	node.Spec.PodCIDRs = []string{"10.244.1.0/24"}
	node.Annotations = map[string]string{"k8s.ovn.org/node-subnets": `{"default":["10.129.2.0/23"]}`}
	r, _ := newExportReconciler(t, &fakeAssembler{}, readyExport(), node)

	got, err := r.clusterNodeAddresses(context.Background())
	if err != nil {
		t.Fatalf("clusterNodeAddresses: %v", err)
	}
	slices.Sort(got)
	if want := []string{"10.244.1.0/24", testNodeIP}; !slices.Equal(got, want) {
		t.Errorf("clients = %v, want %v", got, want)
	}
}

// An annotation that does not parse admits nothing: the set never widens on an
// input it cannot read.
func TestClientSetIgnoresAMalformedOVNAnnotation(t *testing.T) {
	node := kubeNode("vm01", testNodeIP)
	node.Annotations = map[string]string{"k8s.ovn.org/node-subnets": `{"default":"*"}`}
	r, _ := newExportReconciler(t, &fakeAssembler{}, readyExport(), node)

	got, err := r.clusterNodeAddresses(context.Background())
	if err != nil {
		t.Fatalf("clusterNodeAddresses: %v", err)
	}
	if want := []string{testNodeIP}; !slices.Equal(got, want) {
		t.Errorf("clients = %v, want %v", got, want)
	}
}
