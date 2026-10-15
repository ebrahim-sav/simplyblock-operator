// The mds-proxy as design-pnfs-mds-vm.md §8.5 and §10 need it: on the host's
// network wherever the metadata server may run, from the metadata server's
// image, without privilege; the runner told where it is; and the driver
// reconciler applying it while spec.pnfs.mds is set and removing it after.

package driver

import (
	"context"
	"maps"
	"slices"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	simplyblockv1alpha2 "github.com/simplyblock/simplyblock-operator/api/v1alpha2"
)

func mdsProxy(t *testing.T) *appsv1.DaemonSet {
	t.Helper()
	ds, err := MDSProxyDaemonSet(withMDS(testDriver("simplyblock")))
	if err != nil {
		t.Fatalf("MDSProxyDaemonSet: %v", err)
	}
	return ds
}

func TestTheMDSProxyRunsOnTheHostNetworkWhereverTheMDSMay(t *testing.T) {
	d := withMDS(testDriver("simplyblock"))
	ds := mdsProxy(t)
	_, sts := mdsObjects(t, d)
	pod := ds.Spec.Template.Spec

	if ds.Name != "simplyblock-csi-mds-proxy" || ds.Namespace != d.Namespace {
		t.Errorf("DaemonSet %s/%s, want %s/simplyblock-csi-mds-proxy", ds.Namespace, ds.Name, d.Namespace)
	}
	if !pod.HostNetwork {
		t.Error("the proxy is not on the host's network, where SPDK's listeners accept its connections")
	}
	if !maps.Equal(pod.NodeSelector, sts.Spec.Template.Spec.NodeSelector) {
		t.Errorf("nodeSelector = %v, want the metadata server's %v", pod.NodeSelector, sts.Spec.Template.Spec.NodeSelector)
	}
	if !slices.Equal(pod.Tolerations, sts.Spec.Template.Spec.Tolerations) {
		t.Errorf("tolerations = %v, want the metadata server's %v", pod.Tolerations, sts.Spec.Template.Spec.Tolerations)
	}
	if pod.ServiceAccountName != sts.Spec.Template.Spec.ServiceAccountName {
		t.Errorf("ServiceAccount %q, want the metadata server's %q, which carries no permissions",
			pod.ServiceAccountName, sts.Spec.Template.Spec.ServiceAccountName)
	}
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Error("the proxy mounts a ServiceAccount token it never uses")
	}
	if len(pod.Volumes) != 0 {
		t.Errorf("the proxy mounts %v, want nothing", pod.Volumes)
	}

	if len(pod.Containers) != 1 {
		t.Fatalf("want one container, got %d", len(pod.Containers))
	}
	c := pod.Containers[0]
	if c.Image != d.Spec.PNFS.MDS.Image {
		t.Errorf("image = %q, want the metadata server's %q, so both ends speak one protocol", c.Image, d.Spec.PNFS.MDS.Image)
	}
	if !slices.Equal(c.Command, []string{"/usr/bin/mds-proxy"}) {
		t.Errorf("command = %v, want /usr/bin/mds-proxy", c.Command)
	}
	if !slices.Contains(c.Args, "--listen=$(HOST_IP):9811") {
		t.Errorf("args = %v, want it to listen on the node's address only", c.Args)
	}
	if !hasHostIPEnv(c.Env) {
		t.Errorf("env = %v, want HOST_IP from status.hostIP", c.Env)
	}
	if cpu, ok := c.Resources.Limits[corev1.ResourceCPU]; ok {
		t.Errorf("CPU limit %s: the proxy carries a volume path, which a CPU limit would throttle", cpu.String())
	}
}

func TestTheMDSProxyHasNoPrivilege(t *testing.T) {
	sc := mdsProxy(t).Spec.Template.Spec.Containers[0].SecurityContext
	if sc == nil {
		t.Fatal("no securityContext")
	}
	if sc.Privileged != nil && *sc.Privileged {
		t.Error("privileged")
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Error("privilege escalation is allowed")
	}
	if sc.Capabilities == nil || !slices.Equal(sc.Capabilities.Drop, []corev1.Capability{"ALL"}) || len(sc.Capabilities.Add) != 0 {
		t.Errorf("capabilities = %+v, want every one dropped and none added", sc.Capabilities)
	}
	if sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot || sc.RunAsUser == nil || *sc.RunAsUser == 0 {
		t.Errorf("runAsNonRoot = %v, runAsUser = %v, want a non-root user", sc.RunAsNonRoot, sc.RunAsUser)
	}
	if sc.ReadOnlyRootFilesystem == nil || !*sc.ReadOnlyRootFilesystem {
		t.Error("the root filesystem is writable")
	}
}

func TestTheMDSRunnerIsToldWhereItsNodesProxyIs(t *testing.T) {
	_, sts := mdsObjects(t, withMDS(testDriver("simplyblock")))
	c := runnerContainer(t, sts)
	if !slices.Contains(c.Args, "--host-proxy=$(HOST_IP):9811") {
		t.Errorf("args = %v, want --host-proxy at the node's address", c.Args)
	}
	if !hasHostIPEnv(c.Env) {
		t.Errorf("env = %v, want HOST_IP from status.hostIP", c.Env)
	}
}

func hasHostIPEnv(env []corev1.EnvVar) bool {
	return slices.ContainsFunc(env, func(e corev1.EnvVar) bool {
		return e.Name == "HOST_IP" && e.ValueFrom != nil && e.ValueFrom.FieldRef != nil &&
			e.ValueFrom.FieldRef.FieldPath == "status.hostIP"
	})
}

func reconcileDriver(t *testing.T, d *simplyblockv1alpha2.SimplyblockDriver, objs ...client.Object) client.Client {
	t.Helper()
	scheme := reconcilerScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append(objs, d)...).WithStatusSubresource(d).Build()
	r := &SimplyblockDriverReconciler{Client: c, Scheme: scheme, Snapshots: fixedSnapshotAPI{served: true}}
	if _, err := r.Reconcile(context.Background(), requestFor(d)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return c
}

func TestADriverWithAMetadataServerRunsTheProxy(t *testing.T) {
	d := withMDS(testDriver("simplyblock"))
	c := reconcileDriver(t, d)

	var ds appsv1.DaemonSet
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: d.Namespace, Name: "simplyblock-csi-mds-proxy"}, &ds); err != nil {
		t.Fatalf("the proxy DaemonSet: %v", err)
	}
	if owner := metav1ControllerOf(&ds); owner != d.Name {
		t.Errorf("owned by %q, want the driver %q, so deleting the driver removes it", owner, d.Name)
	}
}

func TestADriverWithoutAMetadataServerRemovesTheProxy(t *testing.T) {
	withProxy := withMDS(testDriver("simplyblock"))
	stale, err := MDSProxyDaemonSet(withProxy)
	if err != nil {
		t.Fatalf("MDSProxyDaemonSet: %v", err)
	}
	if err := setOwnership(withProxy, stale, reconcilerScheme(t)); err != nil {
		t.Fatalf("setOwnership: %v", err)
	}

	d := testDriver("simplyblock")
	c := reconcileDriver(t, d, stale)

	var ds appsv1.DaemonSet
	err = c.Get(context.Background(), client.ObjectKeyFromObject(stale), &ds)
	if !apierrors.IsNotFound(err) {
		t.Errorf("the proxy DaemonSet is still there (%v) after spec.pnfs.mds was unset", err)
	}
}

func TestAProxyDaemonSetTheDriverDoesNotOwnIsLeftAlone(t *testing.T) {
	stale, err := MDSProxyDaemonSet(withMDS(testDriver("simplyblock")))
	if err != nil {
		t.Fatalf("MDSProxyDaemonSet: %v", err)
	}
	d := testDriver("simplyblock")
	c := reconcileDriver(t, d, stale)

	var ds appsv1.DaemonSet
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(stale), &ds); err != nil {
		t.Errorf("a DaemonSet the driver does not own was removed: %v", err)
	}
}

// metav1ControllerOf names the controlling owner of obj, or returns the empty
// string when it has none.
func metav1ControllerOf(obj client.Object) string {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller {
			return ref.Name
		}
	}
	return ""
}
