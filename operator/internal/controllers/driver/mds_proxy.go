// The mds-proxy: a DaemonSet on the host's network that opens the metadata
// server guest's connections to the storage node on its own node, which that
// storage node refuses from a pod (design-pnfs-mds-vm.md §8.5).
//
// It lives beside the metadata server's StatefulSet because it shares the
// metadata server's image, node selector, tolerations, and ServiceAccount. It
// is applied by the driver reconciler while spec.pnfs.mds is set, rather than
// with the StatefulSet on the first export, so it already runs, and has pulled
// the image, wherever the metadata server starts.

package driver

import (
	"context"
	"fmt"
	"maps"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/simplyblock/atlas/ptr"
	simplyblockv1alpha2 "github.com/simplyblock/simplyblock-operator/api/v1alpha2"
	"github.com/simplyblock/simplyblock-operator/internal/utils"
)

const (
	// mdsProxyPort is where the proxy listens on its node's address, and where
	// the runner reaches it.
	mdsProxyPort = 9811

	// mdsProxyUser is the proxy's user: nobody, which owns nothing in the
	// image and needs nothing, since the port is unprivileged.
	mdsProxyUser = 65534

	// hostIPEnv carries the node's address (status.hostIP) into both ends.
	hostIPEnv = "HOST_IP"
)

// mdsProxyAddress is the proxy's host:port as both containers spell it, with
// the node's address substituted by kubelet from hostIPEnv.
var mdsProxyAddress = "$(" + hostIPEnv + "):" + strconv.Itoa(mdsProxyPort)

// mdsProxyName is the proxy's DaemonSet.
func mdsProxyName(d *simplyblockv1alpha2.SimplyblockDriver) string {
	return names(d).prefix + "mds-proxy"
}

// mdsNodeSelector is where the metadata server may run: the KVM-capable nodes
// matching spec.pnfs.mds.nodeSelector.
func mdsNodeSelector(spec *simplyblockv1alpha2.DriverPNFSMDS) map[string]string {
	selector := maps.Clone(spec.NodeSelector)
	if selector == nil {
		selector = map[string]string{}
	}
	selector[KVMCapableLabel] = KVMCapableValue
	return selector
}

// hostIPEnvVar hands a container its node's address.
func hostIPEnvVar() corev1.EnvVar {
	return corev1.EnvVar{
		Name: hostIPEnv,
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.hostIP"},
		},
	}
}

// MDSProxyDaemonSet renders the proxy for a driver whose spec.pnfs.mds is set.
// The caller sets ownership.
func MDSProxyDaemonSet(d *simplyblockv1alpha2.SimplyblockDriver) (*appsv1.DaemonSet, error) {
	spec := d.Spec.PNFS.MDS
	if spec == nil {
		return nil, fmt.Errorf("spec.pnfs.mds is unset, so there is no metadata server to proxy for")
	}
	image, err := mdsImage(d)
	if err != nil {
		return nil, err
	}
	name := mdsProxyName(d)
	labels := map[string]string{"app": name}
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: d.Namespace},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: map[string]string{utils.AnnotationLogCollector: "true"},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName:           names(d).mdsServiceAccount,
					AutomountServiceAccountToken: ptr.To(false),
					NodeSelector:                 mdsNodeSelector(spec),
					Tolerations:                  spec.Tolerations,
					// rbac-justified: the proxy opens the guest's connections
					// to its node's storage node from the host, because SPDK's
					// listeners are bound to the node's data interface and
					// refuse a pod's connection to its own node. It holds no
					// privilege, capability, or host path (design-pnfs-mds-vm.md
					// §10).
					HostNetwork: true,
					DNSPolicy:   corev1.DNSClusterFirstWithHostNet,
					Containers:  []corev1.Container{mdsProxyContainer(d, image)},
				},
			},
		},
	}, nil
}

func mdsProxyContainer(d *simplyblockv1alpha2.SimplyblockDriver, image string) corev1.Container {
	return corev1.Container{
		Name:            "mds-proxy",
		Image:           image,
		ImagePullPolicy: pullPolicy(d),
		Command:         []string{"/usr/bin/mds-proxy"},
		Args:            []string{"--listen=" + mdsProxyAddress},
		Env:             []corev1.EnvVar{hostIPEnvVar()},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			RunAsNonRoot:             ptr.To(true),
			RunAsUser:                ptr.To(int64(mdsProxyUser)),
			ReadOnlyRootFilesystem:   ptr.To(true),
		},
		// No CPU limit: the proxy carries one path of the guest's volumes, and
		// throttling it would throttle their I/O.
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("32Mi"),
			},
			Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Mi")},
		},
		Ports: []corev1.ContainerPort{{Name: "proxy", ContainerPort: mdsProxyPort, Protocol: corev1.ProtocolTCP}},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:  corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: named("proxy")}},
			PeriodSeconds: 10,
		},
	}
}

// ensureMDSProxy applies the proxy while spec.pnfs.mds is set and deletes the
// one this driver owns once it is not. A metadata server image that cannot be
// resolved is reported and leaves the proxy as it is: the metadata server
// cannot start either, and that is reported where it fails.
func (r *SimplyblockDriverReconciler) ensureMDSProxy(ctx context.Context, d *simplyblockv1alpha2.SimplyblockDriver) error {
	if d.Spec.PNFS.MDS == nil {
		var ds appsv1.DaemonSet
		key := client.ObjectKey{Namespace: d.Namespace, Name: mdsProxyName(d)}
		if err := r.Get(ctx, key, &ds); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("reading the mds-proxy DaemonSet: %w", err)
		}
		if !metav1.IsControlledBy(&ds, d) {
			return nil
		}
		if err := r.Delete(ctx, &ds); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("deleting the mds-proxy DaemonSet: %w", err)
		}
		return nil
	}

	ds, err := MDSProxyDaemonSet(d)
	if err != nil {
		r.event(d, corev1.EventTypeWarning, reasonNoImage, fmt.Sprintf("cannot run the mds-proxy: %v", err))
		return nil
	}
	if err := setOwnership(d, ds, r.Scheme); err != nil {
		return fmt.Errorf("set ownership on the mds-proxy DaemonSet: %w", err)
	}
	cfg, err := r.applyConfiguration(ds)
	if err != nil {
		return fmt.Errorf("encode the mds-proxy DaemonSet for apply: %w", err)
	}
	if err := r.Apply(ctx, cfg, fieldOwner, client.ForceOwnership); err != nil {
		return fmt.Errorf("apply the mds-proxy DaemonSet: %w", err)
	}
	return nil
}
