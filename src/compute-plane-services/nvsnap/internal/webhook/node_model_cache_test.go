// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"path"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelcache"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelvolume"
)

type fixedTenants map[string]string

func (f fixedTenants) Scope(_ context.Context, ns string) string {
	if s, ok := f[ns]; ok {
		return s
	}
	return "namespace:" + ns
}

func nodeCacheMutator(t *testing.T) *Mutator {
	t.Helper()
	return &Mutator{
		Backend:        newBackend(t),
		NodeModelCache: &NodeModelCache{Root: "/var/lib/nvsnap-model-cache", Wait: 600, Tenants: fixedTenants{"sr-fn": "tenant:nca=a"}},
	}
}

func initByName(pod *corev1.Pod, name string) *corev1.Container {
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == name {
			return &pod.Spec.InitContainers[i]
		}
	}
	return nil
}

func TestNodeModelCacheWrapsTheDownloadInit(t *testing.T) {
	m := nodeCacheMutator(t)
	pod := ngcFunctionPod()
	orig := containerArgv(initByName(pod, "download-ngc-model"))
	out := applyPatches(t, pod, m.nodeModelCachePatches(context.Background(), pod, true))

	key := out.Labels[ModelCacheKeyLabel]
	if !modelcache.ValidKey(key) {
		t.Fatalf("key label = %q", key)
	}
	if out.Annotations[ModelCacheVolumeAnnotation] != "ngc-models" || out.Annotations[ModelCacheInitAnnotation] != "download-ngc-model" {
		t.Fatalf("annotations = %v", out.Annotations)
	}
	init := initByName(out, "download-ngc-model")
	want := append([]string{HelperBin, "model-cache", "--cache", ModelCacheMount, "--to", "/config/models", "--wait", "600", "--"}, orig...)
	if !reflect.DeepEqual(init.Command, want) || init.Args != nil {
		t.Fatalf("init command = %q args %q\nwant %q", init.Command, init.Args, want)
	}
	var cacheRO, helper bool
	for _, vm := range init.VolumeMounts {
		cacheRO = cacheRO || (vm.Name == ModelCacheVolume && vm.MountPath == ModelCacheMount && vm.ReadOnly)
		helper = helper || vm.Name == helperVolumeName
	}
	if !cacheRO || !helper {
		t.Fatalf("init mounts = %+v; want the cache read-only and the helper", init.VolumeMounts)
	}
	var vol *corev1.Volume
	for i := range out.Spec.Volumes {
		if out.Spec.Volumes[i].Name == ModelCacheVolume {
			vol = &out.Spec.Volumes[i]
		}
	}
	if vol == nil || vol.HostPath == nil || vol.HostPath.Path != path.Join("/var/lib/nvsnap-model-cache", key) {
		t.Fatalf("cache volume = %+v; want hostPath <root>/<key>", vol)
	}
	// The main container and the download volume are untouched: restore
	// and serving never see the cache.
	if !reflect.DeepEqual(out.Spec.Containers, pod.Spec.Containers) {
		t.Fatal("the main container changed")
	}
	for _, v := range out.Spec.Volumes {
		if v.Name == "ngc-models" && v.EmptyDir == nil {
			t.Fatal("the download volume is no longer an emptyDir")
		}
	}
}

func TestNodeModelCacheKeyIsPerTenantAndPerRecipe(t *testing.T) {
	m := nodeCacheMutator(t)
	key := func(mut func(*corev1.Pod)) string {
		pod := ngcFunctionPod()
		mut(pod)
		return applyPatches(t, pod, m.nodeModelCachePatches(context.Background(), pod, true)).Labels[ModelCacheKeyLabel]
	}
	base := key(func(*corev1.Pod) {})
	same := key(func(p *corev1.Pod) {
		// Per-deployment env the download does not use does not split the key.
		initByName(p, "download-ngc-model").Env = append(initByName(p, "download-ngc-model").Env, corev1.EnvVar{Name: "GROVE_HEADLESS_SERVICE", Value: "x.other-ns.svc"})
	})
	if same != base {
		t.Fatal("unrelated env changed the key")
	}
	for name, mut := range map[string]func(*corev1.Pod){
		"tenant": func(p *corev1.Pod) { p.Namespace = "other" },
		"image":  func(p *corev1.Pod) { initByName(p, "download-ngc-model").Image = "nvcr.io/org/ultra:vllm2" },
		"command": func(p *corev1.Pod) {
			initByName(p, "download-ngc-model").Args = []string{"ngc registry model download-version --dest /config/models x"}
		},
		"model": func(p *corev1.Pod) {
			for _, c := range []*corev1.Container{initByName(p, "download-ngc-model")} {
				c.Env[0].Value = "org/team/other:1"
			}
		},
		"any other env": func(p *corev1.Pod) {
			initByName(p, "download-ngc-model").Env = append(initByName(p, "download-ngc-model").Env, corev1.EnvVar{Name: "STORE_ENDPOINT", Value: "https://other"})
		},
		"envFrom": func(p *corev1.Pod) {
			initByName(p, "download-ngc-model").EnvFrom = []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "dl"}}}}
		},
		"a mount": func(p *corev1.Pod) {
			for i := range p.Spec.Volumes {
				if p.Spec.Volumes[i].Name == "scripts" {
					p.Spec.Volumes[i].ConfigMap.Name = "other-scripts"
				}
			}
		},
		"envFrom order": func(p *corev1.Pod) {
			c := initByName(p, "download-ngc-model")
			c.EnvFrom = []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "b"}}},
				{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "a"}}}}
		},
		"fsGroup": func(p *corev1.Pod) {
			g := int64(2000)
			p.Spec.SecurityContext = &corev1.PodSecurityContext{FSGroup: &g}
		},
		"configmap items": func(p *corev1.Pod) {
			for i := range p.Spec.Volumes {
				if p.Spec.Volumes[i].Name == "scripts" {
					p.Spec.Volumes[i].ConfigMap.Items = []corev1.KeyToPath{{Key: "v2.sh", Path: "start.sh"}}
				}
			}
		},
		"ngc env": func(p *corev1.Pod) {
			initByName(p, "download-ngc-model").Env = append(initByName(p, "download-ngc-model").Env, corev1.EnvVar{Name: "NGC_CLI_MAX_CONCURRENCY", Value: "3"})
		},
	} {
		if k := key(mut); k == base || k == "" {
			t.Fatalf("%s: key %q did not change", name, k)
		}
	}
}

func TestNodeModelCacheLeavesOtherPodsAlone(t *testing.T) {
	m := nodeCacheMutator(t)
	for name, pod := range map[string]*corev1.Pod{
		"engine downloads itself": stockVLLMPod(),
		"no gpu": func() *corev1.Pod {
			p := ngcFunctionPod()
			p.Spec.Containers[0].Resources = corev1.ResourceRequirements{}
			return p
		}(),
		"init without a command": func() *corev1.Pod {
			p := ngcFunctionPod()
			initByName(p, "download-ngc-model").Command = nil
			return p
		}(),
		"landing is the customer's claim": func() *corev1.Pod {
			p := ngcFunctionPod()
			for i := range p.Spec.Volumes {
				if p.Spec.Volumes[i].Name == "ngc-models" {
					p.Spec.Volumes[i].VolumeSource = corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "shared"}}
				}
			}
			return p
		}(),
		"subPath download mount": func() *corev1.Pod {
			p := ngcFunctionPod()
			c := initByName(p, "download-ngc-model")
			c.VolumeMounts[0].SubPath = "data"
			return p
		}(),
		"already wrapped": func() *corev1.Pod {
			p := ngcFunctionPod()
			c := initByName(p, "download-ngc-model")
			c.Command = append([]string{HelperBin, "model-cache"}, c.Command...)
			return p
		}(),
	} {
		if ops := m.nodeModelCachePatches(context.Background(), pod, true); ops != nil {
			t.Errorf("%s: patched %v", name, ops)
		}
	}

	off := nodeCacheMutator(t)
	off.NodeModelCache = nil
	if ops := off.nodeModelCachePatches(context.Background(), ngcFunctionPod(), true); ops != nil {
		t.Error("feature off: patched")
	}
	if ops := nodeCacheMutator(t).nodeModelCachePatches(context.Background(), ngcFunctionPod(), false); ops != nil {
		t.Error("a pod not on the node bundle: patched")
	}
	both := nodeCacheMutator(t)
	both.ModelVolume = &modelvolume.Provisioner{}
	if ops := both.nodeModelCachePatches(context.Background(), ngcFunctionPod(), true); ops != nil {
		t.Error("model volumes on: the cache patched too")
	}
}

// Wiring: Mutate, the production entry point, applies the cache to a cold
// NGC pod.
func TestMutateAppliesTheNodeModelCache(t *testing.T) {
	m := nodeCacheMutator(t)
	m.HostBundleRoot = "/bundle"
	// Without the gpushare library the pod does not use the node bundle,
	// and is left alone.
	plain := ngcFunctionPod()
	ops, err := m.Mutate(context.Background(), plain)
	if err != nil {
		t.Fatal(err)
	}
	if c := initByName(applyPatches(t, plain, ops), "download-ngc-model"); len(c.Command) > 0 && c.Command[0] == HelperBin {
		t.Fatal("Mutate wrapped the init of a pod that does not use the node bundle")
	}
	pod := ngcFunctionPod()
	pod.Annotations = map[string]string{GPUShareAnnotation: "true"}
	ops, err = m.Mutate(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	out := applyPatches(t, pod, ops)
	init := initByName(out, "download-ngc-model")
	if len(init.Command) < 2 || init.Command[0] != HelperBin || init.Command[1] != "model-cache" {
		t.Fatalf("Mutate did not wrap the download init: %q", init.Command)
	}
	if !strings.Contains(strings.Join(init.Command, " "), "download-version") {
		t.Fatalf("the original download is not the fallback: %q", init.Command)
	}
}

// The agent recomputes the key from the admitted (wrapped) pod; it must
// match the key admission gave the unwrapped pod, and must not follow a
// label the pod sets itself.
func TestModelCacheKeyIsTheSameBeforeAndAfterWrapping(t *testing.T) {
	m := nodeCacheMutator(t)
	pod := ngcFunctionPod()
	before, ok := ResolveModelCacheDownload(context.Background(), m.NodeModelCache.Tenants, pod, 0)
	if !ok || before.Wrapped {
		t.Fatalf("unwrapped: %+v ok=%v", before, ok)
	}
	out := applyPatches(t, pod, m.nodeModelCachePatches(context.Background(), pod, true))
	out.Labels[ModelCacheKeyLabel] = "ffffffffffffffffffffffffffffffff" // a pod's own claim
	after, ok := ResolveModelCacheDownload(context.Background(), m.NodeModelCache.Tenants, out, 0)
	if !ok || !after.Wrapped || after.Key != before.Key {
		t.Fatalf("wrapped: key %q wrapped=%v ok=%v; want key %q", after.Key, after.Wrapped, ok, before.Key)
	}
	if !reflect.DeepEqual(after.Download, containerArgv(initByName(pod, "download-ngc-model"))) {
		t.Fatalf("download = %q", after.Download)
	}
}

// Another writable volume the init mounts and a later container reads may
// be an output: the helper is told to watch it.
func TestNodeModelCacheWatchesOtherWritableVolumes(t *testing.T) {
	m := nodeCacheMutator(t)
	p := ngcFunctionPod()
	p.Spec.Volumes = append(p.Spec.Volumes,
		corev1.Volume{Name: "rendered", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		corev1.Volume{Name: "ro-pvc", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "in", ReadOnly: true}}})
	c := initByName(p, "download-ngc-model")
	c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "rendered", MountPath: "/config/rendered"}, corev1.VolumeMount{Name: "ro-pvc", MountPath: "/in"})
	p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "rendered", MountPath: "/r"}, corev1.VolumeMount{Name: "ro-pvc", MountPath: "/in"})
	out := applyPatches(t, p, m.nodeModelCachePatches(context.Background(), p, true))
	cmd := strings.Join(initByName(out, "download-ngc-model").Command, " ")
	if !strings.Contains(cmd, "--watch /config/rendered --") || strings.Contains(cmd, "--watch /in") {
		t.Fatalf("command %q: want /config/rendered watched and the read-only claim not", cmd)
	}
}

// A writable volume an earlier init filled is an input of the download,
// not an output: the pod stays eligible (NVCA's merged certificates).
func TestNodeModelCacheAcceptsAnInputVolumeFromAnEarlierInit(t *testing.T) {
	m := nodeCacheMutator(t)
	p := ngcFunctionPod()
	p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "certs", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
	certs := corev1.VolumeMount{Name: "certs", MountPath: "/etc/ssl/certs"}
	p.Spec.InitContainers = append([]corev1.Container{{Name: "merge-certs", Image: "x", Command: []string{"merge"}, VolumeMounts: []corev1.VolumeMount{certs}}}, p.Spec.InitContainers...)
	c := initByName(p, "download-ngc-model")
	c.VolumeMounts = append(c.VolumeMounts, certs)
	p.Spec.Containers[0].VolumeMounts = append(p.Spec.Containers[0].VolumeMounts, certs)
	if ops := m.nodeModelCachePatches(context.Background(), p, true); ops == nil {
		t.Fatal("a pod whose download reads an earlier init's volume was left alone")
	}
}

func TestModelCacheKeyFollowsEnvFromOrder(t *testing.T) {
	key := func(names ...string) string {
		p := ngcFunctionPod()
		c := initByName(p, "download-ngc-model")
		for _, n := range names {
			c.EnvFrom = append(c.EnvFrom, corev1.EnvFromSource{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: n}}})
		}
		d, ok := ResolveModelCacheDownload(context.Background(), nil, p, 0)
		if !ok {
			t.Fatal("not a cache download")
		}
		return d.Key
	}
	if key("a", "b") == key("b", "a") {
		t.Fatal("envFrom order does not change the key; the last source wins in Kubernetes")
	}
}

// A pod whose gpushare placement is already in its spec uses the bundle
// even though the gpushare step adds nothing.
func TestMutateTreatsAnAlreadyPlacedBundleAsPlaced(t *testing.T) {
	m := nodeCacheMutator(t)
	m.HostBundleRoot = "/bundle"
	pod := ngcFunctionPod()
	pod.Spec.Volumes = append(pod.Spec.Volumes, m.helperVolume())
	ops, err := m.Mutate(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	if c := initByName(applyPatches(t, pod, ops), "download-ngc-model"); len(c.Command) < 2 || c.Command[1] != "model-cache" {
		t.Fatalf("a pod already on the bundle was not wrapped: %q", c.Command)
	}
}

// Pods of one instance differ in the names of their service account token
// volumes; that must not give each its own key.
func TestModelCacheKeyIgnoresPodLocalVolumeNames(t *testing.T) {
	key := func(tokenVol string) string {
		p := ngcFunctionPod()
		p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: tokenVol, VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
			Sources: []corev1.VolumeProjection{{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}}}}}}})
		c := initByName(p, "download-ngc-model")
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: tokenVol, MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true})
		d, ok := ResolveModelCacheDownload(context.Background(), nil, p, 0)
		if !ok {
			t.Fatal("not a cache download")
		}
		return d.Key
	}
	if key("kube-api-access-mh6zm") != key("kube-api-access-sbg6z") {
		t.Fatal("a pod-local volume name changed the key")
	}
}

// An init that already mounts something else at a path the cache mounts,
// or a pod that already has a volume of the cache's name, is left as it
// is: a second mount at one path is a pod Kubernetes rejects.
func TestNodeModelCacheLeavesAConflictingMountAlone(t *testing.T) {
	m := nodeCacheMutator(t)
	for name, mut := range map[string]func(*corev1.Pod){
		"cache path": func(p *corev1.Pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "theirs", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
			c := initByName(p, "download-ngc-model")
			c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "theirs", MountPath: ModelCacheMount})
		},
		"helper path": func(p *corev1.Pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "theirs", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
			c := initByName(p, "download-ngc-model")
			c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "theirs", MountPath: helperMount().MountPath})
		},
		"cache volume name": func(p *corev1.Pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: ModelCacheVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}})
		},
	} {
		pod := ngcFunctionPod()
		mut(pod)
		if p := m.nodeModelCachePatches(context.Background(), pod, true); p != nil {
			t.Errorf("%s: patched a pod whose mounts conflict: %v", name, p)
		}
	}
	// The helper's own volume already mounted where the helper goes is not
	// a conflict.
	pod := ngcFunctionPod()
	c := initByName(pod, "download-ngc-model")
	c.VolumeMounts = append(c.VolumeMounts, helperMount())
	out := applyPatches(t, pod, m.nodeModelCachePatches(context.Background(), pod, true))
	n := 0
	for _, vm := range initByName(out, "download-ngc-model").VolumeMounts {
		if vm.MountPath == helperMount().MountPath {
			n++
		}
	}
	if out.Labels[ModelCacheKeyLabel] == "" || n != 1 {
		t.Fatalf("helper already mounted: wrapped=%v, helper mounts=%d", out.Labels[ModelCacheKeyLabel] != "", n)
	}
}

// A user can name an env var anything, POD_ and NODE_ included; only the
// prefixes the platform sets per instance are left out of the key.
func TestModelCacheKeyCoversUserPodAndNodeEnv(t *testing.T) {
	for _, name := range []string{"POD_MODEL_VARIANT", "NODE_PRECISION"} {
		key := func(v string) string {
			p := ngcFunctionPod()
			c := initByName(p, "download-ngc-model")
			c.Env = append(c.Env, corev1.EnvVar{Name: name, Value: v})
			d, ok := ResolveModelCacheDownload(context.Background(), nil, p, 0)
			if !ok {
				t.Fatal("not a cache download")
			}
			return d.Key
		}
		if key("a") == key("b") {
			t.Errorf("%s left out of the key", name)
		}
	}
}

// Single flight: the helper reads its pod's plan by the pod's uid, from a
// downward-API env var the wrapper adds; wrapping must not change the key.
func TestNodeModelCacheSingleFlightGivesTheHelperItsPodUID(t *testing.T) {
	m := nodeCacheMutator(t)
	m.NodeModelCache.PlanWait = 60
	pod := ngcFunctionPod()
	before, _ := ResolveModelCacheDownload(context.Background(), m.NodeModelCache.Tenants, pod, 0)
	out := applyPatches(t, pod, m.nodeModelCachePatches(context.Background(), pod, true))
	init := initByName(out, "download-ngc-model")
	var uid *corev1.EnvVar
	for i := range init.Env {
		if init.Env[i].Name == PodUIDEnv {
			uid = &init.Env[i]
		}
	}
	if uid == nil || uid.ValueFrom == nil || uid.ValueFrom.FieldRef == nil || uid.ValueFrom.FieldRef.FieldPath != "metadata.uid" {
		t.Fatalf("init env = %+v; want %s from metadata.uid", init.Env, PodUIDEnv)
	}
	if !strings.Contains(strings.Join(init.Command, " "), "--plan-wait 60 --signal "+ModelCacheSignalMount+" --") {
		t.Fatalf("init command = %q; want --plan-wait 60 --signal", init.Command)
	}
	// The signal volume: in memory, tiny, and the init's alone.
	var sig *corev1.Volume
	for i := range out.Spec.Volumes {
		if out.Spec.Volumes[i].Name == ModelCacheSignalVolume {
			sig = &out.Spec.Volumes[i]
		}
	}
	if sig == nil || sig.EmptyDir == nil || sig.EmptyDir.Medium != corev1.StorageMediumMemory || sig.EmptyDir.SizeLimit == nil {
		t.Fatalf("signal volume = %+v", sig)
	}
	var mounted bool
	for _, vm := range init.VolumeMounts {
		mounted = mounted || (vm.Name == ModelCacheSignalVolume && vm.MountPath == ModelCacheSignalMount && !vm.ReadOnly)
	}
	if !mounted {
		t.Fatalf("init mounts = %+v; want the signal volume", init.VolumeMounts)
	}
	for _, c := range append(append([]corev1.Container{}, out.Spec.Containers...), out.Spec.InitContainers...) {
		if c.Name == init.Name {
			continue
		}
		for _, vm := range c.VolumeMounts {
			if vm.Name == ModelCacheSignalVolume {
				t.Fatalf("container %s mounts the signal volume", c.Name)
			}
		}
	}
	after, ok := ResolveModelCacheDownload(context.Background(), m.NodeModelCache.Tenants, out, 0)
	if !ok || after.Key != before.Key {
		t.Fatalf("key after wrapping %q ok=%v; want %q", after.Key, ok, before.Key)
	}
}

func TestNodeModelCacheWithoutSingleFlightAddsNoPlanWait(t *testing.T) {
	m := nodeCacheMutator(t)
	pod := ngcFunctionPod()
	out := applyPatches(t, pod, m.nodeModelCachePatches(context.Background(), pod, true))
	init := initByName(out, "download-ngc-model")
	// The cache still wraps the init: what follows checks the wrapper,
	// not an init admission left alone.
	if len(init.Command) < 2 || init.Command[0] != HelperBin || init.Command[1] != "model-cache" {
		t.Fatalf("init command = %q; want the cache's wrapper", init.Command)
	}
	for _, e := range init.Env {
		if e.Name == PodUIDEnv {
			t.Fatalf("env %s added with single flight off", PodUIDEnv)
		}
	}
	if strings.Contains(strings.Join(init.Command, " "), "--plan-wait") || strings.Contains(strings.Join(init.Command, " "), "--signal") {
		t.Fatalf("init command = %q", init.Command)
	}
	for _, v := range out.Spec.Volumes {
		if v.Name == ModelCacheSignalVolume {
			t.Fatal("signal volume added with single flight off")
		}
	}
}

// An init that already names the variable keeps its own; the wrapper does
// not wrap it rather than shadow or remove the user's value.
func TestNodeModelCacheSingleFlightLeavesAnInitThatNamesThePodUIDEnv(t *testing.T) {
	m := nodeCacheMutator(t)
	m.NodeModelCache.PlanWait = 60
	pod := ngcFunctionPod()
	c := initByName(pod, "download-ngc-model")
	c.Env = append(c.Env, corev1.EnvVar{Name: PodUIDEnv, Value: "mine"})
	if p := m.nodeModelCachePatches(context.Background(), pod, true); p != nil {
		t.Fatalf("wrapped an init that names %s: %+v", PodUIDEnv, p)
	}
}

// The user's own variable of that name, with no wrapper, is part of the
// recipe like any other.
func TestModelCacheKeyKeepsAUserPodUIDEnvOnAnUnwrappedInit(t *testing.T) {
	key := func(v string) string {
		p := ngcFunctionPod()
		c := initByName(p, "download-ngc-model")
		c.Env = append(c.Env, corev1.EnvVar{Name: PodUIDEnv, Value: v})
		d, _ := ResolveModelCacheDownload(context.Background(), nil, p, 0)
		return d.Key
	}
	if key("a") == key("b") {
		t.Fatal("a user's own variable left out of the key")
	}
}

// Without single flight the wrapper adds no pod-uid variable, so an init
// that sets one itself keeps it in its key after wrapping.
func TestModelCacheKeyKeepsAUserPodUIDEnvWithoutSingleFlight(t *testing.T) {
	m := nodeCacheMutator(t)
	pod := ngcFunctionPod()
	c := initByName(pod, "download-ngc-model")
	c.Env = append(c.Env, corev1.EnvVar{Name: PodUIDEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}})
	before, _ := ResolveModelCacheDownload(context.Background(), m.NodeModelCache.Tenants, pod, 0)
	out := applyPatches(t, pod, m.nodeModelCachePatches(context.Background(), pod, true))
	if init := initByName(out, "download-ngc-model"); len(init.Command) < 2 || init.Command[0] != HelperBin || init.Command[1] != "model-cache" {
		t.Fatalf("init command = %q; want the cache's wrapper", init.Command)
	}
	after, ok := ResolveModelCacheDownload(context.Background(), m.NodeModelCache.Tenants, out, 0)
	if !ok || !after.Wrapped || after.Key != before.Key {
		t.Fatalf("after wrapping: key %q wrapped=%v ok=%v; want the wrapped init with key %q", after.Key, after.Wrapped, ok, before.Key)
	}
}

// A label or annotation can differ between pods of one recipe, and can
// change after admission; a download that reads one (an env field
// reference, or a downward-API volume) would write another tree under the
// same key. Such a download is not cached. A per-pod value of the pod's
// own identity (its name) is keyed by its path, as before.
func TestModelCacheLeavesADownloadThatReadsALabelOrAnnotation(t *testing.T) {
	m := nodeCacheMutator(t)
	field := func(path string) *corev1.EnvVarSource {
		return &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: path}}
	}
	items := func(path string) []corev1.DownwardAPIVolumeFile {
		return []corev1.DownwardAPIVolumeFile{{Path: "v", FieldRef: &corev1.ObjectFieldSelector{FieldPath: path}}}
	}
	for name, edit := range map[string]func(*corev1.Pod){
		"env label": func(p *corev1.Pod) {
			c := initByName(p, "download-ngc-model")
			c.Env = append(c.Env, corev1.EnvVar{Name: "MODEL_REVISION", ValueFrom: field("metadata.labels['revision']")})
		},
		"env annotation": func(p *corev1.Pod) {
			c := initByName(p, "download-ngc-model")
			c.Env = append(c.Env, corev1.EnvVar{Name: "MODEL_REVISION", ValueFrom: field("metadata.annotations['revision']")})
		},
		"downward API volume": func(p *corev1.Pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "podinfo", VolumeSource: corev1.VolumeSource{
				DownwardAPI: &corev1.DownwardAPIVolumeSource{Items: items("metadata.labels")}}})
			c := initByName(p, "download-ngc-model")
			c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "podinfo", MountPath: "/etc/podinfo", ReadOnly: true})
		},
		"projected downward API volume": func(p *corev1.Pod) {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{Name: "podinfo", VolumeSource: corev1.VolumeSource{
				Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{{DownwardAPI: &corev1.DownwardAPIProjection{Items: items("metadata.annotations['revision']")}}}}}})
			c := initByName(p, "download-ngc-model")
			c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: "podinfo", MountPath: "/etc/podinfo", ReadOnly: true})
		},
	} {
		pod := ngcFunctionPod()
		edit(pod)
		if _, ok := ResolveModelCacheDownload(context.Background(), m.NodeModelCache.Tenants, pod, 0); ok {
			t.Errorf("%s: resolved a download that reads a label or annotation", name)
		}
		if p := m.nodeModelCachePatches(context.Background(), pod, true); p != nil {
			t.Errorf("%s: wrapped a download that reads a label or annotation", name)
		}
	}
	pod := ngcFunctionPod()
	c := initByName(pod, "download-ngc-model")
	c.Env = append(c.Env, corev1.EnvVar{Name: "POD_NAME", ValueFrom: field("metadata.name")})
	if _, ok := ResolveModelCacheDownload(context.Background(), m.NodeModelCache.Tenants, pod, 0); !ok {
		t.Fatal("a download with its pod's name by field reference is no longer cached")
	}
}

func wrappedByCache(c *corev1.Container) bool {
	return c != nil && len(c.Command) > 1 && c.Command[0] == HelperBin && c.Command[1] == "model-cache"
}

// A pod the platform stamped with restore-from, whose hash has no capture
// yet, is a cold start: its download seeds from the node's copy like any
// other cold pod's.
func TestMutateAppliesTheNodeModelCacheToAStampedColdStart(t *testing.T) {
	m := nodeCacheMutator(t)
	m.HostBundleRoot = "/bundle"
	pod := ngcFunctionPod()
	pod.Annotations = map[string]string{GPUShareAnnotation: "true", RestoreFromAnnotation: "deadbeef"}
	ops, err := m.Mutate(context.Background(), pod)
	if err != nil {
		t.Fatal(err)
	}
	if c := initByName(applyPatches(t, pod, ops), "download-ngc-model"); !wrappedByCache(c) {
		t.Fatalf("a stamped cold start's download was not wrapped: %q", c.Command)
	}
}

// A volume the pod declares under the helper's name, from anything but
// the node bundle, is not the bundle: the helper is not in it.
func TestMutateDoesNotTakeAPodsOwnVolumeForTheBundle(t *testing.T) {
	m := nodeCacheMutator(t)
	m.HostBundleRoot = "/bundle"
	own := corev1.Volume{Name: helperVolumeName, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
	for name, annotations := range map[string]map[string]string{
		"without gpushare": nil,
		"with gpushare":    {GPUShareAnnotation: "true"},
	} {
		t.Run(name, func(t *testing.T) {
			pod := ngcFunctionPod()
			pod.Annotations = annotations
			pod.Spec.Volumes = append(pod.Spec.Volumes, own)
			ops, err := m.Mutate(context.Background(), pod)
			if err != nil {
				t.Fatal(err)
			}
			if c := initByName(applyPatches(t, pod, ops), "download-ngc-model"); wrappedByCache(c) {
				t.Fatalf("the download was wrapped to run a helper from the pod's own %s emptyDir: %q", helperVolumeName, c.Command)
			}
		})
	}
	// The bundle's own volume, from an earlier admission of this or
	// another bundle version, still counts.
	for _, p := range []string{m.bundleDir(""), m.bundleDir("v2")} {
		pod := ngcFunctionPod()
		dir := corev1.HostPathDirectory
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: helperVolumeName,
			VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: p, Type: &dir}}})
		ops, err := m.Mutate(context.Background(), pod)
		if err != nil {
			t.Fatal(err)
		}
		if c := initByName(applyPatches(t, pod, ops), "download-ngc-model"); !wrappedByCache(c) {
			t.Fatalf("a pod with the bundle at %s was not wrapped: %q", p, c.Command)
		}
	}
}

// A restartable sidecar started before the download keeps running beside
// it and the engine: if it can write the download volume, what the volume
// holds is not only the download's.
func TestNodeModelCacheLeavesADownloadVolumeASidecarWrites(t *testing.T) {
	m := nodeCacheMutator(t)
	always := corev1.ContainerRestartPolicyAlways
	sidecar := func(readOnly bool) *corev1.Pod {
		p := ngcFunctionPod()
		vm := corev1.VolumeMount{Name: "ngc-models", MountPath: "/models", ReadOnly: readOnly}
		p.Spec.InitContainers = append([]corev1.Container{{Name: "sync", Image: "x", Command: []string{"sync"},
			RestartPolicy: &always, VolumeMounts: []corev1.VolumeMount{vm}}}, p.Spec.InitContainers...)
		return p
	}
	if ops := m.nodeModelCachePatches(context.Background(), sidecar(false), true); ops != nil {
		t.Fatal("a download whose volume an earlier sidecar can write was wrapped")
	}
	if ops := m.nodeModelCachePatches(context.Background(), sidecar(true), true); ops == nil {
		t.Fatal("a download whose volume an earlier sidecar only reads was left alone")
	}
	// An earlier init that is not a sidecar has ended before the download.
	p := sidecar(false)
	p.Spec.InitContainers[0].RestartPolicy = nil
	if ops := m.nodeModelCachePatches(context.Background(), p, true); ops == nil {
		t.Fatal("a download after an ordinary init that wrote its volume was left alone")
	}
}

// The platform's per-instance variables (Grove and NVCF metadata) read
// labels and annotations by field reference in every function's download
// init. They are left out of the key because they do not steer what a
// download writes; they must not turn the cache off either. A user's own
// variable that reads a label still does.
func TestNodeModelCacheWrapsAnInitThatReadsPlatformLabels(t *testing.T) {
	m := nodeCacheMutator(t)
	pod := ngcFunctionPod()
	c := initByName(pod, "download-ngc-model")
	ref := func(name, path string) corev1.EnvVar {
		return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: path}}}
	}
	c.Env = append(c.Env,
		ref("GROVE_PCSG_INDEX", "metadata.labels['grove.io/podcliquescalinggroup-replica-index']"),
		ref("NVCF_FUNCTION_ID", "metadata.labels['function-id']"),
		ref("NVCF_REGION", "metadata.annotations['nvcf.nvidia.io/region']"))
	if p := m.nodeModelCachePatches(context.Background(), pod, true); p == nil {
		t.Fatal("an init that reads only the platform's labels is not wrapped")
	}
	user := ngcFunctionPod()
	u := initByName(user, "download-ngc-model")
	u.Env = append(u.Env, ref("MODEL_VARIANT", "metadata.labels['variant']"))
	if p := m.nodeModelCachePatches(context.Background(), user, true); p != nil {
		t.Fatal("an init whose own variable reads a label is wrapped")
	}
}
