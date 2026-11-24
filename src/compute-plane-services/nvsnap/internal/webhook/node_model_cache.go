// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/election"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelcache"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/modelid"
)

// Node-local model cache (docs/design/node-model-cache.md). A cold pod
// whose init downloads its model into an emptyDir gets that init wrapped
// by nvsnap-helper model-cache: with the node's cache of the same
// download mounted read-only into the init (and only the init), the
// helper seeds the emptyDir from a verified copy, or runs the download
// as the pod would have. The emptyDir and the main container are left as
// they are, so a capture still finds the download volume unused and a
// group restore still skips the download, and nothing that serves or is
// checkpointed ever mounts the cache.
const (
	// ModelCacheVolume is the pod volume of the node's cache directory for
	// the pod's download key.
	ModelCacheVolume = "nvsnap-model-cache"
	// ModelCacheMount is where the download init sees it.
	ModelCacheMount = "/nvsnap-model-cache"
	// ModelCacheSignalVolume, with single flight, is an in-memory emptyDir
	// only the wrapped init mounts, at ModelCacheSignalMount: the helper
	// tells the agent there that its download is over.
	ModelCacheSignalVolume = "nvsnap-model-cache-signal"
	ModelCacheSignalMount  = "/nvsnap-model-cache-signal"
	// ModelCacheKeyLabel carries the download's cache key, for the agent
	// on the pod's node (harvest, peer fetch, eviction pins).
	ModelCacheKeyLabel = "nvsnap.io/model-cache-key"
	// ModelCacheVolumeAnnotation names the download volume the agent
	// harvests from.
	ModelCacheVolumeAnnotation = "nvsnap.io/model-cache-volume"
	// ModelCacheInitAnnotation names the wrapped download init, whose
	// successful exit tells the agent the volume is complete.
	ModelCacheInitAnnotation = "nvsnap.io/model-cache-init"
)

// TenantScoper names the tenant of a namespace (tenant.Resolver).
type TenantScoper interface {
	Scope(ctx context.Context, ns string) string
}

// NodeModelCache configures the node-local model cache.
type NodeModelCache struct {
	// Root is the cache directory on every node.
	Root string
	// Wait bounds, in seconds, how long a download init waits for a peer
	// fetch the agent has in progress.
	Wait int
	// Tenants scopes keys: a tree downloaded with one tenant's
	// credentials is never seeded into another tenant's pod.
	Tenants TenantScoper
	// PlanWait, when positive, turns on single flight
	// (docs/design/model-cache-single-flight.md): the helper waits that
	// many seconds for the agent's plan for its pod (download, or wait
	// for a node that downloads), and reads it by the pod's uid.
	PlanWait int
}

// PodUIDEnv gives a wrapped download init its pod's uid, from the
// downward API, so the helper can read its pod's plan. It is the wrapper's
// own, like its command and mounts, so it is not part of the key.
const PodUIDEnv = modelcache.PodUIDEnv

// orchestrationEnv are env var prefixes the platform sets per instance or
// deployment (names, namespaces, ids). They do not steer what a download
// writes and would split every redeploy's key; every other env var is part
// of the recipe. POD_ and NODE_ are not here: a user may name a variable
// that steers the download so, and the platform's per-pod values arrive by
// field reference, whose path, not value, is in the recipe.
var orchestrationEnv = []string{"GROVE_", "NVCF_", "KUBERNETES_", "HOSTNAME", "OTEL_"}

// nodeModelCachePatches wraps the pod's model download init, or returns
// nil when the pod has none the cache can serve.
//
// bundled says the pod already mounts the node's nvsnap bundle (the
// gpushare library is placed): the helper comes from that bundle, and a
// pod that did not depend on it before must not start to, or it could
// wait for a directory its node never has.
func (m *Mutator) nodeModelCachePatches(ctx context.Context, pod *corev1.Pod, bundled bool) []PatchOp {
	c := m.NodeModelCache
	if c == nil || c.Root == "" || m.ModelVolume != nil || !bundled || gpuRequest(pod) == 0 {
		return nil
	}
	// The pod's own volume under the helper's name would be mounted where
	// the helper runs from, and the helper is not in it.
	if declared, bundle := m.bundleHelperVolume(pod); declared && !bundle {
		m.logger().WithField("pod", election.PodIdentity(pod)).
			Info("node model cache: not wrapping; the pod declares " + helperVolumeName + " from outside the node bundle")
		return nil
	}
	d, ok := ResolveModelCacheDownload(ctx, c.Tenants, pod, m.MainContainer)
	if !ok || d.Wrapped {
		// A wrapped init is a pod admitted twice; leave it as it is.
		return nil
	}
	idx, init, land, key := d.InitIndex, &pod.Spec.InitContainers[d.InitIndex], d.Landing, d.Key
	log := m.logger().WithFields(logrus.Fields{"pod": election.PodIdentity(pod), "model": d.ShareKey, "key": key})

	to := initMountFor(init, land)
	wait := c.Wait
	if wait <= 0 {
		wait = 600
	}
	argv := []string{HelperBin, "model-cache", "--cache", ModelCacheMount, "--to", to, "--wait", strconv.Itoa(wait)}
	for _, w := range d.Watch {
		argv = append(argv, "--watch", w)
	}
	if c.PlanWait > 0 {
		for _, e := range init.Env {
			if e.Name == PodUIDEnv {
				log.WithField("init", init.Name).Info("node model cache: not wrapping; the init sets " + PodUIDEnv + " itself")
				return nil
			}
		}
		argv = append(argv, "--plan-wait", strconv.Itoa(c.PlanWait), "--signal", ModelCacheSignalMount)
	}
	argv = append(append(argv, "--"), containerArgv(init)...)
	base := fmt.Sprintf("/spec/initContainers/%d", idx)
	patches := []PatchOp{{Op: "replace", Path: base + "/command", Value: argv}}
	if init.Args != nil {
		patches = append(patches, PatchOp{Op: "remove", Path: base + "/args"})
	}
	mounts, ok := cacheMounts(pod, init, c.PlanWait > 0)
	if !ok {
		log.WithField("init", init.Name).Info("node model cache: not wrapping; the init already uses a path or volume name the cache needs")
		return nil
	}
	if c.PlanWait > 0 {
		uid := corev1.EnvVar{Name: PodUIDEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}}
		if init.Env == nil {
			patches = append(patches, PatchOp{Op: "add", Path: base + "/env", Value: []corev1.EnvVar{uid}})
		} else {
			patches = append(patches, PatchOp{Op: "add", Path: base + "/env/-", Value: uid})
		}
	}
	if init.VolumeMounts == nil {
		patches = append(patches, PatchOp{Op: "add", Path: base + "/volumeMounts", Value: mounts})
	} else {
		for _, vm := range mounts {
			patches = append(patches, PatchOp{Op: "add", Path: base + "/volumeMounts/-", Value: vm})
		}
	}
	dir := corev1.HostPathDirectoryOrCreate
	vols := []corev1.Volume{{Name: ModelCacheVolume, VolumeSource: corev1.VolumeSource{
		HostPath: &corev1.HostPathVolumeSource{Path: path.Join(c.Root, key), Type: &dir}}}}
	if c.PlanWait > 0 {
		limit := resource.MustParse("1Mi")
		vols = append(vols, corev1.Volume{Name: ModelCacheSignalVolume, VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: &limit}}})
	}
	patches = append(patches, addVolumes(pod, vols)...)
	patches = m.withHelperVolume(pod, patches)
	mp := newMetaPatcher(pod)
	patches = append(patches, mp.label(ModelCacheKeyLabel, key)...)
	patches = append(patches, mp.annotation(ModelCacheVolumeAnnotation, land.VolumeName)...)
	patches = append(patches, mp.annotation(ModelCacheInitAnnotation, init.Name)...)
	log.WithField("init", init.Name).Info("node model cache: download init seeds from the node's copy when it has one")
	return patches
}

// ModelCacheDownload is a pod's model download the node cache can serve.
type ModelCacheDownload struct {
	Key       string
	ShareKey  string
	Landing   modelid.Landing
	InitIndex int
	// Download is the init's own download command, with the helper's
	// wrapping removed.
	Download []string
	// Watch are the mount paths, in the init, of the other writable
	// volumes a later container reads: the helper checks the download
	// left them as it found them before the download is cached.
	Watch []string
	// Wrapped reports whether the init already runs under the helper.
	Wrapped bool
}

// modelCachePrefix starts a wrapped init's command.
var modelCachePrefix = []string{HelperBin, "model-cache"}

// ResolveModelCacheDownload finds the pod's model download init and its
// cache key, from the pod spec and the namespace's tenant only. The agent
// calls it too, on the admitted pod: it never takes a key from a label a
// pod could set itself, so no pod can put its files in another tenant's
// or another model's cache. A wrapped init is resolved as it was before
// admission wrapped it, so both find the same init and key.
func ResolveModelCacheDownload(ctx context.Context, tenants TenantScoper, pod *corev1.Pod, mainContainer int) (ModelCacheDownload, bool) {
	var d ModelCacheDownload
	pod, wrapped, ok := unwrapModelCache(pod)
	if !ok {
		return d, false
	}
	res, ok := modelid.Resolve(pod, mainContainer)
	if !ok || res.Identity.Scheme == "path" {
		return d, false
	}
	land := res.Landing
	if land.Downloader != modelid.DownloaderInit || land.Kind != modelid.VolumeEmptyDir || land.VolumeName == "" {
		return d, false
	}
	idx := -1
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == land.InitContainer {
			idx = i
		}
	}
	if idx < 0 {
		return d, false
	}
	init := pod.Spec.InitContainers[idx]
	// Without its own command the init runs its image's entrypoint, which
	// cannot be wrapped; another helper step is not a download.
	if len(init.Command) == 0 || init.Command[0] == HelperBin {
		return d, false
	}
	if readsLabelsOrAnnotations(pod, &init) {
		return d, false
	}
	watch, ok := seedable(pod, idx, land.VolumeName)
	if !ok {
		return d, false
	}
	scope := "namespace:" + pod.Namespace
	if tenants != nil {
		scope = tenants.Scope(ctx, pod.Namespace)
	}
	d.Key = modelcache.Key(scope, res.ShareKey(), init.Image, downloadRecipe(pod, &init))
	d.ShareKey, d.Landing, d.InitIndex, d.Download, d.Watch, d.Wrapped = res.ShareKey(), land, idx, containerArgv(&init), watch, wrapped
	return d, true
}

// unwrapModelCache returns pod as it was before admission wrapped its
// download init: the init's own command back, nvsnap's mounts gone. A pod
// with no wrapped init is returned as is; a malformed wrapper is not ok.
func unwrapModelCache(pod *corev1.Pod) (*corev1.Pod, bool, bool) {
	for i := range pod.Spec.InitContainers {
		c := &pod.Spec.InitContainers[i]
		argv := containerArgv(c)
		if len(argv) < 2 || argv[0] != modelCachePrefix[0] || argv[1] != modelCachePrefix[1] {
			continue
		}
		dash := -1
		for j, a := range argv {
			if a == "--" {
				dash = j
				break
			}
		}
		if dash < 0 || dash == len(argv)-1 {
			return nil, false, false
		}
		// Single flight's additions (the pod-uid variable, last, and the
		// signal mount) come only with a wrapper that has --plan-wait: a
		// user's own variable or mount of that name stays in the recipe.
		single := false
		for _, a := range argv[:dash] {
			single = single || a == "--plan-wait"
		}
		out := pod.DeepCopy()
		oc := &out.Spec.InitContainers[i]
		oc.Command, oc.Args = append([]string{}, argv[dash+1:]...), nil
		var mounts []corev1.VolumeMount
		for _, vm := range oc.VolumeMounts {
			if vm.Name != ModelCacheVolume && vm.Name != helperVolumeName && !(single && vm.Name == ModelCacheSignalVolume) {
				mounts = append(mounts, vm)
			}
		}
		oc.VolumeMounts = mounts
		// The wrapper does not wrap an init that set the variable itself.
		if n := len(oc.Env); single && n > 0 && isPodUIDEnv(oc.Env[n-1]) {
			oc.Env = oc.Env[:n-1]
			if len(oc.Env) == 0 {
				oc.Env = nil
			}
		}
		return out, true, true
	}
	return pod, false, true
}

// readsLabelsOrAnnotations reports whether the init can read a label or
// an annotation of its pod: by an env field reference, or through a
// downward-API volume (plain or projected) it mounts. Their values can
// differ between pods of one recipe and can change after admission, and
// the key names a field reference only by its path; a download steered
// by one could write another tree under the same key, so it is not
// cached. The pod's own identity (name, namespace, uid, node, addresses)
// stays keyed by its path: a download that wrote a different tree per pod
// could not be shared at all.
func readsLabelsOrAnnotations(pod *corev1.Pod, init *corev1.Container) bool {
	mutable := func(f *corev1.ObjectFieldSelector) bool {
		return f != nil && (strings.HasPrefix(f.FieldPath, "metadata.labels") || strings.HasPrefix(f.FieldPath, "metadata.annotations"))
	}
	for _, e := range init.Env {
		// The platform's per-instance metadata (orchestrationEnv) reads
		// labels and annotations in every function's init; it does not
		// steer the download, as the key already holds.
		if e.ValueFrom != nil && mutable(e.ValueFrom.FieldRef) && !hasAnyPrefix(e.Name, orchestrationEnv) {
			return true
		}
	}
	items := func(files []corev1.DownwardAPIVolumeFile) bool {
		for _, f := range files {
			if mutable(f.FieldRef) {
				return true
			}
		}
		return false
	}
	for _, vm := range init.VolumeMounts {
		for i := range pod.Spec.Volumes {
			v := &pod.Spec.Volumes[i]
			if v.Name != vm.Name {
				continue
			}
			if v.DownwardAPI != nil && items(v.DownwardAPI.Items) {
				return true
			}
			if v.Projected != nil {
				for _, src := range v.Projected.Sources {
					if src.DownwardAPI != nil && items(src.DownwardAPI.Items) {
						return true
					}
				}
			}
		}
	}
	return false
}

func isPodUIDEnv(e corev1.EnvVar) bool {
	return e.Name == PodUIDEnv && e.Value == "" && e.ValueFrom != nil && e.ValueFrom.FieldRef != nil &&
		e.ValueFrom.FieldRef.FieldPath == "metadata.uid"
}

// seedable reports whether the download init at idx can be replaced by a
// seed of its volume vol, and which other volumes to watch. The init must
// see vol whole (a subPath mount sees part of what a harvest keeps). Every
// other volume it can write and a later container reads might be an
// output a seed would not produce; nothing at admission tells, so the
// helper fingerprints each before and after the download, and a download
// that changed one is not cached.
func seedable(pod *corev1.Pod, idx int, vol string) ([]string, bool) {
	init := &pod.Spec.InitContainers[idx]
	sources := map[string]*corev1.Volume{}
	for i := range pod.Spec.Volumes {
		sources[pod.Spec.Volumes[i].Name] = &pod.Spec.Volumes[i]
	}
	mountedBy := func(cs []corev1.Container, name string) bool {
		for i := range cs {
			for _, vm := range cs[i].VolumeMounts {
				if vm.Name == name {
					return true
				}
			}
		}
		return false
	}
	// A restartable sidecar started before the download runs beside it
	// and the engine: if it can write vol, the tree is not only the
	// download's.
	for j := 0; j < idx; j++ {
		sc := &pod.Spec.InitContainers[j]
		if sc.RestartPolicy == nil || *sc.RestartPolicy != corev1.ContainerRestartPolicyAlways {
			continue
		}
		for _, vm := range sc.VolumeMounts {
			if vm.Name == vol && !vm.ReadOnly {
				return nil, false
			}
		}
	}
	var watch []string
	for _, vm := range init.VolumeMounts {
		if vm.Name == vol {
			if vm.SubPath != "" || vm.SubPathExpr != "" {
				return nil, false
			}
			continue
		}
		if vm.ReadOnly {
			continue
		}
		if v := sources[vm.Name]; v != nil && (v.Secret != nil || v.ConfigMap != nil || v.DownwardAPI != nil || v.Projected != nil ||
			(v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ReadOnly)) {
			continue // read-only by kind
		}
		if mountedBy(pod.Spec.InitContainers[idx+1:], vm.Name) || mountedBy(pod.Spec.Containers, vm.Name) {
			watch = append(watch, vm.MountPath)
		}
	}
	return watch, true
}

// downloadRecipe is what, besides the image, decides the tree a download
// init writes: its command and arguments; its env in declared order (a
// later entry can refer to an earlier one), but the platform's
// per-instance names (orchestrationEnv); its envFrom sources in order (the
// last wins); its volume mounts with each volume's full source; and the
// pod's and the init's security contexts (the owners and group a download
// leaves). Values from a Secret, ConfigMap or field are named, not read;
// a Secret or ConfigMap changed under the same name is the same recipe. A
// download that reads a label or annotation is not cached at all
// (readsLabelsOrAnnotations).
func downloadRecipe(pod *corev1.Pod, c *corev1.Container) string {
	var b strings.Builder
	line := func(kind, s string) { fmt.Fprintf(&b, "%s %d:%s\n", kind, len(s), s) }
	for _, s := range containerArgv(c) {
		line("arg", s)
	}
	for _, e := range c.Env {
		if hasAnyPrefix(e.Name, orchestrationEnv) {
			continue
		}
		v := e.Value
		if e.ValueFrom != nil {
			v = "<from " + valueFromRef(e.ValueFrom) + ">"
		}
		line("env", e.Name+"="+v)
	}
	for _, f := range c.EnvFrom {
		line("envFrom", jsonOf(f))
	}
	for _, vm := range c.VolumeMounts {
		src := "?"
		for i := range pod.Spec.Volumes {
			if pod.Spec.Volumes[i].Name == vm.Name {
				src = jsonOf(pod.Spec.Volumes[i].VolumeSource)
			}
		}
		// The volume's name is the pod's own label for it (a service
		// account token volume's is random per pod); where it is mounted
		// and what it is decide the download.
		vm.Name = ""
		line("mount", jsonOf(vm)+" <- "+src)
	}
	line("podSecurity", jsonOf(pod.Spec.SecurityContext))
	line("initSecurity", jsonOf(c.SecurityContext))
	return b.String()
}

func jsonOf(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "?"
	}
	return string(b)
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

func valueFromRef(v *corev1.EnvVarSource) string {
	switch {
	case v.SecretKeyRef != nil:
		return "secret " + v.SecretKeyRef.Name + "/" + v.SecretKeyRef.Key
	case v.ConfigMapKeyRef != nil:
		return "configmap " + v.ConfigMapKeyRef.Name + "/" + v.ConfigMapKeyRef.Key
	case v.FieldRef != nil:
		return "field " + v.FieldRef.FieldPath
	case v.ResourceFieldRef != nil:
		return "resource " + v.ResourceFieldRef.Resource
	}
	return "?"
}

// cacheMounts returns the mounts the download init needs added: the cache,
// read-only, and the helper, unless the init already mounts the helper's
// own volume there. ok is false when the init mounts something else at
// either path, or the pod already has a volume of the cache's name: a
// second mount at one path is a pod Kubernetes rejects, and another
// volume under the cache's name would be mounted in its place.
func cacheMounts(pod *corev1.Pod, init *corev1.Container, signal bool) (mounts []corev1.VolumeMount, ok bool) {
	if podHasVolume(pod, ModelCacheVolume) || (signal && podHasVolume(pod, ModelCacheSignalVolume)) {
		return nil, false
	}
	wants := []corev1.VolumeMount{{Name: ModelCacheVolume, MountPath: ModelCacheMount, ReadOnly: true}, helperMount()}
	if signal {
		wants = append(wants, corev1.VolumeMount{Name: ModelCacheSignalVolume, MountPath: ModelCacheSignalMount})
	}
	for _, want := range wants {
		have := false
		for _, vm := range init.VolumeMounts {
			if path.Clean(vm.MountPath) != path.Clean(want.MountPath) {
				continue
			}
			if vm.Name != want.Name || want.Name == ModelCacheVolume || want.Name == ModelCacheSignalVolume {
				return nil, false
			}
			have = true
		}
		if !have {
			mounts = append(mounts, want)
		}
	}
	return mounts, true
}
