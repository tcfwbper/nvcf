// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The shape crit gives a multi-node vLLM worker's dump: the model
// emptyDir is mounted, but no file the processes hold is on it.
const dumpMounts = `{"entries":[
 {"mnt_id":11136,"mountpoint":"/","root":"/"},
 {"mnt_id":11140,"mountpoint":"/dev","root":"/"},
 {"mnt_id":11154,"mountpoint":"/dev/shm","root":"/"},
 {"mnt_id":11153,"mountpoint":"/config/models","root":"/pods/u/volumes/kubernetes.io~empty-dir/ngc-models"},
 {"mnt_id":11160,"mountpoint":"/run/sock","root":"/pods/u/volumes/kubernetes.io~empty-dir/sock"}
]}`

func dumpFiles(extra ...string) []byte {
	entries := make([]string, 0, 7+len(extra))
	entries = append(entries,
		`{"type":"REG","reg":{"name":"/usr/lib/libc.so.6","mnt_id":11136}}`,
		`{"type":"REG","reg":{"name":"/dev/shm/psm_1","mnt_id":11154}}`,
		`{"type":"INETSK","isk":{}}`, `{"type":"MEMFD","memfd":{}}`, `{"type":"PIPE","pipe":{}}`,
		`{"type":"UNIXSK","usk":{"name":"=00abstract"}}`,
		`{"type":"UNIXSK","usk":{"name":""}}`)
	return []byte(`{"entries":[` + strings.Join(append(entries, extra...), ",") + `]}`)
}

func TestUsedMounts(t *testing.T) {
	got, err := usedMounts(dumpFiles(), []byte(dumpMounts))
	if err != nil || !reflect.DeepEqual(got, []string{"/", "/dev/shm"}) {
		t.Fatalf("got %v, %v", got, err)
	}
	// A model file open, mapped, or deleted-but-open is a regular-file
	// entry on the model's mount; a socket bound under a mount uses it.
	got, err = usedMounts(dumpFiles(
		`{"type":"REG","reg":{"name":"/config/models/model-00001.safetensors","mnt_id":11153}}`,
		`{"type":"UNIXSK","usk":{"name":"/run/sock/zmq.ipc=00","mnt_id":11160}}`), []byte(dumpMounts))
	if err != nil || !reflect.DeepEqual(got, []string{"/", "/config/models", "/dev/shm", "/run/sock"}) {
		t.Fatalf("got %v, %v", got, err)
	}
	// Placed by its mount id: a relative name, or a long one crit wrapped.
	got, err = usedMounts(dumpFiles(`{"type":"UNIXSK","usk":{"name":"/run/so=\nck/zmq.ipc","mnt_id":11153}}`), []byte(dumpMounts))
	if err != nil || !reflect.DeepEqual(got, []string{"/", "/config/models", "/dev/shm"}) {
		t.Fatalf("by mount id: got %v, %v", got, err)
	}
	// What cannot be accounted for makes the use unknown.
	for name, extra := range map[string]string{
		"unknown mount": `{"type":"REG","reg":{"name":"/x","mnt_id":99}}`,
		// 2^32 + 11153 would wrap to the model's mount id as a uint32.
		"mount id past 32 bits": `{"type":"REG","reg":{"name":"/x","mnt_id":4294978449}}`,
		"no mount id":           `{"type":"REG","reg":{"name":"/x"}}`,
		"inotify watch":         `{"type":"INOTIFY","ify":{}}`,
		"unknown kind":          `{"type":"SOMETHING_NEW"}`,
		// A plugin's file (EXT) may be on any mount.
		"plugin file": `{"type":"EXT","ext":{}}`,
		// A socket bound by a relative name, or one whose long name crit
		// broke across lines, is placed by its mount id, never its name.
		"socket without a mount id":  `{"type":"UNIXSK","usk":{"name":"zmq.ipc"}}`,
		"socket on an unknown mount": `{"type":"UNIXSK","usk":{"name":"/run/sock/x","mnt_id":99}}`,
		"fanotify watches":           `{"type":"FANOTIFY","ffy":{}}`,
	} {
		if got, err := usedMounts(dumpFiles(extra), []byte(dumpMounts)); err == nil {
			t.Errorf("%s: got %v, want an error", name, got)
		}
	}
}

func TestDumpUsedMounts(t *testing.T) {
	dir := t.TempDir()
	orig := critShow
	defer func() { critShow = orig }()
	critShow = func(img string) ([]byte, error) {
		if strings.HasPrefix(filepath.Base(img), "mountpoints-") {
			return []byte(dumpMounts), nil
		}
		return dumpFiles(), nil
	}
	if _, err := dumpUsedMounts(dir); err == nil {
		t.Error("no mount table: want an error")
	}
	for _, n := range []string{"files.img", "mountpoints-11.img"} {
		if err := os.WriteFile(filepath.Join(dir, n), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := dumpUsedMounts(dir); err != nil || !reflect.DeepEqual(got, []string{"/", "/dev/shm"}) {
		t.Fatalf("got %v, %v", got, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "mountpoints-12.img"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := dumpUsedMounts(dir); err == nil {
		t.Error("two mount namespaces: want an error")
	}
}

func volumeUsePod(name string) *corev1.Pod {
	yes := true
	empty := corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "fn",
			OwnerReferences: []metav1.OwnerReference{{Kind: "PodClique", Name: "w", Controller: &yes}}},
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{Name: "ngc-models", VolumeSource: empty}},
			InitContainers: []corev1.Container{{Name: "download-ngc-model", Command: []string{"download"},
				VolumeMounts: []corev1.VolumeMount{{Name: "ngc-models", MountPath: "/config/models"}}}},
			Containers: []corev1.Container{{Name: "main", Image: "vllm",
				Resources:    corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("4")}},
				VolumeMounts: []corev1.VolumeMount{{Name: "ngc-models", MountPath: "/config/models"}}}},
		},
	}
}

// The group record carries the decision to every placeholder, and only
// when every member's dump was read.
func TestRecordUnusedVolumes(t *testing.T) {
	used := []string{"/", "/dev/shm"}
	members := make([]*corev1.Pod, 0, 3)
	members = append(members, volumeUsePod("w0"), volumeUsePod("w1"))
	results := func() []*CheckpointResult {
		return []*CheckpointResult{
			{CheckpointID: "c0", UsedMounts: &used, LargeEmptyDirs: []string{"/config/models"}},
			{CheckpointID: "c1", UsedMounts: &used, LargeEmptyDirs: []string{"/config/models"}},
		}
	}
	log := logrus.NewEntry(logrus.New())
	data := map[string]string{}
	for k, v := range map[string]string{"state": criuGroupStateComplete, "size": "2", "checkpoint.0": "c0", "checkpoint.1": "c1"} {
		data[k] = v
	}
	recordUnusedVolumes(data, members, 2, nil, results(), log)
	if data[groupUnusedVolumesKey] != "ngc-models" {
		t.Fatalf("record: %v", data)
	}
	g, ok := parseCRIUGroup("k", data)
	if !ok || !reflect.DeepEqual(g.UnusedVolumes, []string{"ngc-models"}) {
		t.Fatalf("parsed: %v %v", ok, g.UnusedVolumes)
	}
	// One member's use unknown (an older agent, a dump that could not be
	// read): nothing recorded.
	r := results()
	r[1].UsedMounts = nil
	d := map[string]string{}
	recordUnusedVolumes(d, members, 2, nil, r, log)
	if _, ok := d[groupUnusedVolumesKey]; ok {
		t.Errorf("recorded with a member's use unknown: %v", d)
	}
	// Ranks first, then companions, in results' order: a companion's
	// container is the one its record names. Here the companion (etcd)
	// mounts no model, and the decision stands.
	etcd := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "fn", OwnerReferences: members[0].OwnerReferences},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar"}, {Name: "etcd"}}}}
	none := []string{"/"}
	r = append(results(), &CheckpointResult{CheckpointID: "c2", UsedMounts: &none})
	d = map[string]string{}
	recordUnusedVolumes(d, append(members, etcd), 2, map[string]string{"etcd": "etcd"}, r, log)
	if d[groupUnusedVolumesKey] != "ngc-models" {
		t.Errorf("with a companion: %v", d)
	}
	// A member that uses the model: nothing recorded.
	withModel := []string{"/", "/config/models"}
	r = results()
	r[0].UsedMounts = &withModel
	d = map[string]string{}
	recordUnusedVolumes(d, members, 2, nil, r, log)
	if _, ok := d[groupUnusedVolumesKey]; ok {
		t.Errorf("recorded with the model in use: %v", d)
	}
}
