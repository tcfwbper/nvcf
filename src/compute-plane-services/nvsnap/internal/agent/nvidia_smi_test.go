// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvsnap/internal/hostlibs"
)

func mkfile(t *testing.T, p string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
}

func symlink(t *testing.T, target, p string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, p); err != nil {
		t.Fatal(err)
	}
}

// A driver root laid out as the driver container's: lib -> usr/lib as an
// absolute link, the loader a link inside it.
func driverRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	tr := hostlibs.Triplet()
	loader := "ld-linux-x86-64.so.2"
	if runtime.GOARCH == "arm64" {
		loader = "ld-linux-aarch64.so.1"
	}
	mkfile(t, filepath.Join(root, "usr/lib", tr, loader))
	mkfile(t, filepath.Join(root, "usr/lib", tr, "libc.so.6"))
	mkfile(t, filepath.Join(root, "usr/bin/nvidia-smi"))
	symlink(t, "/usr/lib", filepath.Join(root, "lib"))
	if runtime.GOARCH != "arm64" {
		symlink(t, "/usr/lib/"+tr, filepath.Join(root, "lib64"))
	}
	return root
}

func TestNvidiaSMICommandRunsUnderTheRootsLoader(t *testing.T) {
	root := driverRoot(t)
	tr := hostlibs.Triplet()
	name, argv, err := nvidiaSMICommand([]struct{ root, bin string }{{root, "usr/bin/nvidia-smi"}}, []string{"-q"})
	if err != nil {
		t.Fatal(err)
	}
	// The loader is the root's, reached through its absolute lib link
	// inside the root, never the agent's /usr/lib.
	if !strings.HasPrefix(name, root+"/usr/lib/"+tr+"/ld-linux-") {
		t.Errorf("loader %s, want the root's", name)
	}
	if len(argv) < 4 || argv[0] != "--library-path" || argv[2] != root+"/usr/bin/nvidia-smi" || argv[3] != "-q" {
		t.Fatalf("argv %v", argv)
	}
	for _, d := range strings.Split(argv[1], ":") {
		if !strings.HasPrefix(d, root+"/") {
			t.Errorf("library dir %s outside the root", d)
		}
	}
	if !strings.Contains(argv[1], root+"/usr/lib/"+tr) {
		t.Errorf("library path %s lacks the root's %s", argv[1], tr)
	}
}

func TestNvidiaSMICommandWithoutLoaderFails(t *testing.T) {
	root := t.TempDir()
	mkfile(t, filepath.Join(root, "usr/bin/nvidia-smi"))
	if _, _, err := nvidiaSMICommand([]struct{ root, bin string }{{root, "usr/bin/nvidia-smi"}}, nil); err == nil || errors.Is(err, errNoNvidiaSMI) {
		t.Errorf("err = %v, want a missing-loader error, not no nvidia-smi", err)
	}
}

func TestNvidiaSMICommandNone(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	if _, _, err := nvidiaSMICommand([]struct{ root, bin string }{{t.TempDir(), "usr/bin/nvidia-smi"}}, nil); !errors.Is(err, errNoNvidiaSMI) {
		t.Errorf("err = %v, want errNoNvidiaSMI", err)
	}
}

func TestResolveInRoot(t *testing.T) {
	root := t.TempDir()
	mkfile(t, filepath.Join(root, "usr/lib/f"))
	symlink(t, "/usr/lib", filepath.Join(root, "lib"))
	symlink(t, "../../usr/lib/f", filepath.Join(root, "a/b/rel"))
	symlink(t, "/etc/passwd", filepath.Join(root, "abs"))
	symlink(t, "loop", filepath.Join(root, "loop"))
	for rel, want := range map[string]string{
		"lib/f":     root + "/usr/lib/f",
		"a/b/rel":   root + "/usr/lib/f",
		"abs":       "", // the root has no /etc/passwd; the agent's is not read
		"loop":      "",
		"../../lib": root + "/usr/lib", // cannot climb out
	} {
		if got := resolveInRoot(root, rel); got != want {
			t.Errorf("resolveInRoot(%q) = %q, want %q", rel, got, want)
		}
	}
}

// The command runs a real binary of the host's root under the host's
// loader: the shape that failed with exit 127 before.
func TestNvidiaSMICommandRunsARealBinary(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("linux only")
	}
	if rootLoader("/") == "" {
		t.Skip("no loader at a known path")
	}
	tru, err := exec.LookPath("true")
	if err != nil {
		t.Skip("no true")
	}
	rel := strings.TrimPrefix(tru, "/")
	name, argv, err := nvidiaSMICommand([]struct{ root, bin string }{{"/", rel}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(name, argv...).CombinedOutput(); err != nil {
		t.Fatalf("%s %v: %v: %s", name, argv, err, out)
	}
}

// A root may also hold another architecture's loader (a cross toolchain):
// only the agent's own can run it.
func TestRootLoaderIsTheAgentsArchitecture(t *testing.T) {
	root := t.TempDir()
	mkfile(t, filepath.Join(root, "lib/ld-linux-aarch64.so.1"))
	mkfile(t, filepath.Join(root, "lib64/ld-linux-x86-64.so.2"))
	want := filepath.Join(root, "lib64/ld-linux-x86-64.so.2")
	if runtime.GOARCH == "arm64" {
		want = filepath.Join(root, "lib/ld-linux-aarch64.so.1")
	}
	if got := rootLoader(root); got != want {
		t.Errorf("rootLoader = %q, want %q", got, want)
	}
}
