// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeRCFixture(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
}

func newGuardFixture(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	workspace, home := filepath.Join(root, "workspace"), filepath.Join(root, "home")
	writeRCFixture(t, filepath.Join(workspace, "MODULE.bazel"), "")
	if err := os.Mkdir(home, 0700); err != nil {
		t.Fatal(err)
	}
	return workspace, home
}

func TestGuardRejectsFixedExternalValues(t *testing.T) {
	for _, flag := range []string{"--action_env", "--host_action_env", "--test_env"} {
		for _, scope := range []string{"common", "build", "test", "build:fixture"} {
			for _, separator := range []string{"=", " "} {
				t.Run(scope+flag+separator, func(t *testing.T) {
					workspace, home := newGuardFixture(t)
					path := filepath.Join(workspace, ".bazelrc")
					writeRCFixture(t, path, scope+" "+flag+separator+`"JAVA_TOOL_OPTIONS=-Dsecret=do-not-log"`+"\n")
					err := checkRCs("", workspace, home, nil, "", "")
					if err == nil || !strings.Contains(err.Error(), path+":1:") || !strings.Contains(err.Error(), flag) {
						t.Fatalf("fixed external option not diagnosed: %v", err)
					}
					if strings.Contains(err.Error(), "do-not-log") {
						t.Fatal("option content leaked")
					}
				})
			}
		}
	}
}

func TestGuardTraversesImportsWithBazelPathSemantics(t *testing.T) {
	workspace, home := newGuardFixture(t)
	writeRCFixture(t, filepath.Join(workspace, ".bazelrc"), "try-import %workspace%/absent.rc\nimport %workspace%/config/first.rc\n")
	// Relative imports are resolved from Bazel cwd, not config/first.rc.
	writeRCFixture(t, filepath.Join(workspace, "config", "first.rc"), "import second.rc\n")
	writeRCFixture(t, filepath.Join(workspace, "config", "second.rc"), "# unused file\n")
	path := filepath.Join(home, "extra.rc")
	writeRCFixture(t, filepath.Join(workspace, "second.rc"), "try-import "+quoteWord(path)+"\n")
	writeRCFixture(t, path, "common --action_env=JAVA_TOOL_OPTIONS=-Dfoo=bar\n")
	err := checkRCs("", workspace, home, nil, "", "")
	if err == nil || !strings.Contains(err.Error(), path+":1:") {
		t.Fatalf("nested import not checked: %v", err)
	}
}

func TestGuardInlineHomeValuesAndInheritedReferences(t *testing.T) {
	workspace, home := newGuardFixture(t)
	writeRCFixture(t, filepath.Join(workspace, ".bazelrc"), "common --action_env=JAVA_TOOL_OPTIONS\ncommon --test_env=UNRELATED=fixed\n")
	inline := "common --action_env=JAVA_TOOL_OPTIONS=-Dfixture=preserved\n"
	if err := checkRCs(inline, workspace, home, nil, "", ""); err != nil {
		t.Fatal(err)
	}
	writeRCFixture(t, filepath.Join(home, "imported.rc"), "common --host_action_env=JAVA_TOOL_OPTIONS=\n")
	if err := checkRCs(inline+"import "+quoteWord(filepath.Join(home, "imported.rc"))+"\n", workspace, home, nil, "", ""); err == nil {
		t.Fatal("home imported fixed value was accepted")
	}
}

func TestGuardChecksEverySelectedWorkspace(t *testing.T) {
	workspace, home := newGuardFixture(t)
	child := filepath.Join(workspace, "child")
	writeRCFixture(t, filepath.Join(child, "MODULE.bazel"), "")
	path := filepath.Join(child, ".bazelrc")
	writeRCFixture(t, path, "common --test_env=JAVA_TOOL_OPTIONS=-Dchild=kept\n")
	if err := checkRCs("", workspace, home, []string{"."}, "", ""); err != nil {
		t.Fatal(err)
	}
	err := checkRCs("", workspace, home, []string{".", "child"}, "", "")
	if err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("child workspace missed: %v", err)
	}
	subdir := filepath.Join(workspace, "inside")
	if err := os.Mkdir(subdir, 0700); err != nil {
		t.Fatal(err)
	}
	if got, err := workspaceRoot(subdir); err != nil || got != workspace {
		t.Fatalf("workspace ancestor: %q %v", got, err)
	}
}

func TestGuardAdditionalAndSystemRC(t *testing.T) {
	workspace, home := newGuardFixture(t)
	writeRCFixture(t, filepath.Join(workspace, "safe.rc"), "# inherited\n")
	writeRCFixture(t, filepath.Join(workspace, "fixed.rc"), "common --action_env=JAVA_TOOL_OPTIONS=-Dfoo=bar\n")
	for _, args := range [][2]string{{"safe.rc,fixed.rc", ""}, {"", filepath.Join(workspace, "fixed.rc")}} {
		err := checkRCs("", workspace, home, nil, args[0], args[1])
		if err == nil || !strings.Contains(err.Error(), "fixed JAVA_TOOL_OPTIONS") {
			t.Fatalf("extra rc missed: %v", err)
		}
	}
	if err := checkRCs("", workspace, home, nil, "", filepath.Join(workspace, "missing-system.rc")); err != nil {
		t.Fatal(err)
	}
}

func TestGuardRejectsUnverifiableImports(t *testing.T) {
	for _, input := range []string{
		"import missing.rc\n",
		"try-import-if-bazel-version >=9.0.0 optional.rc\n",
		"import a b\n",
		"import .bazelrc\n",
	} {
		t.Run(strings.TrimSpace(input), func(t *testing.T) {
			workspace, home := newGuardFixture(t)
			writeRCFixture(t, filepath.Join(workspace, ".bazelrc"), input)
			if err := checkRCs("", workspace, home, nil, "", ""); err == nil {
				t.Fatal("unverifiable import accepted")
			}
		})
	}
}

func TestGuardLiteralImportNames(t *testing.T) {
	for _, name := range []string{"%user.home%/.bazelrc.user", "%unknown%/optional.rc", "sub/%workspace%/optional.rc", "%workspace%literal.rc"} {
		t.Run(name, func(t *testing.T) {
			workspace, home := newGuardFixture(t)
			writeRCFixture(t, filepath.Join(home, ".bazelrc.user"), "common --action_env=JAVA_TOOL_OPTIONS=-Dunrelated=hidden\n")
			writeRCFixture(t, filepath.Join(workspace, ".bazelrc"), "try-import "+name+"\n")
			if err := checkRCs("", workspace, home, nil, "", ""); err != nil {
				t.Fatalf("missing literal optional import should be skipped: %v", err)
			}
			literal := filepath.Join(workspace, name)
			writeRCFixture(t, literal, "common --test_env=JAVA_TOOL_OPTIONS=-Dactual=hidden\n")
			err := checkRCs("", workspace, home, nil, "", "")
			if err == nil || !strings.Contains(err.Error(), literal+":1:") {
				t.Fatalf("existing literal import was not checked: %v", err)
			}
		})
	}
}

func TestGuardRejectsLaterJVMEnablingOptions(t *testing.T) {
	for _, scope := range []string{"startup", "startup:linux", "startup:macos", "build:linux", "test:macos", "run"} {
		flags := []string{"--jvmopt", "--host_jvmopt"}
		if strings.HasPrefix(scope, "startup") {
			flags = []string{"--host_jvm_args"}
		}
		for _, flag := range flags {
			for _, separator := range []string{"=", " "} {
				t.Run(scope+flag+separator, func(t *testing.T) {
					workspace, home := newGuardFixture(t)
					late := filepath.Join(workspace, "late.rc")
					writeRCFixture(t, late, "import %workspace%/enabling.rc\n")
					writeRCFixture(t, filepath.Join(workspace, "enabling.rc"), scope+" "+flag+separator+"-XX:+UseBASE64Intrinsics\n")
					err := checkRCs("", workspace, home, nil, late, "")
					if err == nil || !strings.Contains(err.Error(), "JVM mitigation conflict") || !strings.Contains(err.Error(), flag) {
						t.Fatalf("late enabling option accepted: %v", err)
					}
				})
			}
		}
	}
}

func TestGuardRejectsValuesSplitAcrossStatements(t *testing.T) {
	for _, flag := range []string{"--host_jvm_args", "--jvmopt", "--host_jvmopt", "--action_env", "--host_action_env", "--test_env"} {
		for _, source := range []string{"workspace", "home", "later"} {
			t.Run(source+flag, func(t *testing.T) {
				workspace, home := newGuardFixture(t)
				scope, value := "build:ci", "-XX:+UseBASE64Intrinsics"
				if flag == "--host_jvm_args" {
					scope = "startup:linux"
				} else if strings.HasSuffix(flag, "_env") {
					value = "JAVA_TOOL_OPTIONS=-Dsecret=do-not-log"
				}
				input := "# heading\n" + scope + " " + flag + "\n" + scope + " " + value + "\n"
				path, inline, extra := filepath.Join(workspace, ".bazelrc"), "", ""
				switch source {
				case "home":
					path, inline = filepath.Join(home, ".bazelrc"), input
				case "later":
					path = filepath.Join(workspace, "later.rc")
					extra = path
				}
				writeRCFixture(t, path, input)
				err := checkRCs(inline, workspace, home, nil, extra, "")
				if err == nil || !strings.Contains(err.Error(), path+":2:") || !strings.Contains(err.Error(), "missing value for "+flag) {
					t.Fatalf("cross-statement value accepted: %v", err)
				}
				if strings.Contains(err.Error(), "do-not-log") {
					t.Fatal("option content leaked")
				}
			})
		}
	}
}

func TestGuardRejectsExternalNamedJVMEnablingOptions(t *testing.T) {
	for _, scope := range []string{"common:ci", "build:ci", "test:fixture", "run:custom", "build:linux"} {
		for _, flag := range []string{"--jvmopt", "--host_jvmopt"} {
			for _, source := range []string{"workspace", "system", "home-import", "later"} {
				t.Run(source+scope+flag, func(t *testing.T) {
					workspace, home := newGuardFixture(t)
					path := filepath.Join(workspace, "external.rc")
					inline, extra, system := "", "", ""
					switch source {
					case "workspace":
						writeRCFixture(t, filepath.Join(workspace, ".bazelrc"), "import "+quoteWord(path)+"\n")
					case "system":
						system = path
					case "home-import":
						inline = "import " + quoteWord(path) + "\n"
					case "later":
						extra = path
					}
					writeRCFixture(t, path, scope+" "+flag+" -XX:+UseBASE64Intrinsics\n")
					err := checkRCs(inline, workspace, home, nil, extra, system)
					if err == nil || !strings.Contains(err.Error(), path+":1:") || !strings.Contains(err.Error(), "JVM mitigation conflict") {
						t.Fatalf("external named enabling option accepted: %v", err)
					}
					contents := scope + " " + flag + "=-Dmessage=-XX:+UseBASE64Intrinsics\n"
					writeRCFixture(t, path, contents)
					if err := checkRCs(inline, workspace, home, nil, extra, system); err != nil {
						t.Fatalf("unrelated named JVM argument rejected: %v", err)
					}
					got, err := os.ReadFile(path)
					if err != nil || string(got) != contents {
						t.Fatalf("external named settings changed: %v", err)
					}
				})
			}
		}
	}
}

func TestGuardPreservesNonconflictingJVMArguments(t *testing.T) {
	workspace, home := newGuardFixture(t)
	late := filepath.Join(workspace, "late.rc")
	contents := "startup:linux --host_jvm_args=-Xmx2g\nstartup --host_jvm_args=-Dmessage=-XX:+UseBASE64Intrinsics\nbuild --jvmopt=-XX:-UseBASE64Intrinsics\n"
	contents += "startup --host_jvm_args=\"-Dmessage=literal -XX:+UseBASE64Intrinsics\"\n"
	writeRCFixture(t, late, contents)
	if err := checkRCs("", workspace, home, nil, late, ""); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(late)
	if err != nil || string(got) != contents {
		t.Fatalf("unrelated JVM arguments changed: %v", err)
	}
	// Earlier startup controls remain safe because the home block takes precedence.
	writeRCFixture(t, filepath.Join(workspace, ".bazelrc"), "startup:linux --host_jvm_args=-XX:+UseBASE64Intrinsics\n")
	if err := checkRCs("", workspace, home, nil, "", ""); err != nil {
		t.Fatal(err)
	}
}

func TestBazelRCPathsAreNotImportDeclarations(t *testing.T) {
	workspace, home := newGuardFixture(t)
	writeRCFixture(t, filepath.Join(workspace, "late.rc"), "common --action_env=JAVA_TOOL_OPTIONS=-Dunrelated=hidden\n")
	// A leading workspace token expands in imports, but not BAZELRC paths.
	writeRCFixture(t, filepath.Join(workspace, "%workspace%", "late.rc"), "# literal path\n")
	if err := checkRCs("", workspace, home, nil, "%workspace%/late.rc", ""); err != nil {
		t.Fatal(err)
	}
}

func TestImportParentComponentsFollowSymlinks(t *testing.T) {
	workspace, home := newGuardFixture(t)
	outside := t.TempDir()
	if err := os.Mkdir(filepath.Join(outside, "child"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "child"), filepath.Join(workspace, "linked")); err != nil {
		t.Skip(err)
	}
	writeRCFixture(t, filepath.Join(workspace, ".bazelrc"), "import linked/../actual.rc\n")
	writeRCFixture(t, filepath.Join(outside, "actual.rc"), "common --action_env=JAVA_TOOL_OPTIONS=-Dactual=hidden\n")
	writeRCFixture(t, filepath.Join(workspace, "actual.rc"), "# unrelated file\n")
	if err := checkRCs("", workspace, home, nil, "", ""); err == nil {
		t.Fatal("symlink parent import was cleaned before filesystem resolution")
	}
}

func TestGuardRejectsSymlinkImportCycles(t *testing.T) {
	workspace, home := newGuardFixture(t)
	path := filepath.Join(workspace, ".bazelrc")
	writeRCFixture(t, path, "import alias.rc\n")
	if err := os.Symlink(path, filepath.Join(workspace, "alias.rc")); err != nil {
		t.Skip(err)
	}
	err := checkRCs("", workspace, home, nil, "", "")
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("cycle not detected: %v", err)
	}
}

func TestGuardPhysicalLineNumbersAndNoOutputOnFailure(t *testing.T) {
	workspace, home := newGuardFixture(t)
	path := filepath.Join(workspace, ".bazelrc")
	writeRCFixture(t, path, "# heading\r\nbuild --jvmopt=-Dfirst=one \\\r\n--jvmopt=-Dsecond=two\r\ncommon --action_env=JAVA_TOOL_OPTIONS=-Dsecret=hidden\r\n")
	var output bytes.Buffer
	err := configure(strings.NewReader("build --color=no\n"), &output, func(input string) error {
		return checkRCs(input, workspace, home, nil, "", "")
	})
	if err == nil || !strings.Contains(err.Error(), path+":4:") || strings.Contains(err.Error(), "hidden") || output.Len() != 0 {
		t.Fatalf("incorrect source location or output on failure: %v, %q", err, output.String())
	}
}

func TestGuardMissingWorkspaceAndUnreadableFiles(t *testing.T) {
	workspace, home := newGuardFixture(t)
	if err := checkRCs("", workspace, home, []string{"absent"}, "", ""); err == nil {
		t.Fatal("missing directory accepted")
	}
	if err := checkRCs("", home, home, nil, "", ""); err == nil {
		t.Fatal("missing workspace marker accepted")
	}
	writeRCFixture(t, filepath.Join(workspace, ".bazelrc"), "try-import directory.rc\n")
	if err := os.Mkdir(filepath.Join(workspace, "directory.rc"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := checkRCs("", workspace, home, nil, "", ""); err == nil {
		t.Fatal("unreadable optional file accepted")
	}
}
