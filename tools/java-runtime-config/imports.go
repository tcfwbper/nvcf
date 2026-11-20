// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

type workspaceList []string

func (list *workspaceList) String() string { return strings.Join(*list, ",") }
func (list *workspaceList) Set(value string) error {
	if value == "" {
		return fmt.Errorf("workspace directory must not be empty")
	}
	*list = append(*list, value)
	return nil
}

type rcGuard struct {
	workdir, workspace string
	visiting           map[string]bool
	afterHome          bool
}

type sourceLine struct {
	text   string
	number int
}

// Keep physical source locations while joining Bazel's LF/CRLF continuations.
func sourceLines(input string) []sourceLine {
	var lines []sourceLine
	var text strings.Builder
	start, number := 1, 1
	for _, physical := range strings.SplitAfter(input, "\n") {
		if physical == "" {
			continue
		}
		terminated := strings.HasSuffix(physical, "\n")
		body := strings.TrimSuffix(physical, "\n")
		if terminated {
			body = strings.TrimSuffix(body, "\r")
		}
		if terminated && strings.HasSuffix(body, "\\") {
			text.WriteString(strings.TrimSuffix(body, "\\"))
			number++
			continue
		}
		text.WriteString(body)
		lines = append(lines, sourceLine{text.String(), start})
		text.Reset()
		number++
		start = number
	}
	if text.Len() != 0 {
		lines = append(lines, sourceLine{text.String(), start})
	}
	return lines
}

func (guard *rcGuard) resolve(path string) string {
	// Bazel expands only a leading %workspace%/ in import filenames. Other
	// percent-delimited names, including %user.home%, are literal paths.
	if strings.HasPrefix(path, "%workspace%/") {
		path = guard.workspace + string(os.PathSeparator) + strings.TrimPrefix(path, "%workspace%/")
	}
	// Bazel resolves relative imports against its working directory, not the
	// directory containing the importing file.
	if !filepath.IsAbs(path) {
		path = guard.workdir + string(os.PathSeparator) + path
	}
	// Let filesystem resolution handle symlinks before parent components.
	return path
}

func (guard *rcGuard) file(path string, optional bool) error {
	resolved := guard.resolve(path)
	canonical, err := filepath.EvalSymlinks(resolved)
	if err != nil {
		if optional && os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("%s: cannot resolve rc file", resolved)
	}
	if guard.visiting[canonical] {
		return fmt.Errorf("%s: rc import cycle", resolved)
	}
	input, err := os.ReadFile(canonical)
	if err != nil {
		return fmt.Errorf("%s: cannot read rc file", resolved)
	}
	guard.visiting[canonical] = true
	defer delete(guard.visiting, canonical)
	return guard.contents(string(input), resolved, false)
}

func (guard *rcGuard) contents(input, path string, allowFixed bool) error {
	for _, line := range sourceLines(input) {
		words, err := tokenize(line.text)
		if err != nil {
			return fmt.Errorf("%s:%d: %w", path, line.number, err)
		}
		if len(words) == 0 {
			continue
		}
		switch words[0].value {
		case "import", "try-import":
			if len(words) != 2 {
				return fmt.Errorf("%s:%d: invalid rc import", path, line.number)
			}
			if err := guard.file(words[1].value, words[0].value == "try-import"); err != nil {
				return fmt.Errorf("%s:%d: %w", path, line.number, err)
			}
			continue
		case "try-import-if-bazel-version":
			return fmt.Errorf("%s:%d: conditional rc imports cannot be verified by CI Java setup", path, line.number)
		}
		scope, _, named := strings.Cut(words[0].value, ":")
		for i := 1; i < len(words); i++ {
			option, assignment, joined := strings.Cut(words[i].value, "=")
			jvmOption := option == "--host_jvm_args" || option == "--jvmopt" || option == "--host_jvmopt"
			if !jvmOption && option != "--action_env" && option != "--host_action_env" && option != "--test_env" {
				continue
			}
			if !joined {
				// Bazel can consume a value from another rc statement. The
				// transformer cannot safely merge that value line by line.
				if i+1 == len(words) {
					return fmt.Errorf("%s:%d: missing value for %s; keep the option and value on the same rc statement", path, line.number, option)
				}
				i++
				assignment = words[i].value
			}
			if allowFixed {
				continue
			}
			if jvmOption {
				// BAZELRC and its imports load after the managed home block.
				// Named command configurations can also expand at CLI priority.
				// Each option supplies one JVM argument; property values that
				// merely contain this text are not enabling JVM arguments.
				if (guard.afterHome || (named && scope != "startup")) && assignment == "-XX:+UseBASE64Intrinsics" {
					return fmt.Errorf("%s:%d: JVM mitigation conflict in %s; remove the enabling option from external rc configuration", path, line.number, option)
				}
				continue
			}
			name, _, fixed := strings.Cut(assignment, "=")
			if name == "JAVA_TOOL_OPTIONS" && fixed {
				return fmt.Errorf("%s:%d: fixed JAVA_TOOL_OPTIONS in %s; move options to the CI shell or inline home rc before retrying", path, line.number, option)
			}
		}
	}
	return nil
}

func workspaceRoot(dir string) (string, error) {
	for candidate := dir; ; candidate = filepath.Dir(candidate) {
		for _, marker := range []string{"MODULE.bazel", "WORKSPACE.bazel", "WORKSPACE"} {
			if info, err := os.Stat(filepath.Join(candidate, marker)); err == nil && !info.IsDir() {
				return candidate, nil
			}
		}
		if filepath.Dir(candidate) == candidate {
			break
		}
	}
	return "", fmt.Errorf("%s: no Bazel workspace found", dir)
}

func checkRCs(input, cwd, home string, workspaces []string, extraRC, systemRC string) error {
	if len(workspaces) == 0 {
		workspaces = []string{cwd}
	}
	for _, workdir := range workspaces {
		if !filepath.IsAbs(workdir) {
			workdir = filepath.Join(cwd, workdir)
		}
		info, err := os.Stat(workdir)
		if err != nil || !info.IsDir() {
			return fmt.Errorf("%s: workspace directory unavailable", workdir)
		}
		root, err := workspaceRoot(filepath.Clean(workdir))
		if err != nil {
			return err
		}
		guard := rcGuard{workdir: workdir, workspace: root, visiting: map[string]bool{}}
		if systemRC != "" {
			if err := guard.file(systemRC, true); err != nil {
				return err
			}
		}
		if err := guard.file(filepath.Join(root, ".bazelrc"), true); err != nil {
			return err
		}
		homeRC := filepath.Join(home, ".bazelrc")
		// Inline home values can be merged; imported files are never rewritten.
		if err := guard.contents(input, homeRC, true); err != nil {
			return err
		}
		if extraRC != "" {
			guard.afterHome = true
			for _, path := range strings.Split(extraRC, ",") {
				if path == "" {
					return fmt.Errorf("BAZELRC contains an empty path")
				}
				// BAZELRC entries are paths, not rc import declarations.
				if !filepath.IsAbs(path) {
					path = workdir + string(os.PathSeparator) + path
				}
				if err := guard.file(path, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func runCLI(in io.Reader, out io.Writer, cwd string, workspaces []string) error {
	if cwd == "" {
		var err error
		cwd, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	systemRC := "/etc/bazel.bazelrc"
	if runtime.GOOS == "windows" {
		systemRC = filepath.Join(os.Getenv("ProgramData"), "bazel.bazelrc")
	}
	return configure(in, out, func(input string) error {
		return checkRCs(input, cwd, home, workspaces, os.Getenv("BAZELRC"), systemRC)
	})
}
