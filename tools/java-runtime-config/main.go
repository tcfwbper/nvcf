// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	mitigation = "-XX:-UseBASE64Intrinsics"
	blockBegin = "# BEGIN NVCF CI Java runtime mitigation"
	blockEnd   = "# END NVCF CI Java runtime mitigation"
)

var platforms = []string{"linux", "macos", "windows", "freebsd", "openbsd"}

var defaultControls = makeDefaultControls()

func makeDefaultControls() []string {
	controls := []string{
		"startup --host_jvm_args=" + mitigation,
		"build --host_jvmopt=" + mitigation,
		"build --jvmopt=" + mitigation,
		"test --host_jvmopt=" + mitigation,
		"test --jvmopt=" + mitigation,
	}
	// Startup platform sections always apply after generic startup settings.
	// Command platform sections expand after generic build/test/run settings.
	for _, platform := range platforms {
		controls = append(controls, "startup:"+platform+" --host_jvm_args="+mitigation)
		for _, command := range []string{"build", "test", "run"} {
			for _, option := range []string{"--jvmopt", "--host_jvmopt"} {
				controls = append(controls, command+":"+platform+" "+option+"="+mitigation)
			}
		}
	}
	return controls
}

type word struct {
	start, end int
	value      string
}

// Bazel treats backslashes as escapes inside either quote style, and starts
// comments at any unquoted #. No shell expansion or evaluation takes place.
func tokenize(line string) ([]word, error) {
	var words []word
	for i := 0; i < len(line); {
		if strings.ContainsRune(" \t\r", rune(line[i])) {
			i++
			continue
		}
		if line[i] == '#' {
			break
		}
		start := i
		var value strings.Builder
		var quote byte
		for i < len(line) {
			c := line[i]
			if quote == 0 && (strings.ContainsRune(" \t\r", rune(c)) || c == '#') {
				break
			}
			switch {
			case c == '\\':
				i++
				if i == len(line) {
					return nil, fmt.Errorf("incomplete escape in Bazel configuration")
				}
				value.WriteByte(line[i])
			case quote == 0 && (c == '\'' || c == '"'):
				quote = c
			case quote != 0 && c == quote:
				quote = 0
			default:
				value.WriteByte(c)
			}
			i++
		}
		if quote != 0 {
			return nil, fmt.Errorf("unterminated quote in Bazel configuration")
		}
		if value.Len() != 0 {
			words = append(words, word{start, i, value.String()})
		}
	}
	return words, nil
}

func quoteWord(value string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(value) + `"`
}

func withMitigation(value string) string {
	fields := strings.Fields(value)
	if len(fields) != 0 && fields[len(fields)-1] == mitigation {
		return value
	}
	if value == "" {
		return mitigation
	}
	return value + " " + mitigation
}

type rcState struct {
	commonEnv map[string]bool
	jvmSeen   map[string]bool
	extraJVM  []string
}

func newRCState() *rcState {
	state := &rcState{commonEnv: map[string]bool{}, jvmSeen: map[string]bool{}}
	for _, control := range defaultControls {
		key, _, _ := strings.Cut(control, "=")
		state.jvmSeen[key] = true
	}
	return state
}

func (state *rcState) mergeLine(line string) (string, error) {
	words, err := tokenize(line)
	if err != nil || len(words) == 0 {
		return line, err
	}
	scope := words[0].value
	var result strings.Builder
	last := 0
	for i := 1; i < len(words); i++ {
		current := words[i]
		flag, assignment, joined := strings.Cut(current.value, "=")
		if flag == "--jvmopt" || flag == "--host_jvmopt" {
			key := scope + " " + flag
			if !state.jvmSeen[key] {
				state.jvmSeen[key] = true
				state.extraJVM = append(state.extraJVM, key)
			}
		}
		if flag != "--action_env" && flag != "--host_action_env" && flag != "--test_env" {
			continue
		}
		if !joined {
			if i+1 == len(words) {
				continue // Leave malformed flag validation to Bazel.
			}
			i++
			current = words[i]
			assignment = current.value
		}
		name, value, fixed := strings.Cut(assignment, "=")
		if name != "JAVA_TOOL_OPTIONS" {
			continue
		}
		if scope == "common" {
			state.commonEnv[flag] = true
		}
		if !fixed {
			continue
		}
		updated := name + "=" + withMitigation(value)
		if joined {
			updated = flag + "=" + updated
		}
		result.WriteString(line[last:current.start])
		result.WriteString(quoteWord(updated))
		last = current.end
	}
	result.WriteString(line[last:])
	return result.String(), nil
}

func normalizeLines(input string) []string {
	// Match Bazel: join both accepted continuation formats before tokenizing.
	input = strings.ReplaceAll(input, "\\\r\n", "")
	input = strings.ReplaceAll(input, "\\\n", "")
	input = strings.ReplaceAll(input, "\r\n", "\n")
	input = strings.TrimSuffix(input, "\n")
	if input == "" {
		return nil
	}
	return strings.Split(input, "\n")
}

func transformRC(input string) (string, error) {
	state := newRCState()
	var result strings.Builder
	managed := false
	for i, line := range normalizeLines(input) {
		switch line {
		case blockBegin:
			if managed {
				return "", fmt.Errorf("nested managed Bazel configuration block")
			}
			managed = true
			continue
		case blockEnd:
			if !managed {
				return "", fmt.Errorf("unexpected managed Bazel configuration block end")
			}
			managed = false
			continue
		}
		if managed {
			continue
		}
		updated, err := state.mergeLine(line)
		if err != nil {
			return "", fmt.Errorf("line %d: %w", i+1, err)
		}
		result.WriteString(updated)
		result.WriteByte('\n')
	}
	if managed {
		return "", fmt.Errorf("incomplete managed Bazel configuration block")
	}
	result.WriteString(blockBegin + "\n")
	for _, control := range defaultControls {
		result.WriteString(control + "\n")
	}
	for _, key := range state.extraJVM {
		result.WriteString(key + "=" + mitigation + "\n")
	}
	for _, flag := range []string{"--action_env", "--host_action_env", "--test_env"} {
		if !state.commonEnv[flag] {
			result.WriteString("common " + flag + "=JAVA_TOOL_OPTIONS\n")
		}
	}
	result.WriteString(blockEnd + "\n")
	return result.String(), nil
}

func run(in io.Reader, out io.Writer) error {
	return configure(in, out, nil)
}

func configure(in io.Reader, out io.Writer, check func(string) error) error {
	input, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	if check != nil {
		if err := check(string(input)); err != nil {
			return err
		}
	}
	updated, err := transformRC(string(input))
	if err != nil {
		return err
	}
	_, err = io.WriteString(out, updated)
	return err
}

func main() {
	cwd := flag.String("cwd", "", "caller directory for relative workspace paths")
	var workspaces workspaceList
	flag.Var(&workspaces, "workspace", "Bazel working directory to check (repeatable)")
	flag.Parse()
	if err := runCLI(os.Stdin, os.Stdout, *cwd, workspaces); err != nil {
		fmt.Fprintf(os.Stderr, "configure-java-runtime: %v\n", err)
		os.Exit(1)
	}
}
