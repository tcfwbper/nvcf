// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestTokenize(t *testing.T) {
	for _, test := range []struct {
		name, input string
		want        []string
	}{
		{"whitespace", " build\t --jvmopt=-Dfixture=one ", []string{"build", "--jvmopt=-Dfixture=one"}},
		{"quotes", `test --test_env="JAVA_TOOL_OPTIONS=-Dmessage='hello world'"`, []string{"test", "--test_env=JAVA_TOOL_OPTIONS=-Dmessage='hello world'"}},
		{"escaped single quote", `test '--test_env=JAVA_TOOL_OPTIONS=-Dmessage=it\'s'`, []string{"test", "--test_env=JAVA_TOOL_OPTIONS=-Dmessage=it's"}},
		{"quoted backslash", `build '--action_env=JAVA_TOOL_OPTIONS=-Dpath=C:\\fixture'`, []string{"build", `--action_env=JAVA_TOOL_OPTIONS=-Dpath=C:\fixture`}},
		{"comments", `build --jvmopt=-Dfixture=one# ignored 'quote`, []string{"build", "--jvmopt=-Dfixture=one"}},
		{"quoted comment", `build "--jvmopt=-Dfixture=#kept" # ignored`, []string{"build", "--jvmopt=-Dfixture=#kept"}},
		{"empty words", `"" build '' --jvmopt=-Dfixture=one`, []string{"build", "--jvmopt=-Dfixture=one"}},
		{"shell literals", `build '--action_env=JAVA_TOOL_OPTIONS=-Dliteral=$(ignored)'`, []string{"build", "--action_env=JAVA_TOOL_OPTIONS=-Dliteral=$(ignored)"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			words, err := tokenize(test.input)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, word := range words {
				got = append(got, word.value)
				span, err := tokenize(test.input[word.start:word.end])
				if err != nil || len(span) != 1 || span[0].value != word.value {
					t.Fatalf("invalid token span: %+v, %v", span, err)
				}
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}

func TestTokenizeRejectsIncompleteInput(t *testing.T) {
	for _, input := range []string{`test --test_env="unterminated`, `test --test_env=trailing\`} {
		if _, err := tokenize(input); err == nil {
			t.Errorf("accepted incomplete token %q", input)
		}
	}
}

func TestQuoteWordRoundTrip(t *testing.T) {
	for _, value := range []string{"plain", "hello world", `C:\fixture`, `quote="kept"`, "it's #literal", "$(ignored)"} {
		words, err := tokenize(quoteWord(value))
		if err != nil || len(words) != 1 || words[0].value != value {
			t.Fatalf("round trip %q: %+v, %v", value, words, err)
		}
	}
}

func TestWithMitigation(t *testing.T) {
	for _, input := range []string{"", "-Dfixture=preserved", "-Dfixture=preserved -XX:+UseBASE64Intrinsics", mitigation + "  "} {
		got := withMitigation(input)
		fields := strings.Fields(got)
		if fields[len(fields)-1] != mitigation || !strings.HasPrefix(got, input) {
			t.Errorf("did not preserve and disable %q: %q", input, got)
		}
		if repeated := withMitigation(got); repeated != got {
			t.Errorf("repeat changed %q to %q", got, repeated)
		}
	}
}

func TestNormalizeContinuations(t *testing.T) {
	for _, newline := range []string{"\n", "\r\n"} {
		input := `build --action_env="JAVA_TOOL_OPTIONS=-Dfirst=one ` + "\\" + newline + `-Dsecond=two"` + newline
		want := []string{`build --action_env="JAVA_TOOL_OPTIONS=-Dfirst=one -Dsecond=two"`}
		if got := normalizeLines(input); !reflect.DeepEqual(got, want) {
			t.Fatalf("newline %q: got %q, want %q", newline, got, want)
		}
	}
}

func TestMergeScopedEnvironment(t *testing.T) {
	for _, scope := range []string{"common", "build", "test", "build:fixture"} {
		for _, flag := range []string{"--action_env", "--host_action_env", "--test_env"} {
			for _, separator := range []string{"=", " "} {
				state := newRCState()
				input := scope + " " + flag + separator + `'JAVA_TOOL_OPTIONS=-Dfixture=preserved -XX:+UseBASE64Intrinsics' --color=no # retained`
				got, err := state.mergeLine(input)
				if err != nil {
					t.Fatal(err)
				}
				words, err := tokenize(got)
				if err != nil {
					t.Fatal(err)
				}
				index := 1
				prefix := flag + "="
				if separator == " " {
					index = 2
					prefix = ""
				}
				want := prefix + "JAVA_TOOL_OPTIONS=-Dfixture=preserved -XX:+UseBASE64Intrinsics " + mitigation
				if words[index].value != want || !strings.HasSuffix(got, " --color=no # retained") {
					t.Fatalf("merge %q: %q", input, got)
				}
				if state.commonEnv[flag] != (scope == "common") {
					t.Fatalf("incorrect common scope tracking for %q", input)
				}
			}
		}
	}
}

func TestDiscoverJVMScopes(t *testing.T) {
	state := newRCState()
	for _, line := range []string{
		"test --jvmopt=-Dfixture=one",
		"test:fixture --jvmopt=-XX:+UseBASE64Intrinsics",
		"test:fixture --jvmopt=-Dfixture=two",
		"run --host_jvmopt=-Dfixture=three",
	} {
		if _, err := state.mergeLine(line); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"test:fixture --jvmopt", "run --host_jvmopt"}
	if !reflect.DeepEqual(state.extraJVM, want) {
		t.Fatalf("got scopes %q, want %q", state.extraJVM, want)
	}
}

func TestTransformRCContinuations(t *testing.T) {
	for _, test := range []struct{ name, newline string }{{"LF", "\n"}, {"CRLF", "\r\n"}} {
		t.Run(test.name, func(t *testing.T) {
			input := "build --action_env=\"JAVA_TOOL_OPTIONS=-Dfixture=preserved " + "\\" + test.newline + "-Dmessage='hello world'\"" + test.newline
			got, err := transformRC(input)
			if err != nil {
				t.Fatal(err)
			}
			words, err := tokenize(strings.Split(got, "\n")[0])
			if err != nil || len(words) != 2 || words[1].value != "--action_env=JAVA_TOOL_OPTIONS=-Dfixture=preserved -Dmessage='hello world' "+mitigation {
				t.Fatalf("quoted continuation lost options: %+v, %v", words, err)
			}
			repeated, err := transformRC(got)
			if err != nil || repeated != got {
				t.Fatalf("repeat changed configuration: %v", err)
			}
		})
	}
}

func TestManagedBlockReconstruction(t *testing.T) {
	input := "# caller settings\nbuild --color=no\n" + blockBegin + "\nbuild --jvmopt=stale\n" + blockEnd + "\nstartup --host_jvm_args=-XX:+UseBASE64Intrinsics\ntest:fixture --jvmopt=-XX:+UseBASE64Intrinsics\n"
	got, err := transformRC(input)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(got, blockBegin) != 1 || strings.Contains(got, "stale") || !strings.HasPrefix(got, "# caller settings\nbuild --color=no\n") {
		t.Fatalf("failed to preserve caller and replace managed settings: %q", got)
	}
	for _, setting := range []string{"startup --host_jvm_args", "test:fixture --jvmopt"} {
		if strings.LastIndex(got, setting+"="+mitigation) < strings.LastIndex(got, setting+"=-XX:+UseBASE64Intrinsics") {
			t.Fatalf("enabling option overrides %s", setting)
		}
	}
	repeated, err := transformRC(got)
	if err != nil || repeated != got {
		t.Fatalf("managed block is not stable: %v", err)
	}
}

func TestCommonFixedValueIsNotOverridden(t *testing.T) {
	got, err := transformRC("common --action_env=JAVA_TOOL_OPTIONS=-Dfixture=preserved\n")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "common --action_env=JAVA_TOOL_OPTIONS\n") || !strings.Contains(got, "-Dfixture=preserved "+mitigation) {
		t.Fatalf("inherited default replaced common fixed value: %q", got)
	}
}

func TestPlatformMitigationsAreLastAndStable(t *testing.T) {
	var input strings.Builder
	expected := []string{}
	for _, platform := range []string{"linux", "macos", "windows", "freebsd", "openbsd"} {
		input.WriteString("startup:" + platform + " --host_jvm_args=-XX:+UseBASE64Intrinsics\n")
		expected = append(expected, "startup:"+platform+" --host_jvm_args="+mitigation)
		for _, command := range []string{"build", "test", "run"} {
			for _, flag := range []string{"--jvmopt", "--host_jvmopt"} {
				input.WriteString(command + ":" + platform + " " + flag + "=-XX:+UseBASE64Intrinsics\n")
				expected = append(expected, command+":"+platform+" "+flag+"="+mitigation)
			}
		}
	}
	got, err := transformRC(input.String())
	if err != nil {
		t.Fatal(err)
	}
	for _, control := range expected {
		key, _, _ := strings.Cut(control, "=")
		if strings.LastIndex(got, control) < 0 || strings.LastIndex(got, control) < strings.LastIndex(got, key+"=-XX:+UseBASE64Intrinsics") {
			t.Fatalf("platform mitigation not last: %s", key)
		}
	}
	repeated, err := transformRC(got)
	if err != nil || repeated != got {
		t.Fatalf("platform block unstable: %v", err)
	}
}

func TestRunWritesNothingOnInvalidConfiguration(t *testing.T) {
	for _, input := range []string{blockBegin, blockEnd, blockBegin + "\n" + blockBegin, `test --test_env="unterminated`} {
		var output bytes.Buffer
		if err := run(strings.NewReader(input), &output); err == nil || output.Len() != 0 {
			t.Errorf("invalid input must fail without output: err=%v, output=%q", err, output.String())
		}
	}
}
