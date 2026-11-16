// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/suppress"
)

func requiredEnv(extra map[string]string) map[string]string {
	env := map[string]string{EnvSourceDir: "/records", EnvBackend: string(BackendDebug)}
	for name, value := range extra {
		env[name] = value
	}
	return env
}

// chartNCAHeaders is the NCA id header list a deployment sets, in order.
const chartNCAHeaders = "nvcf-ncaid,nvcf-nca-id"

func ncaRule(values ...string) suppress.Rule {
	return suppress.Rule{Name: NCAIDRuleName, Headers: []string{"nvcf-ncaid", "nvcf-nca-id"}, Values: values, Action: suppress.StripPayload, NCAID: true}
}

func TestLoadSuppressNCAIDs(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		rules []suppress.Rule
	}{
		{name: "unset is off"},
		{name: "empty is off", env: map[string]string{EnvSuppressNCAIDs: ""}},
		{name: "only separators is off", env: map[string]string{EnvSuppressNCAIDs: " , ,,"}},
		{name: "one id", env: map[string]string{EnvSuppressNCAIDs: "acme", EnvSuppressNCAHeaders: chartNCAHeaders}, rules: []suppress.Rule{ncaRule("acme")}},
		{
			name:  "split and trimmed, and normalized like the shell exporter when compiled",
			env:   map[string]string{EnvSuppressNCAIDs: " nca-acme-nca , beta,,nca-gamma-nca", EnvSuppressNCAHeaders: chartNCAHeaders},
			rules: []suppress.Rule{ncaRule("acme", "beta", "gamma")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, warnings, err := Load(mapLookup(requiredEnv(tt.env)))
			if err != nil || len(warnings) != 0 {
				t.Fatalf("Load() error = %v warnings = %v", err, warnings)
			}
			if got := compiledValues(cfg.Suppression.Rules); !reflect.DeepEqual(got, tt.rules) {
				t.Fatalf("Rules = %+v, want %+v", got, tt.rules)
			}
		})
	}
}

// compiledValues returns rules with each value as the engine compiles it:
// normalized once, blank values left out.
func compiledValues(rules []suppress.Rule) []suppress.Rule {
	var out []suppress.Rule
	for _, rule := range rules {
		var values []string
		for _, value := range rule.Values {
			if normalized := suppress.NormalizeID(value); normalized != "" {
				values = append(values, normalized)
			}
		}
		rule.Values = values
		out = append(out, rule)
	}
	return out
}

func TestLoadSuppressNCAHeaders(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		headers []string
		wantErr bool
	}{
		{name: "ids without headers fail", env: map[string]string{EnvSuppressNCAIDs: "acme"}, wantErr: true},
		{name: "ids with empty headers fail", env: map[string]string{EnvSuppressNCAIDs: "acme", EnvSuppressNCAHeaders: " "}, wantErr: true},
		{name: "no ids and no headers load with suppression off"},
		{name: "blank ids and no headers load with suppression off", env: map[string]string{EnvSuppressNCAIDs: " , "}},
		{name: "headers without ids load with suppression off", env: map[string]string{EnvSuppressNCAHeaders: "nvcf-ncaid"}},
		{
			name:    "headers are trimmed, lower-cased and kept in order",
			env:     map[string]string{EnvSuppressNCAIDs: "acme", EnvSuppressNCAHeaders: " X-Org-Id , nvcf-ncaid"},
			headers: []string{"x-org-id", "nvcf-ncaid"},
		},
		{name: "a blank header fails", env: map[string]string{EnvSuppressNCAIDs: "acme", EnvSuppressNCAHeaders: "x-org-id,,nvcf-ncaid"}, wantErr: true},
		{name: "a trailing separator fails", env: map[string]string{EnvSuppressNCAIDs: "acme", EnvSuppressNCAHeaders: "nvcf-ncaid,"}, wantErr: true},
		{name: "a repeated header fails", env: map[string]string{EnvSuppressNCAIDs: "acme", EnvSuppressNCAHeaders: "nvcf-ncaid, NVCF-NCAID"}, wantErr: true},
		{name: "a bad header list fails without ids", env: map[string]string{EnvSuppressNCAHeaders: "nvcf-ncaid,,x"}, wantErr: true},
		{name: "a name that is not an HTTP header fails without ids", env: map[string]string{EnvSuppressNCAHeaders: "nvcf-ncaid,bad/header"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, _, err := Load(mapLookup(requiredEnv(tt.env)))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Load() error = nil, rules = %+v; want a startup error", cfg.Suppression.Rules)
				}
				if !strings.Contains(err.Error(), EnvSuppressNCAHeaders) {
					t.Fatalf("Load() error = %q, want it to name %s", err, EnvSuppressNCAHeaders)
				}
				if strings.Contains(err.Error(), "acme") {
					t.Fatalf("Load() error = %q, echoes a configured id", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if tt.headers == nil {
				if len(cfg.Suppression.Rules) != 0 {
					t.Fatalf("Rules = %+v, want suppression off", cfg.Suppression.Rules)
				}
				return
			}
			want := []suppress.Rule{{Name: NCAIDRuleName, Headers: tt.headers, Values: []string{"acme"}, Action: suppress.StripPayload, NCAID: true}}
			if !reflect.DeepEqual(cfg.Suppression.Rules, want) {
				t.Fatalf("Rules = %+v, want %+v", cfg.Suppression.Rules, want)
			}
		})
	}
}

func TestLoadNamesBothSettingsWhenIDsHaveNoHeaders(t *testing.T) {
	_, _, err := Load(mapLookup(requiredEnv(map[string]string{EnvSuppressNCAIDs: "nca-acme-nca"})))
	if err == nil || !strings.Contains(err.Error(), EnvSuppressNCAIDs) || !strings.Contains(err.Error(), EnvSuppressNCAHeaders) {
		t.Fatalf("Load() error = %v, want one naming %s and %s", err, EnvSuppressNCAIDs, EnvSuppressNCAHeaders)
	}
	if strings.Contains(err.Error(), "acme") {
		t.Fatalf("Load() error = %q, echoes a configured id", err)
	}
}

func TestLoadParsesExplicitSuppressionRules(t *testing.T) {
	cfg, _, err := Load(mapLookup(requiredEnv(map[string]string{
		EnvSuppressNCAIDs:     "acme",
		EnvSuppressNCAHeaders: chartNCAHeaders,
		EnvSuppressionRules:   `[{"name":"tenants","headers":[" X-Tenant "],"values":["t1","t2"],"action":"dropRecord"}]`,
	})))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []suppress.Rule{
		ncaRule("acme"),
		{Name: "tenants", Headers: []string{"x-tenant"}, Values: []string{"t1", "t2"}, Action: suppress.DropRecord},
	}
	if !reflect.DeepEqual(cfg.Suppression.Rules, want) {
		t.Fatalf("Rules = %+v, want %+v", cfg.Suppression.Rules, want)
	}
}

func TestLoadHeaderlessPolicy(t *testing.T) {
	cfg, _, err := Load(mapLookup(requiredEnv(nil)))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Suppression.Headerless != suppress.HeaderlessExport {
		t.Fatalf("default Headerless = %q, want %q", cfg.Suppression.Headerless, suppress.HeaderlessExport)
	}
	cfg, _, err = Load(mapLookup(requiredEnv(map[string]string{EnvSuppressHeaderless: "drop"})))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Suppression.Headerless != suppress.HeaderlessDrop {
		t.Fatalf("Headerless = %q, want %q", cfg.Suppression.Headerless, suppress.HeaderlessDrop)
	}
}

func TestLoadSuppressionStrict(t *testing.T) {
	tests := []struct {
		name  string
		value *string
		want  bool
	}{
		{name: "unset is off"},
		{name: "true", value: ptr("true"), want: true},
		{name: "false", value: ptr("false")},
		{name: "blank is off", value: ptr(" ")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := map[string]string{}
			if tt.value != nil {
				env[EnvSuppressionStrict] = *tt.value
			}
			cfg, warnings, err := Load(mapLookup(requiredEnv(env)))
			if err != nil || len(warnings) > 0 {
				t.Fatalf("Load() = %v, %v", warnings, err)
			}
			if cfg.Suppression.Strict != tt.want {
				t.Fatalf("Strict = %v, want %v", cfg.Suppression.Strict, tt.want)
			}
		})
	}
}

func ptr(s string) *string { return &s }

func TestLoadRejectsInvalidSuppression(t *testing.T) {
	rule := func(body string) map[string]string {
		return map[string]string{EnvSuppressionRules: "[" + body + "]"}
	}
	tests := []struct {
		name string
		env  map[string]string
	}{
		{name: "unparseable", env: map[string]string{EnvSuppressionRules: `[{"name":`}},
		{name: "not a list", env: map[string]string{EnvSuppressionRules: `{"name":"a"}`}},
		{name: "trailing data", env: map[string]string{EnvSuppressionRules: `[] []`}},
		{name: "unknown field", env: rule(`{"name":"a","header":"x-tenant","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}`)},
		{name: "unknown action", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"],"action":"bashStub"}`)},
		{name: "missing action", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"]}`)},
		{name: "empty value list", env: rule(`{"name":"a","headers":["x-tenant"],"values":[],"action":"dropRecord"}`)},
		{name: "only blank values", env: rule(`{"name":"a","headers":["x-tenant"],"values":[" ",""],"action":"dropRecord"}`)},
		{name: "no headers", env: rule(`{"name":"a","headers":[],"values":["t1"],"action":"dropRecord"}`)},
		{name: "blank header", env: rule(`{"name":"a","headers":[" "],"values":["t1"],"action":"dropRecord"}`)},
		{name: "missing name", env: rule(`{"headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}`)},
		{name: "name is not a short lowercase token", env: rule(`{"name":"Tenant Rule","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}`)},
		{name: "duplicate names", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"},{"name":"a","headers":["x-org"],"values":["o1"],"action":"dropRecord"}`)},
		{name: "name taken by the nca id rule", env: map[string]string{
			EnvSuppressNCAIDs:     "acme",
			EnvSuppressNCAHeaders: chartNCAHeaders,
			EnvSuppressionRules:   `[{"name":"nca_id","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}]`,
		}},
		{name: "unknown headerless policy", env: map[string]string{EnvSuppressHeaderless: "reject"}},
		{name: "strict is not a boolean", env: map[string]string{EnvSuppressionStrict: "yes"}},
		{name: "quoted nca id", env: map[string]string{EnvSuppressNCAIDs: `"acme"`}},
		{name: "nca ids joined by a semicolon", env: map[string]string{EnvSuppressNCAIDs: "acme;o1"}},
		{name: "nca ids joined by a space", env: map[string]string{EnvSuppressNCAIDs: "acme o1"}},
		{name: "nca ids joined by a newline", env: map[string]string{EnvSuppressNCAIDs: "acme\no1"}},
		{name: "nca ids as a JSON list", env: map[string]string{EnvSuppressNCAIDs: `["acme"]`}},
		{name: "rule value that is not an id", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1 o1"],"action":"dropRecord"}`)},
		{name: "null instead of a list", env: map[string]string{EnvSuppressionRules: ` null `}},
		{name: "repeated field", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"],"values":["o1"],"action":"dropRecord"}`)},
		{name: "field repeated in another case", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"],"Values":["o1"],"action":"dropRecord"}`)},
		{name: "field repeated with a folded letter", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"],"value\u017f":["o1"],"action":"dropRecord"}`)},
		{name: "repeated action", env: rule(`{"name":"a","headers":["x-tenant"],"values":["t1"],"action":"stripPayload","action":"dropRecord"}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Load(mapLookup(requiredEnv(tt.env)))
			if err == nil {
				t.Fatal("Load() error = nil, want a startup error")
			}
			// A configured value can be a customer identifier, so it must
			// not be echoed into the error.
			for _, value := range []string{"t1", "o1", "acme"} {
				if strings.Contains(err.Error(), value) {
					t.Fatalf("Load() error = %q, echoes a configured value", err)
				}
			}
		})
	}
}

// An id that is empty once its nca- and -nca affixes are removed, such as
// nca--nca, can never match; it stops startup instead of being skipped, which
// could turn suppression off without a word. The error names the setting and
// the entry, never the value.
func TestLoadRejectsAnIDThatReducesToNothing(t *testing.T) {
	tests := map[string]map[string]string{
		"only such an id":  {EnvSuppressNCAIDs: "nca--nca"},
		"one among others": {EnvSuppressNCAIDs: "acme,nca-nca"},
		"a rule value":     {EnvSuppressionRules: `[{"name":"a","headers":["x-tenant"],"values":["nca--nca","t1"],"action":"dropRecord"}]`},
	}
	for name, env := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := Load(mapLookup(requiredEnv(env)))
			if err == nil {
				t.Fatal("Load() accepted an id that is empty once reduced")
			}
			if strings.Contains(err.Error(), "nca-nca") || strings.Contains(err.Error(), "nca--nca") {
				t.Fatalf("Load() error = %q, echoes the configured id", err)
			}
		})
	}
}

// Suppression errors name a rule by its position and a field by its
// canonical name. They never quote text from the configured rules (field
// names as written, rule names, header names, JSON characters), which can
// hold customer identifiers.
func TestLoadSuppressionErrorsNameRulesByIndexAndFieldsCanonically(t *testing.T) {
	const valid = `{"name":"ok","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}`
	tests := []struct {
		name   string
		env    map[string]string
		want   []string
		quoted []string
	}{
		{
			name:   "unknown field",
			env:    map[string]string{EnvSuppressionRules: `[` + valid + `,{"name":"b","headers":["x-tenant"],"values":["t1"],"action":"dropRecord","acme-key":true}]`},
			want:   []string{"rule 1"},
			quoted: []string{"acme-key"},
		},
		{
			name:   "field of the wrong type, written in another case",
			env:    map[string]string{EnvSuppressionRules: `[{"name":"b","HeAdErS":"x-tenant","values":["t1"],"action":"dropRecord"}]`},
			want:   []string{"rule 0", "headers"},
			quoted: []string{"HeAdErS", "ruleSpec"},
		},
		{
			name:   "value of the wrong type",
			env:    map[string]string{EnvSuppressionRules: `[{"name":"b","headers":["x-tenant"],"VALUES":["t1",7],"action":"dropRecord"}]`},
			want:   []string{"rule 0", "values"},
			quoted: []string{"VALUES", "ruleSpec"},
		},
		{
			name:   "rule that is not an object",
			env:    map[string]string{EnvSuppressionRules: `[` + valid + `,"acme-rule"]`},
			want:   []string{"rule 1"},
			quoted: []string{"acme-rule"},
		},
		{
			name:   "invalid JSON",
			env:    map[string]string{EnvSuppressionRules: `[{"name": Zacme}]`},
			want:   []string{"not valid JSON"},
			quoted: []string{"'Z'", "Zacme"},
		},
		{
			name:   "unknown action",
			env:    map[string]string{EnvSuppressionRules: `[{"name":"acme-rule","headers":["x-tenant"],"values":["t1"],"action":"hide"}]`},
			want:   []string{"rule 0", "action"},
			quoted: []string{"acme-rule", "hide"},
		},
		{
			name:   "value that is not an id",
			env:    map[string]string{EnvSuppressionRules: `[{"name":"acme-rule","headers":["x-tenant"],"values":["t1 o1"],"action":"dropRecord"}]`},
			want:   []string{"rule 0", "values"},
			quoted: []string{"acme-rule"},
		},
		{
			name:   "repeated rule name",
			env:    map[string]string{EnvSuppressionRules: `[{"name":"acme-rule","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"},{"name":"acme-rule","headers":["x-org"],"values":["o1"],"action":"dropRecord"}]`},
			want:   []string{"rule 1", "rule 0", "name"},
			quoted: []string{"acme-rule"},
		},
		{
			name:   "header that is not a header name",
			env:    map[string]string{EnvSuppressionRules: `[{"name":"b","headers":["x-tenant","acme(header)"],"values":["t1"],"action":"dropRecord"}]`},
			want:   []string{"rule 0", "headers entry 1"},
			quoted: []string{"acme(header)"},
		},
		{
			name:   "nca id header that is not a header name",
			env:    map[string]string{EnvSuppressNCAIDs: "t1", EnvSuppressNCAHeaders: "nvcf-ncaid,acme header"},
			want:   []string{EnvSuppressNCAHeaders + " entry 1"},
			quoted: []string{"acme header"},
		},
		{
			name:   "name that is not a short lowercase token",
			env:    map[string]string{EnvSuppressionRules: `[{"name":"Acme Rule","headers":["x-tenant"],"values":["t1"],"action":"dropRecord"}]`},
			want:   []string{"rule 0", "name"},
			quoted: []string{"Acme Rule"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Load(mapLookup(requiredEnv(tt.env)))
			if err == nil {
				t.Fatal("Load() error = nil, want a startup error")
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Load() error = %q, want it to name %q", err, want)
				}
			}
			for _, quoted := range append(tt.quoted, "t1", "o1") {
				if strings.Contains(err.Error(), quoted) {
					t.Errorf("Load() error = %q, quotes configured text %q", err, quoted)
				}
			}
		})
	}
}
