// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package suppress

import (
	"slices"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/jsonvalue"
)

func namedRules() Config {
	return Config{Rules: []Rule{
		{Name: "tenants", Headers: []string{"x-tenant"}, Values: []string{"t1"}, Action: DropRecord},
		{Name: "nca_id", Headers: ncaHeaders, Values: []string{"acme"}, Action: StripPayload, NCAID: true},
	}}
}

func TestOutcomeNamesTheMatchedRule(t *testing.T) {
	engine := mustEngine(t, namedRules())
	tests := []struct {
		name string
		line string
		rule string
	}{
		{name: "first rule", line: `{"event_type":"request_payload","payload":{"http_request_headers":{"x-tenant":"t1"}}}`, rule: "tenants"},
		{name: "second rule", line: `{"event_type":"request_payload","payload":{"http_request_headers":{"nvcf-ncaid":"acme"}}}`, rule: "nca_id"},
		{name: "no match", line: `{"event_type":"request_payload","payload":{"http_request_headers":{"nvcf-ncaid":"other"}}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, outcome := engine.Apply(mustDecode(t, tt.line)); outcome.Rule != tt.rule {
				t.Fatalf("outcome.Rule = %q, want %q", outcome.Rule, tt.rule)
			}
		})
	}
}

func TestOutcomeListsEveryRuleWhoseHeaderIsPresent(t *testing.T) {
	engine := mustEngine(t, namedRules())
	tests := []struct {
		name    string
		line    string
		present []string
	}{
		// Presence counts whether or not the value matched, and a match by
		// an earlier rule does not hide a later rule's header.
		{name: "both present, first matches", line: `{"event_type":"request_payload","payload":{"http_request_headers":{"x-tenant":"t1","nvcf-ncaid":"other"}}}`, present: []string{"tenants", "nca_id"}},
		{name: "one present, no match", line: `{"event_type":"request_payload","payload":{"http_request_headers":{"nvcf-nca-id":"other"}}}`, present: []string{"nca_id"}},
		{name: "blank value is absent", line: `{"event_type":"request_payload","payload":{"http_request_headers":{"nvcf-ncaid":" "}}}`},
		{name: "headers not captured", line: `{"event_type":"request_payload","payload":{"request_id":"r1"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, outcome := engine.Apply(mustDecode(t, tt.line))
			if !slices.Equal(outcome.Present, tt.present) {
				t.Fatalf("outcome.Present = %v, want %v", outcome.Present, tt.present)
			}
			if outcome.HeaderPresent != (len(tt.present) > 0) {
				t.Fatalf("outcome.HeaderPresent = %v, want %v", outcome.HeaderPresent, len(tt.present) > 0)
			}
		})
	}
}

func TestHeaderlessRecordsAreExportedByDefault(t *testing.T) {
	engine := mustEngine(t, namedRules())
	record := mustDecode(t, `{"event_type":"request_end","request":{"request_id":"r1"}}`)

	got, outcome := engine.Apply(record)
	if got != record || outcome.Action != "" || outcome.Headerless {
		t.Fatalf("got = %v outcome = %+v, want the record exported as is", got, outcome)
	}
}

func TestHeaderlessDropPolicyDropsRecordsThatCarryNoHeaders(t *testing.T) {
	cfg := namedRules()
	cfg.Headerless = HeaderlessDrop
	engine := mustEngine(t, cfg)

	got, outcome := engine.Apply(mustDecode(t, `{"event_type":"tool_end","tool":{"tool_call_id":"c1"}}`))
	if got != nil || outcome.Action != DropRecord || !outcome.Headerless || outcome.Rule != "" {
		t.Fatalf("got = %v outcome = %+v, want a headerless drop", got, outcome)
	}

	// A payload record with no captured header is still evaluated and kept:
	// a producer that does not capture the header must never cost records.
	payload := mustDecode(t, `{"event_type":"request_payload","payload":{"request_id":"r1"}}`)
	if got, outcome := engine.Apply(payload); got != payload || outcome.Action != "" || !outcome.Evaluated {
		t.Fatalf("got = %v outcome = %+v, want the payload record kept", got, outcome)
	}
}

func TestHeaderlessDropPolicyIsInertWithoutRules(t *testing.T) {
	engine := mustEngine(t, Config{Headerless: HeaderlessDrop})
	record := mustDecode(t, `{"event_type":"request_end"}`)
	if got, outcome := engine.Apply(record); got != record || outcome.Action != "" {
		t.Fatalf("got = %v outcome = %+v, want pass through when suppression is off", got, outcome)
	}
}

func TestConfigValidateRejectsBadNamesAndPolicies(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "duplicate rule name", cfg: Config{Rules: []Rule{
			{Name: "a", Headers: ncaHeaders, Values: []string{"x"}, Action: StripPayload},
			{Name: "a", Headers: []string{"x-tenant"}, Values: []string{"y"}, Action: DropRecord},
		}}},
		{name: "unknown headerless policy", cfg: Config{Headerless: "reject"}},
		{name: "unnamed stripPayload rule", cfg: Config{Rules: []Rule{
			{Headers: []string{"x-user-id"}, Values: []string{"alice"}, Action: StripPayload},
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want an error")
			}
		})
	}
	for _, policy := range []HeaderlessPolicy{"", HeaderlessExport, HeaderlessDrop} {
		if err := (Config{Headerless: policy}).Validate(); err != nil {
			t.Errorf("Validate(headerless %q) error = %v", policy, err)
		}
	}
}

// Only the NCA id rule writes the earlier uploader's stub, which records the
// matched value as an NCA id. Any other stripPayload rule matches on some
// other header, so its stub carries no identifier and names the rule as the
// reason.
func TestStubFromAnotherRuleCarriesNoIdentifier(t *testing.T) {
	engine := mustEngine(t, Config{Rules: []Rule{
		{Name: "users", Headers: []string{"x-user-id"}, Values: []string{"alice@example.com"}, Action: StripPayload},
	}})
	tests := []struct {
		name string
		line string
		want string
	}{
		{
			name: "payload record",
			line: `{"event_type":"request_payload","event_time_unix_ms":1,"payload":{"request_id":"r1","model":"m","body":{"q":"secret"},"http_request_headers":{"x-user-id":"alice@example.com","nvcf-ncaid":"acme"}}}`,
			want: `{"event_type":"request_payload","event_time_unix_ms":1,"payload":{"request_id":"r1","model":"m","payload_complete":false,"payload_drop_reason":"users","payload_suppressed":true,"suppression_reason":"users","http_request_headers":{}}}`,
		},
		{
			name: "bare record",
			line: `{"timestamp":5,"event":{"event_type":"request_payload","request_id":"r1","headers":{"x-user-id":"alice@example.com"},"body":"secret"}}`,
			want: `{"timestamp":5,"event":{"event_type":"request_payload","request_id":"r1","payload_suppressed":true,"suppression_reason":"users","headers":{}}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, outcome := engine.Apply(mustDecode(t, tt.line))
			if outcome.Action != StripPayload || outcome.Rule != "users" {
				t.Fatalf("outcome = %+v, want stripPayload by rule users", outcome)
			}
			if encoded := string(jsonvalue.Encode(got)); encoded != tt.want {
				t.Fatalf("stub = %s\nwant   %s", encoded, tt.want)
			}
		})
	}
}

// The NCA id rule keeps the earlier uploader's stub byte for byte, whatever
// the rule is named.
func TestNCAIDRuleStubRecordsTheID(t *testing.T) {
	cfg := FromIDs([]string{"acme"}, ncaHeaders...)
	cfg.Rules[0].Name = "nca_id"
	engine := mustEngine(t, cfg)
	got, _ := engine.Apply(mustDecode(t, `{"event_type":"request_payload","payload":{"request_id":"r1","http_request_headers":{"nvcf-ncaid":"acme"}}}`))
	want := `{"event_type":"request_payload","payload":{"request_id":"r1","payload_complete":false,"payload_drop_reason":"nca_id","payload_suppressed":true,"suppression_reason":"nca_id","nca_id":"acme","http_request_headers":{"nvcf-ncaid":"acme"}}}`
	if encoded := string(jsonvalue.Encode(got)); encoded != want {
		t.Fatalf("stub = %s\nwant   %s", encoded, want)
	}
}

// The strictest matching rule decides: a drop rule beats a strip rule that
// matched first, so a record an operator asked to drop is never exported as
// a stub.
func TestDropBeatsStripWhenBothMatch(t *testing.T) {
	engine := mustEngine(t, Config{Rules: []Rule{
		{Name: "nca_id", Headers: ncaHeaders, Values: []string{"acme"}, Action: StripPayload, NCAID: true},
		{Name: "tenants", Headers: []string{"x-tenant"}, Values: []string{"t1"}, Action: DropRecord},
	}})
	got, outcome := engine.Apply(mustDecode(t, `{"event_type":"request_payload","payload":{"http_request_headers":{"nvcf-ncaid":"acme","x-tenant":"t1"}}}`))
	if got != nil || outcome.Action != DropRecord || outcome.Rule != "tenants" || outcome.ID != "t1" {
		t.Fatalf("got = %v outcome = %+v, want the record dropped by tenants", got, outcome)
	}
}

// Validate errors give positions, never configured text, which can hold
// customer identifiers.
func TestConfigValidateErrorsQuoteNoConfiguredText(t *testing.T) {
	rule := func(name, header string, action Action) Rule {
		return Rule{Name: name, Headers: []string{header}, Values: []string{"t1"}, Action: action}
	}
	tests := []struct {
		name   string
		cfg    Config
		quoted string
	}{
		{name: "repeated rule name", quoted: "acme-rule", cfg: Config{Rules: []Rule{
			rule("acme-rule", "x-tenant", DropRecord), rule("acme-rule", "x-org", DropRecord),
		}}},
		{name: "unknown action", quoted: "acme-action", cfg: Config{Rules: []Rule{rule("a", "x-tenant", "acme-action")}}},
		{name: "upper case header", quoted: "X-Acme", cfg: Config{Rules: []Rule{rule("a", "X-Acme", DropRecord)}}},
		{name: "header that is not a header name", quoted: "acme header", cfg: Config{Rules: []Rule{rule("a", "acme header", DropRecord)}}},
		{name: "unknown headerless policy", quoted: "acme-policy", cfg: Config{Headerless: "acme-policy"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if err == nil {
				t.Fatal("Validate() error = nil, want an error")
			}
			if strings.Contains(err.Error(), tt.quoted) {
				t.Fatalf("Validate() error = %q, quotes %q", err, tt.quoted)
			}
		})
	}
}
