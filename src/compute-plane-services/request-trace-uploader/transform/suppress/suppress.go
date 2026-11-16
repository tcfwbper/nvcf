// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package suppress applies record suppression rules.
//
// A rule names a header group, the values that match, and an action. Only
// records that carry a request payload are evaluated, because only those
// carry request headers and bodies. stripPayload writes the stub the earlier
// uploader wrote, with the same keys in the same order, so suppressed rows
// keep their shape for their consumers.
package suppress

import (
	"fmt"
	"slices"
	"strings"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/transform/jsonvalue"
)

// Action is what a matching rule does to a record.
type Action string

const (
	// StripPayload replaces the record with a stub that keeps its identity
	// and timing fields, drops every body and header, and records why.
	StripPayload Action = "stripPayload"
	// DropRecord removes the record from the export.
	DropRecord Action = "dropRecord"
)

// Rule matches records whose header group resolves to one of Values.
type Rule struct {
	// Name is the operator-assigned name that labels this rule's counters.
	// It must be unique in a Config. It never holds a header name or value.
	Name string
	// Headers lists lower-case header names. The first name with a
	// non-empty value decides.
	Headers []string
	// Values lists the ids that match, after NormalizeID.
	Values []string
	Action Action
	// NCAID marks the NCA id rule that FromIDs builds. Its stub is the
	// earlier uploader's stub, which records the matched value as nca_id
	// and as the nvcf-ncaid header. The stub of any other rule carries no
	// identifier and gives the rule name as the reason, because its value
	// is not an NCA id.
	NCAID bool
}

// HeaderlessPolicy says what happens to records that rules are not checked
// on, because they are not request_payload records and have no request
// header field (see Evaluable), and to records of another type on which no
// rule's header resolved. In Dynamo v1.4.0 that is every type except
// request_payload. It applies only while some rule is configured.
type HeaderlessPolicy string

const (
	// HeaderlessExport exports such records as they are. It is the default.
	HeaderlessExport HeaderlessPolicy = "export"
	// HeaderlessDrop removes such records from the export.
	HeaderlessDrop HeaderlessPolicy = "drop"
)

// Config is the ordered rule list. The strictest matching rule wins: a
// dropRecord rule beats a stripPayload rule, and among rules with the same
// action the first one wins. No rules means suppression is off.
type Config struct {
	Rules []Rule
	// Headerless is the policy for records that carry no headers. Empty
	// means HeaderlessExport.
	Headerless HeaderlessPolicy
	// Strict strips an evaluated record on which some rule's header has no
	// value, because that rule cannot tell whether the record is one it
	// suppresses, and a record with a payload field of any kind that no
	// rule can check. A body outside a payload field is not covered; the
	// drop headerless policy is the way to keep such records out. A
	// matching dropRecord rule, or the drop headerless policy, still drops
	// it. Off, such a record is exported unless another rule matches or the
	// headerless policy drops it.
	Strict bool
}

// FromIDs builds the configuration for a list of suppressed NCA ids: one
// stripPayload rule on headers, read in the given order. The package has no
// built-in header list, so the caller names the headers that carry the NCA
// id; a rule without one fails Validate. Ids that are blank once reduced are
// ignored, so an empty list turns suppression off; the setting refuses such
// an id before it gets here. Ids are trimmed but not normalized: New
// normalizes them once, as Apply does each header value.
func FromIDs(ids []string, headers ...string) Config {
	values := make([]string, 0, len(ids))
	for _, id := range ids {
		if NormalizeID(id) != "" {
			values = append(values, strings.TrimSpace(id))
		}
	}
	if len(values) == 0 {
		return Config{}
	}
	return Config{Rules: []Rule{{Headers: slices.Clone(headers), Values: values, Action: StripPayload, NCAID: true}}}
}

// Validate rejects a rule with an unknown action, no header, a header that
// is blank or not lower case, no non-blank value, or a name another rule
// already has, a stripPayload rule other than the NCA id rule with no name,
// because its stub gives the name as the reason, and an unknown headerless
// policy.
func (c Config) Validate() error {
	switch c.Headerless {
	case "", HeaderlessExport, HeaderlessDrop:
	default:
		return fmt.Errorf("suppression: headerless policy must be %q or %q", HeaderlessExport, HeaderlessDrop)
	}
	// Errors give rule and header positions, never configured names, which
	// can be customer identifiers.
	names := make(map[string]int, len(c.Rules))
	for i, rule := range c.Rules {
		if rule.Name != "" {
			if first, used := names[rule.Name]; used {
				return fmt.Errorf("suppression rule %d: name is also used by rule %d", i, first)
			}
			names[rule.Name] = i
		}
		switch rule.Action {
		case StripPayload, DropRecord:
		default:
			return fmt.Errorf("suppression rule %d: action must be %q or %q", i, StripPayload, DropRecord)
		}
		if rule.Action == StripPayload && !rule.NCAID && rule.Name == "" {
			return fmt.Errorf("suppression rule %d: a stripPayload rule needs a name", i)
		}
		if len(rule.Headers) == 0 {
			return fmt.Errorf("suppression rule %d: no header", i)
		}
		for j, name := range rule.Headers {
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("suppression rule %d: headers entry %d is blank", i, j)
			}
			if name != strings.ToLower(name) {
				return fmt.Errorf("suppression rule %d: headers entry %d must be lower case", i, j)
			}
			if !isToken(name) {
				return fmt.Errorf("suppression rule %d: headers entry %d is not an HTTP header name", i, j)
			}
		}
		if len(normalizedSet(rule.Values)) == 0 {
			return fmt.Errorf("suppression rule %d: no value", i)
		}
	}
	return nil
}

// isToken reports whether name is an HTTP token (RFC 9110), the form every
// header name takes.
func isToken(name string) bool {
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return name != ""
}

func normalizedSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if normalized := NormalizeID(value); normalized != "" {
			set[normalized] = struct{}{}
		}
	}
	return set
}

type compiledRule struct {
	name    string
	headers []string
	values  map[string]struct{}
	action  Action
	ncaID   bool
}

// Engine applies a validated rule list.
type Engine struct {
	rules          []compiledRule
	dropHeaderless bool
	strict         bool
	// names lists the named rules in order.
	names []string
}

// New validates cfg and builds an engine.
func New(cfg Config) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	engine := &Engine{dropHeaderless: cfg.Headerless == HeaderlessDrop, strict: cfg.Strict}
	for _, rule := range cfg.Rules {
		engine.rules = append(engine.rules, compiledRule{
			name:    rule.Name,
			headers: append([]string(nil), rule.Headers...),
			values:  normalizedSet(rule.Values),
			action:  rule.Action,
			ncaID:   rule.NCAID,
		})
		if rule.Name != "" {
			engine.names = append(engine.names, rule.Name)
		}
	}
	return engine, nil
}

// Active reports whether any rule is configured.
func (e *Engine) Active() bool { return len(e.rules) > 0 }

// Outcome reports what Apply did to one record.
type Outcome struct {
	// Evaluated reports that the record carries a request payload or
	// request headers, so the rules were checked.
	Evaluated bool
	// HeaderPresent reports that some rule's header group had a value. A
	// deployment where this never happens cannot suppress anything.
	HeaderPresent bool
	// Action is the action taken, or empty when no rule matched.
	Action Action
	// ID is the normalized header value that matched.
	ID string
	// Rule is the name of the rule that matched.
	Rule string
	// Present lists, in rule order, the names of the rules whose header
	// group had a value on this record, whether or not the value matched.
	Present []string
	// Headerless reports that the record carries no headers and the
	// headerless policy decided Action.
	Headerless bool
	// Unresolved lists, in rule order, the names of the rules whose header
	// group had no value, when strict mode stripped the record for it.
	Unresolved []string
}

// Apply evaluates record and returns what to export: record itself when no
// rule matches, the stub for StripPayload, and nil for DropRecord. record is
// never changed. Every rule's header group is checked, so Outcome.Present is
// complete even after an earlier rule matched.
func (e *Engine) Apply(record *jsonvalue.Value) (*jsonvalue.Value, Outcome) {
	var outcome Outcome
	if len(e.rules) == 0 {
		return record, outcome
	}
	event := EventOf(record)
	if !Evaluable(event) {
		switch {
		case e.dropHeaderless:
			outcome.Action = DropRecord
			outcome.Headerless = true
			return nil, outcome
		case e.strict && hasPayload(event):
			// A newer type that carries a payload but no header field: no
			// rule can check it, and it may hold a body.
			outcome.Evaluated = true
			outcome.Action = StripPayload
			outcome.Unresolved = slices.Clone(e.names)
			return stub(record, event, unresolvedReason, ""), outcome
		}
		return record, outcome
	}
	outcome.Evaluated = true
	sets := HeaderSets(event)
	var matched *compiledRule
	// unresolved reports that some rule's header group had no value, and
	// unresolvedNames names those rules that have names.
	var unresolved bool
	var unresolvedNames []string
	for i := range e.rules {
		rule := &e.rules[i]
		id := NormalizeID(ResolveHeader(sets, rule.headers...))
		if id == "" {
			unresolved = true
			if rule.name != "" {
				unresolvedNames = append(unresolvedNames, rule.name)
			}
			continue
		}
		outcome.HeaderPresent = true
		if rule.name != "" {
			outcome.Present = append(outcome.Present, rule.name)
		}
		if matched != nil && (matched.action == DropRecord || rule.action != DropRecord) {
			continue
		}
		if _, ok := rule.values[id]; ok {
			matched = rule
			outcome.Action = rule.action
			outcome.ID = id
			outcome.Rule = rule.name
		}
	}
	switch {
	case matched == nil && e.dropHeaderless && !outcome.HeaderPresent && !CarriesPayload(event):
		// A record of another type that has a header field but on which no
		// rule's header resolved is as unchecked as one with no field.
		outcome.Action = DropRecord
		outcome.Headerless = true
		return nil, outcome
	case matched == nil && e.strict && unresolved:
		outcome.Action = StripPayload
		outcome.Unresolved = unresolvedNames
		return stub(record, event, unresolvedReason, ""), outcome
	case matched == nil:
		return record, outcome
	case matched.action == DropRecord:
		return nil, outcome
	case matched.ncaID:
		return stub(record, event, ncaIDReason, outcome.ID), outcome
	default:
		return stub(record, event, matched.name, ""), outcome
	}
}

// EventOf returns the event of a record: the "event" object of a wrapped
// record, the record itself when bare, and nil when the record is not an
// object. A record with its own event_type is bare, whatever else it holds.
func EventOf(record *jsonvalue.Value) *jsonvalue.Value {
	if record == nil || record.Kind() != jsonvalue.Object {
		return nil
	}
	if event := record.Get("event"); event != nil && event.Kind() == jsonvalue.Object && !record.Has("event_type") {
		return event
	}
	return record
}

// Evaluable reports whether suppression rules apply to event: a
// request_payload record, or any record that has a request header field,
// whatever its value. In Dynamo v1.4.0 only request_payload carries headers;
// checking for the fields too means a newer record type that carries them
// is checked rather than exported unseen.
func Evaluable(event *jsonvalue.Value) bool {
	if CarriesPayload(event) {
		return true
	}
	if event == nil || event.Kind() != jsonvalue.Object {
		return false
	}
	if event.Has("headers") {
		return true
	}
	payload := event.Get("payload")
	return payload != nil && payload.Kind() == jsonvalue.Object && payload.Has("http_request_headers")
}

// hasPayload reports whether event has a payload field, of any kind, where
// Dynamo puts request and response bodies.
func hasPayload(event *jsonvalue.Value) bool {
	return event != nil && event.Kind() == jsonvalue.Object && event.Has("payload")
}

// CarriesPayload reports whether event is a request_payload record.
func CarriesPayload(event *jsonvalue.Value) bool {
	if event == nil || event.Kind() != jsonvalue.Object || !event.Has("event_type") {
		return false
	}
	eventType, ok := event.Get("event_type").String()
	return ok && eventType == "request_payload"
}

// HeaderSets returns the header objects of an event in lookup order: the
// top-level "headers", then "payload.http_request_headers".
func HeaderSets(event *jsonvalue.Value) []*jsonvalue.Value {
	if event == nil || event.Kind() != jsonvalue.Object {
		return nil
	}
	var sets []*jsonvalue.Value
	if headers := event.Get("headers"); headers != nil && headers.Kind() == jsonvalue.Object {
		sets = append(sets, headers)
	}
	if payload := event.Get("payload"); payload != nil && payload.Kind() == jsonvalue.Object {
		if headers := payload.Get("http_request_headers"); headers != nil && headers.Kind() == jsonvalue.Object {
			sets = append(sets, headers)
		}
	}
	return sets
}

// ResolveHeader returns the first non-blank value for names across sets. In
// each set, each name takes the first key that equals it ignoring ASCII case
// and has a value, so a blank key in another case does not hide a later one.
// A header value is a string trimmed of white space or a number as it was
// written; a list gives its first item that is a value. Booleans, null and
// objects are not values.
func ResolveHeader(sets []*jsonvalue.Value, names ...string) string {
	for _, headers := range sets {
		for _, name := range names {
			for _, m := range headers.Members() {
				if !asciiEqualFold(m.Key, name) {
					continue
				}
				if text := valueText(m.Value); text != "" {
					return text
				}
			}
		}
	}
	return ""
}

// valueText is the text of a header value, or of the first item of a list
// that is a value.
func valueText(value *jsonvalue.Value) string {
	if value.Kind() != jsonvalue.Array {
		return headerText(value)
	}
	for _, item := range value.Items() {
		if text := headerText(item); text != "" {
			return text
		}
	}
	return ""
}

func headerText(value *jsonvalue.Value) string {
	switch value.Kind() {
	case jsonvalue.String:
		text, _ := value.String()
		return strings.TrimSpace(text)
	case jsonvalue.Int, jsonvalue.Float:
		return jsonvalue.Text(value)
	}
	return ""
}

// asciiEqualFold reports whether key equals the lower-case name when ASCII
// capitals are lower-cased. Header names are ASCII, so no other character
// folds.
func asciiEqualFold(key, name string) bool {
	if len(key) != len(name) {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != name[i] {
			return false
		}
	}
	return true
}

// NormalizeID strips white space and, when both are present, the "nca-"
// prefix and "-nca" suffix, so "nca-abc-nca" and "abc" match.
func NormalizeID(value string) string {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "nca-") || !strings.HasSuffix(value, "-nca") {
		return value
	}
	// The affixes can overlap, as in "nca-nca"; that id is empty.
	if len(value) < len("nca--nca") {
		return ""
	}
	return value[len("nca-") : len(value)-len("-nca")]
}

const (
	// ncaIDReason is the reason the NCA id rule's stub gives.
	ncaIDReason = "nca_id"
	// unresolvedReason is the reason of a stub strict mode writes for a
	// record on which some rule's header had no value.
	unresolvedReason = "header_unresolved"
)

// stub builds the record that replaces a suppressed one, giving reason as
// the cause. Copied keys keep this order and appear only when present; a
// wrapped record keeps only its timestamp. With ncaID set, it is the NCA id
// rule's stub: the earlier uploader's stub, with the matched id. Otherwise
// it keeps no header and no identifier.
func stub(record, event *jsonvalue.Value, reason, ncaID string) *jsonvalue.Value {
	headers := jsonvalue.NewObject()
	if ncaID != "" {
		headers = singleHeader(ncaID)
	}
	var safeEvent *jsonvalue.Value
	if payload := event.Get("payload"); payload != nil && payload.Kind() == jsonvalue.Object {
		safePayload := copyKeys(payload, "request_id", "endpoint", "model")
		safePayload.Set("payload_complete", jsonvalue.NewBool(false))
		safePayload.Set("payload_drop_reason", jsonvalue.NewString(reason))
		safePayload.Set("payload_suppressed", jsonvalue.NewBool(true))
		safePayload.Set("suppression_reason", jsonvalue.NewString(reason))
		if ncaID != "" {
			safePayload.Set("nca_id", jsonvalue.NewString(ncaID))
		}
		safePayload.Set("http_request_headers", headers)
		safeEvent = copyKeys(event, "schema", "event_type", "event_time_unix_ms", "event_source")
		safeEvent.Set("payload", safePayload)
	} else {
		safeEvent = copyKeys(event, "schema", "schema_version", "event_type", "event_time_unix_ms",
			"request_id", "requested_streaming", "model")
		safeEvent.Set("payload_suppressed", jsonvalue.NewBool(true))
		safeEvent.Set("suppression_reason", jsonvalue.NewString(reason))
		if ncaID != "" {
			safeEvent.Set("nca_id", jsonvalue.NewString(ncaID))
		}
		safeEvent.Set("headers", headers)
	}
	if record == event {
		return safeEvent
	}
	safeRecord := copyKeys(record, "timestamp")
	safeRecord.Set("event", safeEvent)
	return safeRecord
}

// copyKeys copies the named keys that hold a plain value. An object or a list
// is left out, so a stub never carries part of the body it replaces.
func copyKeys(from *jsonvalue.Value, keys ...string) *jsonvalue.Value {
	out := jsonvalue.NewObject()
	for _, key := range keys {
		if value := from.Get(key); value != nil && value.Kind() != jsonvalue.Object && value.Kind() != jsonvalue.Array {
			out.Set(key, value)
		}
	}
	return out
}

func singleHeader(id string) *jsonvalue.Value {
	headers := jsonvalue.NewObject()
	headers.Set("nvcf-ncaid", jsonvalue.NewString(id))
	return headers
}
