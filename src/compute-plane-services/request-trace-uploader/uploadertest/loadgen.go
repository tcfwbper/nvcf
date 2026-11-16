// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package uploadertest

import (
	"bufio"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"

	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/request-trace-uploader/record"
)

// LoadProfile shapes a synthetic segment for benchmarks. Each request writes
// a request_payload and a request_end record; an agentic request also writes
// a tool_start and a tool_end per tool call. Text is pseudo-words drawn with
// a skewed frequency, so it compresses about as well as prose. Images are
// random bytes behind a PNG signature, so they compress as little as real
// images and redaction detects them.
type LoadProfile struct {
	// Seed makes the segment reproducible.
	Seed uint64
	// Uncompressed is the target size of the JSON lines before gzip. The
	// segment stops at the first request that reaches it.
	Uncompressed int64
	// LinesPerMember is how many lines go in one gzip member, as one writer
	// flush.
	LinesPerMember int
	// AgenticShare and MediaShare are the shares of requests that are
	// agentic (many turns and tool results) or carry inline images. The
	// rest are chat requests.
	AgenticShare float64
	MediaShare   float64
	// NCAIDs lists the NCA ids requests carry, chosen evenly. SuppressedIDs
	// is the leading part of it that a suppression rule names.
	NCAIDs        []string
	SuppressedIDs int
	// NCAHeader is the header that carries the NCA id.
	NCAHeader string
}

// DefaultLoadProfile is a 256 MiB segment: three in four requests are chat
// (about 4 to 40 KB), one in five agentic (about 50 to 500 KB), one in twenty
// carries one or two images (about 40 KB to 700 KB of base64 each), and four
// of twenty NCA ids are suppressed.
func DefaultLoadProfile() LoadProfile {
	ids := make([]string, 20)
	for i := range ids {
		ids[i] = fmt.Sprintf("nca-load-%02d", i)
	}
	return LoadProfile{
		Seed:           1,
		Uncompressed:   256 << 20,
		LinesPerMember: 64,
		AgenticShare:   0.20,
		MediaShare:     0.05,
		NCAIDs:         ids,
		SuppressedIDs:  4,
		NCAHeader:      "nvcf-ncaid",
	}
}

// Suppressed returns the NCA ids a suppression rule should name.
func (p LoadProfile) Suppressed() []string {
	return append([]string(nil), p.NCAIDs[:p.SuppressedIDs]...)
}

func (p LoadProfile) validate() error {
	switch {
	case p.Uncompressed <= 0:
		return errors.New("load profile needs a positive size")
	case p.LinesPerMember <= 0:
		return errors.New("load profile needs at least one line per member")
	case p.AgenticShare < 0 || p.MediaShare < 0 || p.AgenticShare+p.MediaShare > 1:
		return errors.New("load profile shares must be in [0, 1] and sum to at most 1")
	case len(p.NCAIDs) == 0 || p.SuppressedIDs < 0 || p.SuppressedIDs > len(p.NCAIDs):
		return errors.New("load profile needs NCA ids and a suppressed count within them")
	case p.NCAHeader == "":
		return errors.New("load profile needs an NCA header name")
	}
	return nil
}

// LoadStats describes a written segment.
type LoadStats struct {
	// Uncompressed counts the bytes of the JSON lines, newlines included.
	Uncompressed int64
	Lines        int
	Requests     int
	// Suppressed counts payload records whose NCA id is suppressed.
	Suppressed int
	// MediaParts counts inline images in requests that are not suppressed,
	// which redaction replaces. A suppressed request loses its body whole.
	MediaParts int
}

// WriteLoadSegment writes a gzip JSON-lines segment shaped by p to w. It
// streams, so a large segment does not need its size in memory.
func WriteLoadSegment(w io.Writer, p LoadProfile) (LoadStats, error) {
	if err := p.validate(); err != nil {
		return LoadStats{}, err
	}
	g := &loadGen{p: p, rng: rand.New(rand.NewPCG(p.Seed, p.Seed^0x9e3779b97f4a7c15))}
	g.words = vocabulary(g.rng)
	g.zipf = rand.NewZipf(g.rng, 1.1, 8, uint64(len(g.words)-1))
	out := bufio.NewWriterSize(w, 1<<20)
	var stats LoadStats
	var member []string
	flush := func() error {
		if len(member) == 0 {
			return nil
		}
		zw := gzip.NewWriter(out)
		for _, line := range member {
			if _, err := io.WriteString(zw, line); err != nil {
				return err
			}
			if _, err := zw.Write([]byte{'\n'}); err != nil {
				return err
			}
		}
		member = member[:0]
		return zw.Close()
	}
	for stats.Uncompressed < p.Uncompressed {
		lines, suppressed, media := g.request(stats.Requests)
		stats.Requests++
		if suppressed {
			stats.Suppressed++
		} else {
			stats.MediaParts += media
		}
		for _, line := range lines {
			stats.Uncompressed += int64(len(line)) + 1
			stats.Lines++
			member = append(member, line)
			if len(member) == p.LinesPerMember {
				if err := flush(); err != nil {
					return stats, fmt.Errorf("write load segment: %w", err)
				}
			}
		}
	}
	if err := flush(); err != nil {
		return stats, fmt.Errorf("write load segment: %w", err)
	}
	if err := out.Flush(); err != nil {
		return stats, fmt.Errorf("write load segment: %w", err)
	}
	return stats, nil
}

type loadGen struct {
	p     LoadProfile
	rng   *rand.Rand
	words []string
	zipf  *rand.Zipf
	clock uint64
}

// vocabulary returns pseudo-words of 2 to 10 letters.
func vocabulary(rng *rand.Rand) []string {
	const letters = "etaoinshrdlcumwfgypbvkjxqz"
	words := make([]string, 8192)
	for i := range words {
		n := 2 + rng.IntN(9)
		b := make([]byte, n)
		for j := range b {
			// Skew toward common letters, as text does.
			b[j] = letters[min(rng.IntN(len(letters)), rng.IntN(len(letters)))]
		}
		words[i] = string(b)
	}
	return words
}

// text returns about n bytes of words.
func (g *loadGen) text(n int) string {
	var sb strings.Builder
	sb.Grow(n + 16)
	for sb.Len() < n {
		if sb.Len() > 0 {
			if g.rng.IntN(14) == 0 {
				sb.WriteString(". ")
			} else {
				sb.WriteByte(' ')
			}
		}
		sb.WriteString(g.words[g.zipf.Uint64()])
	}
	return sb.String()
}

func (g *loadGen) between(lo, hi int) int { return lo + g.rng.IntN(hi-lo+1) }

// image returns a base64 data URL of n random bytes behind a PNG signature.
func (g *loadGen) image(n int) string {
	raw := make([]byte, n)
	copy(raw, "\x89PNG\r\n\x1a\n")
	for i := 8; i < n; i += 8 {
		v := g.rng.Uint64()
		for j := 0; j < 8 && i+j < n; j++ {
			raw[i+j] = byte(v >> (8 * j))
		}
	}
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(raw)
}

type chatMessage struct {
	Role       string         `json:"role"`
	Content    any            `json:"content,omitempty"`
	ToolCalls  []chatToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

type chatToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function chatToolFunction `json:"function"`
}

type chatToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type contentPart struct {
	Type     string         `json:"type"`
	Text     string         `json:"text,omitempty"`
	ImageURL *imageURLValue `json:"image_url,omitempty"`
}

type imageURLValue struct {
	URL string `json:"url"`
}

type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	MaxTokens   int           `json:"max_tokens"`
	Temperature float64       `json:"temperature"`
	Stream      bool          `json:"stream"`
}

type chatChoice struct {
	Index        int         `json:"index"`
	Message      chatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

type chatUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type chatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created uint64       `json:"created"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
	Usage   chatUsage    `json:"usage"`
}

// request returns the lines of one request, whether its NCA id is
// suppressed, and how many images it carries.
func (g *loadGen) request(n int) ([]string, bool, int) {
	g.clock += uint64(g.between(1, 40))
	timeMS := uint64(1767225600000) + g.clock
	requestID := fmt.Sprintf("req-%08d", n)
	idIndex := g.rng.IntN(len(g.p.NCAIDs))
	headers := map[string]string{
		g.p.NCAHeader:  g.p.NCAIDs[idIndex],
		"content-type": "application/json",
		"user-agent":   "load-generator/1.0",
	}
	kind := g.rng.Float64()
	messages := []chatMessage{{Role: "system", Content: g.text(g.between(1<<10, 6<<10))}}
	var tools []string
	media := 0
	switch {
	case kind < g.p.MediaShare:
		parts := []contentPart{{Type: "text", Text: g.text(g.between(200, 2<<10))}}
		media = g.between(1, 2)
		for range media {
			parts = append(parts, contentPart{Type: "image_url", ImageURL: &imageURLValue{URL: g.image(g.between(30<<10, 512<<10))}})
		}
		messages = append(messages, chatMessage{Role: "user", Content: parts})
	case kind < g.p.MediaShare+g.p.AgenticShare:
		messages = append(messages, chatMessage{Role: "user", Content: g.text(g.between(500, 4<<10))})
		for turn := range g.between(4, 20) {
			callID := fmt.Sprintf("call-%08d-%02d", n, turn)
			tools = append(tools, callID)
			messages = append(messages,
				chatMessage{Role: "assistant", ToolCalls: []chatToolCall{{
					ID: callID, Type: "function",
					Function: chatToolFunction{Name: "search", Arguments: fmt.Sprintf(`{"query":%q}`, g.text(g.between(20, 200)))},
				}}},
				chatMessage{Role: "tool", ToolCallID: callID, Content: g.text(g.between(1<<10, 24<<10))},
			)
		}
	default:
		for range g.between(0, 6) {
			messages = append(messages,
				chatMessage{Role: "user", Content: g.text(g.between(300, 4<<10))},
				chatMessage{Role: "assistant", Content: g.text(g.between(200, 4<<10))},
			)
		}
		messages = append(messages, chatMessage{Role: "user", Content: g.text(g.between(300, 12<<10))})
	}
	request, _ := json.Marshal(chatRequest{Model: fixtureModel, Messages: messages, MaxTokens: 4096, Temperature: 0.7})
	response, _ := json.Marshal(chatResponse{
		ID: "chatcmpl-" + requestID, Object: "chat.completion", Created: timeMS / 1000, Model: fixtureModel,
		Choices: []chatChoice{{Message: chatMessage{Role: "assistant", Content: g.text(g.between(200, 6<<10))}, FinishReason: "stop"}},
		Usage:   chatUsage{PromptTokens: len(request) / 4, CompletionTokens: 512, TotalTokens: len(request)/4 + 512},
	})
	payload := marshalLine(dynamoRecord{
		Schema:    record.Schema,
		EventType: string(record.EventRequestPayload),
		TimeMS:    timeMS,
		Source:    "dynamo",
		Payload: &dynamoPayload{
			RequestID: requestID,
			Endpoint:  "openai.chat_completion",
			Model:     fixtureModel,
			Request:   request,
			Response:  response,
			Headers:   headers,
			Complete:  true,
		},
	})
	lines := []string{Wrap(payload, g.clock)}
	session := fmt.Sprintf("session-%08d", n)
	for _, callID := range tools {
		lines = append(lines,
			Wrap(Tool(record.EventToolStart, session, callID, timeMS), g.clock),
			Wrap(Tool(record.EventToolEnd, session, callID, timeMS+12), g.clock),
		)
	}
	lines = append(lines, Wrap(RequestEnd(requestID, timeMS+25), g.clock))
	return lines, idIndex < g.p.SuppressedIDs, media
}
