/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FakeNatsToken is the only auth token FakeNatsServer accepts.
const FakeNatsToken = "token"

// NatsAction is how FakeNatsServer answers one client connection.
type NatsAction int

const (
	// NatsAccept accepts the CONNECT and keeps answering PINGs.
	NatsAccept NatsAction = iota
	// NatsAcceptThenDrop accepts the CONNECT, then closes the connection so the
	// client has to reconnect.
	NatsAcceptThenDrop
	// NatsRejectAuth answers the CONNECT with an authorization violation, as a
	// server does when its auth callout is unavailable.
	NatsRejectAuth
)

// FakeNatsAck is how FakeNatsServer answers every request: a JetStream publish
// ack, which a core NATS request also accepts as a reply.
const FakeNatsAck = `{"stream":"fake","seq":1}`

// FakeNatsServer speaks just enough of the NATS client protocol to accept or
// reject a CONNECT, record what clients publish, and answer requests. Connection
// i follows script[i]; connections past the end of the script are accepted. A
// CONNECT without FakeNatsToken is always rejected.
type FakeNatsServer struct {
	URL      string
	listener net.Listener

	mu        sync.Mutex
	conns     []net.Conn
	closed    bool
	refusing  bool
	published []string
	discard   bool
	withheld  map[string]bool
}

func NewFakeNatsServer(script ...NatsAction) (*FakeNatsServer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &FakeNatsServer{URL: "nats://" + listener.Addr().String(), listener: listener}
	go func() {
		for i := 0; ; i++ {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			refused, closed := s.track(conn)
			if closed {
				return
			}
			if refused {
				continue
			}
			action := NatsAccept
			if i < len(script) {
				action = script[i]
			}
			go s.serve(conn, action)
		}
	}()
	return s, nil
}

func (s *FakeNatsServer) track(conn net.Conn) (refused, closed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.refusing {
		_ = conn.Close()
		return s.refusing, s.closed
	}
	s.conns = append(s.conns, conn)
	return false, false
}

// Outage drops every open client connection and closes each new one at once,
// so clients stay reconnecting until EndOutage.
func (s *FakeNatsServer) Outage() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refusing = true
	for _, conn := range s.conns {
		_ = conn.Close()
	}
	s.conns = nil
}

// EndOutage accepts connections again.
func (s *FakeNatsServer) EndOutage() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refusing = false
}

// WithholdReplies stops the server answering requests published to subject,
// as a JetStream publish whose ack never comes. It still records them.
func (s *FakeNatsServer) WithholdReplies(subject string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.withheld == nil {
		s.withheld = map[string]bool{}
	}
	s.withheld[subject] = true
}

func (s *FakeNatsServer) answers(subject string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.withheld[subject]
}

// Published returns the subjects clients have published to, in the order the
// server received them.
func (s *FakeNatsServer) Published() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.published...)
}

// DiscardPublished stops recording publishes, for benchmarks that never read
// them.
func (s *FakeNatsServer) DiscardPublished() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.discard = true
}

func (s *FakeNatsServer) recordPublish(subject string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.discard {
		s.published = append(s.published, subject)
	}
}

// Close stops listening and drops every open client connection, so a client
// can neither stay connected nor reconnect.
func (s *FakeNatsServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for _, conn := range s.conns {
		_ = conn.Close()
	}
	return s.listener.Close()
}

func (s *FakeNatsServer) serve(conn net.Conn, action NatsAction) {
	defer conn.Close()
	info := `INFO {"server_id":"fake","version":"2.10.0","proto":1,"headers":true,"max_payload":1048576,"auth_required":true}` + "\r\n"
	if _, err := io.WriteString(conn, info); err != nil {
		return
	}
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	authorized := false
	// Subscription IDs by subject, for answering requests, and the subjects of
	// subscriptions that end after one message.
	subs := map[string]string{}
	subjects := map[string]string{}
	once := map[string]bool{}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		verb, args, _ := strings.Cut(line, " ")
		switch strings.ToUpper(verb) {
		case "CONNECT":
			var options struct {
				AuthToken string `json:"auth_token"`
			}
			authorized = json.Unmarshal([]byte(args), &options) == nil && options.AuthToken == FakeNatsToken
		case "SUB":
			fields := strings.Fields(args)
			if len(fields) >= 2 {
				sid := fields[len(fields)-1]
				subs[fields[0]] = sid
				subjects[sid] = fields[0]
			}
		case "UNSUB":
			fields := strings.Fields(args)
			if len(fields) == 0 {
				continue
			}
			if len(fields) == 2 && fields[1] == "1" {
				once[fields[0]] = true
				continue
			}
			delete(subs, subjects[fields[0]])
			delete(subjects, fields[0])
			delete(once, fields[0])
		case "PUB", "HPUB":
			fields := strings.Fields(args)
			if len(fields) < 2 {
				return
			}
			size, err := strconv.Atoi(fields[len(fields)-1])
			if err != nil {
				return
			}
			if _, err := io.ReadFull(reader, make([]byte, size+2)); err != nil {
				return
			}
			s.recordPublish(fields[0])
			reply := ""
			if (verb == "PUB" && len(fields) == 3) || (verb == "HPUB" && len(fields) == 4) {
				reply = fields[1]
			}
			if sid, ok := matchSubscription(subs, reply); ok && s.answers(fields[0]) {
				_, _ = fmt.Fprintf(writer, "MSG %s %s %d\r\n%s\r\n", reply, sid, len(FakeNatsAck), FakeNatsAck)
				if writer.Flush() != nil {
					return
				}
				if once[sid] {
					delete(subs, subjects[sid])
					delete(subjects, sid)
					delete(once, sid)
				}
			}
		case "PING":
			if !authorized {
				action = NatsRejectAuth
			}
			switch action {
			case NatsRejectAuth:
				_, _ = io.WriteString(conn, "-ERR 'Authorization Violation'\r\n")
				return
			case NatsAcceptThenDrop:
				_, _ = io.WriteString(conn, "PONG\r\n")
				// Let the client finish connecting before the drop.
				time.Sleep(50 * time.Millisecond)
				return
			default:
				_, _ = io.WriteString(conn, "PONG\r\n")
			}
		}
	}
}

func matchSubscription(subs map[string]string, subject string) (string, bool) {
	if subject == "" {
		return "", false
	}
	if sid, ok := subs[subject]; ok {
		return sid, true
	}
	for pattern, sid := range subs {
		if subjectMatches(pattern, subject) {
			return sid, true
		}
	}
	return "", false
}

func subjectMatches(pattern, subject string) bool {
	patternTokens := strings.Split(pattern, ".")
	subjectTokens := strings.Split(subject, ".")
	for i, token := range patternTokens {
		if token == ">" {
			return len(subjectTokens) > i
		}
		if i >= len(subjectTokens) || (token != "*" && token != subjectTokens[i]) {
			return false
		}
	}
	return len(patternTokens) == len(subjectTokens)
}
