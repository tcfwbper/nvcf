// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Tunnel contract published by the oldest supported release.
//!
//! The frames were encoded by building stargate v0.19.2
//! (d12dd96dfbefb63c7c405baa0d1feba4e14f397b) with the values asserted below.
//! The schema declarations and string constants were copied from that tag.
//! v0.19.1 is identical. Do not regenerate these from the candidate: the
//! released contract is the fixture. Add fixtures for newer releases beside
//! these; retire a release's fixtures only after it leaves the support window
//! and every retained-release obligation for it has closed.

use std::collections::{BTreeMap, BTreeSet};

use capnp::message::TypedReader;
use capnp::traits::Owned;

use crate::protocol::{HandshakeAck, HandshakeRequest, QuicMessage};
use crate::quic_capnp;
use crate::tunnel_contract::REQUIRED_REQUEST_HEADERS;

const V0_19_2_HANDSHAKE_TOKEN: &str = concat!(
    "000000000600000000000000000002000500000042000000050000006a00000070796c6f6e2d6100",
    "776f726b65722d746f6b656e00000000",
);
const V0_19_2_HANDSHAKE_NO_TOKEN: &str =
    "000000000400000000000000000002000500000042000000000000000000000070796c6f6e2d6100";
const V0_19_2_HANDSHAKE_ACK_REJECTED: &str = concat!(
    "000000000500000000000000010001000000000000000000010000006a000000756e617574686f72",
    "697a656400000000",
);
const V0_19_2_HANDSHAKE_ACK_ACCEPTED: &str =
    "000000000400000000000000010001000100000000000000010000000a0000000000000000000000";
const V0_19_2_REQUEST_HEADERS: &str = concat!(
    "00000000260000000000000001000100000000000000000000000000000001000100000067000000",
    "18000000000002002d0000004a000000310000002a000000310000003a00000031000000aa000000",
    "390000006a0000003d0000008a000000450000006a00000049000000320000004900000042000000",
    "490000006a0000004d0000007a0000005100000012000000782d6d6574686f640000000000000000",
    "504f535400000000782d7061746800002f76312f636861742f636f6d706c6574696f6e7300000000",
    "636f6e74656e742d74797065000000006170706c69636174696f6e2f6a736f6e0000000000000000",
    "782d726571756573742d6964000000007265712d31000000782d6d6f64656c00636f6d7061742d6d",
    "6f64656c00000000782d696e7075742d746f6b656e7300003800000000000000",
);
const V0_19_2_RESPONSE_HEADERS: &str = concat!(
    "00000000110000000000000001000100000000000000000000000000000001000100000027000000",
    "08000000000002000d0000004a0000001100000022000000110000006a0000001500000092000000",
    "782d73746174757300000000000000003230300000000000636f6e74656e742d7479706500000000",
    "746578742f6576656e742d73747265616d00000000000000",
);
const V0_19_2_BODY: &str = concat!(
    "00000000060000000000000001000100010000000000000000000000000001000100000072000000",
    "646174613a205b444f4e455d0a0a0000",
);
const V0_19_2_TRAILERS: &str = concat!(
    "000000000a0000000000000001000100020000000000000000000000000001000100000017000000",
    "0400000000000200050000006200000009000000220000007374617475735f636f64650000000000",
    "3230300000000000",
);

/// Field declarations in v0.19.2 `quic.capnp`, keyed by enclosing struct.
const V0_19_2_SCHEMA_FILE_ID: &str = "@0xcccc1d2d6dc1a2ac;";
const V0_19_2_SCHEMA_FIELDS: &[(&str, &str)] = &[
    ("Message.union", "headers @0 :Map(Text, Text);"),
    ("Message.union", "body @1 :Body;"),
    ("Message.union", "trailers @2 :Map(Text, Text);"),
    ("Body", "content @0 :Data;"),
    ("Map", "entries @0 :List(Entry);"),
    ("Map.Entry", "key @0 :Key;"),
    ("Map.Entry", "value @1 :Value;"),
    ("Handshake", "inferenceServerId @0 :Text;"),
    ("Handshake", "authToken @1 :Text;"),
    ("HandshakeAck", "accepted @0 :Bool;"),
    ("HandshakeAck", "reason @1 :Text;"),
];

/// String constants in v0.19.2 `tunnel_contract.rs`.
const V0_19_2_TUNNEL_STRINGS: &[(&str, &str)] = &[
    ("HEADER_REQUEST_ID", "x-request-id"),
    ("HEADER_MODEL", "x-model"),
    ("HEADER_ROUTING_KEY", "x-routing-key"),
    ("HEADER_INPUT_TOKENS", "x-input-tokens"),
    ("HEADER_PRIORITY", "x-priority"),
    (
        "HEADER_STARGATE_EXPECTED_QUEUE_MS",
        "x-stargate-expected-queue-ms",
    ),
    (
        "HEADER_STARGATE_UPSTREAM_RETRYABLE",
        "x-stargate-upstream-retryable",
    ),
    ("HEADER_STARGATE_RETRYABLE", "x-stargate-retryable"),
    ("HEADER_STARGATE_RETRY_REASON", "x-stargate-retry-reason"),
    (
        "HEADER_STARGATE_RETRY_AFTER_MS",
        "x-stargate-retry-after-ms",
    ),
    (
        "RETRY_REASON_QUEUE_ESTIMATE_MISMATCH",
        "queue_estimate_mismatch",
    ),
    (
        "RETRY_REASON_UPSTREAM_ADMISSION_REJECTED",
        "upstream_admission_rejected",
    ),
    (
        "RETRY_REASON_CHAT_USAGE_REWRITE_SATURATED",
        "chat_usage_rewrite_saturated",
    ),
    ("HEADER_INFERENCE_SERVER_ID", "x-inference-server-id"),
    ("HEADER_REVERSE_AUTH_TOKEN", "x-stargate-auth-token"),
    ("WEBTRANSPORT_TUNNEL_PATH", "/_stargate/webtransport"),
];

/// Request headers that v0.19.2 Stargate always sends and v0.19.2 Pylon
/// requires. Stargate must keep sending them; Pylon must not require more.
const V0_19_2_REQUIRED_REQUEST_HEADERS: [&str; 3] = ["x-request-id", "x-model", "x-input-tokens"];

fn reader<T: Owned>(hex: &str) -> TypedReader<capnp::serialize::OwnedSegments, T> {
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
        .collect();
    let message =
        capnp::serialize::read_message(&mut bytes.as_slice(), capnp::message::ReaderOptions::new())
            .unwrap();
    TypedReader::new(message)
}

fn entries(message: QuicMessage) -> Vec<(String, String)> {
    match message {
        QuicMessage::Header(header) => header.entries,
        QuicMessage::Trailer(trailer) => trailer.entries,
        QuicMessage::Body(_) => panic!("expected headers or trailers"),
    }
}

fn pairs(values: &[(&str, &str)]) -> Vec<(String, String)> {
    values
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[test]
fn released_handshakes_decode() {
    let request = HandshakeRequest::from_reader(reader::<quic_capnp::handshake::Owned>(
        V0_19_2_HANDSHAKE_TOKEN,
    ))
    .unwrap();
    assert_eq!(request.inference_server_id, "pylon-a");
    assert_eq!(request.auth_token.as_deref(), Some("worker-token"));

    let request = HandshakeRequest::from_reader(reader::<quic_capnp::handshake::Owned>(
        V0_19_2_HANDSHAKE_NO_TOKEN,
    ))
    .unwrap();
    assert_eq!(request.inference_server_id, "pylon-a");
    assert_eq!(request.auth_token, None);

    let ack = HandshakeAck::from_reader(reader::<quic_capnp::handshake_ack::Owned>(
        V0_19_2_HANDSHAKE_ACK_REJECTED,
    ))
    .unwrap();
    assert!(!ack.accepted);
    assert_eq!(ack.reason, "unauthorized");

    let ack = HandshakeAck::from_reader(reader::<quic_capnp::handshake_ack::Owned>(
        V0_19_2_HANDSHAKE_ACK_ACCEPTED,
    ))
    .unwrap();
    assert!(ack.accepted);
    assert_eq!(ack.reason, "");
}

#[test]
fn released_stream_frames_decode() {
    let message =
        |hex| QuicMessage::from_reader(reader::<quic_capnp::message::Owned>(hex)).unwrap();
    assert_eq!(
        entries(message(V0_19_2_REQUEST_HEADERS)),
        pairs(&[
            ("x-method", "POST"),
            ("x-path", "/v1/chat/completions"),
            ("content-type", "application/json"),
            ("x-request-id", "req-1"),
            ("x-model", "compat-model"),
            ("x-input-tokens", "8"),
        ])
    );
    assert_eq!(
        entries(message(V0_19_2_RESPONSE_HEADERS)),
        pairs(&[("x-status", "200"), ("content-type", "text/event-stream")])
    );
    let QuicMessage::Body(body) = message(V0_19_2_BODY) else {
        panic!("expected a body frame");
    };
    assert_eq!(body.content.as_ref(), b"data: [DONE]\n\n");
    assert_eq!(
        entries(message(V0_19_2_TRAILERS)),
        pairs(&[("status_code", "200")])
    );
}

/// Returns `(enclosing struct path, normalized declaration)` for every field.
fn schema_fields(schema: &str) -> BTreeSet<(String, String)> {
    let mut scopes: Vec<String> = Vec::new();
    let mut fields = BTreeSet::new();
    for line in schema.lines() {
        let line = line.split('#').next().unwrap().trim();
        if let Some(rest) = line.strip_prefix("struct ") {
            let name = rest.split(['(', ' ', '{']).next().unwrap();
            scopes.push(name.to_owned());
        } else if line.starts_with("union") && line.ends_with('{') {
            scopes.push("union".to_owned());
        } else if line == "}" {
            scopes.pop();
        } else if line.contains(" @") && line.ends_with(';') {
            let normalized = line.split_whitespace().collect::<Vec<_>>().join(" ");
            fields.insert((scopes.join("."), normalized));
        }
    }
    fields
}

/// Every released field keeps its name, ordinal, type, and struct. Cap'n Proto
/// readers of either version then accept frames from the other, as long as new
/// fields only take new ordinals.
#[test]
fn released_capnp_fields_remain_declared() {
    let schema = include_str!("../quic.capnp");
    assert!(
        schema
            .lines()
            .any(|line| line.trim() == V0_19_2_SCHEMA_FILE_ID),
        "quic.capnp file ID changed"
    );
    let current = schema_fields(schema);
    for (scope, field) in V0_19_2_SCHEMA_FIELDS {
        assert!(
            current.contains(&(scope.to_string(), field.to_string())),
            "released tunnel field `{field}` in {scope} is missing or changed; \
             keep it and add new fields with new ordinals"
        );
    }
}

fn string_constants(source: &str) -> BTreeMap<String, String> {
    source
        .lines()
        .filter_map(|line| {
            let rest = line.trim().strip_prefix("pub const ")?;
            let (name, rest) = rest.split_once(": &str = \"")?;
            let value = rest.strip_suffix("\";")?;
            Some((value.to_owned(), name.to_owned()))
        })
        .collect()
}

/// Released header names, retry reasons, and paths keep their exact values.
/// Constants may be renamed in Rust; the wire strings may not change.
#[test]
fn released_tunnel_strings_remain_defined() {
    let current = string_constants(include_str!("tunnel_contract.rs"));
    for (name, value) in V0_19_2_TUNNEL_STRINGS {
        assert!(
            current.contains_key(*value),
            "released tunnel contract value {value:?} ({name}) is no longer defined"
        );
    }
    assert_eq!(
        REQUIRED_REQUEST_HEADERS, V0_19_2_REQUIRED_REQUEST_HEADERS,
        "required tunnel request headers must match released Stargate and Pylon"
    );
}

#[test]
fn schema_parser_tracks_nested_scopes() {
    let fields = schema_fields(
        "struct A {\n  union {\n    x  @0 :Text; # note\n  }\n}\nstruct M(K, V) {\n  struct E {\n    k @0 :K;\n  }\n}\n",
    );
    assert_eq!(
        fields,
        BTreeSet::from([
            ("A.union".to_owned(), "x @0 :Text;".to_owned()),
            ("M.E".to_owned(), "k @0 :K;".to_owned()),
        ])
    );
}
