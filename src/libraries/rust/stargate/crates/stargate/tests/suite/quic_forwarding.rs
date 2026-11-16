// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::common::sse::{assert_sse_done, chat_completion_contents, parse_sse_events};
use crate::common::{
    MapResolver, base_config, init_crypto, localhost_reverse_tunnel_config,
    reverse_registration_config, start_dummy_backend, wait_for_routing, wait_for_unroutable,
    with_proxy_headers,
};
use axum::Router;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use pylon_lib::{
    InferenceServerRegistrationClient, PylonRuntimeState, ReverseQuicTunnelConfig,
    ReverseQuicTunnelHandle, TunnelTransportProtocol, start_reverse_quic_tunnel,
};
use stargate::runtime::{BoundStargateListeners, ReverseTunnelConfig};
use stargate_forwarding::ForwardingResolver;
use stargate_proto::pb::stargate_control_plane_client::StargateControlPlaneClient;
use stargate_proto::pb::{
    InferenceServerModelRegistration, InferenceServerRegistration, InferenceServerStatus,
    ModelStats, StargateInfo,
};

fn make_registration(
    id: &str,
    model: &str,
    inference_server_url: String,
    status: InferenceServerStatus,
) -> InferenceServerRegistration {
    let mut models = std::collections::HashMap::new();
    models.insert(
        model.to_string(),
        InferenceServerModelRegistration {
            stats: Some(ModelStats {
                output_tps: 0.0,
                last_mean_input_tps: 100.0,
                max_output_tps: 100.0,
                queue_size: 0,
                queued_input_size: 0,
                kv_cache_capacity_tokens: 0,
                kv_cache_used_tokens: 0,
                kv_cache_free_tokens: 0,
                num_running_queries: 0,
                max_engine_concurrency: 0,
                total_query_input_size: 0,
                queue_time_estimate_ms_by_priority: std::collections::HashMap::new(),
                ..ModelStats::default()
            }),
            status: status.into(),
        },
    );
    InferenceServerRegistration {
        inference_server_id: id.to_string(),
        cluster_id: String::new(),
        inference_server_url,
        models,
        reverse_tunnel: true,
    }
}

struct TwoStargatesQuic {
    grpc_a: SocketAddr,
    http_a: SocketAddr,
    grpc_b: SocketAddr,
    http_b: SocketAddr,
    reverse_a: SocketAddr,
    reverse_b: SocketAddr,
    handle_a: stargate::runtime::StargateHandle,
    handle_b: stargate::runtime::StargateHandle,
}

impl TwoStargatesQuic {
    async fn shutdown(self) {
        self.handle_a.begin_shutdown();
        self.handle_b.begin_shutdown();
        self.handle_a
            .wait_for_shutdown(Duration::from_secs(5))
            .await;
        self.handle_b
            .wait_for_shutdown(Duration::from_secs(5))
            .await;
    }
}

/// Spins up two stargates with reverse tunnel + development-only QUIC peer
/// relay coverage.
/// Resolver on A maps "sg-b.stargate.external" -> B's reverse tunnel addr.
/// Resolver on B maps "sg-a.stargate.external" -> A's reverse tunnel addr.
async fn start_two_stargates_with_quic() -> TwoStargatesQuic {
    start_two_stargates_with_quic_tls(None, None, true, TunnelTransportProtocol::RawQuic).await
}

async fn start_two_stargates_with_quic_tls(
    tls_cert_pem: Option<Vec<u8>>,
    tls_key_pem: Option<Vec<u8>>,
    quic_insecure: bool,
    tunnel_protocol: TunnelTransportProtocol,
) -> TwoStargatesQuic {
    let peers = Arc::new(Mutex::new(Vec::<StargateInfo>::new()));

    let server_tls_identity =
        stargate_tls::ServerTlsIdentity::from_optional_pem(tls_cert_pem.clone(), tls_key_pem)
            .expect("test TLS cert/key pair should be complete");

    let mut config_a = base_config(
        "sg-a",
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
    );
    config_a.dns_poll_interval = Duration::from_secs(1);
    let reverse_tunnel_a = localhost_reverse_tunnel_config("127.0.0.1:0".parse().unwrap());
    // Exercise both secure and insecure QUIC relay modes from the same topology helper.
    config_a.proxy_transport.quic.tls_cert_pem = tls_cert_pem.clone();
    config_a.proxy_transport.quic.server_tls_identity = server_tls_identity.clone();
    config_a.proxy_transport.quic.quic_insecure = quic_insecure;
    config_a.proxy_transport.quic.tunnel_protocol = tunnel_protocol;
    let listeners_a = BoundStargateListeners::bind(&mut config_a).unwrap();
    let grpc_a = config_a.grpc_listen_addr;
    let http_a = config_a.http_listen_addr;
    let reverse_a = reverse_tunnel_a.listen_addr();

    let mut config_b = base_config(
        "sg-b",
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
    );
    config_b.dns_poll_interval = Duration::from_secs(1);
    let reverse_tunnel_b = localhost_reverse_tunnel_config("127.0.0.1:0".parse().unwrap());
    // Exercise both secure and insecure QUIC relay modes from the same topology helper.
    config_b.proxy_transport.quic.tls_cert_pem = tls_cert_pem;
    config_b.proxy_transport.quic.server_tls_identity = server_tls_identity;
    config_b.proxy_transport.quic.quic_insecure = quic_insecure;
    config_b.proxy_transport.quic.tunnel_protocol = tunnel_protocol;
    let listeners_b = BoundStargateListeners::bind(&mut config_b).unwrap();
    let grpc_b = config_b.grpc_listen_addr;
    let http_b = config_b.http_listen_addr;
    let reverse_b = reverse_tunnel_b.listen_addr();

    // QUIC forwarding resolvers match against *.stargate.external SNI
    let mut resolver_a = MapResolver::new("sg-a.stargate.external");
    resolver_a.insert("sg-b.stargate.external", reverse_b);
    let resolver_a: Arc<dyn ForwardingResolver> = Arc::new(resolver_a);

    let mut resolver_b = MapResolver::new("sg-b.stargate.external");
    resolver_b.insert("sg-a.stargate.external", reverse_a);
    let resolver_b: Arc<dyn ForwardingResolver> = Arc::new(resolver_b);

    config_a.forwarding = Some(resolver_a);
    let discovery_a = crate::common::SharedDiscovery::new("sg-a", grpc_a, http_a, peers.clone());
    let runtime_a = stargate::runtime::StargateRuntime::new(
        config_a,
        Box::new(discovery_a),
        listeners_a,
        Some(reverse_tunnel_a),
    );

    config_b.forwarding = Some(resolver_b);
    let discovery_b = crate::common::SharedDiscovery::new("sg-b", grpc_b, http_b, peers.clone());
    let runtime_b = stargate::runtime::StargateRuntime::new(
        config_b,
        Box::new(discovery_b),
        listeners_b,
        Some(reverse_tunnel_b),
    );

    let handle_a = runtime_a.start().await.expect("stargate A failed");
    let handle_b = runtime_b.start().await.expect("stargate B failed");

    crate::common::wait_for_healthy(http_a, Duration::from_secs(5)).await;
    crate::common::wait_for_healthy(http_b, Duration::from_secs(5)).await;

    TwoStargatesQuic {
        grpc_a,
        http_a,
        grpc_b,
        http_b,
        reverse_a,
        reverse_b,
        handle_a,
        handle_b,
    }
}

struct RegistrationHandle {
    _task: tokio::task::JoinHandle<()>,
    _stop: tokio::sync::watch::Sender<bool>,
}

#[derive(Clone)]
struct ReverseTunnelDialOptions {
    tls_cert_pem: Option<Vec<u8>>,
    quic_insecure: bool,
    tunnel_protocol: TunnelTransportProtocol,
}

impl Default for ReverseTunnelDialOptions {
    fn default() -> Self {
        Self {
            tls_cert_pem: None,
            quic_insecure: true,
            tunnel_protocol: TunnelTransportProtocol::RawQuic,
        }
    }
}

/// Register a backend directly on a stargate via gRPC, using reverse_tunnel flag.
/// Spawns a background task that sends periodic heartbeats so the
/// ConnectionWatcher re-checks for the reverse tunnel connection.
async fn register_reverse_backend(
    grpc_addr: SocketAddr,
    id: &str,
    model: &str,
    upstream_url: String,
) -> RegistrationHandle {
    let channel = tonic::transport::Channel::from_shared(format!("http://{grpc_addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = StargateControlPlaneClient::new(channel);

    let (tx, rx) = flume::bounded(16);
    let reg = make_registration(id, model, upstream_url, InferenceServerStatus::Active);
    tx.send_async(reg.clone()).await.unwrap();

    let _resp = client
        .register_inference_server(rx.into_stream())
        .await
        .expect("register failed");

    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        let _client = client;
        let mut heartbeat = tokio::time::interval(Duration::from_millis(200));
        loop {
            tokio::select! {
                _ = stop_rx.changed() => break,
                _ = heartbeat.tick() => {
                    if tx.send_async(reg.clone()).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    RegistrationHandle {
        _task: task,
        _stop: stop_tx,
    }
}

async fn connect_reverse_tunnel_with_sni_retry(
    connect_addr: SocketAddr,
    sni: &str,
    inference_server_id: &str,
    upstream_url: &str,
    timeout: Duration,
) -> ReverseQuicTunnelHandle {
    connect_reverse_tunnel_with_sni_retry_tls(
        connect_addr,
        sni,
        inference_server_id,
        upstream_url,
        timeout,
        ReverseTunnelDialOptions::default(),
    )
    .await
}

async fn connect_reverse_tunnel_with_sni_retry_tls(
    connect_addr: SocketAddr,
    sni: &str,
    inference_server_id: &str,
    upstream_url: &str,
    timeout: Duration,
    options: ReverseTunnelDialOptions,
) -> ReverseQuicTunnelHandle {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut poll = tokio::time::interval(Duration::from_millis(50));
    loop {
        let mut config = ReverseQuicTunnelConfig::new(
            format!("127.0.0.1:{}", connect_addr.port()),
            inference_server_id.to_string(),
            upstream_url.to_string(),
        );
        config.sni_override = Some(sni.to_string());
        config.tls_cert_pem = options.tls_cert_pem.clone();
        config.quic_insecure = options.quic_insecure;
        config.tunnel_protocol = options.tunnel_protocol;
        match start_reverse_quic_tunnel(config).await {
            Ok(handle) => return handle,
            Err(_) if tokio::time::Instant::now() < deadline => {
                poll.tick().await;
            }
            Err(e) => panic!(
                "reverse tunnel for '{}' failed after {}s: {e}",
                inference_server_id,
                timeout.as_secs()
            ),
        }
    }
}

struct ReverseRouteCase<'a> {
    grpc: SocketAddr,
    reverse: SocketAddr,
    http: SocketAddr,
    sni: &'a str,
    backend_id: &'a str,
    model: &'a str,
    dial: ReverseTunnelDialOptions,
}

async fn assert_reverse_route(case: ReverseRouteCase<'_>) {
    let backend_addr = start_dummy_backend(case.model).await;
    let backend_url = format!("http://{backend_addr}");
    let registration =
        register_reverse_backend(case.grpc, case.backend_id, case.model, backend_url.clone()).await;
    let tunnel = connect_reverse_tunnel_with_sni_retry_tls(
        case.reverse,
        case.sni,
        case.backend_id,
        &backend_url,
        Duration::from_secs(5),
        case.dial,
    )
    .await;

    wait_for_routing(case.http, case.model, Duration::from_secs(15)).await;
    tunnel.shutdown().await;
    // Close the gRPC registration stream before stargate shutdown so
    // tonic's graceful shutdown doesn't block on in-flight RPCs.
    drop(registration);
}

/// Reverse tunnel connected to A with SNI for B is relayed to B.
/// Backend registered directly on B becomes routable through B.
#[tokio::test]
async fn reverse_tunnel_relayed_to_correct_peer() {
    init_crypto();
    let sg = start_two_stargates_with_quic().await;
    assert_reverse_route(ReverseRouteCase {
        grpc: sg.grpc_b,
        reverse: sg.reverse_a,
        http: sg.http_b,
        sni: "sg-b.stargate.external",
        backend_id: "relay-backend",
        model: "relay-model",
        dial: ReverseTunnelDialOptions::default(),
    })
    .await;
    sg.shutdown().await;
}

/// HTTP/3 reverse tunnels connected to the wrong pod must still work after the
/// QUIC relay forwards the connection to the advertised peer pod.
#[tokio::test]
async fn reverse_http3_tunnel_relayed_to_correct_peer() {
    init_crypto();
    let sg =
        start_two_stargates_with_quic_tls(None, None, true, TunnelTransportProtocol::Http3).await;
    assert_reverse_route(ReverseRouteCase {
        grpc: sg.grpc_b,
        reverse: sg.reverse_a,
        http: sg.http_b,
        sni: "sg-b.stargate.external",
        backend_id: "relay-http3-backend",
        model: "relay-http3-model",
        dial: ReverseTunnelDialOptions {
            tunnel_protocol: TunnelTransportProtocol::Http3,
            ..ReverseTunnelDialOptions::default()
        },
    })
    .await;
    sg.shutdown().await;
}

/// Verified QUIC relays must dial the peer pod address while preserving the
/// original advertised SNI as the TLS server name.
#[tokio::test]
async fn reverse_tunnel_relay_preserves_peer_sni_for_verified_quic() {
    init_crypto();
    let (cert_pem, key_pem) = stargate_tls::generate_self_signed_cert_for_names(vec![
        "sg-a.stargate.external".to_string(),
        "sg-b.stargate.external".to_string(),
    ])
    .expect("test cert should be generated");
    let sg = start_two_stargates_with_quic_tls(
        Some(cert_pem.clone()),
        Some(key_pem),
        false,
        TunnelTransportProtocol::RawQuic,
    )
    .await;

    assert_reverse_route(ReverseRouteCase {
        grpc: sg.grpc_b,
        reverse: sg.reverse_a,
        http: sg.http_b,
        sni: "sg-b.stargate.external",
        backend_id: "relay-secure-backend",
        model: "relay-secure-model",
        dial: ReverseTunnelDialOptions {
            tls_cert_pem: Some(cert_pem),
            quic_insecure: false,
            tunnel_protocol: TunnelTransportProtocol::RawQuic,
        },
    })
    .await;
    sg.shutdown().await;
}

/// Reverse tunnel connected to B with SNI for A is relayed to A.
/// Symmetric counterpart to `reverse_tunnel_relayed_to_correct_peer`.
#[tokio::test]
async fn reverse_tunnel_relayed_b_to_a() {
    init_crypto();
    let sg = start_two_stargates_with_quic().await;

    assert_reverse_route(ReverseRouteCase {
        grpc: sg.grpc_a,
        reverse: sg.reverse_b,
        http: sg.http_a,
        sni: "sg-a.stargate.external",
        backend_id: "b2a-backend",
        model: "b2a-model",
        dial: ReverseTunnelDialOptions::default(),
    })
    .await;
    sg.shutdown().await;
}

/// Reverse tunnel connected to A with SNI matching A is handled locally.
#[tokio::test]
async fn reverse_tunnel_for_self_handled_locally() {
    init_crypto();
    let sg = start_two_stargates_with_quic().await;

    assert_reverse_route(ReverseRouteCase {
        grpc: sg.grpc_a,
        reverse: sg.reverse_a,
        http: sg.http_a,
        sni: "sg-a.stargate.external",
        backend_id: "local-backend",
        model: "local-model",
        dial: ReverseTunnelDialOptions::default(),
    })
    .await;
    sg.shutdown().await;
}

/// Duplicate reverse tunnel connection for same inference_server_id is rejected.
#[tokio::test]
async fn duplicate_reverse_tunnel_rejected() {
    init_crypto();
    let sg = start_two_stargates_with_quic().await;

    let backend_addr = start_dummy_backend("dup-model").await;
    let backend_url = format!("http://{backend_addr}");
    let reg =
        register_reverse_backend(sg.grpc_a, "dup-backend", "dup-model", backend_url.clone()).await;

    let tunnel1 = connect_reverse_tunnel_with_sni_retry(
        sg.reverse_a,
        "sg-a.stargate.external",
        "dup-backend",
        &backend_url,
        Duration::from_secs(5),
    )
    .await;

    wait_for_routing(sg.http_a, "dup-model", Duration::from_secs(15)).await;

    // Second tunnel for same ID should be rejected
    let config2 = {
        let mut c = ReverseQuicTunnelConfig::new(
            format!("127.0.0.1:{}", sg.reverse_a.port()),
            "dup-backend".to_string(),
            backend_url.clone(),
        );
        c.sni_override = Some("sg-a.stargate.external".to_string());
        c.quic_insecure = true;
        c
    };
    let result = start_reverse_quic_tunnel(config2).await;
    assert!(result.is_err(), "duplicate tunnel should be rejected");

    // First tunnel is still functional
    wait_for_routing(sg.http_a, "dup-model", Duration::from_secs(5)).await;

    // Drop first, then new connection should succeed
    tunnel1.shutdown().await;

    let tunnel3 = connect_reverse_tunnel_with_sni_retry(
        sg.reverse_a,
        "sg-a.stargate.external",
        "dup-backend",
        &backend_url,
        Duration::from_secs(5),
    )
    .await;

    wait_for_routing(sg.http_a, "dup-model", Duration::from_secs(15)).await;

    tunnel3.shutdown().await;
    // Close the gRPC registration stream before stargate shutdown so
    // tonic's graceful shutdown doesn't block on in-flight RPCs.
    drop(reg);
    sg.shutdown().await;
}

/// Relayed tunnel disconnect removes backend from routing on target stargate.
#[tokio::test]
async fn relayed_tunnel_disconnect_cleans_up_routing() {
    init_crypto();
    let sg = start_two_stargates_with_quic().await;

    let backend_addr = start_dummy_backend("dc-model").await;
    let backend_url = format!("http://{backend_addr}");
    let reg =
        register_reverse_backend(sg.grpc_b, "dc-backend", "dc-model", backend_url.clone()).await;

    let tunnel = connect_reverse_tunnel_with_sni_retry(
        sg.reverse_a,
        "sg-b.stargate.external",
        "dc-backend",
        &backend_url,
        Duration::from_secs(5),
    )
    .await;

    wait_for_routing(sg.http_b, "dc-model", Duration::from_secs(15)).await;

    tunnel.shutdown().await;
    // Close the gRPC registration stream so the backend is fully
    // deregistered and tonic's graceful shutdown doesn't block.
    drop(reg);

    wait_for_unroutable(sg.http_b, "dc-model", Duration::from_secs(15)).await;

    sg.shutdown().await;
}

/// Reverse tunnel relayed from A to B for an inference_server_id that has no
/// gRPC registration on B gets a handshake NACK.
#[tokio::test]
async fn reverse_tunnel_unregistered_id_rejected_through_relay() {
    init_crypto();
    let sg = start_two_stargates_with_quic().await;

    let backend_addr = start_dummy_backend("unreg-model").await;

    // Connect reverse tunnel to A with SNI for B, but do NOT register the
    // backend on B via gRPC. The relay should reach B, but B's handshake
    // should NACK because "unreg-backend" is unknown.
    let config = {
        let mut c = ReverseQuicTunnelConfig::new(
            format!("127.0.0.1:{}", sg.reverse_a.port()),
            "unreg-backend".to_string(),
            format!("http://{backend_addr}"),
        );
        c.sni_override = Some("sg-b.stargate.external".to_string());
        c.quic_insecure = true;
        c
    };
    let result = start_reverse_quic_tunnel(config).await;
    assert!(
        result.is_err(),
        "unregistered backend through relay should fail handshake"
    );

    sg.shutdown().await;
}

/// Relay to an unreachable peer address fails gracefully without panics.
/// The forwarding resolver returns a syntactically valid but unreachable
/// address for the peer.
#[tokio::test]
async fn relayed_tunnel_peer_unreachable() {
    init_crypto();

    let peers = Arc::new(Mutex::new(Vec::<StargateInfo>::new()));

    let mut config_a = base_config(
        "sg-a",
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
    );
    config_a.dns_poll_interval = Duration::from_secs(1);
    let reverse_tunnel_a = localhost_reverse_tunnel_config("127.0.0.1:0".parse().unwrap());
    let listeners_a = BoundStargateListeners::bind(&mut config_a).unwrap();
    let grpc_a = config_a.grpc_listen_addr;
    let http_a = config_a.http_listen_addr;
    let reverse_a = reverse_tunnel_a.listen_addr();

    // Point "sg-unreachable.stargate.external" at an address that refuses connections
    let unreachable_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let mut resolver_a = MapResolver::new("sg-a.stargate.external");
    resolver_a.insert("sg-unreachable.stargate.external", unreachable_addr);
    let resolver_a: Arc<dyn ForwardingResolver> = Arc::new(resolver_a);

    config_a.forwarding = Some(resolver_a);
    let discovery_a = crate::common::SharedDiscovery::new("sg-a", grpc_a, http_a, peers.clone());
    let runtime_a = stargate::runtime::StargateRuntime::new(
        config_a,
        Box::new(discovery_a),
        listeners_a,
        Some(reverse_tunnel_a),
    );

    let handle_a = runtime_a.start().await.expect("stargate A failed");

    crate::common::wait_for_healthy(http_a, Duration::from_secs(5)).await;

    let backend_addr = start_dummy_backend("unreachable-model").await;

    let config = {
        let mut c = ReverseQuicTunnelConfig::new(
            format!("127.0.0.1:{}", reverse_a.port()),
            "unreachable-backend".to_string(),
            format!("http://{backend_addr}"),
        );
        c.sni_override = Some("sg-unreachable.stargate.external".to_string());
        c.quic_insecure = true;
        c
    };
    let result = start_reverse_quic_tunnel(config).await;
    assert!(result.is_err(), "tunnel to unreachable peer should fail");

    handle_a.begin_shutdown();
    handle_a.wait_for_shutdown(Duration::from_secs(5)).await;
}

/// Chat backend that answers every streaming request at once with `events`
/// content chunks of `chunk_len` bytes each.
async fn start_large_sse_backend(model: &str, events: usize, chunk_len: usize) -> SocketAddr {
    let chunk = serde_json::json!({
        "id": "chunk-1",
        "object": "chat.completion.chunk",
        "model": model,
        "choices": [{"index": 0, "delta": {"content": "x".repeat(chunk_len)}, "finish_reason": null}],
    });
    let body = format!("data: {chunk}\n\n").repeat(events) + "data: [DONE]\n\n";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route(
            "/v1/chat/completions",
            post(move || {
                let body = body.clone();
                async move { ([(header::CONTENT_TYPE, "text/event-stream")], body).into_response() }
            }),
        )
        .route("/health", get(|| async { "ok" }));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

/// Regression: a relay in front of the router acknowledges response bytes
/// before it passes them on, and drops them when Pylon closes the tunnel.
#[tokio::test]
async fn reverse_tunnel_drain_through_relay_delivers_full_response() {
    const EVENTS: usize = 3_000;
    const CHUNK_LEN: usize = 1_000;
    // Longer than the 2 s accept window and shorter than it plus the final
    // tunnel wait. The relay holds the response tail until the client reads.
    const CLIENT_PAUSE: Duration = Duration::from_millis(2_500);

    init_crypto();
    let peers = Arc::new(Mutex::new(Vec::<StargateInfo>::new()));

    // A is only the relay: Pylon dials it, and it forwards the tunnel by SNI to B.
    let mut config_a = base_config(
        "sg-a",
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
    );
    let reverse_tunnel_a = localhost_reverse_tunnel_config("127.0.0.1:0".parse().unwrap());
    let listeners_a = BoundStargateListeners::bind(&mut config_a).unwrap();
    let (grpc_a, http_a) = (config_a.grpc_listen_addr, config_a.http_listen_addr);
    let reverse_a = reverse_tunnel_a.listen_addr();

    let mut config_b = base_config(
        "sg-b",
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
    );
    let reverse_tunnel_b = ReverseTunnelConfig::bind(
        "127.0.0.1:0".parse().unwrap(),
        "sg-b.stargate.external".to_string(),
        Some(reverse_a.to_string()),
        Duration::from_secs(10),
    )
    .unwrap();
    let listeners_b = BoundStargateListeners::bind(&mut config_b).unwrap();
    let (grpc_b, http_b) = (config_b.grpc_listen_addr, config_b.http_listen_addr);

    let mut resolver_a = MapResolver::new("sg-a.stargate.external");
    resolver_a.insert("sg-b.stargate.external", reverse_tunnel_b.listen_addr());
    config_a.forwarding = Some(Arc::new(resolver_a));
    config_b.forwarding = Some(Arc::new(MapResolver::new("sg-b.stargate.external")));

    let handle_a = stargate::runtime::StargateRuntime::new(
        config_a,
        Box::new(crate::common::SharedDiscovery::new(
            "sg-a",
            grpc_a,
            http_a,
            peers.clone(),
        )),
        listeners_a,
        Some(reverse_tunnel_a),
    )
    .start()
    .await
    .expect("stargate A failed");
    let handle_b = stargate::runtime::StargateRuntime::new(
        config_b,
        Box::new(crate::common::SharedDiscovery::new(
            "sg-b", grpc_b, http_b, peers,
        )),
        listeners_b,
        Some(reverse_tunnel_b),
    )
    .start()
    .await
    .expect("stargate B failed");

    let model = "relay-drain-model";
    let backend_addr = start_large_sse_backend(model, EVENTS, CHUNK_LEN).await;
    let mut registration = InferenceServerRegistrationClient::default();
    registration
        .start(reverse_registration_config(
            vec![grpc_b.to_string()],
            "relay-drain-inst",
            format!("http://{backend_addr}"),
            PylonRuntimeState::new(InferenceServerStatus::Active, &[model.to_string()]),
        ))
        .expect("registration should start");
    wait_for_routing(http_b, model, Duration::from_secs(15)).await;

    let response = with_proxy_headers(
        reqwest::Client::new().post(format!("http://{http_b}/v1/chat/completions")),
        model,
        "req-relay-drain",
    )
    .json(&serde_json::json!({
        "model": model,
        "messages": [{"role": "user", "content": "hi"}],
        "stream": true,
    }))
    .send()
    .await
    .expect("request should reach stargate B");
    assert_eq!(response.status(), StatusCode::OK);

    let shutdown = tokio::spawn(async move { registration.shutdown().await });
    tokio::time::sleep(CLIENT_PAUSE).await;
    let body = response
        .text()
        .await
        .expect("response body should complete");
    shutdown.await.unwrap();

    let events = parse_sse_events(&body).expect("response should be valid SSE");
    assert_sse_done(&events);
    let delivered: usize = chat_completion_contents(&events)
        .iter()
        .map(String::len)
        .sum();
    assert_eq!(delivered, EVENTS * CHUNK_LEN);

    handle_a.begin_shutdown();
    handle_b.begin_shutdown();
    handle_a.wait_for_shutdown(Duration::from_secs(5)).await;
    handle_b.wait_for_shutdown(Duration::from_secs(5)).await;
}
