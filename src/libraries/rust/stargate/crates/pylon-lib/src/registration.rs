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

use std::time::Duration;

mod client;
mod discovery;
mod grpc_endpoint;
mod reverse_tunnel;
mod router_stream;
mod state;
#[cfg(test)]
mod tests;
mod topology;
mod types;
mod urls;

pub use client::InferenceServerRegistrationClient;
pub use types::{ClientError, InferenceServerRegistrationConfig};

use urls::normalize_addr;

const REVERSE_TUNNEL_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
// Routers keep sending requests until they see the registration close, so a
// stopping reverse tunnel keeps accepting streams this long after the close.
const REVERSE_TUNNEL_ACCEPT_WINDOW: Duration = Duration::from_secs(2);
// A relay in front of the router acknowledges response bytes before it passes
// them on and drops them when the tunnel closes, so a drained tunnel stays open
// this long after its last request.
const REVERSE_TUNNEL_CLOSE_LINGER: Duration = Duration::from_secs(1);
/// Default bound on the wait for in-flight requests when Pylon shuts down.
/// Pylon needs up to 5 s more to stop, so the worker pod termination grace
/// period must be at least this bound plus 5 s, which is longer than the
/// Kubernetes default of 30 s.
pub const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(90);
