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

//! Request counts behind the summary Pylon logs when a shutdown drain ends.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::time::Instant;

/// Requests forwarded through Pylon's tunnels, except health probes. One
/// atomic holds both counts so that one load reads them at the same instant:
/// requests started in the high 32 bits, wrapping, and requests in flight in
/// the low 32 bits.
#[derive(Debug, Default)]
pub(crate) struct DrainCounters(AtomicU64);

/// Adds one started request and one request in flight.
const BEGIN_REQUEST: u64 = (1 << 32) + 1;

/// Counts its request as finished when dropped.
#[derive(Debug)]
pub(crate) struct DrainRequestGuard<'a>(&'a DrainCounters);

impl Drop for DrainRequestGuard<'_> {
    fn drop(&mut self) {
        self.0.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Request counts at one instant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DrainSnapshot {
    started: u32,
    in_flight: u32,
}

impl DrainCounters {
    pub(crate) fn begin_request(&self) -> DrainRequestGuard<'_> {
        self.0.fetch_add(BEGIN_REQUEST, Ordering::Relaxed);
        DrainRequestGuard(self)
    }

    pub(crate) fn snapshot(&self) -> DrainSnapshot {
        let counts = self.0.load(Ordering::Relaxed);
        DrainSnapshot {
            started: (counts >> 32) as u32,
            in_flight: counts as u32,
        }
    }
}

impl DrainSnapshot {
    pub(crate) fn in_flight(&self) -> u64 {
        u64::from(self.in_flight)
    }
}

/// Times one drain from its first `start` call and summarizes it. The client
/// starts it before it stops the registration session, and the session starts
/// it when it stops on its own.
#[derive(Debug)]
pub(crate) struct DrainRecorder {
    counters: Arc<DrainCounters>,
    drain_timeout: Duration,
    start: OnceLock<(Instant, DrainSnapshot)>,
}

impl DrainRecorder {
    pub(crate) fn new(counters: Arc<DrainCounters>, drain_timeout: Duration) -> Self {
        Self {
            counters,
            drain_timeout,
            start: OnceLock::new(),
        }
    }

    pub(crate) fn start(&self) {
        self.start
            .get_or_init(|| (Instant::now(), self.counters.snapshot()));
    }

    /// What the drain did from its start until now, or `None` if it has not
    /// started.
    pub(crate) fn summary(&self) -> Option<DrainSummary> {
        let (started_at, start) = self.start.get()?;
        Some(DrainSummary::between(
            *start,
            self.counters.snapshot(),
            started_at.elapsed(),
            self.drain_timeout,
        ))
    }
}

/// What one shutdown drain did, from its start to its end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DrainSummary {
    /// Requests running when the drain started.
    pub in_flight_at_start: u64,
    /// Requests that arrived during the drain, in the accept window.
    pub accepted: u64,
    /// Requests that ended during the drain, with any status.
    pub completed: u64,
    /// Requests still running when the drain ended. Pylon's exit cuts them off.
    pub cut_off: u64,
    pub duration: Duration,
    pub drain_timeout: Duration,
}

impl DrainSummary {
    pub(crate) fn between(
        start: DrainSnapshot,
        end: DrainSnapshot,
        duration: Duration,
        drain_timeout: Duration,
    ) -> Self {
        let in_flight_at_start = start.in_flight();
        let accepted = u64::from(end.started.wrapping_sub(start.started));
        let cut_off = end.in_flight();
        Self {
            in_flight_at_start,
            accepted,
            // A request running at the start or accepted since has either
            // completed or is still running.
            completed: (in_flight_at_start + accepted).saturating_sub(cut_off),
            cut_off,
            duration,
            drain_timeout,
        }
    }

    /// `completed` when no request was left running, `timed_out` otherwise.
    pub fn outcome(&self) -> &'static str {
        if self.cut_off == 0 {
            "completed"
        } else {
            "timed_out"
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use super::*;

    #[test]
    fn summary_counts_requests_across_the_drain() {
        let counters = DrainCounters::default();
        let finished_before = counters.begin_request();
        let running_at_start = counters.begin_request();
        let finishes_during_drain = counters.begin_request();
        drop(finished_before);

        let start = counters.snapshot();
        let accepted = counters.begin_request();
        drop(finishes_during_drain);
        drop(accepted);
        let end = counters.snapshot();

        let summary =
            DrainSummary::between(start, end, Duration::from_secs(3), Duration::from_secs(35));
        assert_eq!(summary.in_flight_at_start, 2);
        assert_eq!(summary.accepted, 1);
        assert_eq!(summary.completed, 2);
        assert_eq!(summary.cut_off, 1);
        assert_eq!(summary.outcome(), "timed_out");
        drop(running_at_start);
    }

    #[test]
    fn drain_with_no_request_left_running_completed() {
        let counters = DrainCounters::default();
        let request = counters.begin_request();
        let start = counters.snapshot();
        drop(request);

        let summary = DrainSummary::between(
            start,
            counters.snapshot(),
            Duration::from_millis(2100),
            Duration::from_secs(90),
        );
        assert_eq!(summary.in_flight_at_start, 1);
        assert_eq!(summary.accepted, 0);
        assert_eq!(summary.completed, 1);
        assert_eq!(summary.cut_off, 0);
        assert_eq!(summary.outcome(), "completed");
    }

    #[test]
    fn idle_drain_completed_with_zero_counts() {
        let counters = DrainCounters::default();
        let snapshot = counters.snapshot();
        let summary =
            DrainSummary::between(snapshot, snapshot, Duration::ZERO, Duration::from_secs(90));
        assert_eq!(
            (
                summary.in_flight_at_start,
                summary.accepted,
                summary.completed,
                summary.cut_off
            ),
            (0, 0, 0, 0)
        );
        assert_eq!(summary.outcome(), "completed");
    }

    #[test]
    fn snapshot_never_counts_more_in_flight_than_ran_at_once() {
        const REQUESTS: u32 = 100_000;
        let counters = DrainCounters::default();
        let done = AtomicBool::new(false);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                // Each request finishes before the next starts.
                for _ in 0..REQUESTS {
                    drop(counters.begin_request());
                }
                done.store(true, Ordering::Release);
            });
            while !done.load(Ordering::Acquire) {
                assert!(counters.snapshot().in_flight() <= 1);
            }
        });
        assert_eq!(
            counters.snapshot(),
            DrainSnapshot {
                started: REQUESTS,
                in_flight: 0
            }
        );
    }

    #[test]
    fn started_count_wraps_without_changing_in_flight() {
        let counters = DrainCounters(AtomicU64::new(u64::from(u32::MAX) << 32));
        let start = counters.snapshot();
        let running = counters.begin_request();
        drop(counters.begin_request());
        let end = counters.snapshot();
        assert_eq!(
            end,
            DrainSnapshot {
                started: 1,
                in_flight: 1
            }
        );

        let summary = DrainSummary::between(start, end, Duration::ZERO, Duration::from_secs(90));
        assert_eq!(
            (
                summary.in_flight_at_start,
                summary.accepted,
                summary.completed,
                summary.cut_off
            ),
            (0, 2, 1, 1)
        );
        drop(running);
    }

    #[test]
    fn recorder_summarizes_from_its_first_start() {
        let counters = Arc::new(DrainCounters::default());
        let recorder = DrainRecorder::new(Arc::clone(&counters), Duration::from_secs(90));
        assert_eq!(recorder.summary(), None);

        recorder.start();
        let accepted = counters.begin_request();
        recorder.start();
        let summary = recorder
            .summary()
            .expect("a started drain should have a summary");
        assert_eq!(
            (
                summary.in_flight_at_start,
                summary.accepted,
                summary.cut_off
            ),
            (0, 1, 1)
        );
        drop(accepted);
    }
}
