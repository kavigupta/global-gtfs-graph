//! Transit graph: for each (from_stop, to_stop) keep minimum duration in minutes.
//! Duration between consecutive stops in a journey: (t_next - t_prev + 1440) % 1440.

use std::collections::HashMap;

use crate::load::JourneyRecord;

/// Adjacency list: from_stop_id -> [(to_stop_id, duration_minutes), ...]
pub type TransitAdj = HashMap<i32, Vec<(i32, i32)>>;

const MINUTES_PER_DAY: i32 = 24 * 60;

pub fn build_transit_graph(journeys: &[JourneyRecord]) -> TransitAdj {
    let mut edges: HashMap<(i32, i32), i32> = HashMap::new();
    for j in journeys {
        let stops = &j.stops;
        let times = &j.times_within_day;
        if times.len() != stops.len() {
            continue;
        }
        for i in 0..stops.len().saturating_sub(1) {
            let a = stops[i];
            let b = stops[i + 1];
            let t_a = times[i];
            let t_b = times[i + 1];
            let duration = (t_b - t_a + MINUTES_PER_DAY) % MINUTES_PER_DAY;
            let key = (a, b);
            edges
                .entry(key)
                .and_modify(|d| *d = (*d).min(duration))
                .or_insert(duration);
        }
    }
    let mut adj: TransitAdj = HashMap::new();
    for ((a, b), d) in edges {
        adj.entry(a).or_default().push((b, d));
    }
    adj
}
