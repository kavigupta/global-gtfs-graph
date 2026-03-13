//! Dijkstra with remaining time (max-heap) and walking (3 km/h).
//! State: remaining[stop] = best remaining minutes from any seed.
//! Transit: rem - duration; walking: rem - (d_km / 3 * 60) using haversine and R-tree bbox.

use std::collections::HashMap;
use std::collections::BinaryHeap;

use crate::spatial::{
    lon_km_per_deg_at_lat, walk_minutes, StopPoint, KM_PER_DEG_LAT, WALK_SPEED_KM_H,
    MAX_TRAVEL_MINUTES,
};
use crate::transit_graph::TransitAdj;
use rstar::RTree;

/// Dense array indexed by stop_id (0..N-1): (lat, lon)
pub type StopCoords = Vec<(f64, f64)>;

/// Returns minutes_used per stop reachable from seed_stops within MAX_TRAVEL_MINUTES:
/// minutes_used = MAX_TRAVEL_MINUTES - remaining_time, so 0 for seed stops inside
/// the cell and >0 for stops reached outside the cell, always < MAX_TRAVEL_MINUTES.
pub fn dijkstra_reachable(
    transit_adj: &TransitAdj,
    rtree: &RTree<StopPoint>,
    stop_coords: &StopCoords,
    seed_stops: &[i32],
) -> HashMap<i32, i32> {
    let mut remaining: HashMap<i32, i32> = HashMap::new();
    let mut heap: BinaryHeap<(i32, i32)> = BinaryHeap::new(); // (remaining_minutes, stop_id)
    for &s in seed_stops {
        remaining.insert(s, MAX_TRAVEL_MINUTES);
        heap.push((MAX_TRAVEL_MINUTES, s));
    }

    while let Some((rem, u)) = heap.pop() {
        if rem <= 0 {
            continue;
        }
        if *remaining.get(&u).unwrap_or(&0) != rem {
            continue;
        }

        // Transit
        if let Some(neighbors) = transit_adj.get(&u) {
            for &(v, duration) in neighbors {
                let new_rem = rem - duration;
                if new_rem > 0 {
                    let entry = remaining.entry(v).or_insert(0);
                    if new_rem > *entry {
                        *entry = new_rem;
                        heap.push((new_rem, v));
                    }
                }
            }
        }

        // Walking: d_max_km = rem * (3/60) = rem * 0.05
        let d_max_km = (rem as f64) * (WALK_SPEED_KM_H / 60.0);
        let idx = u as usize;
        if idx >= stop_coords.len() {
            continue;
        }
        let (u_lat, u_lon) = stop_coords[idx];
        let lat_deg_km = KM_PER_DEG_LAT;
        let lon_deg_km = lon_km_per_deg_at_lat(u_lat);
        let d_lat_deg = (d_max_km / lat_deg_km).min(90.0);
        let d_lon_deg = (d_max_km / lon_deg_km).min(180.0);
        let envelope = rstar::AABB::from_corners(
            [u_lon - d_lon_deg, u_lat - d_lat_deg],
            [u_lon + d_lon_deg, u_lat + d_lat_deg],
        );
        for w in rtree.locate_in_envelope_intersecting(&envelope) {
            if w.stop_id == u {
                continue;
            }
            let walk_min = walk_minutes(u_lat, u_lon, w.lat, w.lon, WALK_SPEED_KM_H);
            let walk_min_i = walk_min.round() as i32;
            let new_rem = rem - walk_min_i;
            if new_rem > 0 {
                let entry = remaining.entry(w.stop_id).or_insert(0);
                if new_rem > *entry {
                    *entry = new_rem;
                    heap.push((new_rem, w.stop_id));
                }
            }
        }
    }

    let mut minutes_used: HashMap<i32, i32> = HashMap::new();
    for (sid, rem) in remaining {
        minutes_used.insert(sid, MAX_TRAVEL_MINUTES - rem);
    }
    minutes_used
}
