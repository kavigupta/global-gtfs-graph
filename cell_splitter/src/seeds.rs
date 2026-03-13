//! Seed stops per 1x1 cell: stops whose (lat, lon) fall inside that 1x1° cell.

use std::collections::HashMap;

use crate::load::StopRecord;

/// Cell key (lat_floor, lon_floor). Returns map cell -> list of stop_ids in that cell.
pub fn cells_with_seeds(stops: &[StopRecord]) -> HashMap<(i32, i32), Vec<i32>> {
    let mut cell_to_stops: HashMap<(i32, i32), Vec<i32>> = HashMap::new();

    for s in stops {
        let lat_floor = s.lat.floor() as i32;
        let lon_floor = s.lon.floor() as i32;
        cell_to_stops
            .entry((lat_floor, lon_floor))
            .or_default()
            .push(s.stop_id);
    }

    for v in cell_to_stops.values_mut() {
        v.sort_unstable();
        v.dedup();
    }
    cell_to_stops
}
