//! Build FeedGraph for one cell: reachable stops, filtered journeys/lines, renumber to 0..N-1.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::graph::graph::{FeedGraph, Journey, Line, Stop};
use crate::load::{JourneyRecord, LineRecord, StopRecord};

/// Reachable stop_ids (sorted) and their minutes_used. Used line_ids. Build subgraph and write to path.
pub fn write_cell_subgraph(
    reachable: &[i32],
    minutes_used: &HashMap<i32, i32>,
    journeys: &[JourneyRecord],
    _lines: &[LineRecord],
    stop_by_id: &HashMap<i32, &StopRecord>,
    line_by_id: &HashMap<i32, &LineRecord>,
    out_path: &Path,
) -> Result<(), anyhow::Error> {
    let reachable_set: std::collections::HashSet<i32> = reachable.iter().copied().collect();

    let sub_journeys: Vec<&JourneyRecord> = journeys
        .iter()
        .filter(|j| j.stops.iter().all(|s| reachable_set.contains(s)))
        .collect();

    let used_line_ids: std::collections::HashSet<i32> =
        sub_journeys.iter().map(|j| j.line_id).collect();

    let old_stop_ids: Vec<i32> = reachable.to_vec();
    let old_to_new_stop: HashMap<i32, i32> = old_stop_ids
        .iter()
        .enumerate()
        .map(|(i, &old)| (old, i as i32))
        .collect();

    let old_line_ids: Vec<i32> = {
        let mut v: Vec<i32> = used_line_ids.into_iter().collect();
        v.sort_unstable();
        v
    };
    let old_to_new_line: HashMap<i32, i32> = old_line_ids
        .iter()
        .enumerate()
        .map(|(i, &old)| (old, i as i32))
        .collect();

    let mut pb = FeedGraph::default();

    for &old_sid in &old_stop_ids {
        let s = stop_by_id.get(&old_sid).unwrap();
        let mu = *minutes_used.get(&old_sid).unwrap_or(&0);
        pb.stops.push(Stop {
            stop_id: *old_to_new_stop.get(&old_sid).unwrap(),
            name: s.name.clone(),
            lat: s.lat,
            lon: s.lon,
            minutes_used: Some(mu),
        });
    }
    for &old_lid in &old_line_ids {
        let ln = line_by_id.get(&old_lid).unwrap();
        pb.lines.push(Line {
            line_id: *old_to_new_line.get(&old_lid).unwrap(),
            name: ln.name.clone(),
            color: ln.color.clone(),
            type_id: ln.type_id,
            agency_id: ln.agency_id.clone(),
        });
    }
    for j in sub_journeys {
        let mut journey = Journey::default();
        journey.line_id = *old_to_new_line.get(&j.line_id).unwrap();
        journey.stops = j.stops.iter().map(|s| *old_to_new_stop.get(s).unwrap()).collect();
        journey.times_within_day = j.times_within_day.clone();
        journey.days = j.days.clone();
        pb.journeys.push(journey);
    }

    let buf = prost::Message::encode_to_vec(&pb);
    fs::write(out_path, buf)?;
    Ok(())
}
