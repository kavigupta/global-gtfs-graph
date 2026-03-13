use std::fs;
use std::path::Path;

use crate::graph::graph::FeedGraph;

#[derive(Clone, Debug)]
pub struct StopRecord {
    pub stop_id: i32,
    pub name: String,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Clone, Debug)]
pub struct LineRecord {
    pub line_id: i32,
    pub name: String,
    pub color: String,
    pub type_id: i32,
    pub agency_id: String,
}

#[derive(Clone, Debug)]
pub struct JourneyRecord {
    pub line_id: i32,
    pub stops: Vec<i32>,
    pub times_within_day: Vec<i32>,
    pub days: Vec<i32>,
}

/// Load combined graph from data/combined_graphs: structures, all stop chunks, all journey chunks.
pub fn load_combined_graph(base: &Path) -> Result<(Vec<StopRecord>, Vec<LineRecord>, Vec<JourneyRecord>), anyhow::Error> {
    let combined_dir = base.join("combined_graphs");
    if !combined_dir.exists() {
        anyhow::bail!("Combined graphs directory not found: {}", combined_dir.display());
    }

    // Lines from structures
    let structures_path = combined_dir.join("graphs_all_structures.pb");
    if !structures_path.exists() {
        anyhow::bail!("Structures not found: {}", structures_path.display());
    }
    let buf = fs::read(&structures_path)?;
    let pb: FeedGraph = prost::Message::decode(buf.as_slice())?;
    let lines: Vec<LineRecord> = pb
        .lines
        .into_iter()
        .map(|ln| LineRecord {
            line_id: ln.line_id,
            name: ln.name,
            color: ln.color,
            type_id: ln.type_id,
            agency_id: ln.agency_id,
        })
        .collect();

    // Stops from all chunks
    let mut stops: Vec<StopRecord> = Vec::new();
    let mut stop_files: Vec<_> = fs::read_dir(&combined_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let n = e.file_name();
            let n = n.to_str().unwrap_or("");
            n.starts_with("graphs_all_stops_") && n.ends_with(".pb")
        })
        .collect();
    stop_files.sort_by_key(|e| e.file_name());
    for entry in stop_files {
        let buf = fs::read(entry.path())?;
        let pb: FeedGraph = prost::Message::decode(buf.as_slice())?;
        for s in pb.stops {
            stops.push(StopRecord {
                stop_id: s.stop_id,
                name: s.name,
                lat: s.lat,
                lon: s.lon,
            });
        }
    }

    // Journeys from all chunks
    let mut journeys: Vec<JourneyRecord> = Vec::new();
    let mut journey_files: Vec<_> = fs::read_dir(&combined_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let n = e.file_name();
            let n = n.to_str().unwrap_or("");
            n.starts_with("graphs_all_journeys_") && n.ends_with(".pb")
        })
        .collect();
    journey_files.sort_by_key(|e| e.file_name());
    for entry in journey_files {
        let buf = fs::read(entry.path())?;
        let pb: FeedGraph = prost::Message::decode(buf.as_slice())?;
        for j in pb.journeys {
            if j.stops.len() < 2 {
                continue;
            }
            journeys.push(JourneyRecord {
                line_id: j.line_id,
                stops: j.stops,
                times_within_day: j.times_within_day,
                days: j.days,
            });
        }
    }

    Ok((stops, lines, journeys))
}
