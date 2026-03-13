mod dijkstra;
mod graph;
mod load;
mod seeds;
mod spatial;
mod transit_graph;
mod write_cell;

use std::collections::HashMap;
use std::path::Path;

use dijkstra::{dijkstra_reachable, StopCoords};
use load::{LineRecord, StopRecord};
use seeds::cells_with_seeds;
use spatial::build_rtree;
use transit_graph::build_transit_graph;
use write_cell::write_cell_subgraph;

/// Default data directory: parent of crate root / data, i.e. repo/data when run from repo.
fn default_data_dir() -> std::path::PathBuf {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
    Path::new(&manifest).join("..").join("data").canonicalize()
        .unwrap_or_else(|_| Path::new(&manifest).join("..").join("data"))
}

/// Run cell splitting: load combined graph, for each 1x1 cell with seeds (or a single
/// specified cell) run Dijkstra (transit + walking), and write subgraph(s) to
/// data/combined_graphs/cells/<lat>_<lon>.pb.
pub fn run_with_cell(
    base: Option<&str>,
    cell: Option<(i32, i32)>,
) -> Result<Vec<std::path::PathBuf>, anyhow::Error> {
    let base = match base {
        Some(p) => Path::new(p).to_path_buf(),
        None => default_data_dir(),
    };

    println!("Loading combined graph...");
    let (stops, lines, journeys) = load::load_combined_graph(&base)?;
    println!(
        "Loaded {} stops, {} lines, {} journeys",
        stops.len(),
        lines.len(),
        journeys.len()
    );

    let stop_by_id: HashMap<i32, &StopRecord> = stops.iter().map(|s| (s.stop_id, s)).collect();
    let line_by_id: HashMap<i32, &LineRecord> = lines.iter().map(|l| (l.line_id, l)).collect();

    // Combined export uses dense 0..N-1 stop_ids, so store coords in a Vec
    // indexed directly by stop_id for speed.
    let mut stop_coords: StopCoords = Vec::with_capacity(stops.len());
    stop_coords.resize(stops.len(), (0.0, 0.0));
    for s in &stops {
        let idx = s.stop_id as usize;
        if idx < stop_coords.len() {
            stop_coords[idx] = (s.lat, s.lon);
        }
    }

    println!("Building R-tree and transit graph...");
    let rtree = build_rtree(&stops);
    let transit_adj = build_transit_graph(&journeys);

    println!("Finding cells with seeds (no padding; walking handles reach)...",);
    let cell_to_seeds = cells_with_seeds(&stops);
    let mut cells: Vec<_> = cell_to_seeds.keys().copied().collect();
    cells.sort_unstable();
    if let Some(target) = cell {
        cells.retain(|&c| c == target);
        println!("Processing 1 cell (filtered to {:?})...", target);
    } else {
        println!("Processing {} cells...", cells.len());
    }

    let out_dir = base.join("combined_graphs").join("cells");
    std::fs::create_dir_all(&out_dir)?;
    let mut written = Vec::new();

    let pb = indicatif::ProgressBar::new(cells.len() as u64);
    for (lat_floor, lon_floor) in cells {
        let seeds = cell_to_seeds.get(&(lat_floor, lon_floor)).unwrap();
        let minutes_used = dijkstra_reachable(&transit_adj, &rtree, &stop_coords, seeds);
        let mut reachable: Vec<i32> = minutes_used.keys().copied().collect();
        reachable.sort_unstable();
        let out_path = out_dir.join(format!("{}_{}.pb", lat_floor, lon_floor));
        write_cell_subgraph(
            &reachable,
            &minutes_used,
            &journeys,
            &lines,
            &stop_by_id,
            &line_by_id,
            &out_path,
        )?;
        written.push(out_path);
        pb.inc(1);
    }
    pb.finish_with_message("done");

    println!(
        "Wrote {} cell subgraph(s) to {}.",
        written.len(),
        out_dir.display()
    );
    Ok(written)
}

/// Convenience wrapper: process all cells (no explicit cell filter).
pub fn run(base: Option<&str>) -> Result<Vec<std::path::PathBuf>, anyhow::Error> {
    run_with_cell(base, None)
}
