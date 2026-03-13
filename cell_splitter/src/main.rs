fn main() -> Result<(), anyhow::Error> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    // Supported forms:
    //   (no args)                  -> default data dir, all cells
    //   <data_dir>                 -> that data dir, all cells
    //   <lat> <lon>                -> default data dir, single cell
    //   <data_dir> <lat> <lon>     -> that data dir, single cell
    let (base, cell) = match args.len() {
        0 => (None, None),
        1 => (Some(args[0].as_str()), None),
        2 => {
            let lat: i32 = args[0].parse().expect("lat must be integer");
            let lon: i32 = args[1].parse().expect("lon must be integer");
            (None, Some((lat, lon)))
        }
        3 => {
            let lat: i32 = args[1].parse().expect("lat must be integer");
            let lon: i32 = args[2].parse().expect("lon must be integer");
            (Some(args[0].as_str()), Some((lat, lon)))
        }
        _ => {
            eprintln!(
                "Usage:\n  global_gtfs_graph [data_dir]\n  global_gtfs_graph [data_dir] <lat> <lon>\n  global_gtfs_graph <lat> <lon>"
            );
            std::process::exit(1);
        }
    };

    let written = global_gtfs_graph::run_with_cell(base, cell)?;
    let _ = written;
    Ok(())
}
