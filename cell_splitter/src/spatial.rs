//! R-tree of stops by (lon, lat) for bbox queries. Haversine for walk distance.

use rstar::{RTreeObject, AABB, PointDistance, RTree};
use std::f64::consts::PI;

use crate::load::StopRecord;

/// Point in R-tree: (lon, lat) so that x=lon, y=lat for rstar's 2D.
#[derive(Clone)]
pub struct StopPoint {
    pub lon: f64,
    pub lat: f64,
    pub stop_id: i32,
}

impl RTreeObject for StopPoint {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.lon, self.lat])
    }
}

impl PointDistance for StopPoint {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dlon = self.lon - point[0];
        let dlat = self.lat - point[1];
        dlon * dlon + dlat * dlat
    }

    fn distance_2_if_less_or_equal(&self, point: &[f64; 2], max_dist_2: f64) -> Option<f64> {
        let d2 = self.distance_2(point);
        if d2 <= max_dist_2 {
            Some(d2)
        } else {
            None
        }
    }
}

pub fn build_rtree(stops: &[StopRecord]) -> RTree<StopPoint> {
    let points: Vec<StopPoint> = stops
        .iter()
        .map(|s| StopPoint {
            lon: s.lon,
            lat: s.lat,
            stop_id: s.stop_id,
        })
        .collect();
    RTree::bulk_load(points)
}

/// Haversine distance in km between (lat1, lon1) and (lat2, lon2).
pub fn haversine_km(lat1_deg: f64, lon1_deg: f64, lat2_deg: f64, lon2_deg: f64) -> f64 {
    let r = 6371.0; // Earth radius km
    let to_rad = PI / 180.0;
    let lat1 = lat1_deg * to_rad;
    let lon1 = lon1_deg * to_rad;
    let lat2 = lat2_deg * to_rad;
    let lon2 = lon2_deg * to_rad;
    let dlat = lat2 - lat1;
    let dlon = lon2 - lon1;
    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    r * c
}

pub const KM_PER_DEG_LAT: f64 = 111.32;

/// Default walking speed in km/h for inter-stop walking.
pub const WALK_SPEED_KM_H: f64 = 3.0;

/// Max travel time used for reachability (minutes).
pub const MAX_TRAVEL_MINUTES: i32 = 120;

/// Border buffer in km around each 1x1° cell, derived from max travel time
/// and walking speed (MAX_TRAVEL_MINUTES * WALK_SPEED_KM_H).
pub const MAX_DISTANCE_KM: f64 =
    (MAX_TRAVEL_MINUTES as f64 / 60.0) * WALK_SPEED_KM_H;

/// Walking time in minutes between two coordinates at the given speed (km/h).
pub fn walk_minutes(
    lat1_deg: f64,
    lon1_deg: f64,
    lat2_deg: f64,
    lon2_deg: f64,
    speed_km_h: f64,
) -> f64 {
    let d_km = haversine_km(lat1_deg, lon1_deg, lat2_deg, lon2_deg);
    (d_km / speed_km_h) * 60.0
}

/// Approx km per degree longitude at latitude.
pub fn lon_km_per_deg_at_lat(lat_deg: f64) -> f64 {
    KM_PER_DEG_LAT * (lat_deg.to_radians().cos().abs().max(1e-6))
}

/// Latitude buffer in degrees for a given km buffer.
pub fn buffer_lat_deg(buffer_km: f64) -> f64 {
    buffer_km / KM_PER_DEG_LAT
}
