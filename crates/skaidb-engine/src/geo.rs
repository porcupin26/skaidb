//! Spatial encoding for the geo index — the Morton (Z-order) machinery that
//! turns a `geo_distance` / `geo_bbox` predicate into a set of byte ranges over
//! an ordinary secondary-index engine.
//!
//! A point `(lat, lon)` is quantized to two 32-bit fixed-point values and
//! **bit-interleaved** into a 64-bit **Morton code** — latitude on the even bit
//! positions, longitude on the odd. The geo index stores one entry per row keyed
//! by `morton_be8 ++ row_key`, so entries sort by Z-order and a spatial query is
//! a handful of contiguous range scans (candidates re-read + exact-filtered by
//! the caller, exactly like the secondary-index candidate path).
//!
//! **Why a *cover* and not one range.** The single range `[Z(min_corner),
//! Z(max_corner)]` is a correct superset of a rectangle (lat and lon bits are
//! disjoint, so the corner codes bound every interior code — see
//! [`morton`]), but the Z-curve wanders far outside the rectangle between the
//! corners, so that one range can sweep in most of the globe. [`cover_ranges`]
//! instead descends a quadtree, keeping only cells that overlap the rectangle,
//! yielding a bounded set of *tight* ranges. Boundary cells that would subdivide
//! forever are emitted whole once a budget is hit — still a superset, just
//! looser at the fringe, which the exact re-read filter cleans up.
//!
//! Everything here is pure integer/float math over the WGS84-ish lat/lon
//! domains; there is no state to persist (the index lives in the storage
//! engine), so this module is just encode/decode + range planning.

/// Latitude domain (degrees). Points outside are clamped into range.
const LAT_MIN: f64 = -90.0;
const LAT_MAX: f64 = 90.0;
/// Longitude domain (degrees).
const LON_MIN: f64 = -180.0;
const LON_MAX: f64 = 180.0;

/// Metres per degree of latitude (mean sphere) — used to size a radius bbox.
const M_PER_DEG_LAT: f64 = 111_320.0;

/// Default cap on the number of scan ranges a bbox expands to. A handful of
/// tight ranges prune well without exploding the per-query scan count; boundary
/// cells past the budget are emitted whole (a looser superset).
pub const DEFAULT_MAX_RANGES: usize = 32;

/// Parse a distance with an optional unit suffix into **metres** — `"5km"`,
/// `"500m"`, `"1mi"`, `"3.5 NM"`; a bare number is metres. `None` on an
/// unknown unit or a non-finite/negative value. Shared by the SQL
/// `distance('…')` scalar and the ES `_search` geo DSL.
pub fn parse_distance_m(s: &str) -> Option<f64> {
    let s = s.trim();
    let unit_at = s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len());
    let (num, unit) = s.split_at(unit_at);
    let n: f64 = num.trim().parse().ok()?;
    let factor = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "m" | "meters" | "meter" => 1.0,
        "km" | "kilometers" | "kilometer" => 1000.0,
        "mi" | "miles" | "mile" => 1609.344,
        "yd" | "yards" | "yard" => 0.9144,
        "ft" | "feet" | "foot" => 0.3048,
        "in" | "inch" | "inches" => 0.0254,
        "cm" | "centimeters" | "centimeter" => 0.01,
        "mm" | "millimeters" | "millimeter" => 0.001,
        // ES `NM` (nautical miles); arrives lowercased here.
        "nm" | "nmi" | "nauticalmiles" | "nauticalmile" => 1852.0,
        _ => return None,
    };
    let meters = n * factor;
    (meters.is_finite() && meters >= 0.0).then_some(meters)
}

/// Quantize `v` from `[min, max]` onto the full `u32` range (clamped).
fn quantize(v: f64, min: f64, max: f64) -> u32 {
    let t = ((v - min) / (max - min)).clamp(0.0, 1.0);
    (t * u32::MAX as f64).round() as u32
}

/// Spread the 32 low bits of `x` into the even bit positions of a `u64`.
fn spread(x: u32) -> u64 {
    let mut r = x as u64;
    r = (r | (r << 16)) & 0x0000_FFFF_0000_FFFF;
    r = (r | (r << 8)) & 0x00FF_00FF_00FF_00FF;
    r = (r | (r << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    r = (r | (r << 2)) & 0x3333_3333_3333_3333;
    r = (r | (r << 1)) & 0x5555_5555_5555_5555;
    r
}

/// Inverse of [`spread`]: gather the even bit positions of `x` back into a `u32`.
fn compact(x: u64) -> u32 {
    let mut r = x & 0x5555_5555_5555_5555;
    r = (r | (r >> 1)) & 0x3333_3333_3333_3333;
    r = (r | (r >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    r = (r | (r >> 4)) & 0x00FF_00FF_00FF_00FF;
    r = (r | (r >> 8)) & 0x0000_FFFF_0000_FFFF;
    r = (r | (r >> 16)) & 0x0000_0000_FFFF_FFFF;
    r as u32
}

/// Morton (Z-order) code for `(lat, lon)`: lat on even bits, lon on odd bits.
/// Because the two coordinates occupy disjoint bit positions, the code is
/// monotone in each — so over any lat/lon rectangle the min-corner has the
/// minimum code and the max-corner the maximum (the range-scan superset).
pub fn morton(lat: f64, lon: f64) -> u64 {
    let la = quantize(lat, LAT_MIN, LAT_MAX);
    let lo = quantize(lon, LON_MIN, LON_MAX);
    spread(la) | (spread(lo) << 1)
}

/// The 8-byte big-endian Morton code — the sort prefix of a geo index entry key
/// (`morton_be(point) ++ row_key`), so entries order by Z-code.
pub fn morton_key(lat: f64, lon: f64) -> [u8; 8] {
    morton(lat, lon).to_be_bytes()
}

/// Axis-aligned latitude/longitude rectangle in degrees. `min_lon > max_lon`
/// denotes a box crossing the antimeridian; the caller splits that into two
/// non-wrapping boxes before planning (this planner assumes `min <= max`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BBox {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

impl BBox {
    /// The bounding box of the circle of radius `radius_m` metres around
    /// `(lat, lon)` — the conservative envelope for a `geo_distance` query.
    /// Longitude degrees shrink with latitude (`cos φ`); near the poles the
    /// envelope widens to the whole longitude span.
    pub fn around(lat: f64, lon: f64, radius_m: f64) -> BBox {
        let dlat = radius_m / M_PER_DEG_LAT;
        let cos = lat.to_radians().cos().abs();
        let dlon = if cos < 1e-6 {
            360.0 // pole: every meridian is within reach
        } else {
            (radius_m / (M_PER_DEG_LAT * cos)).min(360.0)
        };
        // A circle near ±180° WRAPS (min_lon > max_lon — the split convention),
        // it does not clamp: clamping cut the far-side half of the envelope,
        // so an index-served radius query near the antimeridian missed rows
        // the exact (scan) predicate matched.
        let (min_lon, max_lon) = if dlon >= 180.0 {
            (LON_MIN, LON_MAX)
        } else {
            let wrap = |x: f64| {
                if x < LON_MIN {
                    x + 360.0
                } else if x > LON_MAX {
                    x - 360.0
                } else {
                    x
                }
            };
            (wrap(lon - dlon), wrap(lon + dlon))
        };
        BBox {
            min_lat: (lat - dlat).max(LAT_MIN),
            min_lon,
            max_lat: (lat + dlat).min(LAT_MAX),
            max_lon,
        }
    }

    /// Split an antimeridian-crossing box (`min_lon > max_lon`) into two
    /// non-wrapping boxes; a normal box passes through unsplit. The planner
    /// covers each half separately (`cover_ranges` assumes `min <= max`).
    pub fn split_antimeridian(&self) -> (BBox, Option<BBox>) {
        if self.min_lon <= self.max_lon {
            return (*self, None);
        }
        (
            BBox { min_lon: self.min_lon, max_lon: LON_MAX, ..*self },
            Some(BBox { min_lon: LON_MIN, max_lon: self.max_lon, ..*self }),
        )
    }

    /// The quantized rectangle `(qla_min, qlo_min, qla_max, qlo_max)`.
    fn quantized(&self) -> (u32, u32, u32, u32) {
        (
            quantize(self.min_lat, LAT_MIN, LAT_MAX),
            quantize(self.min_lon, LON_MIN, LON_MAX),
            quantize(self.max_lat, LAT_MIN, LAT_MAX),
            quantize(self.max_lon, LON_MIN, LON_MAX),
        )
    }
}

/// Inclusive `[lo, hi]` Morton-code ranges whose union is a **superset** of the
/// points in `bbox` — the geo index scan plan. Descends a Z-order quadtree,
/// dropping cells disjoint from the rectangle, emitting cells fully inside it,
/// and (once `max_ranges` pending work is reached, or a cell shrinks to a
/// single quantized unit) emitting a straddling cell whole. Adjacent ranges are
/// merged, so the result is a small set of contiguous scans.
pub fn cover_ranges(bbox: &BBox, max_ranges: usize) -> Vec<(u64, u64)> {
    let (qla_min, qlo_min, qla_max, qlo_max) = bbox.quantized();
    let mut out: Vec<(u64, u64)> = Vec::new();
    // Each stack entry is a cell: (base code with its low `free` bits zero, free).
    let mut stack: Vec<(u64, u32)> = vec![(0, 64)];
    while let Some((base, free)) = stack.pop() {
        // Quantized extent of this cell: the free bits split evenly between the
        // two axes, so each axis spans `2^(free/2)` units from its de-interleaved
        // base.
        let side: u64 = 1u64 << (free / 2);
        let la_base = compact(base) as u64;
        let lo_base = compact(base >> 1) as u64;
        let la_hi = la_base + side - 1;
        let lo_hi = lo_base + side - 1;
        // Disjoint from the query rectangle → prune this whole subtree.
        if la_hi < qla_min as u64
            || la_base > qla_max as u64
            || lo_hi < qlo_min as u64
            || lo_base > qlo_max as u64
        {
            continue;
        }
        let fully_inside = la_base >= qla_min as u64
            && la_hi <= qla_max as u64
            && lo_base >= qlo_min as u64
            && lo_hi <= qlo_max as u64;
        let cell_hi = if free >= 64 {
            u64::MAX
        } else {
            base | ((1u64 << free) - 1)
        };
        // Emit whole when the cell is fully covered, is a single unit, or we've
        // spent our range budget (a looser superset — the exact filter cleans up).
        if fully_inside || free == 0 || out.len() + stack.len() >= max_ranges {
            out.push((base, cell_hi));
            continue;
        }
        // Subdivide into the 4 Z-order children (top two free bits = 00/01/10/11).
        let child_free = free - 2;
        for q in 0..4u64 {
            stack.push((base | (q << child_free), child_free));
        }
    }
    merge_ranges(&mut out);
    out
}

/// Sort and coalesce overlapping / adjacent inclusive ranges.
fn merge_ranges(ranges: &mut Vec<(u64, u64)>) {
    if ranges.is_empty() {
        return;
    }
    ranges.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for &(lo, hi) in ranges.iter() {
        match merged.last_mut() {
            // Adjacent (`hi + 1 == lo`) or overlapping → extend. `checked_add`
            // guards the u64::MAX sentinel from wrapping.
            Some(last) if lo <= last.1.saturating_add(1) => {
                if hi > last.1 {
                    last.1 = hi;
                }
            }
            _ => merged.push((lo, hi)),
        }
    }
    *ranges = merged;
}

/// The byte range `(start_inclusive, end_exclusive)` scanning every geo index
/// entry with a Morton code in `[lo, hi]`. `end` is `None` when `hi` is the
/// maximum code (no representable exclusive upper bound).
pub fn range_bytes(lo: u64, hi: u64) -> (Vec<u8>, Option<Vec<u8>>) {
    let start = lo.to_be_bytes().to_vec();
    let end = hi.checked_add(1).map(|h| h.to_be_bytes().to_vec());
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
        const R: f64 = 6_371_000.0;
        let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
        let dlat = (lat2 - lat1).to_radians();
        let dlon = (lon2 - lon1).to_radians();
        let a = (dlat / 2.0).sin().powi(2) + p1.cos() * p2.cos() * (dlon / 2.0).sin().powi(2);
        2.0 * R * a.sqrt().atan2((1.0 - a).sqrt())
    }

    fn in_ranges(ranges: &[(u64, u64)], code: u64) -> bool {
        ranges.iter().any(|&(lo, hi)| lo <= code && code <= hi)
    }

    #[test]
    fn spread_compact_roundtrip() {
        for x in [0u32, 1, 255, 4096, 0x1234_5678, u32::MAX, u32::MAX / 3] {
            assert_eq!(compact(spread(x)), x);
        }
    }

    #[test]
    fn morton_corners_bound_the_rectangle() {
        let bbox = BBox { min_lat: 10.0, min_lon: 20.0, max_lat: 40.0, max_lon: 70.0 };
        let lo = morton(bbox.min_lat, bbox.min_lon);
        let hi = morton(bbox.max_lat, bbox.max_lon);
        assert!(lo <= hi);
        for i in 0..=20 {
            for j in 0..=20 {
                let lat = bbox.min_lat + (bbox.max_lat - bbox.min_lat) * i as f64 / 20.0;
                let lon = bbox.min_lon + (bbox.max_lon - bbox.min_lon) * j as f64 / 20.0;
                let c = morton(lat, lon);
                assert!(lo <= c && c <= hi, "code {c} outside [{lo},{hi}]");
            }
        }
    }

    /// Antimeridian handling: `around` near ±180° WRAPS instead of clamping,
    /// `split_antimeridian` yields two boxes whose combined cover is a
    /// superset of every point within the radius — including the far side.
    #[test]
    fn antimeridian_split_covers_both_sides() {
        // 300 km around (0, 179.5): reaches past +180 to ≈ -177.8.
        let bbox = BBox::around(0.0, 179.5, 300_000.0);
        assert!(bbox.min_lon > bbox.max_lon, "envelope must wrap: {bbox:?}");
        let (east, west) = bbox.split_antimeridian();
        let west = west.expect("wrapping box splits");
        assert!(east.min_lon <= east.max_lon && west.min_lon <= west.max_lon);
        let mut ranges = cover_ranges(&east, DEFAULT_MAX_RANGES);
        ranges.extend(cover_ranges(&west, DEFAULT_MAX_RANGES));
        for lon in [179.0, 179.9, 180.0, -180.0, -179.5, -178.0] {
            assert!(
                haversine_m(0.0, 179.5, 0.0, lon) < 300_000.0,
                "test point not in radius"
            );
            assert!(in_ranges(&ranges, morton(0.0, lon)), "dropped lon {lon}");
        }
        // A non-wrapping box passes through unsplit.
        let plain = BBox { min_lat: 0.0, min_lon: 10.0, max_lat: 1.0, max_lon: 11.0 };
        assert_eq!(plain.split_antimeridian(), (plain, None));
    }

    #[test]
    fn cover_is_a_superset_and_prunes() {
        let bbox = BBox { min_lat: 10.0, min_lon: 20.0, max_lat: 40.0, max_lon: 70.0 };
        let ranges = cover_ranges(&bbox, DEFAULT_MAX_RANGES);
        assert!(!ranges.is_empty());
        assert!(ranges.len() <= DEFAULT_MAX_RANGES + 4);
        // Every point inside the rectangle is covered (no false negatives)...
        let mut covered_span = 0u128;
        for &(lo, hi) in &ranges {
            covered_span += (hi - lo) as u128 + 1;
        }
        for i in 0..=30 {
            for j in 0..=30 {
                let lat = bbox.min_lat + (bbox.max_lat - bbox.min_lat) * i as f64 / 30.0;
                let lon = bbox.min_lon + (bbox.max_lon - bbox.min_lon) * j as f64 / 30.0;
                assert!(in_ranges(&ranges, morton(lat, lon)), "dropped an in-rect point");
            }
        }
        // ...while the total covered code span is a small fraction of the globe
        // (genuine pruning, not a scan of everything).
        assert!(
            covered_span < (u64::MAX as u128) / 4,
            "cover spans {covered_span} codes — not pruning"
        );
    }

    #[test]
    fn cover_ranges_are_sorted_and_disjoint() {
        let bbox = BBox { min_lat: -5.0, min_lon: -12.0, max_lat: 33.0, max_lon: 48.0 };
        let ranges = cover_ranges(&bbox, DEFAULT_MAX_RANGES);
        for w in ranges.windows(2) {
            assert!(w[0].1 < w[1].0, "ranges overlap or touch after merge");
        }
        for &(lo, hi) in &ranges {
            assert!(lo <= hi);
        }
    }

    #[test]
    fn radius_cover_holds_every_in_range_point() {
        let (clat, clon, radius) = (48.8566, 2.3522, 50_000.0); // 50 km around Paris
        let ranges = cover_ranges(&BBox::around(clat, clon, radius), DEFAULT_MAX_RANGES);
        let mut any_inside = false;
        for dlat in -30..=30 {
            for dlon in -30..=30 {
                let lat = clat + dlat as f64 * 0.05;
                let lon = clon + dlon as f64 * 0.05;
                if haversine_m(clat, clon, lat, lon) <= radius {
                    any_inside = true;
                    assert!(in_ranges(&ranges, morton(lat, lon)), "radius cover dropped a point");
                }
            }
        }
        assert!(any_inside);
    }

    #[test]
    fn whole_globe_is_one_range() {
        let bbox = BBox { min_lat: -90.0, min_lon: -180.0, max_lat: 90.0, max_lon: 180.0 };
        let ranges = cover_ranges(&bbox, DEFAULT_MAX_RANGES);
        assert_eq!(ranges, vec![(0, u64::MAX)]);
        let (start, end) = range_bytes(0, u64::MAX);
        assert_eq!(start, vec![0u8; 8]);
        assert_eq!(end, None);
    }

    #[test]
    fn range_bytes_bound_the_codes() {
        let (start, end) = range_bytes(0x0000_0000_0000_0010, 0x0000_0000_0000_00FF);
        assert_eq!(start, 0x10u64.to_be_bytes().to_vec());
        assert_eq!(end, Some(0x100u64.to_be_bytes().to_vec()));
    }
}
