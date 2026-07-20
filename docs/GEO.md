# Geospatial search

skaidb stores geographic points as ordinary document fields and queries them
with two scalar functions — `geo_distance` and `geo_bbox` — that work anywhere
in a `WHERE` or `ORDER BY`. A **geo index** makes those predicates prune to a
neighborhood instead of scanning the table, transparently: the same query runs
against the index when one exists on the column, and against a scan when it
doesn't.

> Status: **distributed** (sharded scatter-gather), **on-disk** (durable
> entries, nothing rebuilds on open), self-maintaining on writes, backfilled in
> the background like a secondary index.

## Points

A point is a document field shaped as a `{lat, lon}` object (`lng` is also
accepted) or a `[lat, lon]` array:

```sql
CREATE TABLE places (PRIMARY KEY (id));
INSERT INTO places (id, name, loc) VALUES
  (1, 'HQ',   {lat: 40.7128, lon: -74.0060}),
  (2, 'Depot', [40.73, -73.99]);
```

The store is schema-less: a field that is not a readable point (absent, NULL, or
the wrong shape) simply never matches a geo predicate and is never indexed — one
bad row can't fail a query.

## Querying (works with or without an index)

```sql
-- Everything within 5 km of a point (metres, or a distance('…') literal):
SELECT id FROM places WHERE geo_distance(loc, 40.71, -74.0) <= 5000;
SELECT id FROM places WHERE geo_distance(loc, 40.71, -74.0) <= distance('5km');

-- Nearest-first, bounded:
SELECT id, geo_distance(loc, 40.71, -74.0) AS m
FROM places
WHERE geo_distance(loc, 40.71, -74.0) <= 5000
ORDER BY geo_distance(loc, 40.71, -74.0) LIMIT 10;

-- Inside a bounding box (min_lon > max_lon crosses the antimeridian):
SELECT id FROM places WHERE geo_bbox(loc, 40.4, -74.3, 40.9, -73.7);
```

`geo_distance(point, lat, lon)` is the great-circle (haversine) distance in
**metres**; `geo_bbox(point, min_lat, min_lon, max_lat, max_lon)` is a boolean
point-in-rectangle test; `distance('<n><unit>')` converts a unit-suffixed
distance literal (`m`/`km`/`mi`/`yd`/`ft`/`NM`/…) to metres — constant, so
the geo index prunes through it. Full grammar in
[`QUERY_SYNTAX.md`](QUERY_SYNTAX.md).

ES clients get the same predicates through the `_search` DSL — `geo_distance`
and `geo_bounding_box` queries map onto these functions (with ES unit
suffixes like `"5km"` converted to metres, and object/GeoJSON-array/string/WKT
point shapes accepted); see [SEARCH.md](SEARCH.md#es-compatible-rest-subset).

## Creating a geo index (SQL — works cluster-wide)

```sql
CREATE GEO INDEX places_geo ON places (loc);
DROP   GEO INDEX places_geo;
```

Nothing to configure. This is **broadcast DDL**: every node builds and maintains
an index over its own shard. Existing rows are backfilled in paged background
work (like secondary indexes) — while a node backfills, `SHOW INDEXES` reports
its `local` state as `building`; on a single-node/embedded database the backfill
completes before the DDL returns. Once created, the index is used automatically —
no query change. `EXPLAIN SELECT … WHERE geo_distance(…)` shows the `geo index
scan` access path when the index is engaged.

The index maintains itself on `INSERT`/`UPDATE`/`DELETE`: a move updates the
point's position, a delete removes it, and (because it lives in an on-disk index
engine) entries persist across restarts — no rebuild, unlike the in-memory
vector index.

## How it works

- **Morton (Z-order) codes.** Each point's latitude and longitude are quantized
  to 32-bit fixed-point values and bit-interleaved into a single 64-bit code
  (latitude on the even bits, longitude on the odd). The index stores one entry
  per row keyed by `morton_be(point) ++ row_key`, so entries sort by Z-order.
- **A query is a range cover.** A `geo_distance <= r` radius is turned into its
  bounding box (with a `cos φ` longitude correction); a `geo_bbox` is the box
  directly. The box is covered by a small, bounded set of contiguous Morton-code
  ranges (a Z-order quadtree descent, capped so a fringe never explodes the scan
  count). Each range is a candidate scan.
- **Superset + exact re-read.** The range cover is always a *superset* of the
  true matches (the Z-curve wanders outside the box between corners), so every
  candidate row is re-read and re-checked with the exact `geo_distance` /
  `geo_bbox` predicate — no false negatives, and false positives are filtered
  out. Ordering by distance is applied by the executor after the gather (Z-order
  is not distance order).
- **Distributed.** A geo scan is just a *multi-range* secondary-index scan, so it
  reuses the existing scatter: each shard scans the code ranges over its local
  index, the coordinator unions the candidate keys, re-reads each at the read
  quorum (authoritative last-writer-wins point), and applies the exact filter.

## Antimeridian

A `geo_bbox` with `min_lon > max_lon` crosses the antimeridian (±180°); the
planner splits it into two non-wrapping halves and the index serves both. A
`geo_distance` radius straddling ±180° likewise wraps its envelope (it used
to clamp, so an index-served radius query near the antimeridian missed
far-side rows).

## Limits (v1)

- Points only — `geo_shape` polygons are not supported.
- No geo aggregations (geohash-grid / geo-bounds facets) yet.
