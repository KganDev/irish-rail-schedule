#!/usr/bin/env python3
"""Builds the Dublin city bus GTFS feeds into per-stop departure files.

Dublin Bus and Go-Ahead Ireland share stops, so their feeds are built into one
timetable: a stop then lists every bus that calls there, whoever runs it.

The bus timetable has millions of stop times, far too many for an app to download
whole. Stops are grouped into shards of neighbouring stops, and an app fetches only
the shards for the stops it shows.
"""

import csv
import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Union

from gtfs_json_builder import ZipView, _date_to_yyyymmdd, _yyyymmdd_to_date, download_zip
from gtfs_quality import DEFAULT_MIN_COVERAGE_DAYS, FeedValidationError, _time_seconds

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# One feed per operator. The NTA gives every stop, trip, service and shape an id that is
# unique across its feeds, and shared stops carry the same id in each.
GTFS_URLS_DEFAULT = (
    "https://www.transportforireland.ie/transitData/Data/GTFS_Dublin_Bus.zip",
    "https://www.transportforireland.ie/transitData/Data/GTFS_GoAhead.zip",
)
# Bump when the published files change in shape or in what they hold, so apps never read
# a layout they do not understand and never keep a stale copy of a version.
FORMAT_VERSION = 3
DEFAULT_STOPS_PER_SHARD = 8
WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

REQUIRED_COLUMNS = {
    "stops.txt": ("stop_id", "stop_name", "stop_lat", "stop_lon"),
    "routes.txt": ("route_id", "route_short_name"),
    "trips.txt": ("route_id", "service_id", "trip_id"),
    "stop_times.txt": ("trip_id", "departure_time", "stop_id", "stop_sequence"),
    "feed_info.txt": ("feed_version",),
}

# A departure at one stop: seconds after midnight, trip, stop_sequence, headsign.
Departure = Tuple[int, str, int, str]
# One stop of a trip: stop_sequence, stop, seconds after midnight.
TripStop = Tuple[int, str, int]


def _today(agencies: List[Dict]) -> date:
    tz_name = agencies[0].get("agency_timezone") if agencies else None
    if tz_name and ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(tz_name)).date()
        except Exception:
            pass
    return date.today()


def _service_dates(calendar_rows: List[Dict], calendar_dates: List[Dict], services: set) -> List[date]:
    """First and last dates of every service that has trips, as a flat list."""
    dates: List[date] = []
    for row in calendar_rows:
        if row.get("service_id") not in services:
            continue
        if not any(row.get(day) == "1" for day in WEEKDAYS):
            continue
        for field in ("start_date", "end_date"):
            parsed = _yyyymmdd_to_date(row.get(field, ""))
            if parsed:
                dates.append(parsed)
    for row in calendar_dates:
        if row.get("service_id") in services and row.get("exception_type") == "1":
            parsed = _yyyymmdd_to_date(row.get("date", ""))
            if parsed:
                dates.append(parsed)
    return dates


def _encode_polyline(points: List[Tuple[float, float]]) -> str:
    """Packs a line of (latitude, longitude) points into Google's polyline text.

    Each point is stored as its difference from the one before, to five decimal places
    (about a metre), so a route's shape is a few kilobytes and not a long list of numbers.
    """
    out: List[str] = []
    previous = (0, 0)
    for lat, lon in points:
        current = (round(lat * 1e5), round(lon * 1e5))
        for value in (current[0] - previous[0], current[1] - previous[1]):
            value = ~(value << 1) if value < 0 else value << 1
            while value >= 0x20:
                out.append(chr((0x20 | (value & 0x1F)) + 63))
                value >>= 5
            out.append(chr(value + 63))
        previous = current
    return "".join(out)


def _read_shapes(z: ZipView) -> Dict[str, str]:
    """Every shape in shapes.txt as polyline text. Empty when the feed has no shapes."""
    shapes_file = z.open_text("shapes.txt")
    if shapes_file is None:
        return {}
    reader = csv.reader(shapes_file)
    column = {name: index for index, name in enumerate(next(reader, []))}
    needed = ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence")
    if any(name not in column for name in needed):
        return {}
    points: Dict[str, List[Tuple[int, float, float]]] = {}
    for row in reader:
        try:
            point = (
                int(row[column["shape_pt_sequence"]]),
                float(row[column["shape_pt_lat"]]),
                float(row[column["shape_pt_lon"]]),
            )
        except ValueError:
            continue
        points.setdefault(row[column["shape_id"]], []).append(point)
    return {
        shape_id: _encode_polyline([(lat, lon) for _, lat, lon in sorted(shape_points)])
        for shape_id, shape_points in points.items()
    }


def _route_sort_key(name: str) -> Tuple[int, str]:
    digits = re.match(r"\d+", name)
    return (int(digits.group()) if digits else 10**6, name)


def _spread_bits(value: int) -> int:
    result = 0
    for bit in range(16):
        result |= ((value >> bit) & 1) << (2 * bit)
    return result


def _shard_stops(stops: List[Dict], stops_per_shard: int) -> Dict[str, int]:
    """Groups stops that are close together, so both sides of a road share a shard.

    Stops are ordered along a Z-order curve, which keeps neighbours on the map near
    each other in the list, then cut into equal groups.
    """
    lats = [stop["lat"] for stop in stops]
    lons = [stop["lon"] for stop in stops]
    lat_span = (max(lats) - min(lats)) or 1.0
    lon_span = (max(lons) - min(lons)) or 1.0

    def curve_position(stop: Dict) -> Tuple[int, str]:
        x = int((stop["lon"] - min(lons)) / lon_span * 65535)
        y = int((stop["lat"] - min(lats)) / lat_span * 65535)
        return (_spread_bits(x) | (_spread_bits(y) << 1), stop["id"])

    ordered = sorted(stops, key=curve_position)
    return {stop["id"]: index // stops_per_shard for index, stop in enumerate(ordered)}


def _dump(obj, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def build(
    gtfs_urls: Union[str, Sequence[str]],
    out_dir: Path,
    target_date: Optional[date],
    *,
    stops_per_shard: int = DEFAULT_STOPS_PER_SHARD,
    minimum_stops: int = 1000,
    minimum_trips: int = 10000,
    minimum_departures: int = 1000000,
    minimum_coverage_days: int = DEFAULT_MIN_COVERAGE_DAYS,
) -> None:
    if stops_per_shard < 1:
        raise ValueError("STOPS_PER_SHARD must be positive")
    if isinstance(gtfs_urls, str):
        gtfs_urls = [gtfs_urls]
    if not gtfs_urls:
        raise ValueError("at least one GTFS feed is required")

    tmp = out_dir.parent / ".tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    feeds: List[ZipView] = []
    for number, gtfs_url in enumerate(gtfs_urls):
        zip_path = tmp / f"bus_{number}.zip"
        print(f"Downloading {gtfs_url}")
        download_zip(gtfs_url, zip_path)
        feeds.append(ZipView(zip_path))

    # The feeds are read as one. A stop both feeds have is kept once; anything else that
    # two feeds both claim would mix one operator's buses into another's, so it is refused.
    errors: List[str] = []
    agencies: List[Dict] = []
    stop_rows: List[Dict] = []
    route_rows: List[Dict] = []
    trip_rows: List[Dict] = []
    calendar_rows: List[Dict] = []
    calendar_dates: List[Dict] = []
    feed_versions: List[str] = []
    stop_times_readers = []
    seen_stops: set = set()
    seen: Dict[str, set] = {"route": set(), "trip": set(), "service": set()}
    for number, z in enumerate(feeds):
        name = f"feed {number + 1}"
        feed_agencies = z.read_csv("agency.txt")
        feed_stops = z.read_csv("stops.txt")
        feed_routes = z.read_csv("routes.txt")
        feed_trips = z.read_csv("trips.txt")
        feed_calendar = z.read_csv("calendar.txt")
        feed_calendar_dates = z.read_csv("calendar_dates.txt")
        feed_info = z.read_csv("feed_info.txt")
        stop_times = z.open_text("stop_times.txt")

        reader = csv.reader(stop_times) if stop_times else None
        header = next(reader, []) if reader else []
        headers = {**z.headers, "stop_times.txt": tuple(header)}
        feed_errors = []
        for filename, columns in REQUIRED_COLUMNS.items():
            missing = [column for column in columns if column not in headers.get(filename, ())]
            if missing:
                feed_errors.append(f"{name}: {filename} is missing or lacks {', '.join(missing)}")
        if not feed_calendar and not feed_calendar_dates:
            feed_errors.append(f"{name}: calendar.txt and calendar_dates.txt are both missing or empty")
        if feed_errors:
            errors.extend(feed_errors)
            continue

        feed_version = (feed_info[0].get("feed_version") or "").strip() if feed_info else ""
        if not feed_version:
            errors.append(f"{name}: feed_info.txt has no feed_version")
        elif feed_version not in feed_versions:
            feed_versions.append(feed_version)

        services = {row["service_id"] for row in feed_trips}
        for kind, ids in (
            ("route", {row["route_id"] for row in feed_routes}),
            ("trip", {row["trip_id"] for row in feed_trips}),
            ("service", services),
        ):
            shared = seen[kind] & ids
            if shared:
                errors.append(f"{name}: {len(shared)} {kind} ids are also in an earlier feed, such as {min(shared)}")
            seen[kind] |= ids

        agencies.extend(feed_agencies)
        for row in feed_stops:
            if row["stop_id"] not in seen_stops:
                seen_stops.add(row["stop_id"])
                stop_rows.append(row)
        route_rows.extend(feed_routes)
        trip_rows.extend(feed_trips)
        # A feed lists only its own services, but only rows its trips use are kept, so a
        # stray row can never change another feed's running days.
        calendar_rows.extend(row for row in feed_calendar if row.get("service_id") in services)
        calendar_dates.extend(row for row in feed_calendar_dates if row.get("service_id") in services)
        stop_times_readers.append((reader, header))
    if errors:
        raise FeedValidationError(errors)

    target_date = target_date or _today(agencies)
    # The feeds usually share a version. When one is republished ahead of the others,
    # naming each keeps the published version changing with any of them.
    version = f"bus-{FORMAT_VERSION}-{'-'.join(feed_versions)}"

    route_names = {row["route_id"]: row["route_short_name"].strip() for row in route_rows}
    # Route numbers name the trip files, so they must be safe in a path.
    for name in sorted(set(route_names.values())):
        if not re.fullmatch(r"[A-Za-z0-9]+", name):
            errors.append(f"route number {name!r} has characters other than letters and digits")
    trips: Dict[str, Tuple[str, str, str]] = {}
    for row in trip_rows:
        route_name = route_names.get(row["route_id"])
        if route_name is None:
            errors.append(f"trip {row['trip_id']} uses unknown route {row['route_id']}")
            continue
        trips[row["trip_id"]] = (route_name, (row.get("trip_headsign") or "").strip(), row["service_id"])
    trip_shapes = {row["trip_id"]: row.get("shape_id") or "" for row in trip_rows}
    trip_routes = {row["trip_id"]: row["route_id"] for row in trip_rows}

    services = {service for _, _, service in trips.values()}
    service_dates = _service_dates(calendar_rows, calendar_dates, services)
    if not service_dates:
        raise FeedValidationError(["no service with trips has any running dates"])
    service_start, service_end = min(service_dates), max(service_dates)
    required_end = target_date + timedelta(days=minimum_coverage_days)
    if service_start > target_date:
        errors.append(f"service starts on {service_start}, after the target date {target_date}")
    if service_end < required_end:
        errors.append(
            f"service ends on {service_end}; at least {minimum_coverage_days} days "
            f"after {target_date} are required"
        )

    stops: List[Dict] = []
    for row in stop_rows:
        try:
            lat, lon = float(row["stop_lat"]), float(row["stop_lon"])
        except ValueError:
            errors.append(f"stop {row['stop_id']} has no usable position")
            continue
        stops.append({
            "id": row["stop_id"],
            "code": (row.get("stop_code") or "").strip(),
            "name": row["stop_name"].strip(),
            "lat": lat,
            "lon": lon,
        })
    known_stops = {stop["id"] for stop in stops}

    departures: Dict[str, List[Departure]] = {}
    trip_stops: Dict[str, List[TripStop]] = {}
    unknown_trips = unknown_stops = bad_rows = 0
    for stop_times_reader, stop_times_header in stop_times_readers:
        column = {name: index for index, name in enumerate(stop_times_header)}
        trip_at, departure_at, arrival_at = column["trip_id"], column["departure_time"], column.get("arrival_time")
        stop_at, sequence_at = column["stop_id"], column["stop_sequence"]
        headsign_at, pickup_at = column.get("stop_headsign"), column.get("pickup_type")
        for row in stop_times_reader:
            trip = trips.get(row[trip_at])
            if trip is None:
                unknown_trips += 1
                continue
            if row[stop_at] not in known_stops:
                unknown_stops += 1
                continue
            seconds = _time_seconds(row[departure_at])
            if seconds is None and arrival_at is not None:
                seconds = _time_seconds(row[arrival_at])
            if seconds is None or not row[sequence_at].isdigit():
                bad_rows += 1
                continue
            sequence = int(row[sequence_at])
            trip_stops.setdefault(row[trip_at], []).append((sequence, row[stop_at], seconds))
            # A stop that only lets passengers off is not a departure anyone can catch.
            if pickup_at is not None and row[pickup_at] == "1":
                continue
            headsign = (row[headsign_at].strip() if headsign_at is not None else "") or trip[1]
            departures.setdefault(row[stop_at], []).append((seconds, row[trip_at], sequence, headsign))

    # The trip files give a stop's sequence by its place in the list, so every trip must
    # count its stops 1, 2, 3 with none missing.
    uneven_trips = 0
    for stops_of_trip in trip_stops.values():
        stops_of_trip.sort()
        if [sequence for sequence, _, _ in stops_of_trip] != list(range(1, len(stops_of_trip) + 1)):
            uneven_trips += 1

    departure_count = sum(len(rows) for rows in departures.values())
    for count, problem in (
        (unknown_trips, "stop times use a trip that trips.txt does not have"),
        (unknown_stops, "stop times use a stop that stops.txt does not have"),
        (bad_rows, "stop times have an unreadable time or stop_sequence"),
        (uneven_trips, "trips do not number their stops 1, 2, 3 in order"),
    ):
        if count:
            errors.append(f"{count} {problem}")
    for count, minimum, name in (
        (len(stops), minimum_stops, "stops"),
        (len(trips), minimum_trips, "trips"),
        (departure_count, minimum_departures, "departures"),
    ):
        if count < minimum:
            errors.append(f"only {count} {name}; at least {minimum} are required")
    if errors:
        raise FeedValidationError(errors)

    shard_of = _shard_stops(stops, stops_per_shard)
    shard_count = max(shard_of.values()) + 1
    width = max(3, len(str(shard_count - 1)))
    version_dir = out_dir / "bus" / version

    shards: List[List[str]] = [[] for _ in range(shard_count)]
    for stop in stops:
        shards[shard_of[stop["id"]]].append(stop["id"])
    for index, stop_ids in enumerate(shards):
        route_index: Dict[str, int] = {}
        headsign_index: Dict[str, int] = {}
        service_index: Dict[str, int] = {}
        shard_stops = {}
        for stop_id in sorted(stop_ids):
            rows = []
            for seconds, trip_id, sequence, headsign in sorted(departures.get(stop_id, [])):
                route_name, _, service = trips[trip_id]
                rows.append([
                    seconds,
                    trip_id,
                    sequence,
                    route_index.setdefault(route_name, len(route_index)),
                    headsign_index.setdefault(headsign, len(headsign_index)),
                    service_index.setdefault(service, len(service_index)),
                ])
            shard_stops[stop_id] = rows
        _dump(
            {
                "version": FORMAT_VERSION,
                "routes": list(route_index),
                "headsigns": list(headsign_index),
                "services": list(service_index),
                "stops": shard_stops,
            },
            version_dir / "shards" / f"{index:0{width}d}.json",
        )

    # One file per route with every stop of every trip, for showing a whole journey. Trips
    # on a route share a few stop patterns, and the gaps between stops repeat, so this
    # stays small. Each trip also names the line it follows on a map, where the feed has it.
    shapes: Dict[str, str] = {}
    for z in feeds:
        shapes.update(_read_shapes(z))
    trips_by_route: Dict[str, List[str]] = {}
    for trip_id in trip_stops:
        trips_by_route.setdefault(trips[trip_id][0], []).append(trip_id)
    for route_name, trip_ids in trips_by_route.items():
        stop_index: Dict[str, int] = {}
        pattern_index: Dict[Tuple[int, ...], int] = {}
        shape_index: Dict[str, int] = {}
        # Two operators can use one route number for different routes. The file then says
        # which route each stop pattern belongs to, so the two can be told apart.
        route_ids = sorted({trip_routes[trip_id] for trip_id in trip_ids})
        pattern_routes: List[int] = []
        route_trips = {}
        for trip_id in sorted(trip_ids):
            stops_of_trip = trip_stops[trip_id]
            pattern = tuple(stop_index.setdefault(stop_id, len(stop_index)) for _, stop_id, _ in stops_of_trip)
            times = [seconds for _, _, seconds in stops_of_trip]
            if pattern not in pattern_index:
                pattern_index[pattern] = len(pattern_index)
                pattern_routes.append(route_ids.index(trip_routes[trip_id]))
            route_trips[trip_id] = [
                pattern_index[pattern],
                times[0],
                [later - earlier for earlier, later in zip(times, times[1:])],
            ]
            shape_id = trip_shapes.get(trip_id, "")
            if shape_id in shapes:
                route_trips[trip_id].append(shape_index.setdefault(shape_id, len(shape_index)))
        route_file = {
            "version": FORMAT_VERSION,
            "stops": list(stop_index),
            "patterns": [list(pattern) for pattern in pattern_index],
            "shapes": [shapes[shape_id] for shape_id in shape_index],
            "trips": route_trips,
        }
        if len(route_ids) > 1:
            route_file["routes"] = route_ids
            route_file["patternRoutes"] = pattern_routes
        _dump(route_file, version_dir / "trips" / f"{route_name}.json")

    for stop in stops:
        stop_departures = departures.get(stop["id"], [])
        names = {trips[trip_id][0] for _, trip_id, _, _ in stop_departures}
        stop["routes"] = sorted(names, key=_route_sort_key)
        # Where most buses from this stop are heading, which tells the two sides of a
        # road apart.
        headsigns: Dict[str, int] = {}
        for _, _, _, headsign in stop_departures:
            if headsign:
                headsigns[headsign] = headsigns.get(headsign, 0) + 1
        stop["towards"] = sorted(headsigns, key=lambda name: (-headsigns[name], name))[:2]
        stop["shard"] = f"{shard_of[stop['id']]:0{width}d}"
    stops.sort(key=lambda stop: stop["id"])
    _dump(stops, version_dir / "stops.json")

    _dump(
        sorted(
            (
                {
                    "id": row["route_id"],
                    "name": row["route_short_name"].strip(),
                    "description": (row.get("route_long_name") or "").strip(),
                }
                for row in route_rows
            ),
            key=lambda route: _route_sort_key(route["name"]),
        ),
        version_dir / "routes.json",
    )

    calendar = {}
    for row in calendar_rows:
        if row["service_id"] in services:
            calendar[row["service_id"]] = {
                "days": [row.get(day) == "1" for day in WEEKDAYS],
                "start": row.get("start_date", ""),
                "end": row.get("end_date", ""),
                "added": [],
                "removed": [],
            }
    for row in calendar_dates:
        if row.get("service_id") not in services:
            continue
        service = calendar.setdefault(
            row["service_id"], {"days": [False] * 7, "start": "", "end": "", "added": [], "removed": []}
        )
        if row.get("exception_type") == "1":
            service["added"].append(row.get("date", ""))
        elif row.get("exception_type") == "2":
            service["removed"].append(row.get("date", ""))
    _dump(
        {
            "timezone": (agencies[0].get("agency_timezone") if agencies else "") or "Europe/Dublin",
            "services": calendar,
        },
        version_dir / "calendar.json",
    )

    latest = {
        "latest": version,
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "shards": shard_count,
    }
    counts = {
        "stops": len(stops),
        "routes": len(route_rows),
        "trips": len(trips),
        "departures": departure_count,
        "shards": shard_count,
        "tripFiles": len(trips_by_route),
    }
    _dump(latest, out_dir / "bus" / "latest.json")
    _dump(
        {
            "ok": True,
            **latest,
            "counts": counts,
            "service": {"startDate": _date_to_yyyymmdd(service_start), "endDate": _date_to_yyyymmdd(service_end)},
        },
        out_dir / "bus" / "status.json",
    )

    print("\nSummary")
    print(f"  version: {version}")
    for name, count in counts.items():
        print(f"  {name}: {count}")
    print(f"  service: {service_start} to {service_end}")


if __name__ == "__main__":
    try:
        # Several feeds are separated by commas or spaces.
        gtfs_urls = re.split(r"[,\s]+", os.environ.get("BUS_GTFS_URL", "").strip())
        gtfs_urls = [url for url in gtfs_urls if url] or list(GTFS_URLS_DEFAULT)
        out_dir = Path(os.environ.get("OUT_DIR", "out"))
        stops_per_shard = int(os.environ.get("STOPS_PER_SHARD") or DEFAULT_STOPS_PER_SHARD)
        minimum_stops = int(os.environ.get("MIN_BUS_STOPS") or 1000)
        minimum_trips = int(os.environ.get("MIN_BUS_TRIPS") or 10000)
        minimum_departures = int(os.environ.get("MIN_BUS_DEPARTURES") or 1000000)
        minimum_coverage_days = int(os.environ.get("MIN_COVERAGE_DAYS") or DEFAULT_MIN_COVERAGE_DAYS)
        tgt = os.environ.get("TARGET_DATE")
        target_date = _yyyymmdd_to_date(tgt) if tgt else None
        if tgt and target_date is None:
            raise ValueError("TARGET_DATE must be a valid date in YYYYMMDD format")
    except ValueError as error:
        print(f"Invalid builder configuration: {error}", file=sys.stderr)
        sys.exit(1)
    try:
        build(
            gtfs_urls,
            out_dir,
            target_date,
            stops_per_shard=stops_per_shard,
            minimum_stops=minimum_stops,
            minimum_trips=minimum_trips,
            minimum_departures=minimum_departures,
            minimum_coverage_days=minimum_coverage_days,
        )
    except (FeedValidationError, ValueError) as error:
        print(error, file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        sys.exit(130)
