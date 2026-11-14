#!/usr/bin/env python3
"""Builds the Dublin Bus GTFS feed into per-stop departure files.

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
from typing import Dict, List, Optional, Tuple

from gtfs_json_builder import ZipView, _date_to_yyyymmdd, _yyyymmdd_to_date, download_zip
from gtfs_quality import DEFAULT_MIN_COVERAGE_DAYS, FeedValidationError, _time_seconds

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

GTFS_URL_DEFAULT = "https://www.transportforireland.ie/transitData/Data/GTFS_Dublin_Bus.zip"
# Bump when the shape of the published files changes, so apps never read a layout
# they do not understand.
FORMAT_VERSION = 1
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
    gtfs_url: str,
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

    tmp = out_dir.parent / ".tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    zip_path = tmp / "dublin_bus.zip"
    print(f"Downloading {gtfs_url}")
    download_zip(gtfs_url, zip_path)
    z = ZipView(zip_path)

    agencies = z.read_csv("agency.txt")
    stop_rows = z.read_csv("stops.txt")
    route_rows = z.read_csv("routes.txt")
    trip_rows = z.read_csv("trips.txt")
    calendar_rows = z.read_csv("calendar.txt")
    calendar_dates = z.read_csv("calendar_dates.txt")
    feed_info = z.read_csv("feed_info.txt")
    stop_times = z.open_text("stop_times.txt")

    errors: List[str] = []
    stop_times_reader = csv.reader(stop_times) if stop_times else None
    stop_times_header = next(stop_times_reader, []) if stop_times_reader else []
    headers = {**z.headers, "stop_times.txt": tuple(stop_times_header)}
    for filename, columns in REQUIRED_COLUMNS.items():
        missing = [column for column in columns if column not in headers.get(filename, ())]
        if missing:
            errors.append(f"{filename} is missing or lacks {', '.join(missing)}")
    if not calendar_rows and not calendar_dates:
        errors.append("calendar.txt and calendar_dates.txt are both missing or empty")
    if errors:
        raise FeedValidationError(errors)

    target_date = target_date or _today(agencies)
    raw_version = (feed_info[0].get("feed_version") or "").strip() if feed_info else ""
    if not raw_version:
        raise FeedValidationError(["feed_info.txt has no feed_version"])
    version = f"bus-{FORMAT_VERSION}-{raw_version}"

    route_names = {row["route_id"]: row["route_short_name"].strip() for row in route_rows}
    trips: Dict[str, Tuple[str, str, str]] = {}
    for row in trip_rows:
        route_name = route_names.get(row["route_id"])
        if route_name is None:
            errors.append(f"trip {row['trip_id']} uses unknown route {row['route_id']}")
            continue
        trips[row["trip_id"]] = (route_name, (row.get("trip_headsign") or "").strip(), row["service_id"])

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

    column = {name: index for index, name in enumerate(stop_times_header)}
    trip_at, departure_at, arrival_at = column["trip_id"], column["departure_time"], column.get("arrival_time")
    stop_at, sequence_at = column["stop_id"], column["stop_sequence"]
    headsign_at, pickup_at = column.get("stop_headsign"), column.get("pickup_type")

    departures: Dict[str, List[Departure]] = {}
    unknown_trips = unknown_stops = bad_rows = 0
    for row in stop_times_reader:
        # A stop that only lets passengers off is not a departure anyone can catch.
        if pickup_at is not None and row[pickup_at] == "1":
            continue
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
        headsign = (row[headsign_at].strip() if headsign_at is not None else "") or trip[1]
        departures.setdefault(row[stop_at], []).append((seconds, row[trip_at], int(row[sequence_at]), headsign))

    departure_count = sum(len(rows) for rows in departures.values())
    for count, problem in (
        (unknown_trips, "stop times use a trip that trips.txt does not have"),
        (unknown_stops, "stop times use a stop that stops.txt does not have"),
        (bad_rows, "stop times have an unreadable time or stop_sequence"),
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

    for stop in stops:
        names = {trips[trip_id][0] for _, trip_id, _, _ in departures.get(stop["id"], [])}
        stop["routes"] = sorted(names, key=_route_sort_key)
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
        gtfs_url = os.environ.get("BUS_GTFS_URL", GTFS_URL_DEFAULT)
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
            gtfs_url,
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
