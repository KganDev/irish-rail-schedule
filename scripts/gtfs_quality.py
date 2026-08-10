#!/usr/bin/env python3

"""Fail-closed, app-specific quality checks for the Irish Rail GTFS feed."""

from __future__ import annotations

import math
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Iterable, List, Mapping, Sequence, Set, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


WEEKDAYS = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)
SAFE_VERSION = re.compile(r"^[A-Za-z0-9-]+$")
GTFS_TIME = re.compile(r"^(\d{1,2}):([0-5]\d):([0-5]\d)$")


class FeedValidationError(ValueError):
    def __init__(self, errors: Sequence[str]):
        self.errors = list(errors)
        details = "\n".join(f"  - {error}" for error in self.errors)
        super().__init__(f"GTFS quality gate rejected the feed:\n{details}")


@dataclass
class QualityResult:
    calendar_dates: List[Dict]
    service_start: date
    service_end: date
    diagnostics: Dict


def _text(row: Mapping, field: str) -> str:
    value = row.get(field, "")
    return "" if value is None else str(value).strip()


def _parse_date(value: str) -> date | None:
    if len(value) != 8 or not value.isdigit():
        return None
    try:
        return date(int(value[:4]), int(value[4:6]), int(value[6:]))
    except ValueError:
        return None


def _format_date(value: date) -> str:
    return value.strftime("%Y%m%d")


def _time_seconds(value: str) -> int | None:
    match = GTFS_TIME.fullmatch(value)
    if not match:
        return None
    hour, minute, second = map(int, match.groups())
    return hour * 3600 + minute * 60 + second


def _require_columns(
    errors: List[str],
    headers: Mapping[str, Sequence[str]],
    filename: str,
    required: Iterable[str],
) -> None:
    if filename not in headers:
        errors.append(f"missing required file {filename}")
        return
    fields = set(headers[filename])
    missing = sorted(set(required) - fields)
    if missing:
        errors.append(f"{filename} is missing columns: {', '.join(missing)}")


def _unique_key(
    errors: List[str], rows: Sequence[Mapping], filename: str, fields: Sequence[str]
) -> Set[Tuple[str, ...]]:
    seen: Set[Tuple[str, ...]] = set()
    for row_number, row in enumerate(rows, start=2):
        key = tuple(_text(row, field) for field in fields)
        if any(not value for value in key):
            errors.append(
                f"{filename}:{row_number} has an empty key field ({', '.join(fields)})"
            )
        elif key in seen:
            errors.append(f"{filename}:{row_number} duplicates key {key!r}")
        seen.add(key)
    return seen


def _active_dates(
    calendar_rows: Sequence[Mapping], calendar_dates: Sequence[Mapping]
) -> Dict[str, Set[date]]:
    active: Dict[str, Set[date]] = defaultdict(set)
    for row in calendar_rows:
        service_id = _text(row, "service_id")
        start = _parse_date(_text(row, "start_date"))
        end = _parse_date(_text(row, "end_date"))
        if not service_id or not start or not end:
            continue
        mask = tuple(_text(row, day) == "1" for day in WEEKDAYS)
        current = start
        while current <= end:
            if mask[current.weekday()]:
                active[service_id].add(current)
            current += timedelta(days=1)
    for row in calendar_dates:
        service_id = _text(row, "service_id")
        service_date = _parse_date(_text(row, "date"))
        if not service_id or not service_date:
            continue
        if _text(row, "exception_type") == "1":
            active[service_id].add(service_date)
        elif _text(row, "exception_type") == "2":
            active[service_id].discard(service_date)
    return dict(active)


def _is_subsequence(left: Sequence[str], right: Sequence[str]) -> bool:
    iterator = iter(right)
    return all(any(candidate == value for candidate in iterator) for value in left)


def _mostly_same_stops(left: Sequence[str], right: Sequence[str]) -> bool:
    if not left or not right or left[-1] != right[-1]:
        return False
    if _is_subsequence(left, right) or _is_subsequence(right, left):
        return True
    overlap = len(set(left) & set(right))
    return overlap / max(len(set(left)), len(set(right))) >= 0.9


def _trip_profiles(
    trips: Sequence[Mapping], stop_times: Sequence[Mapping]
) -> Dict[str, Dict[Tuple[str, str, str], Dict]]:
    calls: Dict[str, List[Mapping]] = defaultdict(list)
    for row in stop_times:
        calls[_text(row, "trip_id")].append(row)
    for rows in calls.values():
        rows.sort(key=lambda row: int(_text(row, "stop_sequence")))

    result: Dict[str, Dict[Tuple[str, str, str], Dict]] = defaultdict(dict)
    for trip in trips:
        short_name = _text(trip, "trip_short_name")
        if not short_name:
            continue
        key = (
            _text(trip, "route_id"),
            short_name,
            _text(trip, "direction_id"),
        )
        trip_calls = calls.get(_text(trip, "trip_id"), [])
        result[_text(trip, "service_id")][key] = {
            "trip_id": _text(trip, "trip_id"),
            "stops": [_text(row, "stop_id") for row in trip_calls],
            "departure": _time_seconds(_text(trip_calls[0], "departure_time"))
            if trip_calls
            else None,
        }
    return dict(result)


def _compatible_trip(left: Mapping, right: Mapping) -> bool:
    left_departure = left.get("departure")
    right_departure = right.get("departure")
    return (
        left_departure is not None
        and right_departure is not None
        and abs(left_departure - right_departure) <= 10 * 60
        and _mostly_same_stops(left.get("stops", []), right.get("stops", []))
    )


def _sanitize_revision_overlaps(
    calendar_rows: Sequence[Mapping],
    calendar_dates: Sequence[Mapping],
    trips: Sequence[Mapping],
    stop_times: Sequence[Mapping],
    *,
    similarity_min: float,
    minimum_shared_trips: int,
) -> Tuple[List[Dict], List[Dict]]:
    """Suppress only proven stale timetable revisions on their overlapping dates."""

    calendars = {_text(row, "service_id"): row for row in calendar_rows}
    active = _active_dates(calendar_rows, calendar_dates)
    profiles = _trip_profiles(trips, stop_times)
    services = sorted(profiles)
    actions: List[Dict] = []
    ambiguous: List[str] = []
    removals: Dict[str, Set[date]] = defaultdict(set)

    for index, left_id in enumerate(services):
        left_calendar = calendars.get(left_id)
        if not left_calendar:
            continue
        left_mask = tuple(_text(left_calendar, day) for day in WEEKDAYS)
        for right_id in services[index + 1 :]:
            right_calendar = calendars.get(right_id)
            if not right_calendar:
                continue
            if left_mask != tuple(_text(right_calendar, day) for day in WEEKDAYS):
                continue
            overlap_dates = active.get(left_id, set()) & active.get(right_id, set())
            if not overlap_dates:
                continue

            left_profiles = profiles[left_id]
            right_profiles = profiles[right_id]
            common = set(left_profiles) & set(right_profiles)
            compatible = sum(
                1
                for key in common
                if _compatible_trip(left_profiles[key], right_profiles[key])
            )
            denominator = min(len(left_profiles), len(right_profiles))
            similarity = compatible / denominator if denominator else 0.0
            if compatible < minimum_shared_trips:
                continue
            if similarity < similarity_min:
                if similarity >= 0.5:
                    ambiguous.append(
                        f"services {left_id}/{right_id} overlap on {len(overlap_dates)} dates "
                        f"and share {compatible}/{denominator} fuzzy trips ({similarity:.1%})"
                    )
                continue

            left_start = _parse_date(_text(left_calendar, "start_date"))
            right_start = _parse_date(_text(right_calendar, "start_date"))
            if not left_start or not right_start or left_start == right_start:
                ambiguous.append(
                    f"services {left_id}/{right_id} are duplicate revisions but no newer start date is provable"
                )
                continue
            loser, winner = (
                (left_id, right_id) if left_start < right_start else (right_id, left_id)
            )
            removals[loser].update(overlap_dates)
            actions.append(
                {
                    "olderService": loser,
                    "newerService": winner,
                    "overlapDatesRemoved": len(overlap_dates),
                    "firstDate": _format_date(min(overlap_dates)),
                    "lastDate": _format_date(max(overlap_dates)),
                    "compatibleTrips": compatible,
                    "similarity": round(similarity, 4),
                }
            )

    if ambiguous:
        raise FeedValidationError(
            [f"ambiguous overlapping timetable revision: {message}" for message in ambiguous]
        )

    sanitized = [dict(row) for row in calendar_dates]
    exception_index = {
        (_text(row, "service_id"), _text(row, "date")): index
        for index, row in enumerate(sanitized)
    }
    for service_id, dates in removals.items():
        for service_date in sorted(dates):
            key = (service_id, _format_date(service_date))
            replacement = {
                "service_id": service_id,
                "date": key[1],
                "exception_type": "2",
            }
            if key in exception_index:
                sanitized[exception_index[key]] = replacement
            else:
                exception_index[key] = len(sanitized)
                sanitized.append(replacement)
    sanitized.sort(key=lambda row: (_text(row, "date"), _text(row, "service_id")))
    return sanitized, actions


def validate_and_sanitize(
    tables: Mapping[str, List[Dict]],
    headers: Mapping[str, Sequence[str]],
    *,
    target_date: date,
    window_days: int,
    minimum_trips: int,
    minimum_stop_times: int,
    similarity_min: float = 0.9,
    minimum_shared_trips: int = 20,
) -> QualityResult:
    errors: List[str] = []
    required = {
        "agency.txt": ("agency_name", "agency_url", "agency_timezone"),
        "stops.txt": ("stop_id", "stop_name", "stop_lat", "stop_lon"),
        "routes.txt": ("route_id", "route_type"),
        "trips.txt": ("route_id", "service_id", "trip_id", "trip_short_name"),
        "stop_times.txt": (
            "trip_id",
            "arrival_time",
            "departure_time",
            "stop_id",
            "stop_sequence",
        ),
        "feed_info.txt": ("feed_start_date", "feed_end_date", "feed_version"),
    }
    for filename, columns in required.items():
        _require_columns(errors, headers, filename, columns)
    if "calendar.txt" not in headers and "calendar_dates.txt" not in headers:
        errors.append("calendar.txt and calendar_dates.txt are both missing")
    if "calendar.txt" in headers:
        _require_columns(
            errors,
            headers,
            "calendar.txt",
            ("service_id", *WEEKDAYS, "start_date", "end_date"),
        )
    if "calendar_dates.txt" in headers:
        _require_columns(
            errors,
            headers,
            "calendar_dates.txt",
            ("service_id", "date", "exception_type"),
        )

    for filename in ("agency.txt", "stops.txt", "routes.txt", "trips.txt", "stop_times.txt"):
        if filename in headers and not tables.get(filename):
            errors.append(f"{filename} contains no rows")
    if len(tables.get("feed_info.txt", [])) != 1:
        errors.append("feed_info.txt must contain exactly one row")
    if len(tables.get("trips.txt", [])) < minimum_trips:
        errors.append(f"trips.txt has fewer than the required {minimum_trips} rows")
    if len(tables.get("stop_times.txt", [])) < minimum_stop_times:
        errors.append(
            f"stop_times.txt has fewer than the required {minimum_stop_times} rows"
        )

    agencies = tables.get("agency.txt", [])
    stops = tables.get("stops.txt", [])
    routes = tables.get("routes.txt", [])
    trips = tables.get("trips.txt", [])
    stop_times = tables.get("stop_times.txt", [])
    calendar_rows = tables.get("calendar.txt", [])
    calendar_dates = tables.get("calendar_dates.txt", [])
    feed_info = tables.get("feed_info.txt", [])

    stop_ids = {key[0] for key in _unique_key(errors, stops, "stops.txt", ("stop_id",))}
    route_ids = {key[0] for key in _unique_key(errors, routes, "routes.txt", ("route_id",))}
    trip_ids = {key[0] for key in _unique_key(errors, trips, "trips.txt", ("trip_id",))}
    calendar_ids = {
        key[0]
        for key in _unique_key(errors, calendar_rows, "calendar.txt", ("service_id",))
    }
    _unique_key(
        errors,
        calendar_dates,
        "calendar_dates.txt",
        ("service_id", "date"),
    )
    _unique_key(
        errors,
        stop_times,
        "stop_times.txt",
        ("trip_id", "stop_sequence"),
    )
    service_ids = calendar_ids | {
        _text(row, "service_id") for row in calendar_dates if _text(row, "service_id")
    }

    if feed_info:
        version = _text(feed_info[0], "feed_version")
        if not version or not SAFE_VERSION.fullmatch(version):
            errors.append("feed_info.txt feed_version is empty or unsafe for the API URL")
        source_start = _parse_date(_text(feed_info[0], "feed_start_date"))
        source_end = _parse_date(_text(feed_info[0], "feed_end_date"))
        if not source_start or not source_end or source_start > source_end:
            errors.append("feed_info.txt has an invalid date range")

    for row_number, row in enumerate(agencies, start=2):
        timezone = _text(row, "agency_timezone")
        if not timezone:
            errors.append(f"agency.txt:{row_number} has no agency_timezone")
        else:
            try:
                ZoneInfo(timezone)
            except ZoneInfoNotFoundError:
                errors.append(f"agency.txt:{row_number} has invalid timezone {timezone}")
    for row_number, row in enumerate(stops, start=2):
        try:
            latitude = float(_text(row, "stop_lat"))
            longitude = float(_text(row, "stop_lon"))
            if not math.isfinite(latitude) or not -90 <= latitude <= 90:
                raise ValueError
            if not math.isfinite(longitude) or not -180 <= longitude <= 180:
                raise ValueError
        except ValueError:
            errors.append(f"stops.txt:{row_number} has invalid coordinates")
        parent = _text(row, "parent_station")
        if parent and parent not in stop_ids:
            errors.append(f"stops.txt:{row_number} references missing parent_station {parent}")

    for row_number, row in enumerate(trips, start=2):
        if _text(row, "route_id") not in route_ids:
            errors.append(f"trips.txt:{row_number} references a missing route")
        if _text(row, "service_id") not in service_ids:
            errors.append(f"trips.txt:{row_number} references a missing service")
        if not _text(row, "trip_short_name"):
            errors.append(f"trips.txt:{row_number} has no train/trip short name")
        if _text(row, "direction_id") not in {"", "0", "1"}:
            errors.append(f"trips.txt:{row_number} has invalid direction_id")

    stop_times_by_trip: Dict[str, List[Tuple[int, int, int]]] = defaultdict(list)
    normalized_sequences: Set[Tuple[str, int]] = set()
    for row_number, row in enumerate(stop_times, start=2):
        trip_id = _text(row, "trip_id")
        if trip_id not in trip_ids:
            errors.append(f"stop_times.txt:{row_number} references missing trip {trip_id}")
        if _text(row, "stop_id") not in stop_ids:
            errors.append(f"stop_times.txt:{row_number} references a missing stop")
        sequence_text = _text(row, "stop_sequence")
        if not sequence_text.isdigit():
            errors.append(f"stop_times.txt:{row_number} has invalid stop_sequence")
            continue
        normalized_key = (trip_id, int(sequence_text))
        if normalized_key in normalized_sequences:
            errors.append(
                f"stop_times.txt:{row_number} duplicates normalized trip/stop_sequence {normalized_key!r}"
            )
        normalized_sequences.add(normalized_key)
        for field in ("pickup_type", "drop_off_type"):
            if _text(row, field) not in {"", "0", "1", "2", "3"}:
                errors.append(f"stop_times.txt:{row_number} has invalid {field}")
        if _text(row, "timepoint") not in {"", "0", "1"}:
            errors.append(f"stop_times.txt:{row_number} has invalid timepoint")
        arrival = _time_seconds(_text(row, "arrival_time"))
        departure = _time_seconds(_text(row, "departure_time"))
        if arrival is None or departure is None or departure < arrival:
            errors.append(f"stop_times.txt:{row_number} has invalid or reversed times")
            continue
        stop_times_by_trip[trip_id].append((int(sequence_text), arrival, departure))
    for trip_id in trip_ids:
        calls = sorted(stop_times_by_trip.get(trip_id, []))
        if len(calls) < 2:
            errors.append(f"trip {trip_id} has fewer than two valid stop times")
            continue
        for previous, current in zip(calls, calls[1:]):
            if current[1] < previous[2]:
                errors.append(f"trip {trip_id} travels backwards in time")
                break

    for row_number, row in enumerate(calendar_rows, start=2):
        if any(_text(row, day) not in {"0", "1"} for day in WEEKDAYS):
            errors.append(f"calendar.txt:{row_number} has an invalid weekday flag")
        start = _parse_date(_text(row, "start_date"))
        end = _parse_date(_text(row, "end_date"))
        if not start or not end or start > end:
            errors.append(f"calendar.txt:{row_number} has an invalid date range")
    for row_number, row in enumerate(calendar_dates, start=2):
        if not _parse_date(_text(row, "date")):
            errors.append(f"calendar_dates.txt:{row_number} has an invalid date")
        if _text(row, "exception_type") not in {"1", "2"}:
            errors.append(f"calendar_dates.txt:{row_number} has an invalid exception_type")

    if errors:
        raise FeedValidationError(errors[:100])

    sanitized_dates, revision_actions = _sanitize_revision_overlaps(
        calendar_rows,
        calendar_dates,
        trips,
        stop_times,
        similarity_min=similarity_min,
        minimum_shared_trips=minimum_shared_trips,
    )
    active = _active_dates(calendar_rows, sanitized_dates)
    used_services = {_text(row, "service_id") for row in trips}
    empty_services = sorted(service for service in used_services if not active.get(service))
    if empty_services:
        raise FeedValidationError(
            [f"trip-bearing services have no effective dates: {', '.join(empty_services)}"]
        )

    all_dates = set().union(*(active.get(service, set()) for service in used_services))
    if not all_dates:
        raise FeedValidationError(["feed has no effective passenger service dates"])
    service_start, service_end = min(all_dates), max(all_dates)
    required_end = target_date + timedelta(days=window_days)
    if not service_start <= target_date <= service_end:
        raise FeedValidationError(
            [f"target date {target_date} is outside effective service {service_start}..{service_end}"]
        )
    if service_end < required_end:
        raise FeedValidationError(
            [f"effective service ends {service_end}, before required horizon {required_end}"]
        )

    # A train number may occur only once per service date after revision sanitation.
    trips_by_service: Dict[str, List[Mapping]] = defaultdict(list)
    for trip in trips:
        trips_by_service[_text(trip, "service_id")].append(trip)
    conflicts: List[str] = []
    for service_date in sorted(all_dates):
        seen: Dict[Tuple[str, str, str], Tuple[str, str]] = {}
        for service_id in used_services:
            if service_date not in active.get(service_id, set()):
                continue
            for trip in trips_by_service[service_id]:
                key = (
                    _text(trip, "route_id"),
                    _text(trip, "trip_short_name"),
                    _text(trip, "direction_id"),
                )
                if key in seen:
                    other_trip, other_service = seen[key]
                    conflicts.append(
                        f"{service_date}: train {key[1]} is active as {other_trip}/{other_service} "
                        f"and {_text(trip, 'trip_id')}/{service_id}"
                    )
                else:
                    seen[key] = (_text(trip, "trip_id"), service_id)
    if conflicts:
        raise FeedValidationError(
            [f"unresolved overlapping timetable conflict: {message}" for message in conflicts[:50]]
        )

    source_start = _parse_date(_text(feed_info[0], "feed_start_date")) if feed_info else None
    source_end = _parse_date(_text(feed_info[0], "feed_end_date")) if feed_info else None
    diagnostics = {
        "qualityGate": "passed",
        "sourceCounts": {
            "stops": len(stops),
            "routes": len(routes),
            "trips": len(trips),
            "stopTimes": len(stop_times),
            "services": len(service_ids),
        },
        "effectiveServiceWindow": {
            "from": _format_date(service_start),
            "to": _format_date(service_end),
        },
        "sourceFeedWindow": {
            "from": _format_date(source_start) if source_start else None,
            "to": _format_date(source_end) if source_end else None,
        },
        "sourceWindowMatchesService": source_start == service_start and source_end == service_end,
        "revisionActions": revision_actions,
    }
    return QualityResult(sanitized_dates, service_start, service_end, diagnostics)
