import csv
import gzip
import io
import json
import sys
import tempfile
import unittest
import zipfile
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import bus_json_builder as builder
import bus_upload_list
from gtfs_quality import FeedValidationError


TARGET = date(2026, 9, 14)

FIXTURE = {
    "agency.txt": (
        ["agency_id", "agency_name", "agency_url", "agency_timezone"],
        [["1", "Dublin Bus", "https://example.test", "Europe/Dublin"]],
    ),
    "stops.txt": (
        ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon"],
        [
            # Two pairs of facing stops, far apart, and a terminus.
            ["A1", "101", "Tallaght Road", "53.3000", "-6.3000"],
            ["A2", "102", "Tallaght Road", "53.3001", "-6.3001"],
            ["B1", "201", "Howth Road", "53.4000", "-6.1000"],
            ["B2", "202", "Howth Road", "53.4001", "-6.1001"],
            ["Z", "900", "Terminus", "53.5000", "-6.0000"],
        ],
    ),
    "routes.txt": (
        ["route_id", "route_short_name", "route_long_name"],
        [["r77a", "77A", "Citywest - Ringsend"], ["r27", "27", "Jobstown - Clare Hall"]],
    ),
    "trips.txt": (
        ["route_id", "service_id", "trip_id", "trip_headsign"],
        [
            ["r27", "weekday", "t1", "Jobstown"],
            ["r77a", "weekday", "t2", "Citywest"],
            ["r27", "saturday", "t3", "Jobstown"],
        ],
    ),
    "stop_times.txt": (
        ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "stop_headsign", "pickup_type"],
        [
            ["t1", "08:00:00", "08:00:00", "A1", "1", "Via Town", "0"],
            ["t1", "08:10:00", "08:10:30", "B1", "2", "", "0"],
            ["t1", "08:30:00", "08:30:00", "Z", "3", "", "1"],
            ["t2", "07:30:00", "07:30:00", "A1", "1", "", "0"],
            ["t2", "07:31:00", "07:31:00", "A2", "2", "", "0"],
            ["t2", "07:50:00", "07:50:00", "Z", "3", "", "1"],
            ["t3", "24:10:00", "24:10:00", "A1", "1", "", "0"],
            ["t3", "24:40:00", "24:40:00", "Z", "2", "", "1"],
        ],
    ),
    "calendar.txt": (
        ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
        [
            ["weekday", "1", "1", "1", "1", "1", "0", "0", "20260901", "20261231"],
            ["saturday", "0", "0", "0", "0", "0", "1", "0", "20260901", "20261231"],
        ],
    ),
    "calendar_dates.txt": (
        ["service_id", "date", "exception_type"],
        [["weekday", "20261225", "2"], ["saturday", "20261225", "1"]],
    ),
    "feed_info.txt": (["feed_version"], [["fixture"]]),
}


def write_feed(path, changes=None):
    files = {**FIXTURE, **(changes or {})}
    with zipfile.ZipFile(path, "w") as archive:
        for name, (header, rows) in files.items():
            out = io.StringIO()
            writer = csv.writer(out)
            writer.writerow(header)
            writer.writerows(rows)
            archive.writestr(name, out.getvalue())


class BusJsonBuilderTest(unittest.TestCase):
    def build(self, temp, changes=None, **options):
        write_feed(temp / "feed.zip", changes)
        settings = {"stops_per_shard": 2, "minimum_stops": 1, "minimum_trips": 1, "minimum_departures": 1}
        builder.build((temp / "feed.zip").as_uri(), temp / "out", TARGET, **{**settings, **options})
        latest = json.loads((temp / "out" / "bus" / "latest.json").read_text())
        return temp / "out" / "bus" / latest["latest"], latest

    def read(self, version_dir, name):
        return json.loads((version_dir / name).read_text())

    def departures(self, version_dir, stop_id):
        stop = next(stop for stop in self.read(version_dir, "stops.json") if stop["id"] == stop_id)
        shard = self.read(version_dir, f"shards/{stop['shard']}.json")
        return [
            (seconds, trip, sequence, shard["routes"][route], shard["headsigns"][headsign], shard["services"][service])
            for seconds, trip, sequence, route, headsign, service in shard["stops"][stop_id]
        ]

    def test_each_stop_lists_its_departures_in_time_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            version_dir, latest = self.build(Path(temp_dir))
            self.assertEqual(latest["latest"], "bus-1-fixture")
            self.assertEqual(
                self.departures(version_dir, "A1"),
                [
                    (27000, "t2", 1, "77A", "Citywest", "weekday"),
                    # The stop's own headsign wins over the trip's.
                    (28800, "t1", 1, "27", "Via Town", "weekday"),
                    # Times after midnight stay on the service day they belong to.
                    (87000, "t3", 1, "27", "Jobstown", "saturday"),
                ],
            )
            self.assertEqual(self.departures(version_dir, "B1"), [(29430, "t1", 2, "27", "Jobstown", "weekday")])

    def test_a_stop_that_only_lets_passengers_off_has_no_departures(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            version_dir, _ = self.build(Path(temp_dir))
            self.assertEqual(self.departures(version_dir, "Z"), [])
            terminus = next(stop for stop in self.read(version_dir, "stops.json") if stop["id"] == "Z")
            self.assertEqual(terminus["routes"], [])

    def test_stops_carry_their_routes_and_share_a_shard_with_neighbours(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            version_dir, latest = self.build(Path(temp_dir))
            stops = {stop["id"]: stop for stop in self.read(version_dir, "stops.json")}
            self.assertEqual(stops["A1"]["routes"], ["27", "77A"])
            self.assertEqual(stops["A1"]["code"], "101")
            self.assertEqual(stops["A1"]["shard"], stops["A2"]["shard"])
            self.assertEqual(stops["B1"]["shard"], stops["B2"]["shard"])
            self.assertNotEqual(stops["A1"]["shard"], stops["B1"]["shard"])
            self.assertEqual(latest["shards"], 3)
            self.assertEqual(len(list((version_dir / "shards").iterdir())), 3)

    def test_routes_and_calendar_are_published(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            version_dir, _ = self.build(Path(temp_dir))
            self.assertEqual([route["name"] for route in self.read(version_dir, "routes.json")], ["27", "77A"])
            calendar = self.read(version_dir, "calendar.json")
            self.assertEqual(calendar["timezone"], "Europe/Dublin")
            self.assertEqual(
                calendar["services"]["weekday"],
                {
                    "days": [True, True, True, True, True, False, False],
                    "start": "20260901",
                    "end": "20261231",
                    "added": [],
                    "removed": ["20261225"],
                },
            )
            self.assertEqual(calendar["services"]["saturday"]["added"], ["20261225"])

    def test_status_reports_counts_and_service_dates(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            version_dir, _ = self.build(Path(temp_dir))
            status = json.loads((version_dir.parent / "status.json").read_text())
            self.assertTrue(status["ok"])
            self.assertEqual(status["counts"], {"stops": 5, "routes": 2, "trips": 3, "departures": 5, "shards": 3})
            self.assertEqual(status["service"], {"startDate": "20260901", "endDate": "20261231"})

    def test_rejected_feeds_write_nothing(self):
        calendar_header = FIXTURE["calendar.txt"][0]
        stop_times_header, stop_times = FIXTURE["stop_times.txt"]
        cases = {
            "service ends": {
                "calendar.txt": (calendar_header, [["weekday", "1", "1", "1", "1", "1", "0", "0", "20260901", "20260916"]]),
                "calendar_dates.txt": (FIXTURE["calendar_dates.txt"][0], []),
            },
            "trip that trips.txt does not have": {"stop_times.txt": (stop_times_header, stop_times + [["ghost", "09:00:00", "09:00:00", "A1", "1", "", "0"]])},
            "stop that stops.txt does not have": {"stop_times.txt": (stop_times_header, stop_times + [["t1", "09:00:00", "09:00:00", "nowhere", "4", "", "0"]])},
            "unreadable time": {"stop_times.txt": (stop_times_header, stop_times + [["t1", "soon", "soon", "A2", "4", "", "0"]])},
            "lacks stop_sequence": {"stop_times.txt": (["trip_id", "departure_time", "stop_id"], [["t1", "08:00:00", "A1"]])},
        }
        for message, changes in cases.items():
            with self.subTest(message=message), tempfile.TemporaryDirectory() as temp_dir:
                with self.assertRaisesRegex(FeedValidationError, message):
                    self.build(Path(temp_dir), changes)
                self.assertFalse((Path(temp_dir) / "out").exists())

    def test_a_truncated_feed_is_rejected_by_the_minimum_counts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with self.assertRaisesRegex(FeedValidationError, "only 5 departures; at least 6 are required"):
                self.build(Path(temp_dir), minimum_departures=6)

    def test_upload_list_gzips_every_file_under_its_published_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            version_dir, _ = self.build(Path(temp_dir))
            entries = bus_upload_list.pack(version_dir, Path(temp_dir) / "staging")
            keys = [entry["key"] for entry in entries]
            self.assertIn("bus/bus-1-fixture/stops.json", keys)
            self.assertIn("bus/bus-1-fixture/shards/000.json", keys)
            self.assertEqual(len(keys), 6)
            for entry in entries:
                source = version_dir / entry["key"].removeprefix("bus/bus-1-fixture/")
                self.assertEqual(gzip.decompress(Path(entry["file"]).read_bytes()), source.read_bytes())


if __name__ == "__main__":
    unittest.main()
