import csv
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

import gtfs_json_builder as builder


class GtfsJsonBuilderTest(unittest.TestCase):
    def test_build_preserves_disjoint_service_periods(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            source = temp / "fixture.zip"
            output = temp / "out"
            self._write_fixture(source)

            builder.build(
                source.as_uri(),
                output,
                date(2026, 8, 10),
                2,
                minimum_trips=1,
                minimum_stop_times=2,
            )

            latest = json.loads((output / "latest.json").read_text())
            self.assertEqual(latest["latest"], "kg-26-fixture-1")

            version_dir = output / "gtfs" / latest["latest"]
            trips = json.loads((version_dir / "trips.json").read_text())
            stop_times = json.loads((version_dir / "stop_times.json").read_text())
            calendars = json.loads((version_dir / "calendar.json").read_text())
            calendar_dates = json.loads((version_dir / "calendar_dates.json").read_text())

            self.assertEqual(
                {trip["trip_id"] for trip in trips},
                {"current-early", "future-late"},
            )
            self.assertEqual(
                {calendar["service_id"] for calendar in calendars},
                {"current", "future"},
            )
            self.assertEqual(len(stop_times), 4)
            self.assertEqual(
                {stop_time["trip_id"] for stop_time in stop_times},
                {"current-early", "future-late"},
            )
            self.assertEqual(
                calendar_dates,
                [{"service_id": "current", "date": "20260815", "exception_type": "2"}],
            )
            self.assertIn(
                {
                    "trip_id": "current-early",
                    "arrival_time": "06:00:00",
                    "departure_time": "06:00:00",
                    "stop_id": "HOWTH",
                    "stop_sequence": 1,
                    "stop_headsign": "",
                    "pickup_type": "0",
                    "drop_off_type": "1",
                    "timepoint": "1",
                },
                stop_times,
            )

    def test_overlapping_old_revision_is_removed_only_on_shared_dates(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            source = temp / "overlap.zip"
            output = temp / "out"
            self._write_overlap_fixture(source)

            builder.build(
                source.as_uri(),
                output,
                date(2026, 8, 10),
                2,
                minimum_trips=1,
                minimum_stop_times=2,
            )

            latest = json.loads((output / "latest.json").read_text())
            version_dir = output / "gtfs" / latest["latest"]
            calendar_dates = json.loads((version_dir / "calendar_dates.json").read_text())
            removals = {
                row["date"]
                for row in calendar_dates
                if row["service_id"] == "old" and row["exception_type"] == "2"
            }
            self.assertEqual(removals, {"20260811", "20260812", "20260813", "20260814"})

            status = json.loads((output / "status.json").read_text())
            self.assertEqual(
                status["validation"]["revisionActions"][0]["olderService"],
                "old",
            )
            self.assertEqual(
                status["validation"]["revisionActions"][0]["newerService"],
                "new",
            )

    def test_malformed_feed_fails_before_publish(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            source = temp / "broken.zip"
            output = temp / "out"
            with zipfile.ZipFile(source, "w") as archive:
                archive.writestr(
                    "agency.txt",
                    self._csv(
                        ["agency_name", "agency_url", "agency_timezone"],
                        [["Irish Rail", "https://example.test", "Europe/Dublin"]],
                    ),
                )

            with self.assertRaises(builder.FeedValidationError):
                builder.build(
                    source.as_uri(),
                    output,
                    date(2026, 8, 10),
                    2,
                    minimum_trips=0,
                    minimum_stop_times=0,
                )
            self.assertFalse((output / "latest.json").exists())

    def test_unresolved_overlapping_timetables_fail_before_publish(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            source = temp / "ambiguous.zip"
            output = temp / "out"
            self._write_overlap_fixture(source, compatible=False)

            with self.assertRaises(builder.FeedValidationError):
                builder.build(
                    source.as_uri(),
                    output,
                    date(2026, 8, 10),
                    2,
                    minimum_trips=1,
                    minimum_stop_times=2,
                )
            self.assertFalse((output / "latest.json").exists())

    def _write_fixture(self, path):
        files = {
            "agency.txt": self._csv(
                ["agency_id", "agency_name", "agency_url", "agency_timezone"],
                [["IE", "Irish Rail", "https://example.test", "Europe/Dublin"]],
            ),
            "feed_info.txt": self._csv(
                [
                    "feed_publisher_name",
                    "feed_publisher_url",
                    "feed_lang",
                    "feed_start_date",
                    "feed_end_date",
                    "feed_version",
                ],
                [["TFI", "https://example.test", "en", "20260807", "20261211", "fixture-1"]],
            ),
            "stops.txt": self._csv(
                ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon"],
                [
                    ["HOWTH", "1", "Howth", "53.389", "-6.074"],
                    ["CITY", "2", "City", "53.350", "-6.260"],
                ],
            ),
            "routes.txt": self._csv(
                ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
                [["DART", "IE", "DART", "Howth", "2"]],
            ),
            "trips.txt": self._csv(
                ["route_id", "service_id", "trip_id", "trip_headsign", "trip_short_name", "direction_id"],
                [
                    ["DART", "current", "current-early", "Bray", "E201", "1"],
                    ["DART", "future", "future-late", "Bray", "E215", "1"],
                ],
            ),
            "stop_times.txt": self._csv(
                [
                    "trip_id",
                    "arrival_time",
                    "departure_time",
                    "stop_id",
                    "stop_sequence",
                    "stop_headsign",
                    "pickup_type",
                    "drop_off_type",
                    "timepoint",
                ],
                [
                    ["current-early", "06:00:00", "06:00:00", "HOWTH", "1", "", "0", "1", "1"],
                    ["current-early", "06:30:00", "06:30:00", "CITY", "2", "", "1", "0", "1"],
                    ["future-late", "09:20:00", "09:20:00", "HOWTH", "1", "", "0", "1", "1"],
                    ["future-late", "09:50:00", "09:50:00", "CITY", "2", "", "1", "0", "1"],
                ],
            ),
            "calendar.txt": self._csv(
                [
                    "service_id",
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                    "start_date",
                    "end_date",
                ],
                [
                    ["current", "1", "1", "1", "1", "1", "0", "0", "20260807", "20260918"],
                    ["future", "1", "1", "1", "1", "1", "0", "0", "20260921", "20261211"],
                ],
            ),
            "calendar_dates.txt": self._csv(
                ["service_id", "date", "exception_type"],
                [["current", "20260815", "2"]],
            ),
        }

        with zipfile.ZipFile(path, "w") as archive:
            for name, contents in files.items():
                archive.writestr(name, contents)

    def _write_overlap_fixture(self, path, *, compatible=True):
        trips = []
        stop_times = []
        for index in range(20):
            train = f"E{200 + index}"
            old_trip = f"old-{train}"
            new_trip = f"new-{train}"
            trips.extend(
                [
                    ["DART", "old", old_trip, train, "1"],
                    ["DART", "new", new_trip, train, "1"],
                ]
            )
            minute = 10 + index
            new_departure = (
                f"06:{minute - 3:02d}:00" if compatible else f"05:{minute + 30:02d}:00"
            )
            stop_times.extend(
                [
                    [old_trip, f"06:{minute:02d}:00", f"06:{minute:02d}:00", "HOWTH", "1"],
                    [old_trip, f"07:{minute:02d}:00", f"07:{minute:02d}:00", "CITY", "2"],
                    [new_trip, new_departure, new_departure, "HOWTH", "1"],
                    [new_trip, f"06:{minute + 27:02d}:00", f"06:{minute + 27:02d}:00", "WBROK", "2"],
                    [new_trip, f"07:{minute:02d}:00", f"07:{minute:02d}:00", "CITY", "3"],
                ]
            )
        trips.extend(
            [
                ["DART", "old", "old-only", "D900", "1"],
                ["DART", "new", "new-only", "D901", "1"],
            ]
        )
        stop_times.extend(
            [
                ["old-only", "12:00:00", "12:00:00", "HOWTH", "1"],
                ["old-only", "12:30:00", "12:30:00", "CITY", "2"],
                ["new-only", "13:00:00", "13:00:00", "HOWTH", "1"],
                ["new-only", "13:30:00", "13:30:00", "CITY", "2"],
            ]
        )
        files = {
            "agency.txt": self._csv(
                ["agency_id", "agency_name", "agency_url", "agency_timezone"],
                [["IE", "Irish Rail", "https://example.test", "Europe/Dublin"]],
            ),
            "feed_info.txt": self._csv(
                ["feed_start_date", "feed_end_date", "feed_version"],
                [["20260810", "20261211", "overlap-1"]],
            ),
            "stops.txt": self._csv(
                ["stop_id", "stop_name", "stop_lat", "stop_lon"],
                [
                    ["HOWTH", "Howth", "53.389", "-6.074"],
                    ["WBROK", "Woodbrook", "53.20", "-6.10"],
                    ["CITY", "City", "53.350", "-6.260"],
                ],
            ),
            "routes.txt": self._csv(
                ["route_id", "route_short_name", "route_type"],
                [["DART", "DART", "2"]],
            ),
            "trips.txt": self._csv(
                ["route_id", "service_id", "trip_id", "trip_short_name", "direction_id"],
                trips,
            ),
            "stop_times.txt": self._csv(
                ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
                stop_times,
            ),
            "calendar.txt": self._csv(
                ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
                [
                    ["old", "1", "1", "1", "1", "1", "0", "0", "20260810", "20260814"],
                    ["new", "1", "1", "1", "1", "1", "0", "0", "20260811", "20261211"],
                ],
            ),
            "calendar_dates.txt": self._csv(
                ["service_id", "date", "exception_type"],
                [],
            ),
        }
        with zipfile.ZipFile(path, "w") as archive:
            for name, contents in files.items():
                archive.writestr(name, contents)

    @staticmethod
    def _csv(fieldnames, rows):
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(fieldnames)
        writer.writerows(rows)
        return output.getvalue()


if __name__ == "__main__":
    unittest.main()
