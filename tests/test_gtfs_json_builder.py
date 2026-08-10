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

            builder.build(source.as_uri(), output, date(2026, 8, 10), 2)

            latest = json.loads((output / "latest.json").read_text())
            self.assertEqual(latest["latest"], "kg-fixture-1")

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
                ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id"],
                [
                    ["DART", "current", "current-early", "Bray", "1"],
                    ["DART", "future", "future-late", "Bray", "1"],
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

    @staticmethod
    def _csv(fieldnames, rows):
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(fieldnames)
        writer.writerows(rows)
        return output.getvalue()


if __name__ == "__main__":
    unittest.main()
