import csv
import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import zipfile
from datetime import date, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import gtfs_json_builder as builder


class GtfsJsonBuilderTest(unittest.TestCase):
    def test_feed_remains_publishable_after_crossing_the_scan_horizon(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            self._write_coverage_fixture(temp / "coverage.zip")
            for target, truncated in (
                (date(2026, 9, 13), False),
                (date(2026, 9, 14), True),
                (date(2026, 9, 15), True),
            ):
                with self.subTest(target=target):
                    windows, status, log = self._build_coverage(temp, target_date=target)
                    self.assertEqual(windows["scan"], {"from": target.strftime("%Y%m%d"), "to": "20261212"})
                    self.assertEqual(windows["feed"]["endDate"], "20261212")
                    self.assertEqual(windows["feed"]["sourceEndDate"], "20270913")
                    self.assertEqual(windows["windows"][-1]["to"], "20261212")
                    self.assertTrue(all(window["from"] <= window["to"] <= "20261212" for window in windows["windows"]))
                    self.assertTrue(status["ok"])
                    self.assertEqual(status["validation"]["coverage"]["scanTruncated"], truncated)
                    self.assertEqual(status["validation"]["warnings"], [])
                    if truncated:
                        self.assertIn("scan limited to the available service dates", log)

    def test_full_feed_keeps_the_requested_scan_and_all_future_trips(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            self._write_coverage_fixture(temp / "coverage.zip", end="20261231")
            windows, status, _ = self._build_coverage(temp)
            self.assertEqual(windows["scan"]["to"], "20261213")
            self.assertEqual(windows["windows"][-1]["to"], "20261213")
            self.assertFalse(status["validation"]["coverage"]["scanTruncated"])
            version_dir = temp / "out" / "gtfs" / status["latest"]
            self.assertEqual(len(json.loads((version_dir / "trips.json").read_text())), 2)
            self.assertTrue(all(row["end_date"] == "20261231" for row in json.loads((version_dir / "calendar.json").read_text())))

    def test_coverage_warning_and_minimum_boundaries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            self._write_coverage_fixture(temp / "coverage.zip")
            for days, warned in ((30, False), (29, True), (7, True)):
                with self.subTest(days=days):
                    target = date(2026, 12, 12) - timedelta(days=days)
                    windows, status, log = self._build_coverage(temp, target_date=target)
                    coverage = status["validation"]["coverage"]
                    self.assertEqual(coverage["daysRemaining"], days)
                    self.assertEqual(coverage["requiredThrough"], (target + timedelta(days=7)).strftime("%Y%m%d"))
                    self.assertEqual(coverage["scanThrough"], windows["scan"]["to"])
                    self.assertEqual(bool(status["validation"]["warnings"]), warned)
                    if warned:
                        self.assertIn(f"only {days} days ahead", log)

    def test_rejected_coverage_preserves_every_existing_output_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            self._write_coverage_fixture(temp / "coverage.zip")
            self._build_coverage(temp)
            output = temp / "out"
            before = {p.relative_to(output): p.read_bytes() for p in output.rglob("*") if p.is_file()}
            for target, expected in (
                (date(2026, 12, 6), "before minimum coverage date"),
                (date(2026, 12, 12), "before minimum coverage date"),
                (date(2026, 12, 13), "outside effective service"),
                (date(2026, 8, 31), "outside effective service"),
            ):
                with self.subTest(target=target):
                    with self.assertRaisesRegex(builder.FeedValidationError, expected):
                        self._build_coverage(temp, target_date=target)
                    after = {p.relative_to(output): p.read_bytes() for p in output.rglob("*") if p.is_file()}
                    self.assertEqual(before, after)

    def test_scan_length_cannot_bypass_minimum_coverage(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            self._write_coverage_fixture(temp / "coverage.zip", end="20260920")
            with self.assertRaisesRegex(builder.FeedValidationError, "2026-09-21"):
                self._build_coverage(temp, window_days=2)
            self.assertFalse((temp / "out").exists())

    def test_custom_minimum_is_honoured(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            self._write_coverage_fixture(temp / "coverage.zip")
            with self.assertRaisesRegex(builder.FeedValidationError, "90 days ahead"):
                self._build_coverage(temp, minimum_coverage_days=90, coverage_warning_days=90)

    def test_zero_day_scan_includes_the_target_date(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            self._write_coverage_fixture(temp / "coverage.zip")
            windows, _, _ = self._build_coverage(temp, window_days=0)
            self.assertEqual(windows["scan"], {"from": "20260914", "to": "20260914"})
            self.assertEqual(windows["windows"], [{"from": "20260914", "to": "20260914"}])

    def test_calendar_exceptions_determine_the_actual_service_end(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            additions = [(service, "20260921", "1") for service in ("current", "future")]
            self._write_coverage_fixture(temp / "coverage.zip", end="20260920", exceptions=additions)
            windows, _, _ = self._build_coverage(temp)
            self.assertEqual(windows["scan"]["to"], "20260921")

            removals = [(service, "20260921", "2") for service in ("current", "future")]
            self._write_coverage_fixture(temp / "coverage.zip", end="20260921", exceptions=removals)
            with self.assertRaisesRegex(builder.FeedValidationError, "effective service ends 2026-09-20"):
                self._build_coverage(temp)

    def test_unused_calendar_cannot_extend_coverage(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            self._write_coverage_fixture(temp / "coverage.zip", end="20260920", unused_end="20270913")
            with self.assertRaisesRegex(builder.FeedValidationError, "effective service ends 2026-09-20"):
                self._build_coverage(temp)

    def test_calendar_dates_only_feed_is_supported(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            exceptions = [
                (service, (date(2026, 9, 14) + timedelta(days=offset)).strftime("%Y%m%d"), "1")
                for service in ("current", "future") for offset in range(8)
            ]
            self._write_coverage_fixture(temp / "coverage.zip", exceptions=exceptions, exceptions_only=True)
            windows, status, _ = self._build_coverage(temp)
            self.assertEqual(windows["scan"]["to"], "20260921")
            self.assertEqual(status["validation"]["coverage"]["daysRemaining"], 7)

    def test_explicit_no_service_day_is_preserved(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            exceptions = [(service, "20260915", "2") for service in ("current", "future")]
            self._write_coverage_fixture(temp / "coverage.zip", exceptions=exceptions)
            windows, status, _ = self._build_coverage(temp)
            self.assertIn({"from": "20260915", "to": "20260915"}, windows["windows"])
            path = temp / "out" / "gtfs" / status["latest"] / "calendar_dates.json"
            self.assertEqual(json.loads(path.read_text()), [
                {"service_id": service, "date": "20260915", "exception_type": "2"}
                for service in ("current", "future")
            ])

    def test_invalid_coverage_settings_fail_before_output(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            self._write_coverage_fixture(temp / "coverage.zip")
            for options in (
                {"window_days": -1},
                {"window_days": 1.5},
                {"window_days": True},
                {"window_days": 10**20},
                {"minimum_coverage_days": 0},
                {"minimum_coverage_days": -1},
                {"coverage_warning_days": 6},
            ):
                with self.subTest(options=options):
                    with self.assertRaises(builder.FeedValidationError):
                        self._build_coverage(temp, **options)
                    self.assertFalse((temp / "out").exists())

    def test_cli_coverage_settings_and_warning_annotation(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            source = temp / "coverage.zip"
            self._write_coverage_fixture(source, end="20261004")
            environment = {
                **os.environ,
                "GTFS_URL": source.as_uri(), "OUT_DIR": str(temp / "out"),
                "TARGET_DATE": "20260914", "WINDOW_DAYS": "90",
                "MIN_COVERAGE_DAYS": "14", "COVERAGE_WARNING_DAYS": "21",
                "MIN_TRIPS": "1", "MIN_STOP_TIMES": "2",
                "REVISION_SIMILARITY": "0.9", "REVISION_MIN_SHARED_TRIPS": "20",
                "GITHUB_ACTIONS": "true",
            }
            command = [sys.executable, str(ROOT / "scripts" / "gtfs_json_builder.py")]
            result = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("::warning::effective service ends 2026-10-04, only 20 days ahead", result.stdout)
            status = json.loads((temp / "out" / "status.json").read_text())
            self.assertEqual(status["validation"]["coverage"]["minimumDays"], 14)
            self.assertEqual(status["validation"]["coverage"]["warningDays"], 21)
            before = (temp / "out" / "latest.json").read_bytes()
            for invalid in (
                {"TARGET_DATE": "20260931"},
                {"TARGET_DATE": "202609 1"},
                {"WINDOW_DAYS": "invalid"},
            ):
                with self.subTest(invalid=invalid):
                    result = subprocess.run(command, env={**environment, **invalid}, capture_output=True, text=True, timeout=30)
                    self.assertEqual(result.returncode, 1)
                    self.assertIn("Invalid builder configuration:", result.stderr)
                    self.assertEqual((temp / "out" / "latest.json").read_bytes(), before)

    def _build_coverage(self, temp, *, target_date=date(2026, 9, 14), window_days=90, **options):
        output = temp / "out"
        log = io.StringIO()
        with contextlib.redirect_stdout(log):
            builder.build(
                (temp / "coverage.zip").as_uri(), output, target_date, window_days,
                minimum_trips=1, minimum_stop_times=2, **options,
            )
        return (
            json.loads((output / "windows.json").read_text()),
            json.loads((output / "status.json").read_text()),
            log.getvalue(),
        )

    def _write_coverage_fixture(self, path, *, end="20261212", exceptions=(), unused_end=None, exceptions_only=False):
        self._write_fixture(path)
        with zipfile.ZipFile(path) as archive:
            files = {name: archive.read(name).decode() for name in archive.namelist()}
        files["feed_info.txt"] = self._csv(
            ["feed_start_date", "feed_end_date", "feed_version"],
            [["20260901", "20270913", "coverage-1"]],
        )
        calendars = [[service, *(["1"] * 7), "20260901", end] for service in ("current", "future")]
        if unused_end:
            calendars.append(["unused", *(["1"] * 7), "20260901", unused_end])
        files["calendar.txt"] = self._csv(
            ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
            calendars,
        )
        if exceptions_only:
            del files["calendar.txt"]
        files["calendar_dates.txt"] = self._csv(["service_id", "date", "exception_type"], exceptions)
        with zipfile.ZipFile(path, "w") as archive:
            for name, contents in files.items():
                archive.writestr(name, contents)

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

    def test_newer_subset_cannot_delete_broader_service(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            source = temp / "subset.zip"
            output = temp / "out"
            self._write_overlap_fixture(source, extra_old_trips=79)

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

    def _write_overlap_fixture(self, path, *, compatible=True, extra_old_trips=0):
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
        for index in range(extra_old_trips):
            trip_id = f"old-extra-{index}"
            trips.append(["DART", "old", trip_id, f"X{index:03d}", "1"])
            stop_times.extend(
                [
                    [trip_id, "14:00:00", "14:00:00", "HOWTH", "1"],
                    [trip_id, "14:30:00", "14:30:00", "CITY", "2"],
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
