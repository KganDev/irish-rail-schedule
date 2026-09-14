# Irish Rail schedule

Builds the Irish Rail GTFS feed into versioned JSON for the schedule API. The daily
workflow downloads one source archive, validates it, builds the JSON, and publishes
it to R2. Changes to the builder, publishing workflow, or Worker on `main` also
trigger a build. Publishing runs are serialized and restricted to `main`.

## Service coverage

Coverage comes from actual trip-bearing services in `calendar.txt` and
`calendar_dates.txt`, after timetable revision sanitation. The publisher's
`feed_info.txt` dates are retained for reference but cannot extend actual service.

| Setting | Default | Meaning |
| --- | --- | --- |
| `WINDOW_DAYS` | `90` | Desired scan through the target date plus this many days. Zero scans only the target date. |
| `MIN_COVERAGE_DAYS` | `7` | Required service end, at least this many days after the target date. Must be positive. |
| `COVERAGE_WARNING_DAYS` | `30` | Warn when fewer days remain. Must be at least `MIN_COVERAGE_DAYS`. |
| `TARGET_DATE` | Agency's current date | Optional date override in `YYYYMMDD` format. |

The scan ends at the earlier of the requested horizon and the actual service end.
All scan dates are inclusive. For example, on 14 September 2026 the seven day
minimum requires an effective service end on or after 21 September. A feed ending
on 12 December is accepted, and the 90 day scan stops on 12 December.

The minimum is independent of the scan length. Reducing `WINDOW_DAYS` cannot bypass
the publication minimum. The defaults follow the GTFS recommendation of at least
[seven days of validity, with 30 days preferred](https://gtfs.org/documentation/schedule/schedule-best-practices/#dataset-publishing-general-practices).
These are calendar coverage limits, not a promise that trains run every day;
weekday patterns, holidays, and explicit service exceptions remain intact.

`windows.json` reports the actual scan range. `status.json` includes the remaining
coverage, requested and actual scan endpoints, whether the scan was shortened, and
any warnings. Short scans are reported in the build log; low coverage also creates
a GitHub Actions warning.

Malformed, expired, future-only, conflicting, or insufficiently covered feeds fail
before output is written. The workflow skips publishing after any validation or
build failure, preserving the previously published data. Rejected builds do not
refresh the published `status.json`; its `generatedAt` describes the last successful
build. Check Actions for the latest attempt's result.

## Local checks

```sh
python -m unittest discover -s tests -v
python scripts/gtfs_json_builder.py
```

The builder writes to `out/` by default and does not publish from a local run.
Tests use local fixtures and run on pull requests without deployment credentials.
