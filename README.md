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
python scripts/bus_json_builder.py
node --test tests/live.test.ts
```

The builder writes to `out/` by default and does not publish from a local run.
Tests use local fixtures and run on pull requests without deployment credentials.

## Dublin Bus timetable (test)

The Dublin Bus feed has close to four million stop times, too many for an app to
download whole. `scripts/bus_json_builder.py` groups neighbouring stops into shards of
eight, so an app fetches only the shards for the stops it shows.

| Path | Contents |
| --- | --- |
| `/bus/latest.json` | The current version and its shard count. |
| `/bus/status.json` | Counts and service dates of the last successful build. |
| `/bus/<version>/stops.json` | Every stop: id, stop number, name, position, routes, and its shard. |
| `/bus/<version>/routes.json` | Route numbers and descriptions. |
| `/bus/<version>/calendar.json` | Which days each service runs, with added and removed dates. |
| `/bus/<version>/shards/<shard>.json` | Departures for each stop in the shard. |
| `/bus/<version>/trips/<route>.json` | Every stop of every trip on one route, for showing a whole journey. |

A shard lists its `routes`, `headsigns` and `services` once. Each departure is
`[seconds after midnight, trip_id, stop_sequence, route, headsign, service]`, where the
last three are positions in those lists. Times past midnight run over 86400 and belong
to the service day they started on. Stops where a trip only lets passengers off are
left out. `trip_id` and `stop_sequence` match the live feed below.

A trip file lists the route's `stops` once and its stop `patterns` as positions in that
list. Each trip is `[pattern, seconds after midnight at the first stop, seconds between
each stop and the next]`. A stop's `stop_sequence` is its place in the pattern, counting
from 1, and the last stop is included.

Versioned files are stored gzipped in R2 and sent as stored. Feeds with missing
columns, unknown trips or stops, unreadable times, too few rows, or fewer than
`MIN_COVERAGE_DAYS` of service left are rejected before anything is written.

| Setting | Default | Meaning |
| --- | --- | --- |
| `STOPS_PER_SHARD` | `8` | Stops grouped into one file. |
| `MIN_BUS_STOPS` | `1000` | Fewest stops a publishable feed may have. |
| `MIN_BUS_TRIPS` | `10000` | Fewest trips. |
| `MIN_BUS_DEPARTURES` | `1000000` | Fewest departures. |

Pushing to `big-test` builds and uploads it under `bus/` in the bucket.

## Live Dublin Bus feed (test)

The NTA allows each key one call to its GTFS Realtime feed every 60 seconds, so apps
cannot call it directly. A Durable Object in the Worker fetches the feed about once a
minute, keeps only Dublin Bus trips, and writes one small file to R2 that every app
reads.

| Path | Contents |
| --- | --- |
| `/live/dublin-bus.json` | Delays, skipped stops, cancelled and added trips, and where each bus is. `feedTime` is when the NTA produced the data. |
| `/live/status.json` | Run and failure counts, sizes, and the last error. |

In each trip, `updates` holds `[stop_sequence, seconds late]` pairs. A delay applies to
the stops after it until the next pair. `vehicles` holds
`[trip_id, service date, latitude, longitude]` for each bus, and is empty when the NTA
positions feed could not be read.

This runs as a separate test Worker, `dartdublin-live-test`, set up in
`wrangler.live-test.toml`. Pushing to `big-test` deploys it and sets the `NTA_API`
secret from the repository secret of the same name. The production Worker is not
changed: without the `LIVE` binding it only serves the timetable.

To run it locally, put `NTA_API=<key>` in `.dev.vars` (ignored by git), then:

```sh
npx wrangler dev --config wrangler.live-test.toml --test-scheduled
curl "http://localhost:8787/__scheduled?cron=*/5+*+*+*+*"
curl http://localhost:8787/live/status.json
```

Data from the NTA is licensed under Creative Commons Attribution 4.0.
