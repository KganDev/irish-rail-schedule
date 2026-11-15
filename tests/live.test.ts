import assert from "node:assert/strict"
import { test } from "node:test"
import { buildLiveFile, type Feed, type VehicleFeed } from "../src/live.ts"

const feed: Feed = {
  header: { timestamp: "1794696546" },
  entity: [
    {
      trip_update: {
        trip: {
          trip_id: "6055_10141",
          start_time: "20:40:00",
          start_date: "20261114",
          schedule_relationship: "SCHEDULED",
          route_id: "1_27_c_a",
          direction_id: 0
        },
        stop_time_update: [
          { stop_sequence: 86, stop_id: "8230DB002354", schedule_relationship: "SKIPPED" },
          { stop_sequence: 87, stop_id: "8230DB002355", arrival: { delay: 140 } },
          { stop_sequence: 88, stop_id: "8230DB002356", departure: { delay: -30 } }
        ]
      }
    },
    {
      trip_update: {
        trip: { trip_id: "6055_2", start_date: "20261114", schedule_relationship: "CANCELED", route_id: "1_1_e_a" }
      }
    },
    {
      trip_update: {
        trip: { start_time: "18:40:00", start_date: "20261114", schedule_relationship: "ADDED", route_id: "1_40_d", direction_id: 1 },
        stop_time_update: [{ stop_sequence: 15, stop_id: "8220DB000277", arrival: { time: "1794689400" } }]
      }
    },
    {
      // Another operator's trip, which must be left out.
      trip_update: {
        trip: { trip_id: "6020_66428", start_date: "20261114", route_id: "2_51_c_b" },
        stop_time_update: [{ stop_sequence: 21, stop_id: "8470B6311301", arrival: { delay: -226 } }]
      }
    }
  ]
}

test("keeps only the operator's trips and counts what it kept", () => {
  const { file, stats } = buildLiveFile(feed, "1_")

  assert.equal(file.version, 1)
  assert.equal(file.feedTime, 1794696546)
  assert.deepEqual(file.trips.map(trip => trip.route), ["1_27_c_a", "1_1_e_a", "1_40_d"])
  assert.deepEqual(stats, { feedTrips: 4, trips: 3, updates: 4, vehicles: 0 })
  assert.deepEqual(file.vehicles, [])
})

test("bus positions are kept for the operator's trips", () => {
  const vehicles: VehicleFeed = {
    entity: [
      {
        vehicle: {
          trip: { trip_id: "6055_10141", start_date: "20261114", route_id: "1_27_c_a" },
          position: { latitude: 53.2095375, longitude: -6.73847294 }
        }
      },
      // Another operator, a bus with no trip, and a bus with no position.
      { vehicle: { trip: { trip_id: "6018_977", start_date: "20261114", route_id: "03C_126_e_a" }, position: { latitude: 53.2, longitude: -6.7 } } },
      { vehicle: { trip: { start_date: "20261114", route_id: "1_40_d" }, position: { latitude: 53.3, longitude: -6.2 } } },
      { vehicle: { trip: { trip_id: "6055_2", start_date: "20261114", route_id: "1_1_e_a" } } }
    ]
  }
  const { file, stats } = buildLiveFile(feed, "1_", vehicles)

  assert.deepEqual(file.vehicles, [["6055_10141", "20261114", 53.20954, -6.73847]])
  assert.equal(stats.vehicles, 1)
})

test("a running trip carries its delays and skipped stops", () => {
  const [trip] = buildLiveFile(feed, "1_").file.trips

  assert.deepEqual(trip, {
    id: "6055_10141",
    route: "1_27_c_a",
    date: "20261114",
    start: "20:40:00",
    direction: 0,
    updates: [[87, 140], [88, -30]],
    skipped: [86]
  })
})

test("cancelled and added trips are marked", () => {
  const [, canceled, added] = buildLiveFile(feed, "1_").file.trips

  assert.deepEqual(canceled, { id: "6055_2", route: "1_1_e_a", date: "20261114", canceled: true })
  assert.deepEqual(added, {
    route: "1_40_d",
    date: "20261114",
    start: "18:40:00",
    direction: 1,
    added: true,
    times: [[15, "8220DB000277", 1794689400]]
  })
})

test("an empty feed gives an empty file", () => {
  const { file, stats } = buildLiveFile({}, "1_")

  assert.deepEqual(file, { version: 1, feedTime: 0, trips: [], vehicles: [] })
  assert.deepEqual(stats, { feedTrips: 0, trips: 0, updates: 0, vehicles: 0 })
})
