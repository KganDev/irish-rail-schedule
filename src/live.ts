// Shrinks the NTA GTFS Realtime trip updates feed to the trips of one operator.
// Kept free of Worker APIs so it runs under `node --test`.

export interface FeedStopTimeEvent {
  delay?: number
  time?: string | number
}

export interface FeedStopTimeUpdate {
  stop_sequence?: number
  stop_id?: string
  schedule_relationship?: string
  arrival?: FeedStopTimeEvent
  departure?: FeedStopTimeEvent
}

export interface FeedEntity {
  trip_update?: {
    trip?: {
      trip_id?: string
      route_id?: string
      start_date?: string
      start_time?: string
      direction_id?: number
      schedule_relationship?: string
    }
    stop_time_update?: FeedStopTimeUpdate[]
  }
}

export interface Feed {
  header?: { timestamp?: string | number }
  entity?: FeedEntity[]
}

export interface VehicleFeed {
  entity?: Array<{
    vehicle?: {
      trip?: { trip_id?: string; route_id?: string; start_date?: string }
      position?: { latitude?: number; longitude?: number }
    }
  }>
}

export interface LiveTrip {
  // Missing for a trip the operator added on the day, which the timetable does not have.
  id?: string
  route: string
  date: string
  start?: string
  direction?: number
  canceled?: true
  added?: true
  // [stop_sequence, seconds late]. Negative is early. A delay holds for the stops after
  // it until the next entry.
  updates?: Array<[number, number]>
  // stop_sequence of each stop the trip will not serve.
  skipped?: number[]
  // [stop_sequence, stop_id, unix seconds], for stops given a time and no delay.
  times?: Array<[number, string, number]>
}

export interface LiveFile {
  version: 1
  // Unix seconds the NTA stamped on the feed.
  feedTime: number
  trips: LiveTrip[]
  // Where each bus is: [trip_id, service date, latitude, longitude]. Empty when the
  // positions feed could not be read.
  vehicles: Array<[string, string, number, number]>
}

export interface LiveStats {
  feedTrips: number
  trips: number
  updates: number
  vehicles: number
}

function toNumber(value: string | number | undefined): number | undefined {
  if (value === undefined || value === "") return undefined
  const n = Number(value)
  return Number.isFinite(n) ? n : undefined
}

export function buildLiveFile(
  feed: Feed,
  routePrefix: string,
  vehicleFeed?: VehicleFeed
): { file: LiveFile; stats: LiveStats } {
  const entities = feed.entity ?? []
  const trips: LiveTrip[] = []
  let updateCount = 0

  for (const entity of entities) {
    const tripUpdate = entity.trip_update
    const trip = tripUpdate?.trip
    if (!trip?.route_id || !trip.start_date || !trip.route_id.startsWith(routePrefix)) continue

    const out: LiveTrip = { route: trip.route_id, date: trip.start_date }
    if (trip.trip_id) out.id = trip.trip_id
    if (trip.start_time) out.start = trip.start_time
    if (trip.direction_id !== undefined) out.direction = trip.direction_id
    if (trip.schedule_relationship === "CANCELED") out.canceled = true
    if (trip.schedule_relationship === "ADDED") out.added = true

    const updates: Array<[number, number]> = []
    const skipped: number[] = []
    const times: Array<[number, string, number]> = []
    for (const update of tripUpdate?.stop_time_update ?? []) {
      const sequence = update.stop_sequence
      if (sequence === undefined) continue
      if (update.schedule_relationship === "SKIPPED") {
        skipped.push(sequence)
        continue
      }
      const delay = update.arrival?.delay ?? update.departure?.delay
      if (delay !== undefined) {
        updates.push([sequence, delay])
        continue
      }
      const time = toNumber(update.arrival?.time ?? update.departure?.time)
      if (time !== undefined && update.stop_id) times.push([sequence, update.stop_id, time])
    }
    if (updates.length) out.updates = updates
    if (skipped.length) out.skipped = skipped
    if (times.length) out.times = times

    updateCount += updates.length + skipped.length + times.length
    trips.push(out)
  }

  const vehicles: Array<[string, string, number, number]> = []
  for (const entity of vehicleFeed?.entity ?? []) {
    const trip = entity.vehicle?.trip
    const position = entity.vehicle?.position
    if (!trip?.trip_id || !trip.start_date || !trip.route_id?.startsWith(routePrefix)) continue
    if (typeof position?.latitude !== "number" || typeof position.longitude !== "number") continue
    // Five decimal places is about a metre.
    vehicles.push([
      trip.trip_id,
      trip.start_date,
      Math.round(position.latitude * 1e5) / 1e5,
      Math.round(position.longitude * 1e5) / 1e5
    ])
  }

  return {
    file: { version: 1, feedTime: toNumber(feed.header?.timestamp) ?? 0, trips, vehicles },
    stats: { feedTrips: entities.length, trips: trips.length, updates: updateCount, vehicles: vehicles.length }
  }
}
