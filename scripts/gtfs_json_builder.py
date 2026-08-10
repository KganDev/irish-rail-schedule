#!/usr/bin/env python3

import csv
import io
import json
import os
import sys
import zipfile
import urllib.request
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional, Set
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
GTFS_URL_DEFAULT = "https://www.transportforireland.ie/transitData/Data/GTFS_Irish_Rail.zip"
def _safe_float(v) -> float:
    try:
        s = ("" if v is None else str(v)).strip()
        return float(s) if s else 0.0
    except Exception:
        return 0.0
def _safe_int(v, default=0) -> int:
    try:
        s = ("" if v is None else str(v)).strip()
        if s == "":
            return default
        try:
            return int(s)
        except ValueError:
            return int(float(s))
    except Exception:
        return default
def _yyyymmdd_to_date(s: str) -> Optional[date]:
    if not s or len(s) != 8:
        return None
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except Exception:
        return None
def _date_to_yyyymmdd(d: date) -> str:
    return f"{d:%Y%m%d}"
class ZipView:
    def __init__(self, zip_path: Path):
        self.zf = zipfile.ZipFile(zip_path, "r")
        self.map = {n.lower(): n for n in self.zf.namelist()}
    def _find(self, filename: str) -> Optional[str]:
        target = filename.lower()
        for name in self.map.values():
            lo = name.lower()
            if lo.endswith("/" + target) or lo == target:
                return name
        return None
    def read_csv(self, filename: str) -> List[Dict]:
        name = self._find(filename)
        if not name:
            return []
        raw = self.zf.read(name)
        f = io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8-sig", newline="")
        return list(csv.DictReader(f))
def download_zip(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, dest)
    return dest
def _weekday_mask(cal_row: Dict) -> Tuple[bool, bool, bool, bool, bool, bool, bool]:
    return tuple(cal_row.get(day, "0") == "1" for day in
                 ("monday","tuesday","wednesday","thursday","friday","saturday","sunday"))
def _active_services_on(D: date, calendar_rows: List[Dict], calendar_dates_rows: List[Dict]) -> Set[str]:
    wd = D.weekday()  
    active = set()
    for row in calendar_rows:
        start = _yyyymmdd_to_date(row.get("start_date",""))
        end   = _yyyymmdd_to_date(row.get("end_date",""))
        if start and end and start <= D <= end:
            if _weekday_mask(row)[wd]:
                active.add(row["service_id"])
    key = _date_to_yyyymmdd(D)
    for exc in calendar_dates_rows:
        if exc.get("date") == key:
            sid = exc.get("service_id","")
            t = exc.get("exception_type","")
            if t == "1": active.add(sid)
            elif t == "2": active.discard(sid)
    return active
def _effective_windows(start: date, days: int, cal: List[Dict], cd: List[Dict]) -> List[Tuple[date, date]]:
    if days <= 0: return []
    def sig(sids: Set[str]) -> Tuple[int, int]:
        if not sids: return (0,0)
        payload = "|".join(sorted(sids)).encode()
        import hashlib
        return (len(sids), int(hashlib.sha1(payload).hexdigest()[:8], 16))
    cur_from = start
    cur_set = _active_services_on(start, cal, cd)
    cur_sig = sig(cur_set)
    wins = []
    for i in range(1, days+1):
        d = start + timedelta(days=i)
        s = _active_services_on(d, cal, cd)
        sg = sig(s)
        if sg != cur_sig:
            wins.append((cur_from, d - timedelta(days=1)))
            cur_from, cur_set, cur_sig = d, s, sg
    wins.append((cur_from, start + timedelta(days=days)))
    return wins
def build(gtfs_url: str, out_dir: Path, target_date: Optional[date], window_days: int) -> None:
    tmp = out_dir.parent / ".tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    zip_path = tmp / "irish_rail.zip"
    print(f"Downloading {gtfs_url}")
    download_zip(gtfs_url, zip_path)
    z = ZipView(zip_path)
    agencies       = z.read_csv("agency.txt")
    stops          = z.read_csv("stops.txt")
    routes         = z.read_csv("routes.txt")
    trips          = z.read_csv("trips.txt")
    stop_times     = z.read_csv("stop_times.txt")
    calendar_rows  = z.read_csv("calendar.txt")
    calendar_dates = z.read_csv("calendar_dates.txt")
    feed_info      = z.read_csv("feed_info.txt")
    today = date.today()
    if not target_date:
        tz = None
        if agencies:
            tz_name = agencies[0].get("agency_timezone")
            if tz_name and ZoneInfo is not None:
                try:
                    tz = ZoneInfo(tz_name)
                except Exception:
                    tz = None
        if tz:
            today = datetime.now(tz).date()
        target_date = today
    else:
        today = target_date
    raw_version = None
    if feed_info:
        candidate = (feed_info[0].get("feed_version") or "").strip()
        raw_version = candidate or None
    if not raw_version:
        raw_version = datetime.utcnow().strftime("%Y%m%d")
    version = f"kg-{raw_version}"
    stops_typed = []
    for s in stops:
        stops_typed.append({
            "stop_id": s.get("stop_id",""),
            "stop_code": s.get("stop_code",""),
            "stop_name": s.get("stop_name",""),
            "stop_desc": s.get("stop_desc",""),
            "stop_lat": _safe_float(s.get("stop_lat")),
            "stop_lon": _safe_float(s.get("stop_lon")),
            "zone_id": s.get("zone_id",""),
            "stop_url": s.get("stop_url",""),
            "location_type": s.get("location_type",""),
            "parent_station": s.get("parent_station",""),
        })
    stop_times_typed = []
    for st in stop_times:
        stop_times_typed.append({
            "trip_id": st.get("trip_id",""),
            "arrival_time": st.get("arrival_time",""),
            "departure_time": st.get("departure_time",""),
            "stop_id": st.get("stop_id",""),
            "stop_sequence": _safe_int(st.get("stop_sequence")),
            "stop_headsign": st.get("stop_headsign",""),
            "pickup_type": st.get("pickup_type",""),
            "drop_off_type": st.get("drop_off_type",""),
            "timepoint": st.get("timepoint",""),
        })
    calendar_typed = []
    for c in calendar_rows:
        row = dict(c)
        for day in ("monday","tuesday","wednesday","thursday","friday","saturday","sunday"):
            row[day] = (row.get(day,"0") == "1")
        calendar_typed.append(row)
    feed_version_meta = None
    if feed_info:
        raw_feed_version = (feed_info[0].get("feed_version") or "").strip()
        feed_version_meta = raw_feed_version or None
    if not feed_version_meta:
        feed_version_meta = raw_version

    wins = _effective_windows(today, window_days, calendar_rows, calendar_dates)
    windows_json = {
        "generatedAt": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "scan": {"from": _date_to_yyyymmdd(today),
                 "to": _date_to_yyyymmdd(today + timedelta(days=window_days))},
        "feed": {
            "version": feed_version_meta,
            "startDate": (feed_info[0].get("feed_start_date") if feed_info else None),
            "endDate":   (feed_info[0].get("feed_end_date")   if feed_info else None),
        },
        "windows": [{"from": _date_to_yyyymmdd(a), "to": _date_to_yyyymmdd(b)} for a,b in wins]
    }
    version_dir = out_dir / "gtfs" / version
    version_dir.mkdir(parents=True, exist_ok=True)
    def dump(obj, name):
        p = version_dir / name
        with open(p, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        print(f"wrote {p}")
    dump(stops_typed, "stops.json")
    with open(version_dir / "routes.json","w",encoding="utf-8") as f:
        json.dump(routes, f, ensure_ascii=False, indent=2)
    print(f"wrote {version_dir/'routes.json'}")
    with open(version_dir / "trips.json","w",encoding="utf-8") as f:
        json.dump(trips, f, ensure_ascii=False, indent=2)
    print(f"wrote {version_dir/'trips.json'}")
    dump(stop_times_typed, "stop_times.json")
    dump(calendar_typed,    "calendar.json")
    with open(version_dir / "calendar_dates.json","w",encoding="utf-8") as f:
        json.dump(calendar_dates, f, ensure_ascii=False, indent=2)
    print(f"wrote {version_dir/'calendar_dates.json'}")
    with open(version_dir / "agencies.json","w",encoding="utf-8") as f:
        json.dump(agencies, f, ensure_ascii=False, indent=2)
    print(f"wrote {version_dir/'agencies.json'}")
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(out_dir / "windows.json","w",encoding="utf-8") as f:
        json.dump(windows_json, f, ensure_ascii=False, indent=2)
    latest = {"latest": version, "generatedAt": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
    with open(out_dir / "latest.json","w",encoding="utf-8") as f:
        json.dump(latest, f, ensure_ascii=False, indent=2)
    status = {"ok": True, **latest}
    with open(out_dir / "status.json","w",encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
    print("\nSummary")
    print(f"  version: {version}")
    print(f"  stops: {len(stops_typed)}")
    print(f"  routes: {len(routes)}")
    print(f"  trips: {len(trips)}")
    print(f"  stop_times: {len(stop_times_typed)}")
    print(f"  calendar rows: {len(calendar_rows)}  calendar_dates: {len(calendar_dates)}")
    if len(wins) > 1:
        print(f"  next timetable change: {_date_to_yyyymmdd(wins[1][0])}")
if __name__ == "__main__":
    gtfs_url    = os.environ.get("GTFS_URL", GTFS_URL_DEFAULT)
    out_dir     = Path(os.environ.get("OUT_DIR", "out"))
    window_days = int(os.environ.get("WINDOW_DAYS", "90") or "90")
    tgt         = os.environ.get("TARGET_DATE")
    target_date = _yyyymmdd_to_date(tgt) if tgt else None
    try:
        build(gtfs_url, out_dir, target_date, window_days)
    except KeyboardInterrupt:
        sys.exit(130)
