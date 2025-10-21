import argparse
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry

BASE = "https://www.advancedeventsystems.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AES-Scraper/1.0; +https://example.com)",
    "Accept": "text/html,application/json",
}


@dataclass
class EventRecord:
    event_id: Optional[str] = None
    event_url: Optional[str] = None
    name: Optional[str] = None
    tournament_type: Optional[str] = None
    host: Optional[str] = None
    location: Optional[str] = None
    address: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


@dataclass
class DivisionRecord:
    description: Optional[str] = None
    entry_fee: Optional[str] = None
    event_division_assignment_id: Optional[str] = None
    event_id: Optional[str] = None
    maximum_teams: Optional[str] = None


def _fmt_date(iso: Optional[str]) -> str:
    if not iso:
        return ""
    try:
        return datetime.fromisoformat(iso.replace("Z", "")).strftime("%m/%d/%Y")
    except ValueError:
        return ""


class AESScraper:
    def __init__(
        self,
        delay_sec: float = 0.4,
        session: Optional[requests.Session] = None,
        is_past_events: bool = False,
    ):
        self._tls = threading.local()
        self.delay_sec = delay_sec
        self.sess = session or self._build_session()
        self.is_past_events = is_past_events

    def _thread_session(self) -> requests.Session:
        """One Session per thread (requests.Session is not strictly thread-safe)."""
        if not hasattr(self._tls, "sess"):
            self._tls.sess = self._build_session()
        return self._tls.sess

    def _build_session(self) -> requests.Session:
        sess = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            pool_connections=500, pool_maxsize=500, max_retries=retries
        )
        sess.mount("https://", adapter)
        sess.mount("http://", adapter)
        sess.headers.update(HEADERS)
        return sess

    def _fetch_one_event(self, item: dict):
        event_id = item["eventId"]

        url = f"https://www.advancedeventsystems.com/api/landing/events/{event_id}"

        if event_id == None:
            event_scheduler_id = item["eventSchedulerId"]
            url = f"https://www.advancedeventsystems.com/api/landing/events/scheduler/{event_scheduler_id}"

        with self.sess.get(url, timeout=(5, 120)) as r:
            r.raise_for_status()
            data = r.json()
            return self.parse_event_api(data)

    def _fetch_one_division(self, event: Dict[str, Any]) -> List[DivisionRecord]:
        event_id = event.get("eventId")
        if event_id is None:
            return []

        url = f"https://www.advancedeventsystems.com/api/landing/events/{int(event_id)}/divisions"
        with self.sess.get(url, timeout=(5, 120)) as r:
            r.raise_for_status()
            payload = r.json()

        divisions = (
            payload["value"]
            if isinstance(payload, dict) and "value" in payload
            else payload
        ) or []

        return [self.parse_division_api(d) for d in divisions]

    def fetch_total_counts(self):
        request = requests.get(
            f"https://www.advancedeventsystems.com/api/landing/events?$count=true&$filter=isPastEvent+eq+{str(self.is_past_events).lower()}&$format=json&$orderby=startDate,name&$top=100"
        )

        api_events = request.json()
        return api_events["@odata.count"]

    def fetch_events(self, count):
        url = f"https://www.advancedeventsystems.com/api/landing/events?$count=true&$filter=isPastEvent+eq+{str(self.is_past_events).lower()}&$format=json&$orderby=startDate,name&$top={count}"
        request = requests.get(url)
        api_events = request.json()
        return api_events["value"]

    @staticmethod
    def _event_url_from_id(event_id: str) -> str:
        return f"{BASE}/events/{event_id}"

    def parse_event_api(self, data: Dict[str, Any]) -> EventRecord:
        rec = EventRecord()
        rec.event_id = str(data.get("eventId") or "")
        rec.event_url = (
            f"https://www.advancedeventsystems.com/events/{rec.event_id}"
            if rec.event_id
            else ""
        )
        rec.name = data.get("name") or ""

        aff = (data.get("affiliation") or {}).get("description") or ""
        et = (data.get("eventType") or {}).get("description") or ""
        rec.tournament_type = (f"{aff} {et}").strip() if (aff or et) else ""

        rec.host = data.get("hostName") or data.get("bossOrganizationName") or ""
        rec.location = data.get("locationName") or ""

        addr = data.get("address") or {}
        line1 = addr.get("line1") or ""
        city = addr.get("city") or ""
        state_abbr = ((addr.get("state") or {}).get("abbreviation")) or ""
        zip_code = addr.get("zip") or ""
        rec.address = f"{line1}\n{city}, {state_abbr} {zip_code}".strip().rstrip(", ")

        rec.website = data.get("website") or ""
        rec.email = data.get("email") or ""

        rec.start_date = _fmt_date(data.get("startDate"))
        rec.end_date = _fmt_date(data.get("endDate"))

        return rec

    def parse_division_api(self, data: Dict[str, Any]) -> DivisionRecord:
        rec = DivisionRecord()
        rec.event_id = str(data.get("eventId") or "")
        rec.description = str(data.get("description") or "")

        rec.entry_fee = str(data.get("entryFee") or "0")

        rec.event_division_assignment_id = str(
            data.get("eventDivisionAssignmentId") or ""
        )
        rec.event_id = str(data.get("eventId") or "")
        rec.maximum_teams = str(data.get("maximumTeams") or "")
        return rec

    def run(self, out_path: str, workers: int) -> None:
        count = self.fetch_total_counts()
        events = self.fetch_events(round(count, 2))

        event_records: List[EventRecord] = []
        event_errors: List[Dict[str, str]] = []

        division_records: List[DivisionRecord] = []
        division_errors: List[Dict[str, str]] = []

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {
                ex.submit(self._fetch_one_division, event): event for event in events
            }
            for fut in as_completed(futures):
                try:
                    recs = fut.result()
                    if recs:
                        division_records.extend(recs)
                except Exception as e:
                    item = futures[fut]
                    division_errors.append(
                        {
                            "where": "detail",
                            "message": repr(e),
                            "item": json.dumps(item)[:500],
                        }
                    )

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {
                ex.submit(self._fetch_one_event, event): event for event in events
            }
            for fut in as_completed(futures):
                try:
                    rec = fut.result()
                    event_records.append(rec)
                except Exception as e:
                    item = futures[fut]
                    event_errors.append(
                        {
                            "where": "detail",
                            "message": repr(e),
                            "item": json.dumps(item)[:500],
                        }
                    )

        df_events = pd.DataFrame([asdict(r) for r in event_records])
        df_divisions = pd.DataFrame([asdict(r) for r in division_records])

        with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
            df_events.to_excel(xw, index=False, sheet_name="events")
            if event_errors:
                pd.DataFrame(event_errors).to_excel(
                    xw, index=False, sheet_name="event_errors"
                )

            df_divisions.to_excel(xw, index=False, sheet_name="divisions")
            if division_errors:
                pd.DataFrame(division_errors).to_excel(
                    xw, index=False, sheet_name="division_errors"
                )

        print(
            f"Wrote {len(event_records)} events to {out_path}. Errors: {len(event_errors)}"
        )
        print(
            "----------------------------------------------------------------------------"
        )
        print(
            f"Wrote {len(division_records)} divisions to {out_path}. Errors: {len(division_errors)}"
        )


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--workers", type=int, default=8, help="Parallel workers for detail pages"
    )
    p.add_argument("--out", default="aes_events.xlsx", help="Output Excel file path")
    p.add_argument(
        "--delay", type=float, default=0.4, help="Delay between requests (seconds)"
    )

    p.add_argument(
        "--past_events", type=bool, default=False, help="Get Past events(Boolean)"
    )

    args = p.parse_args()

    scraper = AESScraper(delay_sec=args.delay, is_past_events=args.past_events)
    scraper.run(args.out, workers=args.workers)


if __name__ == "__main__":
    main()
