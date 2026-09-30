import json
import os
import time
from datetime import datetime, timedelta, timezone

import requests


OPTIONS_FILE = "/data/options.json"
HA_API = "http://supervisor/core/api"


def log(message):
    print(f"[SmartHop Backfill] {message}", flush=True)


def load_options():
    with open(OPTIONS_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def ha_headers():
    token = os.environ.get("SUPERVISOR_TOKEN")

    if not token:
        raise RuntimeError("Brak SUPERVISOR_TOKEN")

    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def get_history(entity_id, start, end):
    url = f"{HA_API}/history/period/{start.isoformat()}"

    params = {
        "filter_entity_id": entity_id,
        "end_time": end.isoformat(),
        "minimal_response": "false",
        "no_attributes": "true",
    }

    response = requests.get(
        url,
        headers=ha_headers(),
        params=params,
        timeout=120,
    )

    response.raise_for_status()

    data = response.json()

    if not data:
        return []

    return data[0]


def parse_timestamp(value):
    if not value:
        return None

    timestamp = value.replace("Z", "+00:00")

    try:
        dt = datetime.fromisoformat(timestamp)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def escape_tag(value):
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace(" ", "\\ ")
        .replace(",", "\\,")
        .replace("=", "\\=")
    )


def write_to_influx(options, points):
    influx_url = options["influx_url"].rstrip("/")
    organization = options["influx_org"]
    bucket = options["influx_bucket"]
    token = options["influx_token"]

    if not token:
        raise RuntimeError("Brak tokenu InfluxDB")

    url = f"{influx_url}/api/v2/write"

    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "text/plain; charset=utf-8",
        "Accept": "application/json",
    }

    params = {
        "org": organization,
        "bucket": bucket,
        "precision": "ns",
    }

    response = requests.post(
        url,
        headers=headers,
        params=params,
        data="\n".join(points),
        timeout=120,
    )

    if response.status_code >= 300:
        raise RuntimeError(
            f"InfluxDB HTTP {response.status_code}: {response.text}"
        )


def build_line(entity_id, site, state, timestamp):
    value = float(state)

    measurement = "energy"

    tags = (
        f"domain=sensor,"
        f"entity_id={escape_tag(entity_id)},"
        f"site={escape_tag(site)}"
    )

    timestamp_ns = int(timestamp.timestamp() * 1_000_000_000)

    return (
        f"{measurement},{tags} "
        f"value={value} "
        f"{timestamp_ns}"
    )


def backfill_entity(options, entity_id):
    days = int(options.get("days", 30))
    site = options["site"]

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    log(
        f"Backfill {entity_id}: "
        f"{start.isoformat()} -> {end.isoformat()}"
    )

    history = get_history(entity_id, start, end)

    lines = []

    for item in history:
        state = item.get("state")

        if state in (None, "", "unknown", "unavailable"):
            continue

        try:
            float(state)
        except (TypeError, ValueError):
            continue

        timestamp = parse_timestamp(
            item.get("last_updated") or item.get("last_changed")
        )

        if timestamp is None:
            continue

        lines.append(
            build_line(
                entity_id,
                site,
                state,
                timestamp,
            )
        )

    if not lines:
        log(f"{entity_id}: brak danych do zapisania")
        return

    write_to_influx(options, lines)

    log(
        f"{entity_id}: zapisano/uzupełniono "
        f"{len(lines)} punktów"
    )


def run_backfill():
    options = load_options()

    entities = options.get("entities", [])

    if not entities:
        log("Brak skonfigurowanych encji.")
        return

    for entity_id in entities:
        try:
            backfill_entity(options, entity_id)
        except Exception as error:
            log(f"BŁĄD {entity_id}: {error}")


def main():
    interval_days = int(
        load_options().get("interval_days", 7)
    )

    while True:
        log("Rozpoczynam backfill")

        try:
            run_backfill()
        except Exception as error:
            log(f"BŁĄD GŁÓWNY: {error}")

        log(
            f"Następny backfill za "
            f"{interval_days} dni"
        )

        time.sleep(interval_days * 24 * 60 * 60)


if __name__ == "__main__":
    main()
