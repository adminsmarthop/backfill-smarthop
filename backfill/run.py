import csv
import io
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


def normalize_timestamp(timestamp):
    """
    Influx zapisujemy z nanosekundami,
    ale porównujemy timestampy z dokładnością do milisekundy.
    """

    return timestamp.replace(
        microsecond=(timestamp.microsecond // 1000) * 1000
    )


def influx_headers(token, content_type=None):
    headers = {
        "Authorization": f"Token {token}",
    }

    if content_type:
        headers["Content-Type"] = content_type

    return headers


def get_existing_timestamps(options, entity_id, start, end):
    """
    Pobiera wszystkie timestampy istniejące już w InfluxDB
    dla konkretnej encji.

    Ważne:
    - HA entity_id może być np. sensor.lub_pil_6_le10_r264
    - w Influx entity_id jest lub_pil_6_le10_r264
    """

    influx_url = options["influx_url"].rstrip("/")
    organization = options["influx_org"]
    bucket = options["influx_bucket"]
    token = options["influx_token"]
    site = options["site"]

    influx_entity_id = entity_id

    if influx_entity_id.startswith("sensor."):
        influx_entity_id = influx_entity_id[len("sensor."):]

    query = f'''
from(bucket: "{bucket}")
  |> range(
      start: {start.isoformat()},
      stop: {end.isoformat()}
    )
  |> filter(fn: (r) => r["_measurement"] == "energy")
  |> filter(fn: (r) => r["_field"] == "value")
  |> filter(fn: (r) => r["domain"] == "sensor")
  |> filter(fn: (r) => r["entity_id"] == "{influx_entity_id}")
  |> filter(fn: (r) => r["site"] == "{site}")
  |> keep(columns: ["_time"])
'''

    url = f"{influx_url}/api/v2/query"

    params = {
        "org": organization,
    }

    headers = influx_headers(
        token,
        "application/vnd.flux",
    )

    response = requests.post(
        url,
        params=params,
        headers=headers,
        data=query,
        timeout=120,
    )

    if response.status_code >= 300:
        raise RuntimeError(
            f"Influx query HTTP {response.status_code}: "
            f"{response.text}"
        )

    existing = set()

    # ---------------------------------------------------------
    # InfluxDB zwraca CSV z komentarzami/metadata.
    # Usuwamy linie komentarzy i puste linie.
    # ---------------------------------------------------------

    raw_lines = response.text.splitlines()

    csv_lines = []

    for line in raw_lines:
        line = line.strip()

        if not line:
            continue

        if line.startswith("#"):
            continue

        csv_lines.append(line)

    if not csv_lines:
        return existing

    # ---------------------------------------------------------
    # Obsługa wielu tabel CSV.
    #
    # Influx może zwrócić więcej niż jeden nagłówek:
    #
    # _time
    # ...
    #
    # _time
    # ...
    #
    # Dlatego nie zakładamy, że cały wynik jest jednym
    # klasycznym CSV.
    # ---------------------------------------------------------

    header = None

    for line in csv_lines:

        # Szukamy nagłówka zawierającego _time.
        if "_time" in line.split(","):

            header = line.split(",")

            continue

        # Jeżeli nie znaleźliśmy jeszcze nagłówka,
        # pomijamy linię.
        if header is None:
            continue

        values = next(
            csv.reader(
                io.StringIO(line)
            )
        )

        if len(values) != len(header):
            continue

        row = dict(
            zip(header, values)
        )

        value = row.get("_time")

        if not value:
            continue

        timestamp = parse_timestamp(value)

        if timestamp is None:
            continue

        normalized = normalize_timestamp(
            timestamp
        )

        existing.add(normalized)

    return existing


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
        raise RuntimeError(
            "Brak tokenu InfluxDB"
        )

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
            f"InfluxDB HTTP {response.status_code}: "
            f"{response.text}"
        )


def build_line(entity_id, site, state, timestamp):
    value = float(state)

    measurement = "energy"

    influx_entity_id = entity_id

    if influx_entity_id.startswith("sensor."):
        influx_entity_id = influx_entity_id[len("sensor."):]

    tags = (
        f"domain=sensor,"
        f"entity_id={escape_tag(influx_entity_id)},"
        f"site={escape_tag(site)}"
    )

    timestamp_ns = int(
        timestamp.timestamp() * 1_000_000_000
    )

    return (
        f"{measurement},{tags} "
        f"value={value} "
        f"{timestamp_ns}"
    )


def backfill_entity(options, entity_id):
    days = int(
        options.get(
            "days",
            30,
        )
    )

    site = options["site"]

    end = datetime.now(
        timezone.utc
    )

    start = end - timedelta(
        days=days
    )

    log(
        f"Backfill {entity_id}: "
        f"{start.isoformat()} -> "
        f"{end.isoformat()}"
    )

    # ---------------------------------------------------------
    # 1. Pobierz historię z Home Assistant
    # ---------------------------------------------------------

    history = get_history(
        entity_id,
        start,
        end,
    )

    log(
        f"{entity_id}: HA zwrócił "
        f"{len(history)} rekordów"
    )

    if not history:
        log(
            f"{entity_id}: brak danych "
            f"w historii HA"
        )

        return

    # ---------------------------------------------------------
    # 2. Pobierz istniejące timestampy z Influx
    # ---------------------------------------------------------

    existing_timestamps = (
        get_existing_timestamps(
            options,
            entity_id,
            start,
            end,
        )
    )

    log(
        f"{entity_id}: w Influx istnieje "
        f"{len(existing_timestamps)} punktów"
    )

    # ---------------------------------------------------------
    # 3. Przygotuj dane z HA
    #
    # Jeżeli HA zwróci ten sam timestamp więcej niż raz,
    # zostawiamy jeden punkt.
    # ---------------------------------------------------------

    history_by_timestamp = {}

    for item in history:

        state = item.get(
            "state"
        )

        if state in (
            None,
            "",
            "unknown",
            "unavailable",
        ):
            continue

        try:
            float(state)
        except (
            TypeError,
            ValueError,
        ):
            continue

        timestamp = parse_timestamp(
            item.get(
                "last_updated"
            )
            or item.get(
                "last_changed"
            )
        )

        if timestamp is None:
            continue

        normalized = normalize_timestamp(
            timestamp
        )

        history_by_timestamp[
            normalized
        ] = (
            state,
            timestamp,
        )

    # ---------------------------------------------------------
    # 4. Znajdź tylko punkty, których NIE MA w Influx
    # ---------------------------------------------------------

    lines = []

    missing_timestamps = []

    for (
        normalized_timestamp,
        (
            state,
            timestamp,
        ),
    ) in history_by_timestamp.items():

        if (
            normalized_timestamp
            in existing_timestamps
        ):
            continue

        lines.append(
            build_line(
                entity_id,
                site,
                state,
                timestamp,
            )
        )

        missing_timestamps.append(
            timestamp
        )

    # ---------------------------------------------------------
    # 5. Nic nie brakuje
    # ---------------------------------------------------------

    if not lines:

        log(
            f"{entity_id}: "
            f"brak brakujących punktów"
        )

        return

    # ---------------------------------------------------------
    # 6. Zapis brakujących punktów do Influx
    # ---------------------------------------------------------

    write_to_influx(
        options,
        lines,
    )

    log(
        f"{entity_id}: uzupełniono "
        f"{len(lines)} brakujących punktów"
    )

    # ---------------------------------------------------------
    # 7. Log zakresu uzupełnienia
    # ---------------------------------------------------------

    if missing_timestamps:

        first = min(
            missing_timestamps
        )

        last = max(
            missing_timestamps
        )

        log(
            f"{entity_id}: zakres "
            f"uzupełnienia "
            f"{first.isoformat()} -> "
            f"{last.isoformat()}"
        )


def run_backfill():
    options = load_options()

    entities = options.get(
        "entities",
        []
    )

    if not entities:

        log(
            "Brak skonfigurowanych encji."
        )

        return

    for entity_id in entities:

        try:

            backfill_entity(
                options,
                entity_id,
            )

        except Exception as error:

            log(
                f"BŁĄD {entity_id}: "
                f"{error}"
            )


def main():

    interval_days = int(
        load_options().get(
            "interval_days",
            7,
        )
    )

    while True:

        log(
            "Rozpoczynam backfill"
        )

        try:

            run_backfill()

        except Exception as error:

            log(
                f"BŁĄD GŁÓWNY: "
                f"{error}"
            )

        log(
            f"Następny backfill za "
            f"{interval_days} dni"
        )

        time.sleep(
            interval_days
            * 24
            * 60
            * 60
        )


if __name__ == "__main__":
    main()
