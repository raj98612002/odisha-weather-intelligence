import logging
import math
from datetime import datetime, timezone
from typing import Tuple

log = logging.getLogger(__name__)

# ─── Validation rules ─────────────────────────────────────────────────────────
VALID_CONDITIONS = {
    "Clear", "Clouds", "Rain", "Drizzle", "Thunderstorm",
    "Snow", "Mist", "Smoke", "Haze", "Dust", "Fog",
    "Sand", "Ash", "Squall", "Tornado",
}

def _calculate_heat_index(temp_c: float, humidity: int) -> float:
    """
    Rothfusz heat index formula.
    Only meaningful above 27°C and 40% humidity.
    """
    if temp_c < 27 or humidity < 40:
        return temp_c
    t = temp_c
    h = humidity
    hi = (-8.78469475556
          + 1.61139411 * t
          + 2.33854883889 * h
          - 0.14611605 * t * h
          - 0.012308094 * t ** 2
          - 0.0164248277778 * h ** 2
          + 0.002211732 * t ** 2 * h
          + 0.00072546 * t * h ** 2
          - 0.000003582 * t ** 2 * h ** 2)
    return round(hi, 2)

def _calculate_dew_point(temp_c: float, humidity: int) -> float:
    """Magnus formula approximation for dew point."""
    a, b = 17.625, 243.04
    gamma = (a * temp_c / (b + temp_c)) + math.log(humidity / 100.0)
    return round((b * gamma) / (a - gamma), 2)

def _severity_score(condition_code, wind, humidity):
    score = 0
    
    # Condition severity
    if condition_code >= 900:    # Extreme
        score += 40
    elif condition_code >= 800:  # Clear/Sunny
        score += 10
    elif condition_code >= 700:  # Atmosphere (haze/fog)
        score += 25
    elif condition_code >= 600:  # Snow
        score += 35
    elif condition_code >= 500:  # Rain
        score += 30
    elif condition_code >= 300:  # Drizzle
        score += 20
    elif condition_code >= 200:  # Thunderstorm
        score += 40
    
    # Wind severity
    if wind >= 50:
        score += 30
    elif wind >= 30:
        score += 20
    elif wind >= 15:
        score += 10
    else:
        score += 5
    
    # Humidity severity
    if humidity >= 85:
        score += 20
    elif humidity >= 70:
        score += 10
    elif humidity >= 50:
        score += 5
    
    return score

def _validate(record: dict) -> Tuple[bool, str]:
    """Returns (is_valid, reason). Rejects nulls and out-of-range values."""
    if not record.get("city_id"):
        return False, "missing city_id"
    if record.get("temperature_c") is None:
        return False, "missing temperature"
    if not (-90 <= record["temperature_c"] <= 60):
        return False, f"temperature out of range: {record['temperature_c']}"
    if not (0 <= record.get("humidity_pct", -1) <= 100):
        return False, f"humidity out of range: {record.get('humidity_pct')}"
    if record.get("condition") not in VALID_CONDITIONS:
        return False, f"unknown condition: {record.get('condition')}"
    return True, "ok"

# ─── Main transform function ──────────────────────────────────────────────────
def transform_weather(**context):
    """
    Pulls raw records from XCom, validates and enriches each one,
    pushes clean records back to XCom for the load task.
    """
    ti = context["ti"]
    raw_records = ti.xcom_pull(task_ids="extract_from_kafka", key="raw_records")

    if not raw_records:
        log.warning("No raw records to transform")
        ti.xcom_push(key="clean_records", value=[])
        return []

    log.info(f"Transforming {len(raw_records)} raw records")

    clean     = []
    rejected  = 0
    seen      = set()

    for rec in raw_records:
        # ── 1. Validate ──────────────────────────────────────────
        valid, reason = _validate(rec)
        if not valid:
            log.warning(f"Rejected record for {rec.get('city_name')}: {reason}")
            rejected += 1
            continue

        # ── 2. Deduplicate ───────────────────────────────────────
        dedup_key = (rec["city_id"], rec["recorded_at"])
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        # ── 3. Enrich with derived fields ────────────────────────
        temp      = rec["temperature_c"]
        humidity  = rec["humidity_pct"]
        wind      = rec.get("wind_speed_kph", 0)
        cond_code = rec.get("condition_code", 800)

        rec["heat_index_c"]   = _calculate_heat_index(temp, humidity)
        rec["dew_point_c"]    = _calculate_dew_point(temp, humidity)
        rec["severity_score"] = _severity_score(cond_code, wind, humidity)

        # ── 4. Standardise timestamp ─────────────────────────────
        rec["transformed_at"] = datetime.now(timezone.utc).isoformat()

        clean.append(rec)

    log.info(
        f"Transform complete — {len(clean)} clean, "
        f"{rejected} rejected, "
        f"{len(raw_records) - len(clean) - rejected} duplicates"
    )

    ti.xcom_push(key="clean_records", value=clean)
    return clean