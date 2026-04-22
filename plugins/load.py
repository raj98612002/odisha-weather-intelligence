import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

log = logging.getLogger(__name__)

# ─── SQL statements ───────────────────────────────────────────────────────────

INSERT_CLEAN = """
    INSERT INTO weather_clean (
        city_id, city_name, state, country,
        temperature_c, feels_like_c, temp_min_c, temp_max_c,
        humidity_pct, pressure_hpa, wind_speed_kph, wind_direction,
        visibility_km, condition, condition_desc, condition_code,
        heat_index_c, dew_point_c, severity_score,
        recorded_at, loaded_at
    )
    VALUES %s
    ON CONFLICT (city_id, recorded_at)
    DO UPDATE SET
        temperature_c   = EXCLUDED.temperature_c,
        humidity_pct    = EXCLUDED.humidity_pct,
        wind_speed_kph  = EXCLUDED.wind_speed_kph,
        condition       = EXCLUDED.condition,
        heat_index_c    = EXCLUDED.heat_index_c,
        dew_point_c     = EXCLUDED.dew_point_c,
        severity_score  = EXCLUDED.severity_score,
        loaded_at       = EXCLUDED.loaded_at;
"""

REFRESH_AGG = """
    INSERT INTO weather_agg (
        city_id, city_name, hour_bucket,
        avg_temp_c, max_temp_c, min_temp_c,
        avg_humidity, avg_wind_kph, reading_count, computed_at
    )
    SELECT
        city_id,
        city_name,
        DATE_TRUNC('hour', recorded_at)     AS hour_bucket,
        ROUND(AVG(temperature_c)::numeric, 2) AS avg_temp_c,
        ROUND(MAX(temperature_c)::numeric, 2) AS max_temp_c,
        ROUND(MIN(temperature_c)::numeric, 2) AS min_temp_c,
        ROUND(AVG(humidity_pct)::numeric, 2)  AS avg_humidity,
        ROUND(AVG(wind_speed_kph)::numeric, 2) AS avg_wind_kph,
        COUNT(*)                              AS reading_count,
        NOW()                                 AS computed_at
    FROM weather_clean
    WHERE recorded_at >= DATE_TRUNC('hour', NOW()) - INTERVAL '2 hours'
    GROUP BY city_id, city_name, DATE_TRUNC('hour', recorded_at)
    ON CONFLICT (city_id, hour_bucket)
    DO UPDATE SET
        avg_temp_c    = EXCLUDED.avg_temp_c,
        max_temp_c    = EXCLUDED.max_temp_c,
        min_temp_c    = EXCLUDED.min_temp_c,
        avg_humidity  = EXCLUDED.avg_humidity,
        avg_wind_kph  = EXCLUDED.avg_wind_kph,
        reading_count = EXCLUDED.reading_count,
        computed_at   = EXCLUDED.computed_at;
"""

# ─── Main load function ───────────────────────────────────────────────────────
def load_to_postgres(host, port, dbname, user, password, **context):
    """
    Upserts clean records into weather_clean, then refreshes weather_agg.

    Interview note:
      - ON CONFLICT DO UPDATE = idempotent upsert (safe to re-run)
      - execute_values() = bulk insert, much faster than row-by-row
      - weather_agg is refreshed by Airflow, not a DB trigger —
        this keeps compute in the pipeline layer, not the DB layer.
    """
    ti = context["ti"]
    clean_records = ti.xcom_pull(task_ids="transform_weather", key="clean_records")

    if not clean_records:
        log.warning("No clean records to load — skipping")
        return 0

    log.info(f"Loading {len(clean_records)} records into PostgreSQL")

    conn = psycopg2.connect(
        host=host, port=port, dbname=dbname, user=user, password=password
    )

    try:
        with conn:
            with conn.cursor() as cur:

                # ── Step 1: Bulk upsert into weather_clean ────────
                rows = [
                    (
                        r["city_id"],       r["city_name"],     r.get("state", ""),
                        r.get("country"),   r["temperature_c"], r.get("feels_like_c"),
                        r.get("temp_min_c"),r.get("temp_max_c"),r["humidity_pct"],
                        r.get("pressure_hpa"), r.get("wind_speed_kph"),
                        r.get("wind_direction"), r.get("visibility_km"),
                        r["condition"],     r.get("condition_desc"),
                        r.get("condition_code"), r.get("heat_index_c"),
                        r.get("dew_point_c"), r.get("severity_score"),
                        r["recorded_at"],   datetime.now(timezone.utc),
                    )
                    for r in clean_records
                ]

                execute_values(cur, INSERT_CLEAN, rows, page_size=100)
                log.info(f"Upserted {len(rows)} rows into weather_clean")

                # ── Step 2: Refresh hourly aggregates ─────────────
                cur.execute(REFRESH_AGG)
                log.info("Refreshed weather_agg table")

        log.info("Load complete — transaction committed")
        return len(clean_records)

    except Exception as e:
        log.error(f"Load failed: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
