import json
import time
import requests
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
API_KEY   = os.getenv("OPENWEATHER_API_KEY")
TOPIC     = os.getenv("KAFKA_TOPIC", "weather.raw")
SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INTERVAL  = 900  # 15 minutes = 960 calls/day with 10 cities ✅

CITIES = [
    {"name": "Bhubaneswar,IN", "state": "Odisha"},
    {"name": "Cuttack,IN",     "state": "Odisha"},
    {"name": "Rourkela,IN",    "state": "Odisha"},
    {"name": "Puri,IN",        "state": "Odisha"},
    {"name": "Sambalpur,IN",   "state": "Odisha"},
    {"name": "Brahmapur,IN",   "state": "Odisha"},
    {"name": "Balasore,IN",    "state": "Odisha"},
    {"name": "Mumbai,IN",      "state": "Maharashtra"},
    {"name": "Kolkata,IN",     "state": "West Bengal"},
    {"name": "New Delhi,IN",   "state": "Delhi"},
]
# ─── Producer setup ───────────────────────────────────────────────────────────
def create_producer():
    return KafkaProducer(
        bootstrap_servers=SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        linger_ms=100,
    )

# ─── Fetch from OpenWeatherMap ────────────────────────────────────────────────
def fetch_weather(city: dict):
    url = (
        f"http://api.openweathermap.org/data/2.5/weather"
        f"?q={city['name']}&appid={API_KEY}&units=metric"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        return {
            "city_id":        str(data["id"]),
            "city_name":      data["name"],
            "state":          city.get("state", ""),
            "country":        data["sys"]["country"],
            "temperature_c":  data["main"]["temp"],
            "feels_like_c":   data["main"]["feels_like"],
            "temp_min_c":     data["main"]["temp_min"],
            "temp_max_c":     data["main"]["temp_max"],
            "humidity_pct":   data["main"]["humidity"],
            "pressure_hpa":   data["main"]["pressure"],
            "wind_speed_kph": round(data["wind"]["speed"] * 3.6, 2),
            "wind_direction": data["wind"].get("deg"),
            "visibility_km":  round(data.get("visibility", 0) / 1000, 2),
            "condition":      data["weather"][0]["main"],
            "condition_desc": data["weather"][0]["description"],
            "condition_code": data["weather"][0]["id"],
            "recorded_at":    datetime.fromtimestamp(
                                  data["dt"], tz=timezone.utc
                              ).isoformat(),
            "ingested_at":    datetime.now(timezone.utc).isoformat(),
        }
    except requests.exceptions.RequestException as e:
        log.error(f"API error for {city['name']}: {e}")
        return None

# ─── Main loop ────────────────────────────────────────────────────────────────
def run():
    if not API_KEY:
        raise ValueError("OPENWEATHER_API_KEY not set in .env")

    producer = create_producer()
    total  = len(CITIES)
    odisha = sum(1 for c in CITIES if c.get("state") == "Odisha")
    log.info(f"Producer started — {total} cities ({odisha} in Odisha)")
    log.info(f"Publishing to topic '{TOPIC}' every {INTERVAL}s")

    try:
        while True:
            success, failed = 0, 0
            for city in CITIES:
                payload = fetch_weather(city)
                if payload:
                    producer.send(
                        topic=TOPIC,
                        key=payload["city_id"],
                        value=payload,
                    )
                    success += 1
                    log.info(
                        f"[{payload['state']:15s}] {payload['city_name']:12s} | "
                        f"{payload['temperature_c']}°C | "
                        f"Humidity: {payload['humidity_pct']}% | "
                        f"{payload['condition']}"
                    )
                else:
                    failed += 1

            producer.flush()
            log.info(f"Batch complete — {success} sent, {failed} failed. "
                     f"Sleeping {INTERVAL}s...")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        log.info("Producer stopped")
    finally:
        producer.close()

if __name__ == "__main__":
    run()