"""Synthetic IoT telemetry generator that writes streaming JSON events."""

import json
import time
import random
from pathlib import Path
from datetime import datetime, timezone

from faker import Faker

from pipeline.common.utils import load_config, ensure_path
from pipeline.common.logging_config import get_logger

logger = get_logger("generator.telemetry")
fake = Faker()

_DEFAULT_DEVICE_IDS = [f"device-{i:04d}" for i in range(1, 51)]
LOCATIONS = [
    "factory-floor-A", "factory-floor-B",
    "warehouse-1", "warehouse-2",
    "rooftop-sensors", "basement-hvac",
]
FIRMWARE_VERSIONS = ["1.0.0", "1.1.0", "1.2.3", "2.0.0-beta"]


def generate_event(device_ids: list[str] | None = None,
                   anomaly_probability: float = 0.05) -> dict:
    if device_ids is None:
        device_ids = _DEFAULT_DEVICE_IDS

    inject_anomaly = random.random() < anomaly_probability

    temperature = random.gauss(22.0, 5.0)
    humidity = random.gauss(55.0, 10.0)

    if inject_anomaly:
        temperature = random.choice([random.uniform(160, 300), random.uniform(-80, -55)])
        humidity = random.choice([random.uniform(105, 200), random.uniform(-20, -1)])

    return {
        "device_id": random.choice(device_ids),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(temperature, 2),
        "humidity": round(max(humidity, 0), 2),
        "pressure": round(random.gauss(1013.25, 10.0), 2),
        "battery_level": round(random.uniform(10.0, 100.0), 2),
        "location": random.choice(LOCATIONS),
        "firmware_version": random.choice(FIRMWARE_VERSIONS),
    }


def write_batch(output_dir: Path, batch_size: int = 20,
                device_ids: list[str] | None = None,
                anomaly_probability: float = 0.05) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    file_path = output_dir / f"telemetry_{ts}.json"

    events = [generate_event(device_ids, anomaly_probability)
              for _ in range(batch_size)]
    with open(file_path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")

    return file_path


def main():
    config = load_config()
    gen_cfg = config.get("generator", {})

    output_dir = ensure_path(config["paths"]["bronze_input"])
    batch_size = gen_cfg.get("batch_size", 20)
    interval = gen_cfg.get("interval_seconds", 5)
    num_devices = gen_cfg.get("num_devices", 50)
    anomaly_prob = gen_cfg.get("anomaly_probability", 0.05)

    device_ids = [f"device-{i:04d}" for i in range(1, num_devices + 1)]

    logger.info("Starting IoT telemetry generator -> %s", output_dir)
    logger.info("  batch_size=%d  interval=%ds  devices=%d  anomaly_prob=%.2f",
                batch_size, interval, num_devices, anomaly_prob)

    batch_num = 0
    try:
        while True:
            path = write_batch(output_dir, batch_size, device_ids, anomaly_prob)
            batch_num += 1
            logger.info("Batch %d written -> %s", batch_num, path.name)
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Generator stopped after %d batches", batch_num)


if __name__ == "__main__":
    main()
