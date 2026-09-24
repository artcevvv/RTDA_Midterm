import datetime
import random
import time
from collections.abc import Iterator

import numpy as np


class SensorStreamGenerator:
    def __init__(
        self,
        sensor_id: str = "SENSOR_COLD_01",
        base_temp: float = 4.5,
        base_humidity: float = 45.0,
        anomaly_rate: float = 0.06,
    ):
        self.sensor_id = sensor_id
        self.base_temp = base_temp
        self.base_humidity = base_humidity
        self.anomaly_rate = anomaly_rate

    def next_record(self) -> dict:
        now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S")

        if random.random() < self.anomaly_rate:
            temp = self.base_temp + random.choice([4.2, 5.0, -3.9])
        else:
            temp = self.base_temp + np.random.normal(0, 0.35)

        humidity = self.base_humidity + np.random.normal(0, 1.2)

        record = {
            "timestamp": now,
            "sensor_id": self.sensor_id,
            "temp_c": round(float(temp), 2),
            "humidity_pct": round(float(humidity), 2),
        }

        if random.random() < 0.03:
            record["temp_c"] = None

        return record

    def generate(self, interval_sec: float = 1.0) -> Iterator[dict]:
        """Continuous generators"""
        while True:
            yield self.next_record()
            time.sleep(interval_sec)
