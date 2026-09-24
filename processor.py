from collections import deque

import numpy as np
import pandas as pd

from config import SAFE_TEMP_MAX, SAFE_TEMP_MIN, STORAGE, Z_SCORE_THRESHOLD


class StreamProcessor:
    def __init__(self, window_size: int = 20, sink_file: str = STORAGE):
        self.window_size = window_size
        self.sink_file = sink_file
        self.raw_window = deque(maxlen=window_size)
        self.total_processed = 0
        self.total_anomalies = 0

        df = pd.DataFrame(
            columns=[
                "timestamp",
                "sensor_id",
                "temp_c",
                "humidity_pct",
                "roll_mean_temp",
                "roll_std_temp",
                "roll_min_temp",
                "roll_max_temp",
                "is_anomaly",
                "alert_reason",
            ]
        )
        df.to_csv(self.sink_file, index=False)

    def clean_record(self, record: dict) -> dict:
        """Sanitizes incoming fields and imputes missing values."""
        clean = record.copy()

        if clean.get("temp_c") is None:
            if len(self.raw_window) > 0:
                recent_temps = [
                    r["temp_c"] for r in self.raw_window if r.get("temp_c") is not None
                ]
                clean["temp_c"] = float(np.mean(recent_temps)) if recent_temps else 4.0
            else:
                clean["temp_c"] = 4.0
        else:
            clean["temp_c"] = float(clean["temp_c"])

        if clean.get("humidity_pct") is None:
            clean["humidity_pct"] = 45.0
        else:
            clean["humidity_pct"] = float(clean["humidity_pct"])

        return clean

    def detect_anomalies(
        self, current_temp: float, roll_mean: float, roll_std: float
    ) -> tuple[bool, str]:
        """Detects threshold breaches and statistical z-score outliers."""
        is_anomaly = False
        reasons = []

        if current_temp < SAFE_TEMP_MIN:
            is_anomaly = True
            reasons.append("CRITICAL_FREEZE_RISK")
        elif current_temp > SAFE_TEMP_MAX:
            is_anomaly = True
            reasons.append("CRITICAL_WARM_EXCURSION")

        if roll_std > 0.05:
            z_score = abs(current_temp - roll_mean) / roll_std
            if z_score >= Z_SCORE_THRESHOLD:
                is_anomaly = True
                reasons.append(f"Z_SCORE_SPIKE (Z={z_score:.2f})")

        alert_message = " | ".join(reasons) if is_anomaly else "NORMAL"
        return is_anomaly, alert_message

    def process_incoming_record(self, record: dict) -> dict:
        """Processes one streaming event, updates state, and appends to sink."""
        clean = self.clean_record(record)
        self.raw_window.append(clean)
        self.total_processed += 1

        window_df = pd.DataFrame(list(self.raw_window))
        temps = window_df["temp_c"].to_numpy()

        roll_mean = float(np.mean(temps))
        roll_std = float(np.std(temps, ddof=1)) if len(temps) > 1 else 0.0
        roll_min = float(np.min(temps))
        roll_max = float(np.max(temps))

        curr_temp = clean["temp_c"]
        is_anomaly, alert_reason = self.detect_anomalies(curr_temp, roll_mean, roll_std)

        if is_anomaly:
            self.total_anomalies += 1

        analyzed_row = {
            "timestamp": clean["timestamp"],
            "sensor_id": clean["sensor_id"],
            "temp_c": curr_temp,
            "humidity_pct": clean["humidity_pct"],
            "roll_mean_temp": round(roll_mean, 3),
            "roll_std_temp": round(roll_std, 3),
            "roll_min_temp": round(roll_min, 2),
            "roll_max_temp": round(roll_max, 2),
            "is_anomaly": is_anomaly,
            "alert_reason": alert_reason,
        }

        pd.DataFrame([analyzed_row]).to_csv(
            self.sink_file, mode="a", header=False, index=False
        )

        return analyzed_row
