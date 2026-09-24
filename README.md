# Real-time sensor data processing CLI and Dashboard

A python-based streaming analytics pipeline that continuously ingests simultaed cold-storage telemetry, computes real-time rolling statistics utilizing Pandas and Numpy, detects thermal anomalies via numerical and statistical rules, and logs processed data to disk.

## What it does?

1. Generates and ingests 1-Hz timestamped IoT records (`temp_c`, `humidity_pct`, `sensor_id`)
2. Data cleaning and imputation
3. Maintains a sliding window using `deque` from `collections` library and computes:
   - Rolling mean
   - Rolling Std.dev.
   - Rolling min/max bounds
4. Detects anomalies on 2 levels:
   - Threshold excursions (temperature < 2.0 or > 8.0)
   - Checks for dynamic spikes of Z-index
5. Stores clean, processed data and anomalies into persistent storage.

## Project structure

```
├── app.py                    # Streamlit (web) dashboard
├── config.py                 # Configuration
├── generator.py              # Stream simulation generator
├── main.py                   # CLI-dashboard
├── processor.py              # Data processor
├── pyproject.toml            # Project dependencies
└── README.md
```

## Quickstart

### 1. Install dependencies

Ensure you have Python 3.10+ installed with pip or uv

```bash
pip install .
```

or with uv (recommended)

```bash
uv sync
```

### 2. Launch the stream

#### Option A: Interactive web dashboard

```bash
streamlit run app.py
```

#### Option B: CLI Tool

```bash
# Continuous stream (1 event/sec)
python main.py

# Custom interval and record limit (e.g., 50 records at 0.1s interval)
python main.py --interval 0.1 --max-records 50

# Custom sensor ID and window size
python main.py --sensor-id VACCINE_FREEZER_02 --window 30
```
