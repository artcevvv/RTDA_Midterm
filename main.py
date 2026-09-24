import argparse
import sys

from config import STORAGE, STREAM_INTERVAL, WINDOW_SIZE
from generator import SensorStreamGenerator
from processor import StreamProcessor


def parse_args():
    parser = argparse.ArgumentParser(description="CLI cold-chain stream processor")
    parser.add_argument(
        "--sensor-id",
        type=str,
        default="SENSOR_COLD_01",
        help="Unique identifier for the cold room sensor",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=STREAM_INTERVAL,
        help="Delay between streaming events in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=WINDOW_SIZE,
        help="Sliding window size (default: 20)",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Maximum records to process before exiting (default: infinite)",
    )
    parser.add_argument(
        "--sink",
        type=str,
        default=STORAGE,
        help="Path to CSV log file",
    )
    return parser.parse_args()


def print_banner(args):
    print("=" * 105)
    print(
        " cold-chain iot telemetry :: standalone cli processor ".upper().center(
            105, "="
        )
    )
    print("=" * 105)
    print(f" Sensor ID     : {args.sensor_id}")
    print(f" Stream Rate   : 1 event / {args.interval}s")
    print(f" Window Size   : {args.window} samples")
    print(f" Sink Storage  : {args.sink}")
    print(
        f" Record Limit  : {'Infinite (Ctrl+C to stop)' if args.max_records is None else args.max_records}"
    )
    print("-" * 105)
    print(
        f"{'STATUS':<9} | {'TIMESTAMP':<10} | {'TEMP (°C)':<9} | "
        f"{'MEAN':<8} | {'STD':<6} | {'RANGE [MIN - MAX]':<18} | {'ALERT REASON'}"
    )
    print("-" * 105)


def run_pipeline():
    args = parse_args()
    print_banner(args)

    generator = SensorStreamGenerator(sensor_id=args.sensor_id)
    processor = StreamProcessor(window_size=args.window, sink_file=args.sink)

    processed_count = 0

    try:
        for record in generator.generate(interval_sec=args.interval):
            result = processor.process_incoming_record(record)
            processed_count += 1

            status_tag = "[ALERT]" if result["is_anomaly"] else "[OK]   "
            range_str = (
                f"[{result['roll_min_temp']:.2f} - {result['roll_max_temp']:.2f}]"
            )

            print(
                f"{status_tag:<9} | "
                f"{result['timestamp']:<10} | "
                f"{result['temp_c']:>9.2f} | "
                f"{result['roll_mean_temp']:>8.2f} | "
                f"{result['roll_std_temp']:>6.2f} | "
                f"{range_str:<18} | "
                f"{result['alert_reason']}"
            )

            if args.max_records and processed_count >= args.max_records:
                print("\nReached requested maximum record count.")
                break

    except KeyboardInterrupt:
        print("\nShutdown signal received. Stopping stream...")

    finally:
        print("=" * 105)
        print("PIPELINE SESSION SUMMARY")
        print("=" * 105)
        print(f"Total records           : {processor.total_processed}")
        print(f"Anomalies flagged       : {processor.total_anomalies}")
        if processor.total_processed > 0:
            rate = (processor.total_anomalies / processor.total_processed) * 100
            print(f"Anomaly rate            : {rate:.2f}%")
        print(f"Output sink  : {args.sink}")
        print("=" * 105)
        sys.exit(0)


if __name__ == "__main__":
    run_pipeline()
