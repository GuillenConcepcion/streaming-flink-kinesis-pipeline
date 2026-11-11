#!/usr/bin/env python3
"""
High-Throughput Synthetic Industrial IoT Telemetry Generator for AWS Kinesis.
Part of Proyecto Odysseus - Streaming MLOps Pipeline.

Features:
- Configurable event generation rate (QPS) and batch dispatching via Kinesis PutRecords.
- Injects temporal network jitter to simulate out-of-order event delivery (Event-Time vs Processing-Time).
- Periodic injection of multi-stage thermal & vibration failure signatures for CEP pattern validation.
- Supports malformed payloads for testing side-output Dead-Letter Queues (DLQ).
"""

import argparse
import json
import logging
import os
import random
import time
from typing import Dict, Any

import boto3
from botocore.config import Config
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("TelemetryGenerator")

DEVICE_TYPES = ["KINETIC_TURBINE", "CENTRIFUGAL_PUMP", "COMPRESSOR_STAGE", "COOLING_TOWER"]
SENSORS = [f"TURBINE-{i:03d}" for i in range(1, 21)]  # 20 active machines


class TelemetryGenerator:
    def __init__(
        self,
        stream_name: str,
        endpoint_url: str,
        region: str,
        rate_qps: int = 1000,
        batch_size: int = 250,
        anomaly_interval: int = 30,
        enable_malformed: bool = True
    ):
        self.stream_name = stream_name
        self.endpoint_url = endpoint_url
        self.region = region
        self.rate_qps = max(10, rate_qps)
        self.batch_size = min(500, max(1, batch_size))
        self.anomaly_interval = anomaly_interval
        self.enable_malformed = enable_malformed

        # Boto3 client initialization
        boto_config = Config(
            region_name=self.region,
            retries={"max_attempts": 3, "mode": "adaptive"}
        )
        self.kinesis = boto3.client(
            "kinesis",
            endpoint_url=self.endpoint_url,
            config=boto_config,
            aws_access_key_id="test",
            aws_secret_access_key="test"
        )

        # Baseline machine states
        self.sensor_baselines: Dict[str, Dict[str, float]] = {}
        for s in SENSORS:
            self.sensor_baselines[s] = {
                "base_temp": random.uniform(50.0, 65.0),
                "base_vib": random.uniform(1.2, 2.2),
                "base_press": random.uniform(101.0, 108.0),
                "base_power": random.uniform(35.0, 52.0),
                "battery": random.uniform(23.8, 24.4),
                "device_type": random.choice(DEVICE_TYPES)
            }

        # Anomaly orchestration state
        self.last_anomaly_trigger = time.time()
        self.active_anomalies: Dict[str, Dict[str, Any]] = {}

    def generate_single_event(self, sensor_id: str) -> Dict[str, Any]:
        """Generates a single telemetry payload with stochastic sensor dynamics."""
        base = self.sensor_baselines[sensor_id]
        now_ms = int(time.time() * 1000)

        # Inject realistic network delivery jitter (-2500ms to +500ms)
        jitter_ms = int(random.gauss(0, 400))
        event_timestamp = now_ms + jitter_ms

        temp = float(np.random.normal(base["base_temp"], 1.1))
        vib = float(np.random.normal(base["base_vib"], 0.25))
        press = float(np.random.normal(base["base_press"], 0.5))
        power = float(np.random.normal(base["base_power"], 1.5))
        battery = max(0.0, base["battery"] - random.uniform(0.0001, 0.0005))
        base["battery"] = battery

        status = "OK"

        # Check if sensor has an active CEP anomaly sequence in flight
        if sensor_id in self.active_anomalies:
            state = self.active_anomalies[sensor_id]
            elapsed = time.time() - state["start_time"]

            if state["phase"] == 1:
                # Stage 1: Thermal Spike (> 85°C)
                temp = float(random.uniform(86.5, 96.0))
                status = "THERMAL_WARNING"
                if elapsed >= 8.0:
                    state["phase"] = 2
                    logger.warning(
                        f"🔥 [ANOMALY PHASE 2 TRANSITION] Sensor {sensor_id}: Thermal spike active, initiating kinetic vibration stage in 5s."
                    )
            elif state["phase"] == 2:
                # Stage 2: Kinetic Vibration Surge (> 4.5 mm/s) occurring 10-25s after thermal spike
                vib = float(random.uniform(4.8, 6.8))
                temp = float(random.uniform(84.0, 92.0))
                status = "CATASTROPHIC_VIBRATION"
                if elapsed >= 25.0:
                    del self.active_anomalies[sensor_id]
                    logger.info(f"✅ [ANOMALY RESOLVED] Sensor {sensor_id}: Anomaly injection sequence concluded.")

        # Rare malformed record injection (0.2% probability) to validate DLQ side-output
        if self.enable_malformed and random.random() < 0.002:
            return {"malformed_corrupt_payload": "corrupted_byte_sequence", "raw_sensor": sensor_id}

        return {
            "sensor_id": sensor_id,
            "device_type": base["device_type"],
            "event_timestamp": event_timestamp,
            "temperature_celsius": round(temp, 2),
            "vibration_rms": round(vib, 3),
            "pressure_kpa": round(press, 2),
            "power_draw_kw": round(power, 2),
            "battery_voltage": round(battery, 3),
            "status_code": status
        }

    def check_anomaly_injection(self):
        """Periodically trigger a multi-stage anomaly on a random sensor."""
        now = time.time()
        if now - self.last_anomaly_trigger >= self.anomaly_interval:
            target_sensor = random.choice(SENSORS)
            self.active_anomalies[target_sensor] = {
                "start_time": now,
                "phase": 1
            }
            self.last_anomaly_trigger = now
            logger.warning(
                f"🚨 [ANOMALY INJECTED] Target: {target_sensor} | Initiating Stage 1: Thermal Spike (T > 85°C)."
            )

    def run(self):
        """Main batch generation and dispatch loop."""
        logger.info(
            f"Starting Telemetry Generator -> Stream: '{self.stream_name}', Rate: {self.rate_qps} QPS, Batch: {self.batch_size}"
        )
        total_sent = 0
        start_time = time.time()

        interval = self.batch_size / float(self.rate_qps)

        try:
            while True:
                batch_start = time.time()
                self.check_anomaly_injection()

                records = []
                for _ in range(self.batch_size):
                    s_id = random.choice(SENSORS)
                    payload = self.generate_single_event(s_id)
                    data_bytes = json.dumps(payload).encode("utf-8")
                    records.append({
                        "Data": data_bytes,
                        "PartitionKey": s_id
                    })

                # Dispatch PutRecords batch to Kinesis
                try:
                    resp = self.kinesis.put_records(
                        Records=records,
                        StreamName=self.stream_name
                    )
                    failed = resp.get("FailedRecordCount", 0)
                    if failed > 0:
                        logger.warning(f"Kinesis PutRecords reported {failed} failed records in batch.")
                except Exception as e:
                    logger.error(f"Error publishing batch to Kinesis: {e}")

                total_sent += len(records)
                elapsed = time.time() - start_time
                if total_sent % (self.batch_size * 10) == 0:
                    current_qps = total_sent / max(0.001, elapsed)
                    logger.info(f"Published {total_sent:,} total records ({current_qps:.1f} events/sec).")

                # Throttle to sustain requested QPS
                processing_time = time.time() - batch_start
                sleep_needed = interval - processing_time
                if sleep_needed > 0:
                    time.sleep(sleep_needed)

        except KeyboardInterrupt:
            logger.info("Telemetry Generator stopped by user.")


def main():
    parser = argparse.ArgumentParser(description="Synthetic Industrial IoT Telemetry Generator")
    parser.add_argument("--stream", type=str, default="odysseus-sensor-stream", help="Kinesis Stream Name")
    parser.add_argument("--endpoint", type=str, default=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"), help="AWS Endpoint URL")
    parser.add_argument("--region", type=str, default=os.getenv("AWS_DEFAULT_REGION", "us-east-1"), help="AWS Region")
    parser.add_argument("--rate", type=int, default=1000, help="Target Events per Second (QPS)")
    parser.add_argument("--batch-size", type=int, default=250, help="Kinesis PutRecords batch size")
    parser.add_argument("--anomaly-interval", type=int, default=30, help="Interval in seconds between anomaly injections")
    parser.add_argument("--no-malformed", action="store_true", help="Disable malformed payload generation")

    args = parser.parse_args()

    gen = TelemetryGenerator(
        stream_name=args.stream,
        endpoint_url=args.endpoint,
        region=args.region,
        rate_qps=args.rate,
        batch_size=args.batch_size,
        anomaly_interval=args.anomaly_interval,
        enable_malformed=not args.no_malformed
    )
    gen.run()


if __name__ == "__main__":
    main()

