package com.odysseus.streaming.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

/**
 * Analytical aggregation model produced by Flink sliding windows.
 * Formatted for Parquet columnar storage in Amazon S3.
 */
public class AggregatedSensorMetric implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("sensor_id")
    private String sensorId;

    @JsonProperty("window_start")
    private long windowStart;

    @JsonProperty("window_end")
    private long windowEnd;

    @JsonProperty("avg_temperature")
    private double avgTemperature;

    @JsonProperty("max_temperature")
    private double maxTemperature;

    @JsonProperty("avg_vibration")
    private double avgVibration;

    @JsonProperty("max_vibration")
    private double maxVibration;

    @JsonProperty("avg_pressure")
    private double avgPressure;

    @JsonProperty("avg_power")
    private double avgPower;

    @JsonProperty("min_battery")
    private double minBattery;

    @JsonProperty("sample_count")
    private long sampleCount;

    @JsonProperty("anomaly_flag")
    private boolean anomalyFlag;

    public AggregatedSensorMetric() {
    }

    public AggregatedSensorMetric(String sensorId, long windowStart, long windowEnd,
                                  double avgTemperature, double maxTemperature,
                                  double avgVibration, double maxVibration,
                                  double avgPressure, double avgPower,
                                  double minBattery, long sampleCount, boolean anomalyFlag) {
        this.sensorId = sensorId;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.avgTemperature = avgTemperature;
        this.maxTemperature = maxTemperature;
        this.avgVibration = avgVibration;
        this.maxVibration = maxVibration;
        this.avgPressure = avgPressure;
        this.avgPower = avgPower;
        this.minBattery = minBattery;
        this.sampleCount = sampleCount;
        this.anomalyFlag = anomalyFlag;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public double getAvgTemperature() {
        return avgTemperature;
    }

    public void setAvgTemperature(double avgTemperature) {
        this.avgTemperature = avgTemperature;
    }

    public double getMaxTemperature() {
        return maxTemperature;
    }

    public void setMaxTemperature(double maxTemperature) {
        this.maxTemperature = maxTemperature;
    }

    public double getAvgVibration() {
        return avgVibration;
    }

    public void setAvgVibration(double avgVibration) {
        this.avgVibration = avgVibration;
    }

    public double getMaxVibration() {
        return maxVibration;
    }

    public void setMaxVibration(double maxVibration) {
        this.maxVibration = maxVibration;
    }

    public double getAvgPressure() {
        return avgPressure;
    }

    public void setAvgPressure(double avgPressure) {
        this.avgPressure = avgPressure;
    }

    public double getAvgPower() {
        return avgPower;
    }

    public void setAvgPower(double avgPower) {
        this.avgPower = avgPower;
    }

    public double getMinBattery() {
        return minBattery;
    }

    public void setMinBattery(double minBattery) {
        this.minBattery = minBattery;
    }

    public long getSampleCount() {
        return sampleCount;
    }

    public void setSampleCount(long sampleCount) {
        this.sampleCount = sampleCount;
    }

    public boolean isAnomalyFlag() {
        return anomalyFlag;
    }

    public void setAnomalyFlag(boolean anomalyFlag) {
        this.anomalyFlag = anomalyFlag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregatedSensorMetric that = (AggregatedSensorMetric) o;
        return windowStart == that.windowStart &&
                windowEnd == that.windowEnd &&
                sampleCount == that.sampleCount &&
                Objects.equals(sensorId, that.sensorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, windowStart, windowEnd, sampleCount);
    }
}

