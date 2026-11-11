package com.odysseus.streaming.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.util.Objects;

/**
 * Strongly typed POJO representing an incoming industrial IoT telemetry event.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SensorEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("sensor_id")
    private String sensorId;

    @JsonProperty("device_type")
    private String deviceType;

    @JsonProperty("event_timestamp")
    private long eventTimestamp;

    @JsonProperty("temperature_celsius")
    private double temperatureCelsius;

    @JsonProperty("vibration_rms")
    private double vibrationRms;

    @JsonProperty("pressure_kpa")
    private double pressureKpa;

    @JsonProperty("power_draw_kw")
    private double powerDrawKw;

    @JsonProperty("battery_voltage")
    private double batteryVoltage;

    @JsonProperty("status_code")
    private String statusCode;

    public SensorEvent() {
    }

    public SensorEvent(String sensorId, String deviceType, long eventTimestamp,
                       double temperatureCelsius, double vibrationRms, double pressureKpa,
                       double powerDrawKw, double batteryVoltage, String statusCode) {
        this.sensorId = sensorId;
        this.deviceType = deviceType;
        this.eventTimestamp = eventTimestamp;
        this.temperatureCelsius = temperatureCelsius;
        this.vibrationRms = vibrationRms;
        this.pressureKpa = pressureKpa;
        this.powerDrawKw = powerDrawKw;
        this.batteryVoltage = batteryVoltage;
        this.statusCode = statusCode;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public long getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(long eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public double getTemperatureCelsius() {
        return temperatureCelsius;
    }

    public void setTemperatureCelsius(double temperatureCelsius) {
        this.temperatureCelsius = temperatureCelsius;
    }

    public double getVibrationRms() {
        return vibrationRms;
    }

    public void setVibrationRms(double vibrationRms) {
        this.vibrationRms = vibrationRms;
    }

    public double getPressureKpa() {
        return pressureKpa;
    }

    public void setPressureKpa(double pressureKpa) {
        this.pressureKpa = pressureKpa;
    }

    public double getPowerDrawKw() {
        return powerDrawKw;
    }

    public void setPowerDrawKw(double powerDrawKw) {
        this.powerDrawKw = powerDrawKw;
    }

    public double getBatteryVoltage() {
        return batteryVoltage;
    }

    public void setBatteryVoltage(double batteryVoltage) {
        this.batteryVoltage = batteryVoltage;
    }

    public String getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SensorEvent that = (SensorEvent) o;
        return eventTimestamp == that.eventTimestamp &&
                Double.compare(that.temperatureCelsius, temperatureCelsius) == 0 &&
                Double.compare(that.vibrationRms, vibrationRms) == 0 &&
                Objects.equals(sensorId, that.sensorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, eventTimestamp, temperatureCelsius, vibrationRms);
    }

    @Override
    public String toString() {
        return "SensorEvent{" +
                "sensorId='" + sensorId + '\'' +
                ", eventTimestamp=" + eventTimestamp +
                ", temp=" + temperatureCelsius +
                ", vib=" + vibrationRms +
                ", status='" + statusCode + '\'' +
                '}';
    }
}

