package com.odysseus.streaming.cep;

import com.odysseus.streaming.model.AlertEvent;
import com.odysseus.streaming.model.SensorEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AnomalyPatternsTest {

    @Test
    @DisplayName("Verify deterministic SHA-256 idempotency calculation")
    void testIdempotencyTokenDeterminism() {
        String token1 = AlertEvent.calculateIdempotencyToken(
                "TURBINE-001",
                AnomalyPatterns.PATTERN_THERMAL_KINETIC,
                1000L,
                25000L
        );
        String token2 = AlertEvent.calculateIdempotencyToken(
                "TURBINE-001",
                AnomalyPatterns.PATTERN_THERMAL_KINETIC,
                1000L,
                25000L
        );
        String tokenDifferentWindow = AlertEvent.calculateIdempotencyToken(
                "TURBINE-001",
                AnomalyPatterns.PATTERN_THERMAL_KINETIC,
                1000L,
                26000L
        );

        assertThat(token1)
                .isNotNull()
                .hasSize(64)
                .isEqualTo(token2);

        assertThat(token1).isNotEqualTo(tokenDifferentWindow);
    }

    @Test
    @DisplayName("Verify CEP Pattern matches thermal spike followed by kinetic vibration within 30s")
    void testCepPatternMatchesCompositeFailure() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long baseTime = 100_000L;
        List<SensorEvent> events = List.of(
                // Baseline event
                new SensorEvent("TURBINE-001", "KINETIC_TURBINE", baseTime, 60.0, 1.5, 101.0, 40.0, 24.0, "OK"),
                // Stage 1: Thermal Spike (> 85C)
                new SensorEvent("TURBINE-001", "KINETIC_TURBINE", baseTime + 2000L, 88.5, 1.8, 101.0, 42.0, 23.9, "THERMAL_WARNING"),
                // Intermediary normal event
                new SensorEvent("TURBINE-001", "KINETIC_TURBINE", baseTime + 8000L, 86.0, 2.0, 101.0, 43.0, 23.9, "OK"),
                // Stage 2: Kinetic Vibration Surge (> 4.5 mm/s) occurring 15s after thermal spike
                new SensorEvent("TURBINE-001", "KINETIC_TURBINE", baseTime + 17000L, 84.0, 5.2, 101.0, 44.0, 23.8, "CATASTROPHIC_VIBRATION")
        );

        DataStream<SensorEvent> stream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        org.apache.flink.api.common.eventtime.WatermarkStrategy
                                .<SensorEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((e, t) -> e.getEventTimestamp())
                );

        Pattern<SensorEvent, ?> pattern = AnomalyPatterns.createCatastrophePattern();
        PatternStream<SensorEvent> patternStream = CEP.pattern(stream.keyBy(SensorEvent::getSensorId), pattern);

        List<AlertEvent> collectedAlerts = new ArrayList<>();
        patternStream.select(new AnomalyPatterns.CatastropheAlertSelectFunction())
                .executeAndCollect()
                .forEachRemaining(collectedAlerts::add);

        assertThat(collectedAlerts).hasSize(1);
        AlertEvent alert = collectedAlerts.get(0);
        assertThat(alert.getSensorId()).isEqualTo("TURBINE-001");
        assertThat(alert.getSeverity()).isEqualTo("CRITICAL");
        assertThat(alert.getTriggerTemperature()).isEqualTo(88.5);
        assertThat(alert.getTriggerVibration()).isEqualTo(5.2);
        assertThat(alert.getAlertId()).isNotBlank();
    }

    @Test
    @DisplayName("Verify CEP Pattern ignores vibration spikes occurring beyond the 30s window")
    void testCepPatternIgnoresEventsOutsideWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        long baseTime = 100_000L;
        List<SensorEvent> events = List.of(
                // Stage 1: Thermal Spike at t=0s
                new SensorEvent("TURBINE-002", "KINETIC_TURBINE", baseTime, 92.0, 1.5, 101.0, 40.0, 24.0, "THERMAL_WARNING"),
                // Stage 2: Vibration Surge at t=45s (> 30s time window)
                new SensorEvent("TURBINE-002", "KINETIC_TURBINE", baseTime + 45_000L, 70.0, 5.5, 101.0, 40.0, 24.0, "CATASTROPHIC_VIBRATION")
        );

        DataStream<SensorEvent> stream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(
                        org.apache.flink.api.common.eventtime.WatermarkStrategy
                                .<SensorEvent>forMonotonousTimestamps()
                                .withTimestampAssigner((e, t) -> e.getEventTimestamp())
                );

        Pattern<SensorEvent, ?> pattern = AnomalyPatterns.createCatastrophePattern();
        PatternStream<SensorEvent> patternStream = CEP.pattern(stream.keyBy(SensorEvent::getSensorId), pattern);

        List<AlertEvent> collectedAlerts = new ArrayList<>();
        patternStream.select(new AnomalyPatterns.CatastropheAlertSelectFunction())
                .executeAndCollect()
                .forEachRemaining(collectedAlerts::add);

        // Expect 0 alerts because delta time was 45s (> 30s constraint)
        assertThat(collectedAlerts).isEmpty();
    }
}

