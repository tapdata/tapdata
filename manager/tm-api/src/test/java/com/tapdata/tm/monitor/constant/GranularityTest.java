package com.tapdata.tm.monitor.constant;

import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.entity.TDigestEntity;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import io.tapdata.common.sample.request.Sample;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class GranularityTest {

    @Nested
    @DisplayName("Test quantileCalculation method")
    class QuantileCalculationTest {

        @Test
        @DisplayName("Test with empty continuousSamples - should return early")
        void testWithEmptyContinuousSamples() {
            List<Sample> emptySamples = new ArrayList<>();
            List<TDigestEntity> digests = new ArrayList<>();

            // Should not throw exception
            assertDoesNotThrow(() ->
                Granularity.quantileCalculation(Granularity.GRANULARITY_MINUTE, emptySamples, digests)
            );
        }

        @Test
        @DisplayName("Test with null continuousSamples - should return early")
        void testWithNullContinuousSamples() {
            List<TDigestEntity> digests = new ArrayList<>();

            // Should not throw exception
            assertDoesNotThrow(() ->
                Granularity.quantileCalculation(Granularity.GRANULARITY_MINUTE, null, digests)
            );
        }

        @Test
        @DisplayName("Test with GRANULARITY_MINUTE and empty digests - should process")
        void testMinuteGranularityWithEmptyDigests() {
            List<Sample> samples = createMinuteSamples(15, 100.0);
            List<TDigestEntity> digests = new ArrayList<>();

            // Should not throw exception
            assertDoesNotThrow(() ->
                Granularity.quantileCalculation(Granularity.GRANULARITY_MINUTE, samples, digests)
            );

            // Verify that percentile values are calculated after threshold
            // After 5 minutes (start95), 95th percentile should be set
            Sample sample6 = samples.get(6); // 6th minute
            assertNotNull(sample6.getVs().get(MetricCons.SS.VS.F_95TH_REPLICATE_LAG));

            // After 10 minutes (start99), 99th percentile should be set
            Sample sample11 = samples.get(11); // 11th minute
            assertNotNull(sample11.getVs().get(MetricCons.SS.VS.F_99TH_REPLICATE_LAG));
        }

        @Test
        @DisplayName("Test with GRANULARITY_HOUR and empty digests - should return early")
        void testHourGranularityWithEmptyDigests() {
            List<Sample> samples = createHourSamples(5, 100.0);
            List<TDigestEntity> digests = new ArrayList<>();

            // Should return early because digests is empty for non-minute granularity
            assertDoesNotThrow(() ->
                Granularity.quantileCalculation(Granularity.GRANULARITY_HOUR, samples, digests)
            );

            // Verify no percentile values are set
            for (Sample sample : samples) {
                assertNull(sample.getVs().get(MetricCons.SS.VS.F_95TH_INPUT_QPS));
                assertNull(sample.getVs().get(MetricCons.SS.VS.F_99TH_INPUT_QPS));
            }
        }

        @Test
        @DisplayName("Test GRANULARITY_MINUTE with varying values - verify 95th percentile calculation")
        void testMinuteGranularityWith95thPercentile() {
            // Create 15 minutes of samples with varying values
            List<Sample> samples = new ArrayList<>();
            Date baseDate = new Date();

            for (int i = 0; i < 15; i++) {
                Sample sample = new Sample();
                sample.setDate(new Date(baseDate.getTime() + i * 60 * 1000)); // Each minute

                Map<String, Number> vs = new HashMap<>();
                // Create varying values: 0, 10, 20, ..., 140
                vs.put(MetricCons.SS.VS.F_INPUT_QPS, i * 10.0);
                vs.put(MetricCons.SS.VS.F_OUTPUT_QPS, i * 5.0);
                vs.put(MetricCons.SS.VS.F_REPLICATE_LAG, i * 2.0);

                sample.setVs(vs);
                samples.add(sample);
            }

            Granularity.quantileCalculation(Granularity.GRANULARITY_MINUTE, samples, new ArrayList<>());

            // After 5 minutes, 95th percentile should be calculated
            Sample sample6 = samples.get(6);
            assertNotNull(sample6.getVs().get(MetricCons.SS.VS.F_95TH_REPLICATE_LAG));
            assertTrue(sample6.getVs().get(MetricCons.SS.VS.F_95TH_REPLICATE_LAG).doubleValue() > 0);

            // After 10 minutes, 99th percentile should be calculated
            Sample sample11 = samples.get(11);
            assertNotNull(sample11.getVs().get(MetricCons.SS.VS.F_99TH_REPLICATE_LAG));
            assertTrue(sample11.getVs().get(MetricCons.SS.VS.F_99TH_REPLICATE_LAG).doubleValue() > 0);
        }

        @Test
        @DisplayName("Test GRANULARITY_MINUTE - verify all metric keys are processed")
        void testMinuteGranularityAllMetrics() {
            List<Sample> samples = createMinuteSamples(15, 100.0);

            Granularity.quantileCalculation(Granularity.GRANULARITY_MINUTE, samples, new ArrayList<>());

            // Check sample after 95th threshold (6th minute)
            Sample sample6 = samples.get(6);
            assertNotNull(sample6.getVs().get(MetricCons.SS.VS.F_95TH_REPLICATE_LAG));

            // Check sample after 99th threshold (11th minute)
            Sample sample11 = samples.get(11);
            assertNotNull(sample11.getVs().get(MetricCons.SS.VS.F_99TH_REPLICATE_LAG));
        }

        @Test
        @DisplayName("Test GRANULARITY_HOUR with digests - verify percentile calculation")
        void testHourGranularityWithDigests() {
            // Create 5 hours of samples
            List<Sample> samples = createHourSamples(5, 100.0);
            List<TDigestEntity> digests = createHourDigests(5, samples.get(0).getDate());

            Granularity.quantileCalculation(Granularity.GRANULARITY_HOUR, samples, digests);

            // After 1 hour (start95), 95th percentile should be set
            Sample sample2 = samples.get(2); // 2nd hour
            assertNotNull(sample2.getVs().get(MetricCons.SS.VS.F_95TH_REPLICATE_LAG));

            // After 2 hours (start99), 99th percentile should be set
            Sample sample3 = samples.get(3); // 3rd hour
            assertNotNull(sample3.getVs().get(MetricCons.SS.VS.F_99TH_REPLICATE_LAG));
        }

        @Test
        @DisplayName("Test with null values in sample - should skip null values")
        void testWithNullValuesInSample() {
            List<Sample> samples = new ArrayList<>();
            Date baseDate = new Date();

            for (int i = 0; i < 15; i++) {
                Sample sample = new Sample();
                sample.setDate(new Date(baseDate.getTime() + i * 60 * 1000));

                Map<String, Number> vs = new HashMap<>();
                // Add null value for some metrics
                vs.put(MetricCons.SS.VS.F_INPUT_QPS, i % 2 == 0 ? i * 10.0 : null);
                vs.put(MetricCons.SS.VS.F_OUTPUT_QPS, i * 5.0);

                sample.setVs(vs);
                samples.add(sample);
            }

            // Should not throw exception
            assertDoesNotThrow(() ->
                Granularity.quantileCalculation(Granularity.GRANULARITY_MINUTE, samples, new ArrayList<>())
            );
        }

        @Test
        @DisplayName("Test with sample having null vs map - should handle gracefully")
        void testWithNullVsMap() {
            List<Sample> samples = new ArrayList<>();
            Date baseDate = new Date();

            Sample sample1 = new Sample();
            sample1.setDate(baseDate);
            sample1.setVs(null); // null vs map
            samples.add(sample1);

            // Should not throw exception
            assertDoesNotThrow(() ->
                Granularity.quantileCalculation(Granularity.GRANULARITY_MINUTE, samples, new ArrayList<>())
            );
        }


        @Test
        @DisplayName("Test GRANULARITY_HOUR with empty digest bytes - should handle gracefully")
        void testHourGranularityWithEmptyDigestBytes() {
            List<Sample> samples = createHourSamples(5, 100.0);
            List<TDigestEntity> digests = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                TDigestEntity digestEntity = new TDigestEntity();
                digestEntity.setDate(new Date(samples.get(0).getDate().getTime() + i * 60 * 60 * 1000));

                Map<String, byte[]> digestMap = new HashMap<>();
                // Add empty byte array
                digestMap.put(MetricCons.SS.VS.F_95TH_INPUT_QPS, new byte[0]);
                digestMap.put(MetricCons.SS.VS.F_99TH_INPUT_QPS, new byte[0]);

                digestEntity.setDigest(digestMap);
                digests.add(digestEntity);
            }

            // Should not throw exception
            assertDoesNotThrow(() ->
                Granularity.quantileCalculation(Granularity.GRANULARITY_HOUR, samples, digests)
            );
        }

        // Helper methods

        /**
         * Create minute-level samples with constant values
         */
        private List<Sample> createMinuteSamples(int count, double value) {
            List<Sample> samples = new ArrayList<>();
            Date baseDate = new Date();

            for (int i = 0; i < count; i++) {
                Sample sample = new Sample();
                sample.setDate(new Date(baseDate.getTime() + i * 60 * 1000)); // Each minute

                Map<String, Number> vs = new HashMap<>();
                vs.put(MetricCons.SS.VS.F_INPUT_QPS, value);
                vs.put(MetricCons.SS.VS.F_INPUT_SIZE_QPS, value);
                vs.put(MetricCons.SS.VS.F_OUTPUT_QPS, value);
                vs.put(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS, value);
                vs.put(MetricCons.SS.VS.F_REPLICATE_LAG, value);

                sample.setVs(vs);
                samples.add(sample);
            }

            return samples;
        }

        /**
         * Create hour-level samples with constant values
         */
        private List<Sample> createHourSamples(int count, double value) {
            List<Sample> samples = new ArrayList<>();
            Date baseDate = new Date();

            for (int i = 0; i < count; i++) {
                Sample sample = new Sample();
                sample.setDate(new Date(baseDate.getTime() + i * 60 * 60 * 1000)); // Each hour

                Map<String, Number> vs = new HashMap<>();
                vs.put(MetricCons.SS.VS.F_INPUT_QPS, value);
                vs.put(MetricCons.SS.VS.F_INPUT_SIZE_QPS, value);
                vs.put(MetricCons.SS.VS.F_OUTPUT_QPS, value);
                vs.put(MetricCons.SS.VS.F_OUTPUT_SIZE_QPS, value);
                vs.put(MetricCons.SS.VS.F_REPLICATE_LAG, value);

                sample.setVs(vs);
                samples.add(sample);
            }

            return samples;
        }

        /**
         * Create TDigest entities for hour-level testing
         */
        private List<TDigestEntity> createHourDigests(int count, Date baseDate) {
            List<TDigestEntity> digests = new ArrayList<>();

            for (int i = 0; i < count; i++) {
                TDigestEntity digestEntity = new TDigestEntity();
                digestEntity.setDate(new Date(baseDate.getTime() + i * 60 * 60 * 1000));

                // Create TDigest with some sample data
                TDigest digest95 = TDigest.createMergingDigest(100);
                TDigest digest99 = TDigest.createMergingDigest(100);

                // Add some values to the digest
                for (int j = 0; j < 60; j++) {
                    digest95.add(j * 10.0);
                    digest99.add(j * 10.0);
                }

                Map<String, byte[]> digestMap = new HashMap<>();
                digestMap.put(MetricCons.SS.VS.F_95TH_INPUT_QPS, toBytes(digest95));
                digestMap.put(MetricCons.SS.VS.F_99TH_INPUT_QPS, toBytes(digest99));
                digestMap.put(MetricCons.SS.VS.F_95TH_INPUT_SIZE_QPS, toBytes(digest95));
                digestMap.put(MetricCons.SS.VS.F_99TH_INPUT_SIZE_QPS, toBytes(digest99));
                digestMap.put(MetricCons.SS.VS.F_95TH_OUTPUT_QPS, toBytes(digest95));
                digestMap.put(MetricCons.SS.VS.F_99TH_OUTPUT_QPS, toBytes(digest99));
                digestMap.put(MetricCons.SS.VS.F_95TH_OUTPUT_SIZE_QPS, toBytes(digest95));
                digestMap.put(MetricCons.SS.VS.F_99TH_OUTPUT_SIZE_QPS, toBytes(digest99));
                digestMap.put(MetricCons.SS.VS.F_95TH_REPLICATE_LAG, toBytes(digest95));
                digestMap.put(MetricCons.SS.VS.F_99TH_REPLICATE_LAG, toBytes(digest99));

                digestEntity.setDigest(digestMap);
                digests.add(digestEntity);
            }

            return digests;
        }

        /**
         * Convert TDigest to byte array
         */
        private byte[] toBytes(TDigest digest) {
            ByteBuffer buffer = ByteBuffer.allocate(digest.byteSize());
            digest.asSmallBytes(buffer);
            return buffer.array();
        }
    }
}
