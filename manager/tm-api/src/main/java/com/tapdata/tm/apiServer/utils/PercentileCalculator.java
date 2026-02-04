package com.tapdata.tm.apiServer.utils;

import lombok.Getter;

import java.util.List;

public class PercentileCalculator {

    private PercentileCalculator() {

    }

    /**
     * Calculate percentiles - automatic processing of small datasets
     */
    public static Double calculatePercentile(List<Double> values, double percentile) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        // Convert to array and sort
        double[] sortedValues = values.stream()
                .mapToDouble(Double::doubleValue)
                .sorted()
                .toArray();

        int n = sortedValues.length;

        // Small dataset processing strategy
        if (n <= 5) {
            return handleSmallDataset(sortedValues, percentile);
        } else {
            return handleLargeDataset(sortedValues, percentile);
        }
    }

    /**
     * Processing small datasets (≤ 5 data points)
     */
    private static Double handleSmallDataset(double[] sortedValues, double percentile) {
        int n = sortedValues.length;

        // Single data point
        if (n == 1) {
            return sortedValues[0];
        }

        // Use the nearest rank method
        double position = percentile * (n + 1);
        int index = (int) Math.round(position) - 1;

        // Bound checking
        if (index < 0) {
            return sortedValues[0];
        } else if (index >= n) {
            return sortedValues[n - 1];
        } else {
            return sortedValues[index];
        }
    }

    /**
     * Processing large datasets (>5 data points)
     */
    private static Double handleLargeDataset(double[] sortedValues, double percentile) {
        int n = sortedValues.length;
        long position = (long) ((n - 1) * percentile);
        int index = (int) position;
        long fraction = position - index;

        if (fraction == 0) {
            return sortedValues[index];
        } else {
            return sortedValues[index] + fraction * (sortedValues[index + 1] - sortedValues[index]);
        }
    }

    /**
     * Calculate multiple percentiles
     */
    public static PercentileResult calculatePercentiles(List<Double> values) {
        return new PercentileResult(
                calculatePercentile(values, 0.50),
                calculatePercentile(values, 0.95),
                calculatePercentile(values, 0.99)
        );
    }

    /**
     * Weighted percentile calculation (applicable to situations where certain data needs to be emphasized)
     */
    public static Double calculateWeightedPercentile(List<WeightedValue> weightedValues, double percentile) {
        if (weightedValues == null || weightedValues.isEmpty()) {
            return null;
        }

        // Sort by value
        weightedValues.sort((a, b) -> Double.compare(a.value, b.value));

        // Calculate cumulative weight
        double totalWeight = weightedValues.stream()
                .mapToDouble(wv -> wv.weight)
                .sum();

        double targetWeight = percentile * totalWeight;
        double cumulativeWeight = 0.0;

        for (WeightedValue wv : weightedValues) {
            cumulativeWeight += wv.weight;
            if (cumulativeWeight >= targetWeight) {
                return wv.value;
            }
        }

        return weightedValues.get(weightedValues.size() - 1).value;
    }

    /**
     * Result encapsulation class
     */
    @Getter
    public static class PercentileResult {
        public final Double p50;
        public final Double p95;
        public final Double p99;

        public PercentileResult(Double p50, Double p95, Double p99) {
            this.p50 = p50;
            this.p95 = p95;
            this.p99 = p99;
        }

        @Override
        public String toString() {
            return String.format("P50: %.0f, P95: %.0f, P99: %.0f", p50, p95, p99);
        }
    }

    /**
     * Weighted values
     */
    public static class WeightedValue {
        public final double value;
        public final double weight;

        public WeightedValue(double value, double weight) {
            this.value = value;
            this.weight = weight;
        }
    }
}