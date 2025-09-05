package com.tapdata.tm.apiCalls.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PercentileCalculator {

    private PercentileCalculator() {

    }

    /**
     * 计算百分位数 - 自动处理小数据集
     */
    public static Long calculatePercentile(List<Long> values, double percentile) {
        if (values == null || values.isEmpty()) {
            return null;
        }

        // 转换为数组并排序
        long[] sortedValues = values.stream()
                .mapToLong(Long::longValue)
                .sorted()
                .toArray();

        int n = sortedValues.length;

        // 小数据集处理策略
        if (n <= 5) {
            return handleSmallDataset(sortedValues, percentile);
        } else {
            return handleLargeDataset(sortedValues, percentile);
        }
    }

    /**
     * 处理小数据集（≤5个数据点）
     */
    private static Long handleSmallDataset(long[] sortedValues, double percentile) {
        int n = sortedValues.length;

        // 单个数据点
        if (n == 1) {
            return sortedValues[0];
        }

        // 使用最近秩方法
        double position = percentile * (n + 1);
        int index = (int) Math.round(position) - 1;

        // 边界检查
        if (index < 0) {
            return sortedValues[0];
        } else if (index >= n) {
            return sortedValues[n - 1];
        } else {
            return sortedValues[index];
        }
    }

    /**
     * 处理大数据集（>5个数据点）
     */
    private static Long handleLargeDataset(long[] sortedValues, double percentile) {
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
     * 计算多个百分位数
     */
    public static PercentileResult calculatePercentiles(List<Long> values) {
        return new PercentileResult(
                calculatePercentile(values, 0.50),
                calculatePercentile(values, 0.95),
                calculatePercentile(values, 0.99)
        );
    }

    /**
     * 带权重的百分位数计算（适用于需要强调某些数据的情况）
     */
    public static double calculateWeightedPercentile(List<WeightedValue> weightedValues, double percentile) {
        if (weightedValues == null || weightedValues.isEmpty()) {
            return 0.0;
        }

        // 按值排序
        weightedValues.sort((a, b) -> Double.compare(a.value, b.value));

        // 计算累计权重
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
     * 结果封装类
     */
    public static class PercentileResult {
        public final Long p50;
        public final Long p95;
        public final Long p99;

        public PercentileResult(Long p50, Long p95, Long p99) {
            this.p50 = p50;
            this.p95 = p95;
            this.p99 = p99;
        }

        public Long getP50() {
            return p50;
        }

        public Long getP95() {
            return p95;
        }

        public Long getP99() {
            return p99;
        }

        @Override
        public String toString() {
            return String.format("P50: %d, P95: %d, P99: %d", p50, p95, p99);
        }
    }

    /**
     * 带权重的值
     */
    public static class WeightedValue {
        public final double value;
        public final double weight;

        public WeightedValue(double value, double weight) {
            this.value = value;
            this.weight = weight;
        }
    }

    /**
     * 测试示例
     */
    public static void main(String[] args) {
        // 测试小数据集
        List<Long> smallDataset = Arrays.asList(10L, 15L, 20L, 25L, 30L);
        System.out.println("小数据集: " + smallDataset);
        System.out.println("百分位数: " + calculatePercentiles(smallDataset));

        // 测试单个数据点
        List<Long> singleValue = Arrays.asList(42L);
        System.out.println("\n单个数据点: " + singleValue);
        System.out.println("百分位数: " + calculatePercentiles(singleValue));

        // 测试两个数据点
        List<Long> twoValues = Arrays.asList(10L, 20L);
        System.out.println("\n两个数据点: " + twoValues);
        System.out.println("百分位数: " + calculatePercentiles(twoValues));

        // 测试空数据集
        List<Long> emptyList = new ArrayList<>();
        System.out.println("\n空数据集: " + emptyList);
        System.out.println("百分位数: " + calculatePercentiles(emptyList));

        // 测试带权重的计算
        List<WeightedValue> weightedValues = Arrays.asList(
                new WeightedValue(10.0, 1.0),
                new WeightedValue(20.0, 2.0),
                new WeightedValue(30.0, 3.0)
        );
        double weightedP50 = calculateWeightedPercentile(weightedValues, 0.5);
        System.out.println("\n带权重的P50: " + weightedP50);
    }
}