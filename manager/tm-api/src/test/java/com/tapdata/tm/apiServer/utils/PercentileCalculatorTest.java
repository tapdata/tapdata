package com.tapdata.tm.apiServer.utils;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class PercentileCalculatorTest {

    @Test
    void testList() {
        // 测试小数据集
        List<Double> smallDataset = Arrays.asList(10D, 15D, 20D, 25D, 30D, 35D, 40D, 45D, 50D, 55D, 60D, 65D, 70D, 75D, 80D, 85D, 90D, 95D, 100D);
        PercentileCalculator.PercentileResult percentileResult = PercentileCalculator.calculatePercentiles(smallDataset);
        Assertions.assertEquals(55D, percentileResult.getP50());
        Assertions.assertEquals(95D, percentileResult.getP95());
        Assertions.assertEquals(95D, percentileResult.getP99());
    }

    @Test
    void testSingle() {
        // 测试单个数据点
        List<Double> singleValue = Arrays.asList(42D);
        PercentileCalculator.PercentileResult percentileResult = PercentileCalculator.calculatePercentiles(singleValue);
        Assertions.assertEquals(42D, percentileResult.getP50());
        Assertions.assertEquals(42D, percentileResult.getP95());
        Assertions.assertEquals(42D, percentileResult.getP99());
    }

    @Test
    void testTwoNumber() {
        // 测试单个数据点
        List<Double> singleValue = Arrays.asList(10D, 20D);
        PercentileCalculator.PercentileResult percentileResult = PercentileCalculator.calculatePercentiles(singleValue);
        Assertions.assertEquals(20D, percentileResult.getP50());
        Assertions.assertEquals(20D, percentileResult.getP95());
        Assertions.assertEquals(20D, percentileResult.getP99());
    }

    @Test
    void testEmpty() {
        List<Double> singleValue = new ArrayList<>();
        PercentileCalculator.PercentileResult percentileResult = PercentileCalculator.calculatePercentiles(singleValue);
        Assertions.assertNull(percentileResult.getP50());
        Assertions.assertNull(percentileResult.getP95());
        Assertions.assertNull(percentileResult.getP99());
    }

    @Test
    void calculatePercentile() {
        // 测试带权重的计算
        List<PercentileCalculator.WeightedValue> weightedValues = Arrays.asList(
                new PercentileCalculator.WeightedValue(10.0, 1.0),
                new PercentileCalculator.WeightedValue(20.0, 2.0),
                new PercentileCalculator.WeightedValue(30.0, 3.0)
        );
        Double weightedP50 = PercentileCalculator.calculateWeightedPercentile(weightedValues, 0.5);
        Assertions.assertEquals(20D, weightedP50);
    }
}