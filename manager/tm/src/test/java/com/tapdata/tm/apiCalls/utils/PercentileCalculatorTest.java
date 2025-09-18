package com.tapdata.tm.apiCalls.utils;

import com.tapdata.tm.apiServer.utils.PercentileCalculator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class PercentileCalculatorTest {

    @Test
    void testList() {
        // 测试小数据集
        List<Long> smallDataset = Arrays.asList(10L, 15L, 20L, 25L, 30L, 35L, 40L, 45L, 50L, 55L, 60L, 65L, 70L, 75L, 80L, 85L, 90L, 95L, 100L);
        PercentileCalculator.PercentileResult percentileResult = PercentileCalculator.calculatePercentiles(smallDataset);
        Assertions.assertEquals(55L, percentileResult.getP50());
        Assertions.assertEquals(95L, percentileResult.getP95());
        Assertions.assertEquals(95L, percentileResult.getP99());
    }

    @Test
    void testSingle() {
        // 测试单个数据点
        List<Long> singleValue = Arrays.asList(42L);
        PercentileCalculator.PercentileResult percentileResult = PercentileCalculator.calculatePercentiles(singleValue);
        Assertions.assertEquals(42L, percentileResult.getP50());
        Assertions.assertEquals(42L, percentileResult.getP95());
        Assertions.assertEquals(42L, percentileResult.getP99());
    }

    @Test
    void testTwoNumber() {
        // 测试单个数据点
        List<Long> singleValue = Arrays.asList(10L, 20L);
        PercentileCalculator.PercentileResult percentileResult = PercentileCalculator.calculatePercentiles(singleValue);
        Assertions.assertEquals(20L, percentileResult.getP50());
        Assertions.assertEquals(20L, percentileResult.getP95());
        Assertions.assertEquals(20L, percentileResult.getP99());
    }

    @Test
    void testEmpty() {
        List<Long> singleValue = new ArrayList<>();
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