package com.tapdata.tm.monitor.entity;

import com.tapdata.tm.monitor.constant.Granularity;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import io.tapdata.common.sample.request.Sample;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class MeasurementEntityTest {

    /**
     * 测试场景1: granularity 为 GRANULARITY_MINUTE 时
     * - digest 为 null
     * - samples 有 60 条数据
     * - 验证 95 分位和 99 分位的准确性
     */
    @Test
    public void testAverageValues_MinuteGranularity_WithoutDigest() {
        // 准备测试数据
        MeasurementEntity entity = new MeasurementEntity();
        entity.setGranularity(Granularity.GRANULARITY_MINUTE);
        entity.setDigest(null); // digest 为 null

        // 创建 60 条样本数据
        List<Sample> samples = new ArrayList<>();
        Date baseDate = new Date();

        // 生成已知分布的数据，便于验证分位数
        // 使用 1-100 的数据，60 个样本
        List<Double> inputQpsValues = generateTestData(60);
        List<Double> outputQpsValues = generateTestData(60);
        List<Double> inputSizeQpsValues = generateTestData(60);
        List<Double> outputSizeQpsValues = generateTestData(60);
        List<Double> replicateLagValues = generateTestData(60);

        for (int i = 0; i < 60; i++) {
            Sample sample = new Sample();
            sample.setDate(new Date(baseDate.getTime() + i * 1000)); // 每秒一个样本

            Map<String, Number> vs = new HashMap<>();
            vs.put(MeasurementEntity.INPUT_QPS, inputQpsValues.get(i));
            vs.put(MeasurementEntity.OUTPUT_QPS, outputQpsValues.get(i));
            vs.put(MeasurementEntity.INPUT_SIZE_QPS, inputSizeQpsValues.get(i));
            vs.put(MeasurementEntity.OUTPUT_SIZE_QPS, outputSizeQpsValues.get(i));
            vs.put(MeasurementEntity.REPLICATE_LAG, replicateLagValues.get(i));

            sample.setVs(vs);
            samples.add(sample);
        }

        entity.setSamples(samples);

        // 执行测试
        Map<String, Number> result = entity.averageValues();

        // 验证结果
        assertNotNull(result);

        // 计算期望的分位数值
        double expected95th = calculatePercentile(inputQpsValues, 0.95);
        double expected99th = calculatePercentile(inputQpsValues, 0.99);

        // 验证 INPUT_QPS 的 95 分位和 99 分位
        assertNull(result.get(MeasurementEntity.INPUT_QPS_95TH));
        assertNull(result.get(MeasurementEntity.INPUT_QPS_99TH));

        // TDigest 有一定的误差容忍度，通常在 1% 以内

        Map<String,byte[]> resultDigestBytes = entity.getDigestBytes();
        double actual95th = MergingDigest.fromBytes(ByteBuffer.wrap(resultDigestBytes.get(MeasurementEntity.INPUT_QPS_95TH))).quantile(0.95);
        double actual99th = MergingDigest.fromBytes(ByteBuffer.wrap(resultDigestBytes.get(MeasurementEntity.INPUT_QPS_99TH))).quantile(0.99);
        System.out.println(expected95th);
        System.out.println(actual95th);
        System.out.println(expected99th);
        System.out.println(actual99th);


        // 允许 5% 的误差范围（TDigest 的近似算法）
        assertEquals(expected95th, actual95th, expected95th * 0.05,
                "INPUT_QPS 95th percentile should be close to expected value");
        assertEquals(expected99th, actual99th, expected99th * 0.05,
                "INPUT_QPS 99th percentile should be close to expected value");

        // 验证最大值
        double maxInputQps = Collections.max(inputQpsValues);
        assertEquals(maxInputQps, result.get(MeasurementEntity.MAX_INPUT_QPS).doubleValue(), 0.01);

        // 验证平均值
        double avgInputQps = inputQpsValues.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        assertEquals(avgInputQps, result.get(MeasurementEntity.INPUT_QPS).doubleValue(), 0.01);

    }

    /**
     * 测试场景2: granularity 为 GRANULARITY_HOUR 时
     * - digest 包含每分钟的 digest95th 和 digest99th
     * - samples 有 60 条数据（代表 60 分钟）
     * - 验证 95 分位和 99 分位的准确性
     */
    @Test
    public void testAverageValues_HourGranularity_WithDigest() {
        // 准备测试数据
        MeasurementEntity entity = new MeasurementEntity();
        entity.setGranularity(Granularity.GRANULARITY_HOUR);

        // 创建 60 条样本数据（代表 60 分钟）
        List<Sample> samples = new ArrayList<>();
        List<TDigestEntity> digestEntities = new ArrayList<>();
        Date baseDate = new Date();

        // 为每分钟创建 TDigest
        List<TDigest> inputQps95thDigests = new ArrayList<>();
        List<TDigest> inputQps99thDigests = new ArrayList<>();
        List<TDigest> outputQps95thDigests = new ArrayList<>();
        List<TDigest> outputQps99thDigests = new ArrayList<>();
        List<TDigest> inputSizeQps95thDigests = new ArrayList<>();
        List<TDigest> inputSizeQps99thDigests = new ArrayList<>();
        List<TDigest> outputSizeQps95thDigests = new ArrayList<>();
        List<TDigest> outputSizeQps99thDigests = new ArrayList<>();
        List<TDigest> replicateLag95thDigests = new ArrayList<>();
        List<TDigest> replicateLag99thDigests = new ArrayList<>();

        // 生成测试数据
        List<Double> minuteInputQpsValues = new ArrayList<>();

        for (int minute = 0; minute < 60; minute++) {
            Date minuteDate = new Date(baseDate.getTime() + minute * 60 * 1000);

            // 为每分钟生成一组数据（假设每分钟有 60 个样本）
            List<Double> secondInputQpsValues = generateTestData(60);
            minuteInputQpsValues.addAll(secondInputQpsValues);

            // 创建该分钟的 TDigest
            TDigest inputQps95Digest = TDigest.createMergingDigest(100);
            TDigest inputQps99Digest = TDigest.createMergingDigest(100);
            TDigest outputQps95Digest = TDigest.createMergingDigest(100);
            TDigest outputQps99Digest = TDigest.createMergingDigest(100);
            TDigest inputSizeQps95Digest = TDigest.createMergingDigest(100);
            TDigest inputSizeQps99Digest = TDigest.createMergingDigest(100);
            TDigest outputSizeQps95Digest = TDigest.createMergingDigest(100);
            TDigest outputSizeQps99Digest = TDigest.createMergingDigest(100);
            TDigest replicateLag95Digest = TDigest.createMergingDigest(100);
            TDigest replicateLag99Digest = TDigest.createMergingDigest(100);

            // 向 TDigest 添加该分钟的所有数据
            for (double value : secondInputQpsValues) {
                inputQps95Digest.add(value);
                inputQps99Digest.add(value);
                outputQps95Digest.add(value);
                outputQps99Digest.add(value);
                inputSizeQps95Digest.add(value);
                inputSizeQps99Digest.add(value);
                outputSizeQps95Digest.add(value);
                outputSizeQps99Digest.add(value);
                replicateLag95Digest.add(value);
                replicateLag99Digest.add(value);
            }

            inputQps95thDigests.add(inputQps95Digest);
            inputQps99thDigests.add(inputQps99Digest);
            outputQps95thDigests.add(outputQps95Digest);
            outputQps99thDigests.add(outputQps99Digest);
            inputSizeQps95thDigests.add(inputSizeQps95Digest);
            inputSizeQps99thDigests.add(inputSizeQps99Digest);
            outputSizeQps95thDigests.add(outputSizeQps95Digest);
            outputSizeQps99thDigests.add(outputSizeQps99Digest);
            replicateLag95thDigests.add(replicateLag95Digest);
            replicateLag99thDigests.add(replicateLag99Digest);

            // 创建该分钟的 Sample（使用该分钟的平均值）
            Sample sample = new Sample();
            sample.setDate(minuteDate);

            double avgValue = secondInputQpsValues.stream().mapToDouble(Double::doubleValue).average().orElse(0);
            Map<String, Number> vs = new HashMap<>();
            vs.put(MeasurementEntity.INPUT_QPS, avgValue);
            vs.put(MeasurementEntity.OUTPUT_QPS, avgValue);
            vs.put(MeasurementEntity.INPUT_SIZE_QPS, avgValue);
            vs.put(MeasurementEntity.OUTPUT_SIZE_QPS, avgValue);
            vs.put(MeasurementEntity.REPLICATE_LAG, avgValue);

            sample.setVs(vs);
            samples.add(sample);

            // 创建 TDigestEntity
            TDigestEntity digestEntity = new TDigestEntity();
            digestEntity.setDate(minuteDate);

            Map<String, byte[]> digestMap = new HashMap<>();
            digestMap.put(MeasurementEntity.INPUT_QPS_95TH, toBytes(inputQps95Digest));
            digestMap.put(MeasurementEntity.INPUT_QPS_99TH, toBytes(inputQps99Digest));
            digestMap.put(MeasurementEntity.OUTPUT_QPS_95TH, toBytes(outputQps95Digest));
            digestMap.put(MeasurementEntity.OUTPUT_QPS_99TH, toBytes(outputQps99Digest));
            digestMap.put(MeasurementEntity.INPUT_SIZE_QPS_95TH, toBytes(inputSizeQps95Digest));
            digestMap.put(MeasurementEntity.INPUT_SIZE_QPS_99TH, toBytes(inputSizeQps99Digest));
            digestMap.put(MeasurementEntity.OUTPUT_SIZE_QPS_95TH, toBytes(outputSizeQps95Digest));
            digestMap.put(MeasurementEntity.OUTPUT_SIZE_QPS_99TH, toBytes(outputSizeQps99Digest));
            digestMap.put(MeasurementEntity.REPLICATE_LAG_95TH, toBytes(replicateLag95Digest));
            digestMap.put(MeasurementEntity.REPLICATE_LAG_99TH, toBytes(replicateLag99Digest));

            digestEntity.setDigest(digestMap);
            digestEntities.add(digestEntity);
        }

        entity.setSamples(samples);
        entity.setDigest(digestEntities);

        // 执行测试
        Map<String, Number> result = entity.averageValues();

        // 验证结果
        assertNotNull(result);

        // 计算期望的分位数值（基于所有 60 分钟 * 60 秒 = 3600 个数据点）
        double expected95th = calculatePercentile(minuteInputQpsValues, 0.95);
        double expected99th = calculatePercentile(minuteInputQpsValues, 0.99);


        Map<String,byte[]> resultDigestBytes = entity.getDigestBytes();
        double actual95th = MergingDigest.fromBytes(ByteBuffer.wrap(resultDigestBytes.get(MeasurementEntity.INPUT_QPS_95TH))).quantile(0.95);
        double actual99th = MergingDigest.fromBytes(ByteBuffer.wrap(resultDigestBytes.get(MeasurementEntity.INPUT_QPS_99TH))).quantile(0.99);


        // 允许 5% 的误差范围（TDigest 的近似算法 + 合并误差）
        assertEquals(expected95th, actual95th, expected95th * 0.05,
                "INPUT_QPS 95th percentile should be close to expected value");
        assertEquals(expected99th, actual99th, expected99th * 0.05,
                "INPUT_QPS 99th percentile should be close to expected value");

        // 验证 digestBytes 已保存
        assertNotNull(entity.getDigestBytes());
        assertFalse(entity.getDigestBytes().isEmpty());
    }

    /**
     * 生成测试数据
     * 生成一个有规律的数据分布，便于验证分位数
     */
    private List<Double> generateTestData(int count) {
        List<Double> data = new ArrayList<>();
        Random random = new Random(42); // 使用固定种子保证可重复性

        // 生成正态分布的数据，均值 50，标准差 15
        for (int i = 0; i < count; i++) {
            double value = 50 + random.nextGaussian() * 15;
            // 确保值为正数
            value = Math.max(1, value);
            data.add(value);
        }

        return data;
    }

    /**
     * 计算精确的分位数（用于验证）
     */
    private double calculatePercentile(List<Double> values, double percentile) {
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);

        int index = (int) Math.ceil(percentile * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));

        return sorted.get(index);
    }

    /**
     * 将 TDigest 转换为字节数组
     */
    private byte[] toBytes(TDigest digest) {
        ByteBuffer buffer = ByteBuffer.allocate(digest.byteSize());
        digest.asSmallBytes(buffer);
        return buffer.array();
    }
}
