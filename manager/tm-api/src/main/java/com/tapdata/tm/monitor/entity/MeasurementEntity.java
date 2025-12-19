package com.tapdata.tm.monitor.entity;

import com.tapdata.tm.monitor.constant.Granularity;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import com.tapdata.tm.commons.metrics.MetricCons;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.utils.NumberUtils;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.codecs.pojo.annotations.BsonId;
import org.springframework.data.mongodb.core.mapping.Field;

import java.nio.ByteBuffer;
import java.util.*;

@Data
@NoArgsConstructor
public class MeasurementEntity {
    public static final String COLLECTION_NAME = "AgentMeasurementV2";
    public static final String FIELD_ID = "_id";
    public static final String FIELD_GRANULARITY = "grnty";
    public static final String FIELD_DATE = "date";
    public static final String FIELD_SAMPLE_SIZE = "size";
    public static final String FIELD_FIRST = "first";
    public static final String FIELD_LAST = "last";
    public static final String FIELD_TAGS = "tags";
    public static final String FIELD_SAMPLES = "ss";
    public static final String INPUT_QPS = "inputQps";
    public static final String OUTPUT_QPS = "outputQps";
    public static final String MAX_INPUT_QPS = "maxInputQps";
    public static final String MAX_OUTPUT_QPS = "maxOutputQps";
    public static final String INPUT_SIZE_QPS = "inputSizeQps";
    public static final String OUTPUT_SIZE_QPS = "outputSizeQps";
    public static final String MAX_INPUT_SIZE_QPS = "maxInputSizeQps";
    public static final String MAX_OUTPUT_SIZE_QPS = "maxOutputSizeQps";
    

    @BsonId
    private String id;

    @Field(MetricCons.F_GRANULARITY)
    private String granularity;
    @Field(MetricCons.F_DATE)
    private Date date;
    @Field(MetricCons.F_SIZE)
    private Long sampleSize;
    @Field(MetricCons.F_FIRST)
    private Date first;
    @Field(MetricCons.F_LAST)
    private Date last;

    @Field(MetricCons.F_TAGS)
    private Map<String, String> tags;
    @Field(MetricCons.F_SAMPLES)
    private List<Sample> samples;

    @Field(FIELD_DIGEST)
    private List<TDigestEntity> digest;

    public MeasurementEntity(List<Sample> samples,List<TDigestEntity> digest) {
        this.samples = samples;
        this.digest = digest;
    }

    private Map<String,byte[]> digestBytes = new HashMap<>();

    public Map<String, Number> averageValues() {
        Map<String, Number> finalKeyValueMap = new HashMap<>();
        if (samples == null) {
            return finalKeyValueMap;
        }

        MetricsTracker tracker = new MetricsTracker();
        Map<String, Number> keyValueMap = new HashMap<>();
        Map<String, Integer> keyCounterMap = new HashMap<>();
        Map<Date, Map<String, TDigest>> digestMap = getDigestMap();
        boolean isMinuteGranularity = Granularity.GRANULARITY_MINUTE.equals(granularity);

        for (Sample sample : samples) {
            Map<String, Number> sampleValues = sample.getVs();
            Map<String, TDigest> keyDigestMap = digestMap.get(sample.getDate());

            for (Map.Entry<String, Number> entry : sampleValues.entrySet()) {
                String key = entry.getKey();
                Number value = entry.getValue();

                // 处理特殊指标（最大值和分位数）
                tracker.processMetric(key, value, isMinuteGranularity, keyDigestMap);

                // 累计值用于平均值计算
                if (value != null) {
                    keyValueMap.merge(key, value, NumberUtils::addNumbers);
                    keyCounterMap.merge(key, 1, Integer::sum);
                }
            }
        }

        // 计算平均值
        calculateAverageValues(keyValueMap, keyCounterMap, finalKeyValueMap);

        // 添加最大值
        tracker.addMaxValues(finalKeyValueMap);

        // 保存 digest 字节
        tracker.saveDigestBytes(digestBytes);

        return finalKeyValueMap;
    }

    /**
     * 计算平均值
     */
    private void calculateAverageValues(Map<String, Number> keyValueMap,
                                       Map<String, Integer> keyCounterMap,
                                       Map<String, Number> finalKeyValueMap) {
        for (Map.Entry<String, Number> entry : keyValueMap.entrySet()) {
            String key = entry.getKey();
            Number value = entry.getValue();
            Integer counter = keyCounterMap.get(key);

            if (counter != null && value != null && counter > 0) {
                finalKeyValueMap.put(key, NumberUtils.divideNumbers(value, counter));
            } else {
                finalKeyValueMap.put(key, null);
            }
        }
    }

    private class MetricsTracker {
        private final Map<String, MetricConfig> metricConfigs = new HashMap<>();

        public MetricsTracker() {
            // 配置需要追踪的指标
            metricConfigs.put(INPUT_QPS, new MetricConfig(INPUT_QPS_95TH, INPUT_QPS_99TH, MAX_INPUT_QPS));
            metricConfigs.put(OUTPUT_QPS, new MetricConfig( OUTPUT_QPS_95TH, OUTPUT_QPS_99TH, MAX_OUTPUT_QPS));
            metricConfigs.put(INPUT_SIZE_QPS, new MetricConfig(INPUT_SIZE_QPS_95TH, INPUT_SIZE_QPS_99TH, MAX_INPUT_SIZE_QPS));
            metricConfigs.put(OUTPUT_SIZE_QPS, new MetricConfig( OUTPUT_SIZE_QPS_95TH, OUTPUT_SIZE_QPS_99TH, MAX_OUTPUT_SIZE_QPS));
            metricConfigs.put(REPLICATE_LAG, new MetricConfig(REPLICATE_LAG_95TH, REPLICATE_LAG_99TH, null));
        }

        public void processMetric(String key, Number value, boolean isMinuteGranularity,
                                 Map<String, TDigest> keyDigestMap) {
            MetricConfig config = metricConfigs.get(key);
            if (config == null || value == null) {
                return;
            }

            // 更新最大值
            if (config.maxKey != null && compareNumbers(value, config.maxValue) > 0) {
                config.maxValue = value;
            }

            // 更新分位数 digest
            if (isMinuteGranularity) {
                config.digest95th.add(value.doubleValue());
                config.digest99th.add(value.doubleValue());
            } else if (keyDigestMap != null) {
                TDigest digest95 = keyDigestMap.get(config.digest95thKey);
                TDigest digest99 = keyDigestMap.get(config.digest99thKey);
                if (digest95 != null) {
                    config.digest95th.add(digest95);
                }
                if (digest99 != null) {
                    config.digest99th.add(digest99);
                }
            }
        }

        public void addMaxValues(Map<String, Number> resultMap) {
            for (MetricConfig config : metricConfigs.values()) {
                if (config.maxKey != null) {
                    resultMap.put(config.maxKey, config.maxValue);
                }
            }
        }

        public void saveDigestBytes(Map<String, byte[]> digestBytesMap) {
            for (MetricConfig config : metricConfigs.values()) {
                digestBytesMap.put(config.digest95thKey, getDigestBytes(config.digest95th));
                digestBytesMap.put(config.digest99thKey, getDigestBytes(config.digest99th));
            }
        }
    }

    public class MetricConfig {
        final String digest95thKey;
        final String digest99thKey;
        final String maxKey;
        final TDigest digest95th;
        final TDigest digest99th;
        Number maxValue;

        MetricConfig(String digest95thKey, String digest99thKey,String maxKey) {
            this.digest95thKey = digest95thKey;
            this.digest99thKey = digest99thKey;
            this.maxKey = maxKey;
            this.digest95th = TDigest.createMergingDigest(100);
            this.digest99th = TDigest.createMergingDigest(100);
            this.maxValue = 0;
        }
    }


    private int compareNumbers(Number a, Number b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        return Double.compare(a.doubleValue(), b.doubleValue());
    }

    private byte[] getDigestBytes(TDigest digest) {
        ByteBuffer buffer = ByteBuffer.allocate(digest.byteSize());
        digest.asSmallBytes(buffer);
        return buffer.array();
    }

    private Map<Date,Map<String,TDigest>> getDigestMap() {
        Map<Date,Map<String,TDigest>> digestMap = new HashMap<>();
        if(CollectionUtils.isEmpty(digest)) {
            return digestMap;
        }
        for(TDigestEntity digestEntity : digest) {
            digestEntity.getDigest().forEach((key, value) -> {
                Map<String,TDigest> keyDigestMap = digestMap.computeIfAbsent(digestEntity.getDate(), k -> new HashMap<>());
                ByteBuffer buffer = ByteBuffer.wrap(value);
                MergingDigest digest = MergingDigest.fromBytes(buffer);
                keyDigestMap.put(key, digest);
            });
        }
        return digestMap;
    }
}
