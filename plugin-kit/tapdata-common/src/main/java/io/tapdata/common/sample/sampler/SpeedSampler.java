package io.tapdata.common.sample.sampler;

import com.google.common.collect.Lists;
import io.tapdata.common.sample.Sampler;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

/**
 * Incremental record value and last upload time.
 * Calculate speed by using current time minus last upload time and update last upload time when upload.
 */
public class SpeedSampler implements Sampler {
    private Long lastCalculateTime;
    private LongAdder totalValue = new LongAdder();
    private Double maxValue;
    private List<Double> valueList;
    public void add(long value) {
        totalValue.add(value);
    }

    public void add() {
        totalValue.add(1);
    }

    @Override
    public Number value() {
        Long temp = lastCalculateTime;
        lastCalculateTime = System.currentTimeMillis();
        if(temp != null) {
            long time = System.currentTimeMillis() - temp;
            if(time > 0) {
                double v = ((double) totalValue.sumThenReset() / time) * 1000;

                if (Objects.isNull(valueList)) {
                    valueList = Lists.newArrayList(v);
                } else {
                    valueList.add(v);
                }

                if (Objects.isNull(maxValue) || v > maxValue) {
                    maxValue = v;
                }
                return v;
            }
        }
        return null;
    }

    public Double getMaxValue() {
        if (Objects.nonNull(maxValue)) {
            return maxValue;
        }
        return null;
    }

    public Double getAvgValue() {
        if (Objects.nonNull(valueList)) {
            return valueList.stream().mapToDouble(value -> value).average().orElse(Double.NaN);
        }
        return null;
    }
}
