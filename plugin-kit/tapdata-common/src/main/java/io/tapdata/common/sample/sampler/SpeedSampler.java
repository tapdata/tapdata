package io.tapdata.common.sample.sampler;

import io.tapdata.common.sample.Sampler;

import java.util.concurrent.atomic.LongAdder;

/**
 * Incremental record value and last upload time.
 * Calculate speed by using current time minus last upload time and update last upload time when upload.
 */
public class SpeedSampler implements Sampler {
    private Long lastCalculateTime;
    private LongAdder totalValue = new LongAdder();

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
                return  ((double) totalValue.sumThenReset() / time) * 1000;
            }
        }
        return 0;
    }

}
