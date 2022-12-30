package io.tapdata.common.sample.sampler;

import io.tapdata.common.sample.Sampler;

import java.util.concurrent.atomic.LongAdder;

/**
 * Incremental record value and auto inc counter.
 * Calculate average value and clear when upload.
 */
public class AverageSampler implements Sampler {
    private final LongAdder counter = new LongAdder();
    private final LongAdder totalValue = new LongAdder();

    public void add(long value) {
        counter.increment();
        totalValue.add(value);
    }

    public void add(long cnt, long value) {
        counter.add(cnt);
        totalValue.add(value);
    }

    @Override
    public Number value() {
        //TODO Not thread safe between two lines below.
        long counterValue = counter.sumThenReset();
        double total = totalValue.sumThenReset();
        if(counterValue > 0) {
            return total / counterValue;
        }
        return null;
    }
}
