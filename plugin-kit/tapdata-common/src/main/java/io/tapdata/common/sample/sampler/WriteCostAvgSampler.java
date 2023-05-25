package io.tapdata.common.sample.sampler;

import io.tapdata.common.sample.Sampler;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Incremental record value and auto inc counter.
 * Calculate average value and clear when upload.
 */
public class WriteCostAvgSampler implements Sampler {
    private final LongAdder counter = new LongAdder();
    private final LongAdder totalValue = new LongAdder();

    private final AtomicLong writeRecordAcceptLastTs = new AtomicLong();

    public void setWriteRecordAcceptLastTs(long ts) {
        writeRecordAcceptLastTs.set(ts);
    }

    public void add(long value) {
        counter.increment();
        totalValue.add(value);
    }

    public void add(long cnt, long accetTime) {
        counter.add(cnt);
        totalValue.add(accetTime - writeRecordAcceptLastTs.getAndSet(accetTime));
    }

    @Override
    public Number value() {
        long counterValue = counter.sum();
        double total = totalValue.sum();
        if(counterValue > 0) {
            return total / counterValue;
        }
        return null;
    }
}
