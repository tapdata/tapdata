package io.tapdata.common.sample.sampler;

import io.tapdata.common.sample.Sampler;

import java.util.concurrent.atomic.LongAdder;

/**
 * Auto inc counter.
 * The counter will be reset after collected.
 *
 * @author dexter
 */
public class ResetCounterSampler implements Sampler {
    private final LongAdder counter = new LongAdder();

    public void inc() {
        counter.increment();
    }
    public void inc(long value) {
        counter.add(value);
    }

    @Override
    public Number value() {
        long value = counter.sumThenReset();
        return value > 0 ? value : null;
    }
}
