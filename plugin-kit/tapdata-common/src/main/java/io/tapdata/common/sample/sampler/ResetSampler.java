package io.tapdata.common.sample.sampler;

import io.tapdata.common.sample.Sampler;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class ResetSampler implements Sampler {
    private AtomicReference<Long> counter;
    private AtomicReference<Long> value;

    private AtomicReference<Long> valueTemp;
    private AtomicReference<Long> counterTemp;

    public ResetSampler() {
    }

    public ResetSampler(Long counter, Long value) {
        this.counter = new AtomicReference<>(counter);
        this.value = new AtomicReference<>(value);
        this.counterTemp = new AtomicReference<>(counter);
        this.valueTemp = new AtomicReference<>(value);
    }

    public void setValue(Long counter, Long value) {
        this.counter = new AtomicReference<>(counter);
        this.value = new AtomicReference<>(value);
        this.counterTemp = new AtomicReference<>(counter);
        this.valueTemp = new AtomicReference<>(value);
    }

    public Long getTemp() {
        if (Objects.nonNull(valueTemp.get()) && Objects.nonNull(counterTemp.get())) {
            return (valueTemp.getAndSet(null)) / (counterTemp.getAndSet(null));
        } else {
            return null;
        }
    }

    @Override
    public Number value() {
        if (Objects.nonNull(value.get()) && Objects.nonNull(value.get()) ) {
            return (value.getAndSet(null)) / (counter.getAndSet(null));
        } else {
            return null;
        }
    }
}
