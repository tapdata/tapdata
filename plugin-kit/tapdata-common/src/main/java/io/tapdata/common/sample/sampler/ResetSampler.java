package io.tapdata.common.sample.sampler;

import io.tapdata.common.sample.Sampler;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class ResetSampler implements Sampler {
    private AtomicReference<Long> value;

    private AtomicReference<Long> temp;

    public ResetSampler() {
    }

    public ResetSampler(Long value, Long temp) {
        this.value = new AtomicReference<>(value);
        this.temp = new AtomicReference<>(temp);
    }

    public void setValue(Long value) {
        this.value = new AtomicReference<>(value);
        this.temp = new AtomicReference<>(value);
    }

    public Long getTemp() {
        if (Objects.nonNull(temp)) {
            return temp.getAndSet(null);
        } else {
            return null;
        }
    }

    @Override
    public Number value() {
        if (Objects.nonNull(value)) {
            return value.getAndSet(null);
        } else {
            return null;
        }
    }
}
