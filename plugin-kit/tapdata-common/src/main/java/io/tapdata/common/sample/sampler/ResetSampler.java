package io.tapdata.common.sample.sampler;

import io.tapdata.common.sample.Sampler;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class ResetSampler implements Sampler {
    private AtomicReference<Long> value;
    private AtomicReference<Long> valueTemp;

    public ResetSampler() {
    }

    public ResetSampler(Long value) {
        this.value = new AtomicReference<>(value);
        this.valueTemp = new AtomicReference<>(value);
    }

    public void setValue(Long value) {
        this.value = new AtomicReference<>(value);
        this.valueTemp = new AtomicReference<>(value);
    }

    public Long getTemp() {
        if (Objects.nonNull(valueTemp.get())) {
            return valueTemp.get();
        } else {
            return null;
        }
    }

    @Override
    public Number value() {
        if (Objects.nonNull(value.get())) {
            return value.get();
        } else {
            return null;
        }
    }
}
