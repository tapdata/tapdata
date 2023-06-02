package io.tapdata.common.sample.sampler;

import io.tapdata.common.sample.Sampler;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class ResetSampler implements Sampler {
    private AtomicReference<Long> value;

    public ResetSampler() {
    }

    public ResetSampler(Long value) {
        this.value = new AtomicReference<>(value);
    }

    public void setValue(Long value) {
        this.value = new AtomicReference<>(value);
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
