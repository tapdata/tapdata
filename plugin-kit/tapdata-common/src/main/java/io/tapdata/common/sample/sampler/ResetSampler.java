package io.tapdata.common.sample.sampler;

import io.tapdata.common.sample.Sampler;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class ResetSampler implements Sampler {
    private AtomicLong value;

    private AtomicLong temp;

    public ResetSampler() {
    }

    public ResetSampler(AtomicLong value, AtomicLong temp) {
        this.value = value;
        this.temp = temp;
    }

    public void setValue(Long value) {
        this.value = new AtomicLong(value);
        this.temp = new AtomicLong(value);
    }

    public Long getTemp() {
        if (Objects.nonNull(temp)) {
            return temp.getAndSet(0L);
        } else {
            return 0L;
        }
    }

    @Override
    public Number value() {
        if (Objects.nonNull(value)) {
            return value.getAndSet(0L);
        } else {
            return 0L;
        }
    }
}
