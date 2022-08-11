package io.tapdata.common.sample.sampler;

import io.tapdata.common.sample.Sampler;

/**
 * Record any type of number.
 * The number will not be cleared when upload, but only change it manually.
 *
 * @param <T> type of number
 */
public class NumberSampler<T extends Number> implements Sampler {
    private T value;

    public NumberSampler() {
    }

    public NumberSampler(T initialValue) {
        setValue(initialValue);
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public Number value() {
        return value;
    }
}
