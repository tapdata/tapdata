package io.tapdata.entity.schema.type;

import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.ToTapDateTimeCodec;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;

import java.time.Instant;

import static io.tapdata.entity.simplify.TapSimplify.tapDateTime;

public class TapDateTime extends TapType {
    public TapDateTime() {
        type = TYPE_DATETIME;
    }
    /**
     * 字段是否有时区信息
     */
    private Boolean withTimeZone;
    public TapDateTime withTimeZone(Boolean withTimeZone) {
        this.withTimeZone = withTimeZone;
        return this;
    }

    private Long bytes;
    public TapDateTime bytes(Long bytes) {
        this.bytes = bytes;
        return this;
    }
    private Instant min;
    public TapDateTime min(Instant min) {
        this.min = min;
        return this;
    }
    private Instant max;
    public TapDateTime max(Instant max) {
        this.max = max;
        return this;
    }
    private Integer fraction;
    public TapDateTime fraction(Integer fraction) {
        this.fraction = fraction;
        return this;
    }
    private Integer defaultFraction;
    public TapDateTime defaultFraction(Integer defaultFraction) {
        this.defaultFraction = defaultFraction;
        return this;
    }

    @Override
    public TapType cloneTapType() {
        return tapDateTime()
                .withTimeZone(withTimeZone)
                .fraction(fraction)
                .defaultFraction(defaultFraction)
                .min(min)
                .max(max)
                .bytes(bytes);
    }

    @Override
    public Class<? extends TapValue<?, ?>> tapValueClass() {
        return TapDateTimeValue.class;
    }

    @Override
    public ToTapValueCodec<?> toTapValueCodec() {
        return InstanceFactory.instance(ToTapValueCodec.class, TapDefaultCodecs.TAP_DATE_TIME_VALUE);
    }

    public Boolean getWithTimeZone() {
        return withTimeZone;
    }

    public void setWithTimeZone(Boolean withTimeZone) {
        this.withTimeZone = withTimeZone;
    }

    public Long getBytes() {
        return bytes;
    }

    public void setBytes(Long bytes) {
        this.bytes = bytes;
    }

    public Instant getMin() {
        return min;
    }

    public void setMin(Instant min) {
        this.min = min;
    }

    public Instant getMax() {
        return max;
    }

    public void setMax(Instant max) {
        this.max = max;
    }

    public Integer getFraction() {
        return fraction;
    }

    public void setFraction(Integer fraction) {
        this.fraction = fraction;
    }

    public Integer getDefaultFraction() {
        return defaultFraction;
    }

    public void setDefaultFraction(Integer defaultFraction) {
        this.defaultFraction = defaultFraction;
    }
}
