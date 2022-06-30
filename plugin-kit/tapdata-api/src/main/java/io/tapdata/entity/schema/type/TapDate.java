package io.tapdata.entity.schema.type;

import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.ToTapDateCodec;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.utils.InstanceFactory;

import java.time.Instant;

import static io.tapdata.entity.simplify.TapSimplify.tapDate;

public class TapDate extends TapType {
    public TapDate() {
        type = TYPE_DATE;
    }
    /**
     * 字段是否有时区信息
     */
    private Boolean withTimeZone;
    public TapDate withTimeZone(Boolean withTimeZone) {
        this.withTimeZone = withTimeZone;
        return this;
    }

    private Long bytes;
    public TapDate bytes(Long bytes) {
        this.bytes = bytes;
        return this;
    }
    private Instant min;
    public TapDate min(Instant min) {
        this.min = min;
        return this;
    }
    private Instant max;
    public TapDate max(Instant max) {
        this.max = max;
        return this;
    }

    @Override
    public TapType cloneTapType() {
        return tapDate().withTimeZone(withTimeZone).bytes(bytes).min(min).max(max);
    }

    @Override
    public Class<? extends TapValue<?, ?>> tapValueClass() {
        return TapDateValue.class;
    }

    @Override
    public ToTapValueCodec<?> toTapValueCodec() {
        return InstanceFactory.instance(ToTapValueCodec.class, TapDefaultCodecs.TAP_DATE_VALUE);
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
}
