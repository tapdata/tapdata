package io.tapdata.entity.schema.type;

import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.impl.ToTapYearCodec;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.entity.utils.InstanceFactory;

import static io.tapdata.entity.simplify.TapSimplify.tapYear;

public class TapYear extends TapType {
    private Integer min;
    public TapYear min(Integer min) {
        this.min = min;
        return this;
    }
    private Integer max;
    public TapYear max(Integer max) {
        this.max = max;
        return this;
    }

    public TapYear() {
        type = TYPE_YEAR;
    }

    @Override
    public TapType cloneTapType() {
        return tapYear().min(min).max(max);
    }

    @Override
    public Class<? extends TapValue<?, ?>> tapValueClass() {
        return TapYearValue.class;
    }

    @Override
    public ToTapValueCodec<?> toTapValueCodec() {
        return InstanceFactory.instance(ToTapValueCodec.class, TapDefaultCodecs.TAP_YEAR_VALUE);
    }

    public Integer getMin() {
        return min;
    }

    public void setMin(Integer min) {
        this.min = min;
    }

    public Integer getMax() {
        return max;
    }

    public void setMax(Integer max) {
        this.max = max;
    }
}
