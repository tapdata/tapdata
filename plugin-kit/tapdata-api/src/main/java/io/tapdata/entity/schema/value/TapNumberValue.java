package io.tapdata.entity.schema.value;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;

import java.math.BigDecimal;


public class TapNumberValue extends TapValue<Double, TapNumber> {
    public TapNumberValue() {}
    public TapNumberValue(Double value) {
        this.value = value;
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE));
    }

    @Override
    public Class<? extends TapType> tapTypeClass() {
        return TapNumber.class;
    }
}
