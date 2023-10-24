package io.tapdata.entity.codec.impl;

import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapDefaultCodecs;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.TapNumberValue;

import java.math.BigDecimal;

@Implementation(value = FromTapValueCodec.class, type = TapDefaultCodecs.TAP_NUMBER_VALUE, buildNumber = 0)
public class FromTapNumberCodec implements FromTapValueCodec<TapNumberValue> {
    @Override
    public Object fromTapValue(TapNumberValue tapValue) {
        if(tapValue == null)
            return null;
        TapType tapType = tapValue.getTapType();
        TapNumber tapNumber = null;
        if(tapType instanceof TapNumber) {
            tapNumber = (TapNumber) tapType;
        }
        if(tapNumber != null) {
            Integer scale = tapNumber.getScale();
            if(scale == null || scale == 0) {
                if(tapNumber.getMaxValue().compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) <= 0 && tapNumber.getMinValue().compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) >= 0) {
                    long maxValue = tapNumber.getMaxValue().longValue();
                    long minValue = tapNumber.getMinValue().longValue();
                    if(Byte.MAX_VALUE >= maxValue && Byte.MIN_VALUE <= minValue) {
                        return tapValue.getValue().byteValue();
                    } else if(Short.MAX_VALUE >= maxValue && Short.MIN_VALUE <= minValue) {
                        return tapValue.getValue().shortValue();
                    } else if(Integer.MAX_VALUE >= maxValue && Integer.MIN_VALUE <= minValue) {
                        return tapValue.getValue().intValue();
                    } else {
                        return tapValue.getValue().longValue();
                    }
                } else {
                    return BigDecimal.valueOf(tapValue.getValue());
                }
            } else {
                if((tapNumber.getBit() != null && tapNumber.getBit() == 32) && (tapNumber.getFixed() == null || !tapNumber.getFixed())) {
                    BigDecimal minValue = tapNumber.getMinValue();
                    BigDecimal maxValue = tapNumber.getMaxValue();
                    if(minValue != null && maxValue != null &&
                            minValue.compareTo(BigDecimal.valueOf(-Float.MAX_VALUE)) >= 0 &&
                            BigDecimal.valueOf(Float.MAX_VALUE).compareTo(maxValue) >= 0) {
                        Double d = tapValue.getValue();
                        if(d != null)
                            return d.floatValue();
                    }

                }
                return tapValue.getValue();
            }
        }

        return tapValue.getValue();
    }

}
