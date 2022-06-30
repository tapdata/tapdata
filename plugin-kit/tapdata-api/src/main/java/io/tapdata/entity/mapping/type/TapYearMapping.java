package io.tapdata.entity.mapping.type;

import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.type.TapYear;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 */
public class TapYearMapping extends TapMapping {
    public static final String KEY_RANGE = "range";

    private Integer minRange;
    private Integer maxRange;

    @Override
    public void from(Map<String, Object> info) {
        Object precisionObj = getObject(info, KEY_RANGE);
        if(precisionObj instanceof List) {
            List<?> list = (List<?>) precisionObj;
            if(list.size() == 2) {
                if(list.get(0) instanceof Number) {
                    minRange = ((Number) list.get(0)).intValue();
                }
                if(list.get(1) instanceof Number) {
                    maxRange = ((Number) list.get(1)).intValue();
                }
            }
        }
    }

    @Override
    public TapType toTapType(String dataType, Map<String, String> params) {
        return new TapYear().min(minRange).max(maxRange);
    }

    @Override
    public TapResult<String> fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapYear) {
            return TapResult.successfully(removeBracketVariables(typeExpression, 0));
        }
        return null;
    }

    @Override
    public BigDecimal matchingScore(TapField field) {
        if (field.getTapType() instanceof TapYear) {
//            TapYear tapYear = (TapYear) field.getTapType();
            if(maxRange != null && minRange != null) {
                return BigDecimal.valueOf((long)maxRange - minRange);
            }
            return BigDecimal.ZERO;
        }
        return TapMapping.MIN_SCORE;
    }

    public Integer getMinRange() {
        return minRange;
    }

    public void setMinRange(Integer minRange) {
        this.minRange = minRange;
    }

    public Integer getMaxRange() {
        return maxRange;
    }

    public void setMaxRange(Integer maxRange) {
        this.maxRange = maxRange;
    }
}
