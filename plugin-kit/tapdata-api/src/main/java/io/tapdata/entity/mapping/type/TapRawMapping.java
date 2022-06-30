package io.tapdata.entity.mapping.type;

import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapRaw;
import io.tapdata.entity.schema.type.TapType;

import java.math.BigDecimal;
import java.util.Map;

/**
 */
public class TapRawMapping extends TapMapping {


    @Override
    public void from(Map<String, Object> info) {

    }

    @Override
    public TapType toTapType(String dataType, Map<String, String> params) {
        return new TapRaw();
    }

    @Override
    public TapResult<String> fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapRaw) {
            return TapResult.successfully(removeBracketVariables(typeExpression, 0));
        }
        return null;
    }

    @Override
    public BigDecimal matchingScore(TapField field) {
        if (field.getTapType() instanceof TapRaw) {
            return BigDecimal.ZERO;
        }
        return TapMapping.MIN_SCORE;
    }
}
