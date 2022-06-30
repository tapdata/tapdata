package io.tapdata.entity.mapping.type;

import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapType;

import java.math.BigDecimal;
import java.util.Map;

/**
 */
public class TapArrayMapping extends TapSizeBase {


    @Override
    public TapType toTapType(String dataType, Map<String, String> params) {
        return new TapArray();
    }

    @Override
    public TapResult<String> fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapArray) {
            return TapResult.successfully(removeBracketVariables(typeExpression, 0));
        }
        return null;
    }

    @Override
    public BigDecimal matchingScore(TapField field) {
        if (field.getTapType() instanceof TapArray) {
            return BigDecimal.ZERO;
        }
        return TapMapping.MIN_SCORE;
    }
}
