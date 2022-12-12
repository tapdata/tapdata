package io.tapdata.entity.mapping.type;

import io.tapdata.entity.result.ResultItem;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapType;

import java.math.BigDecimal;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.tapString;

public class TapStringMapping extends TapBytesBase {
    @Override
    public TapType toTapType(String dataType, Map<String, String> params) {
        return tapString().bytes(getToTapTypeBytes(params)).fixed(fixed).byteRatio(byteRatio).defaultValue(defaultBytes);
    }

    @Override
    public TapResult<String> fromTapType(String typeExpression, TapType tapType) {
        String theFinalExpression = null;
        if (tapType instanceof TapString) {
            TapResult<String> tapResult = new TapResult<>();
            tapResult.result(TapResult.RESULT_SUCCESSFULLY);
            TapString tapString = (TapString) tapType;
            theFinalExpression = typeExpression;
//            if (tapString.getFixed() != null && tapString.getFixed()) {
//                theFinalExpression = clearBrackets(theFinalExpression, fixed);
//            }

            Long bytes = tapString.getBytes();
            if (bytes != null) {
                bytes = getFromTapTypeBytes(bytes, tapString.getByteRatio());
                if(this.bytes != null && bytes > this.bytes) {
                    tapResult.addItem(new ResultItem("TapStringMapping BYTES", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Bytes " + bytes + " from source exceeded the maximum of target bytes " + this.bytes + ", bytes before ratio " + tapString.getBytes() + ", expression {}" + typeExpression));
                    bytes = this.bytes;
                    tapResult.result(TapResult.RESULT_SUCCESSFULLY_WITH_WARN);
                }
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_BYTE, false);
                theFinalExpression = theFinalExpression.replace("$" + KEY_BYTE, String.valueOf(bytes));
            }
            theFinalExpression = removeBracketVariables(theFinalExpression, 0);
            return tapResult.data(theFinalExpression);
        }
        return null;
    }
    final BigDecimal bytesDifValue = BigDecimal.valueOf(10).pow(17);
    final BigDecimal fixedValue = BigDecimal.valueOf(10).pow(17);
    final BigDecimal byteRatioValue = BigDecimal.valueOf(10).pow(16);
    final BigDecimal defaultByteValue = BigDecimal.valueOf(10).pow(6);

    @Override
    public BigDecimal matchingScore(TapField field) {
        if (field.getTapType() instanceof TapString) {
            TapString tapString = (TapString) field.getTapType();

            //field is primary key, but this type is not able to be primary type.
            if(field.getPrimaryKey() != null && field.getPrimaryKey() && pkEnablement != null && !pkEnablement) {
                return TapMapping.MIN_SCORE;
            }

            Boolean comingFixed = tapString.getFixed();
            Integer comingByteRatio = tapString.getByteRatio();
//            Long comingDefaultValue = tapString.getDefaultValue();

            BigDecimal score = BigDecimal.ZERO;

            if(((comingFixed != null && comingFixed) && (fixed != null && fixed)) ||
                    ((comingFixed == null || !comingFixed) && (fixed == null || !fixed))) {
                score = score.add(fixedValue);
            } else {
                score = score.subtract(fixedValue);
            }

            if(comingByteRatio != null &&  byteRatio != null) {
                score = score.add(byteRatioValue);
            } else {
                score = score.subtract(byteRatioValue);
            }

            Long theBytes = bytes;
            if(theBytes != null && byteRatio != null)
                theBytes = theBytes * byteRatio;
            Long width = tapString.getBytes();
            if(byteRatio == null && comingByteRatio != null) {
                width = width * comingByteRatio;
            }
            if(width == null && theBytes != null) {
                return score.add(BigDecimal.valueOf(theBytes));
            } else if(theBytes != null) {
//                width = getFromTapTypeBytes(width);
                if(width <= theBytes) {
                    if(defaultBytes != null) {
                        score = score.add(defaultByteValue);
                    }
                    return score.add(bytesDifValue.subtract(BigDecimal.valueOf(theBytes - width)));
                } else {
                    score = score.subtract(bytesDifValue);
                    return score.add(BigDecimal.valueOf(theBytes - width)); // unacceptable
                }
            }
            return BigDecimal.ZERO;
        }
        return TapMapping.MIN_SCORE;
    }
}
