package io.tapdata.pdk.core.api.impl;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.verification.DiffEntry;
import io.tapdata.entity.verification.MapDiff;
import io.tapdata.entity.verification.ValueVerification;
import org.apache.commons.codec.binary.Base64;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;

@Implementation(value = ValueVerification.class, buildNumber = 0)
public class ValueVerificationImpl implements ValueVerification {
    @Override
    public MapDiff mapEquals(Map<String, Object> leftMap, Map<String, Object> rightMap, int equalsType) {
        return mapEquals(leftMap, rightMap, equalsType, null);
    }

    @Override
    public MapDiff mapEquals(Map<String, Object> leftMap, Map<String, Object> rightMap, int equalsType, StringBuilder builder) {
        switch (equalsType) {
            case EQUALS_TYPE_EXACTLY:
            case EQUALS_TYPE_FUZZY:
                break;
            default:
                equalsType = EQUALS_TYPE_FUZZY;
                break;
        }
        MapDifference<String, Object> difference = Maps.difference(leftMap, rightMap);
        Map<String, MapDifference.ValueDifference<Object>> differenceMap = difference.entriesDiffering();
        if(builder != null) builder.append("Differences: \n");
        boolean different = false;
        MapDiff mapDiff = new MapDiff();
        for (Map.Entry<String, MapDifference.ValueDifference<Object>> entry : differenceMap.entrySet()) {
            MapDifference.ValueDifference<Object> diff = entry.getValue();
            Object leftValue = diff.leftValue();
            Object rightValue = diff.rightValue();

            boolean equalResult = objectIsEqual(leftValue, rightValue);

            if (!equalResult) {
                different = true;
                if(builder != null) {
                    builder.append("\t").append("Key ").append(entry.getKey()).append("\n");
                    builder.append("\t\t").append("Left ").append(diff.leftValue()).append(" class ").append(diff.leftValue().getClass().getSimpleName()).append("\n");
                    builder.append("\t\t").append("Right ").append(diff.rightValue()).append(" class ").append(diff.rightValue().getClass().getSimpleName()).append("\n");
                }
                mapDiff.putDiff(entry.getKey(), DiffEntry.create(entry.getKey()).left(diff.leftValue()).right(diff.rightValue()));
            }
        }
        Map<String, Object> onlyOnLeft = difference.entriesOnlyOnLeft();
        if(!onlyOnLeft.isEmpty()) {
            different = true;
            for(Map.Entry<String, Object> entry : onlyOnLeft.entrySet()) {
                if(builder != null) {
                    builder.append("\t").append("Key ").append(entry.getKey()).append("\n");
                    builder.append("\t\t").append("Left ").append(entry.getValue()).append(" class ").append(entry.getValue().getClass().getSimpleName()).append("\n");
                    builder.append("\t\t").append("Right ").append("N/A").append("\n");
                }
                mapDiff.putDiff(entry.getKey(), DiffEntry.create(entry.getKey()).left(entry.getValue()).missingOnRight(true));
            }
        }
        //Allow more on right when fuzzy match.
        Map<String, Object> onlyOnRight = difference.entriesOnlyOnRight();
        if(!onlyOnRight.isEmpty() && equalsType == EQUALS_TYPE_EXACTLY) {
            different = true;
            for(Map.Entry<String, Object> entry : onlyOnRight.entrySet()) {
                if(builder != null) {
                    builder.append("\t").append("Key ").append(entry.getKey()).append("\n");
                    builder.append("\t\t").append("Left ").append("N/A").append("\n");
                    builder.append("\t\t").append("Right ").append(entry.getValue()).append(" class ").append(entry.getValue().getClass().getSimpleName()).append("\n");
                }

                mapDiff.putDiff(entry.getKey(), DiffEntry.create(entry.getKey()).right(entry.getValue()).missingOnLeft(true));
            }
        }
        if(different) {
            mapDiff.setResult(MapDiff.RESULT_NOT_MATCH);
        }
        return mapDiff;
    }


    public boolean objectIsEqual(Object leftValue, Object rightValue) {
        boolean equalResult = false;
//        if ((leftValue instanceof List) && (rightValue instanceof List)) {
//            if (((List<?>) leftValue).size() == ((List<?>) rightValue).size()) {
//                for (int i = 0; i < ((List<?>) leftValue).size(); i++) {
//                    equalResult = objectIsEqual(((List<?>) leftValue).get(i), ((List<?>) rightValue).get(i));
//                    if (!equalResult) break;
//                }
//            }
//        }

        if ((leftValue instanceof byte[]) && (rightValue instanceof byte[])) {
            equalResult = Arrays.equals((byte[]) leftValue, (byte[]) rightValue);
        } else if ((leftValue instanceof byte[]) && (rightValue instanceof String)) {
            //byte[] vs string, base64 decode string
            try {
//                    byte[] rightBytes = Base64.getDecoder().decode((String) rightValue);
                byte[] rightBytes = Base64.decodeBase64((String) rightValue);
                equalResult = Arrays.equals((byte[]) leftValue, rightBytes);
            } catch (Throwable ignored) {
            }
        } else if ((leftValue instanceof Number) && (rightValue instanceof Number)) {
            //number vs number, equal by value
            BigDecimal leftB = null;
            BigDecimal rightB = null;
            if (leftValue instanceof BigDecimal) {
                leftB = (BigDecimal) leftValue;
            }
            if (rightValue instanceof BigDecimal) {
                rightB = (BigDecimal) rightValue;
            }
            if (leftB == null) {
                leftB = BigDecimal.valueOf(((Number) leftValue).doubleValue());
            }
            if (rightB == null) {
                rightB = BigDecimal.valueOf(((Number) rightValue).doubleValue());
            }
            equalResult = leftB.compareTo(rightB) == 0;
        } else if ((leftValue instanceof Boolean)) {
            if (rightValue instanceof Number) {
                //boolean true == (!=0), false == 0
                Boolean leftBool = (Boolean) leftValue;
                if (Boolean.TRUE.equals(leftBool)) {
                    equalResult = ((Number) rightValue).longValue() != 0;
                } else {
                    equalResult = ((Number) rightValue).longValue() == 0;
                }
            } else if (rightValue instanceof String) {
                //boolean true == "true", false == "false"
                Boolean leftBool = (Boolean) leftValue;
                if (Boolean.TRUE.equals(leftBool)) {
                    equalResult = ((String) rightValue).equalsIgnoreCase("true");
                } else {
                    equalResult = ((String) rightValue).equalsIgnoreCase("false");
                }
            }
        }else{
            equalResult = leftValue.equals(rightValue);
        }
        return equalResult;
    }

}
