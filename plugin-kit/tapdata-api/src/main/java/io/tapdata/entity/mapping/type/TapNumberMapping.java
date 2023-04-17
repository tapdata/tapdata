package io.tapdata.entity.mapping.type;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.error.TapAPIErrorCodes;
import io.tapdata.entity.result.ResultItem;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.utils.TypeUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.tapNumber;

/**
 * "decimal($precision, $scale)[unsigned][zerofill]": {"precision":[1, 65], "scale": [0, 30], "unsigned": true, "to": "typeNumber"},
 * "int[($length)][unsigned][zerofill]": {"length": 32, "unsigned": true, "to": "TapNumber"},
 */
public class TapNumberMapping extends TapMapping {
    public static final String KEY_PRECISION = "precision";
    public static final String KEY_PRECISION_DEFAULT = "defaultPrecision";
    public static final String KEY_PRECISION_PREFER = "preferPrecision";
    public static final String KEY_SCALE = "scale";
    public static final String KEY_SCALE_DEFAULT = "defaultScale";
    public static final String KEY_SCALE_PREFER = "preferScale";
    public static final String KEY_UNSIGNED = "unsigned";
    public static final String KEY_ZEROFILL = "zerofill";
    public static final String KEY_BIT = "bit";
    public static final String KEY_BIT_DEFAULT = "defaultBit";
    public static final String KEY_BIT_PREFER = "preferBit";
    public static final String KEY_BIT_RATIO = "bitRatio";
    public static final String KEY_VALUE = "value";
    public static final String KEY_UNSIGNED_VALUE = "unsignedValue";
    public static final String KEY_FIXED = "fixed";

    private Boolean fixed;

    private Integer bit;
    private Integer defaultBit;
    private Integer preferBit;
    private int bitRatio = 1;

    private Integer minPrecision;
    private Integer maxPrecision;
    private Integer defaultPrecision;
    private Integer preferPrecision;

    private Integer minScale;
    private Integer maxScale;
    private Integer defaultScale;
    private Integer preferScale;

    private String unsigned;
    private String zerofill;

    private BigDecimal minValue;
    private BigDecimal maxValue;

    private BigDecimal unsignedMinValue;
    private BigDecimal unsignedMaxValue;

    protected Integer getFromTapTypeBytes(Integer bit) {
        if(bitRatio == 1)
            return bit;
        // 14 / 8 = 2, 14 / 7 = 2
        return (bit / bitRatio + ((bit % bitRatio) > 0 ? 1 : 0));
    }

    @Override
    public void from(Map<String, Object> info) {
        Object fixedObj = getObject(info, KEY_FIXED);
        if (fixedObj instanceof Boolean) {
            fixed = (Boolean) fixedObj;
        }

        Object bitObj = getObject(info, KEY_BIT);
        if (bitObj instanceof Number) {
            bit = ((Number) bitObj).intValue();
        }
        Object defaultLengthObj = getObject(info, KEY_BIT_DEFAULT);
        if(defaultLengthObj instanceof Number) {
            defaultBit = ((Number) defaultLengthObj).intValue();
        }
        Object preferLengthObj = getObject(info, KEY_BIT_PREFER);
        if(preferLengthObj instanceof Number) {
            preferBit = ((Number) preferLengthObj).intValue();
        }
        Object ratioObj = info.get(KEY_BIT_RATIO);
        if(ratioObj instanceof Number) {
            bitRatio = ((Number) ratioObj).intValue();
        }

        Object precisionObj = getObject(info, KEY_PRECISION);
        if (precisionObj instanceof List) {
            List<?> list = (List<?>) precisionObj;
            if (list.size() == 2) {
                if (list.get(0) instanceof Number) {
                    minPrecision = ((Number) list.get(0)).intValue();
                }
                if (list.get(1) instanceof Number) {
                    maxPrecision = ((Number) list.get(1)).intValue();
                }
            }
        } else if(precisionObj instanceof Number) {
            minPrecision = 1;
            maxPrecision = ((Number) precisionObj).intValue();
        }
        Object defaultPrecisionObj = getObject(info, KEY_PRECISION_DEFAULT);
        if(defaultPrecisionObj instanceof Number) {
            defaultPrecision = ((Number) defaultPrecisionObj).intValue();
        }
        Object preferPrecisionObj = getObject(info, KEY_PRECISION_PREFER);
        if(preferPrecisionObj instanceof Number) {
            preferPrecision = ((Number) preferPrecisionObj).intValue();
        }

        Object scaleObj = getObject(info, KEY_SCALE);
        if (scaleObj instanceof List) {
            List<?> list = (List<?>) scaleObj;
            if (list.size() == 2) {
                if (list.get(0) instanceof Number) {
                    minScale = ((Number) list.get(0)).intValue();
                }
                if (list.get(1) instanceof Number) {
                    maxScale = ((Number) list.get(1)).intValue();
                }
            }
        } else if(scaleObj instanceof Number) {
            minScale = 0;
            maxScale = ((Number) scaleObj).intValue();
        } else if(scaleObj instanceof Boolean) {
            minScale = 0;
            maxScale = 1;
        } else if(scaleObj instanceof String) {
            if(((String) scaleObj).equalsIgnoreCase("true")) {
                minScale = 0;
                maxScale = 1;
            }
        }

        Object defaultScaleObj = getObject(info, KEY_SCALE_DEFAULT);
        if(defaultScaleObj instanceof Number) {
            defaultScale = ((Number) defaultScaleObj).intValue();
        }
        Object preferScaleObj = getObject(info, KEY_SCALE_PREFER);
        if(preferScaleObj instanceof Number) {
            preferScale = ((Number) preferScaleObj).intValue();
        }

        Object valueObj = getObject(info, KEY_VALUE);
        if (valueObj instanceof List) {
            List<?> list = (List<?>) valueObj;
            if (list.size() == 2) {
                if ((list.get(0) instanceof Number) || (list.get(0) instanceof String)) {
                    minValue = new BigDecimal(String.valueOf(list.get(0)));
                }
                if ((list.get(1) instanceof Number) || (list.get(1) instanceof String)) {
                    maxValue = new BigDecimal(String.valueOf(list.get(1)));
                }
            }
        }

        Object unsignedValueObj = getObject(info, KEY_UNSIGNED_VALUE);
        if (unsignedValueObj instanceof List) {
            List<?> list = (List<?>) unsignedValueObj;
            if (list.size() == 2) {
                if ((list.get(0) instanceof Number) || (list.get(0) instanceof String)) {
                    unsignedMinValue = new BigDecimal(String.valueOf(list.get(0)));
                }
                if ((list.get(1) instanceof Number) || (list.get(1) instanceof String)) {
                    unsignedMaxValue = new BigDecimal(String.valueOf(list.get(1)));
                }
            }
        }

        Object unsignedObj = getObject(info, KEY_UNSIGNED);
        if (unsignedObj instanceof String) {
            unsigned = (String) unsignedObj;
        }

        Object zerofillObj = getObject(info, KEY_ZEROFILL);
        if (zerofillObj instanceof String) {
            zerofill = (String) zerofillObj;
        }

        //calculate the min and max value.
        //The accuracy order, min/max value > bit > precision.
        if(minValue == null || maxValue == null || unsignedMinValue == null || unsignedMaxValue == null) {
            //bit is higher priority than precision, lower than given min/max value.
            Integer theBit = bit;
            if(theBit == null)
                theBit = defaultBit;
            if(theBit != null) {
                theBit = theBit * bitRatio;
                if(unsigned != null && unsignedMinValue == null) {
                    unsignedMinValue = TypeUtils.minValueForBit(theBit, true);
                }
                if(minValue == null)
                    minValue = TypeUtils.minValueForBit(theBit, false);
                if(unsigned != null && unsignedMaxValue == null) {
                    unsignedMaxValue = TypeUtils.maxValueForBit(theBit, true);
                }
                if(maxValue == null)
                    maxValue = TypeUtils.maxValueForBit(theBit, false);
            }
        }
        if(minValue == null || maxValue == null || unsignedMinValue == null || unsignedMaxValue == null) {
            //precision is lowest priority, for the value boundary, the most case, the precision is not accurate.
            Integer thePrecision = maxPrecision;
            if(thePrecision == null)
                thePrecision = defaultPrecision;
            if(thePrecision != null) {
                if(unsigned != null && unsignedMinValue == null) {
                    unsignedMinValue = BigDecimal.ZERO;
                }
                if(minValue == null)
                    minValue = TypeUtils.minValueForPrecision(thePrecision);
                if(unsigned != null && unsignedMaxValue == null) {
                    unsignedMaxValue = TypeUtils.maxValueForPrecision(thePrecision);
                }
                if(maxValue == null)
                    maxValue = TypeUtils.maxValueForPrecision(thePrecision);
            }
        }
        if(minValue == null || maxValue == null) {
            minValue = BigDecimal.valueOf(-Double.MAX_VALUE);
            maxValue = BigDecimal.valueOf(Double.MAX_VALUE);
        }
        if(minPrecision == null && maxPrecision == null) {
            minPrecision = 1;
            maxPrecision = maxValue.toPlainString().length();
        }
    }

    @Override
    public TapType toTapType(String dataType, Map<String, String> params) {
        Boolean theUnsigned = null;
        if (unsigned != null && dataType.contains(unsigned)) {
            theUnsigned = true;
        }
        Boolean theZerofill = null;
        if (zerofill != null && dataType.contains(zerofill)) {
            theZerofill = true;
        }
        boolean hasBitVariable = false;
        boolean hasPrecisionScaleVariable = false;

        String lengthStr = getParam(params, KEY_BIT);
        Integer length = null;
        if (lengthStr != null) {
            try {
                length = Integer.parseInt(lengthStr);
                hasBitVariable = true;
            } catch (Throwable throwable) {
                throw new CoreException(TapAPIErrorCodes.ERROR_NUMBER_BIT_PARSE_FAILED, "Parse bit failed, str {}, error {}", lengthStr, throwable.getMessage());
            }
        }
        if(length == null) {
            length = preferBit;
            if(length != null)
                hasBitVariable = true;
        }
        if(length == null)
            length = defaultBit;
        if(length == null)
            length = bit;
        if(length != null)
            length = length * bitRatio;

        String precisionStr = getParam(params, KEY_PRECISION);
        Integer precision = null;
        if (precisionStr != null) {
            precisionStr = precisionStr.trim();
            try {
                precision = Integer.parseInt(precisionStr);
                hasPrecisionScaleVariable = true;
            } catch (Throwable throwable) {
                throw new CoreException(TapAPIErrorCodes.ERROR_NUMBER_PRECISION_PARSE_FAILED, "Parse precision failed, str {}, error {}", precisionStr, throwable.getMessage());
            }
        }
        if(precision == null) {
            precision = preferPrecision;
            if(precision != null)
                hasPrecisionScaleVariable = true;
        }
        if(precision == null)
            precision = defaultPrecision;
        if(precision == null)
            precision = maxPrecision;

        String scaleStr = getParam(params, KEY_SCALE);
        Integer scale = null;
        if (scaleStr != null) {
            scaleStr = scaleStr.trim();
            try {
                scale = Integer.parseInt(scaleStr);
            } catch (Throwable throwable) {
                throw new CoreException(TapAPIErrorCodes.ERROR_NUMBER_SCALE_PARSE_FAILED, "Parse scale failed, str {}, error {}", scaleStr, throwable.getMessage());
            }
        }
        if(scale == null)
            scale = preferScale;
        if(scale == null)
            scale = defaultScale;
        if(scale == null)
            scale = maxScale;

        BigDecimal theMinValue = null;
        BigDecimal theMaxValue = null;
        if(hasBitVariable) {
            theMinValue = TypeUtils.minValueForBit(length, theUnsigned);
            theMaxValue = TypeUtils.maxValueForBit(length, theUnsigned);
        } else if(hasPrecisionScaleVariable){
            Integer newPrecision = precision;
            //Support Oracle scale can be negative value, it will increase the precision length.
            if(scale != null && scale < 0) {
                newPrecision = precision - scale;
            }

            theMinValue = (theUnsigned != null && theUnsigned) ? BigDecimal.ZERO : TypeUtils.minValueForPrecision(newPrecision);
            theMaxValue = TypeUtils.maxValueForPrecision(newPrecision);
        } else if(minValue != null && maxValue != null) {
            if(theUnsigned != null && theUnsigned && unsignedMinValue != null && unsignedMaxValue != null) {
                theMinValue = unsignedMinValue;
                theMaxValue = unsignedMaxValue;
            } else {
                theMinValue = minValue;
                theMaxValue = maxValue;
            }
        } else if(length != null) {
            theMinValue = TypeUtils.minValueForBit(length, theUnsigned);
            theMaxValue = TypeUtils.maxValueForBit(length, theUnsigned);
        } else if(precision != null){
            theMinValue = (theUnsigned != null && theUnsigned) ? BigDecimal.ZERO : TypeUtils.minValueForPrecision(precision);
            theMaxValue = TypeUtils.maxValueForPrecision(precision);
        } else {
            theMinValue = BigDecimal.valueOf(-Double.MAX_VALUE);
            theMaxValue = BigDecimal.valueOf(Double.MAX_VALUE);
        }
        int actualPrecision = theMaxValue.toPlainString().length();
        if(precision > actualPrecision) {
            precision = actualPrecision;
        }

        return tapNumber()
                .fixed(fixed)
                .precision(precision)
                .scale(scale)
                .bit(length)
                .minValue(theMinValue)
                .maxValue(theMaxValue)
                .unsigned(theUnsigned)
                .zerofill(theZerofill);
    }
    final BigDecimal theMaxValue = BigDecimal.valueOf(10).pow(2406);
    final BigDecimal valueValue = BigDecimal.valueOf(10).pow(2405);
    final BigDecimal scaleValue = BigDecimal.valueOf(10).pow(2405);
    final BigDecimal fixedValue = BigDecimal.valueOf(10).pow(1600);
    final BigDecimal unsignedValue = BigDecimal.valueOf(10).pow(2404);

    @Override
    public BigDecimal matchingScore(TapField field) {
        if (field.getTapType() instanceof TapNumber) {
            TapNumber tapNumber = (TapNumber) field.getTapType();

            //field is primary key, but this type is not able to be primary type.
            if(field.getPrimaryKey() != null && field.getPrimaryKey() && pkEnablement != null && !pkEnablement) {
                return TapMapping.MIN_SCORE;
            }

            BigDecimal score = BigDecimal.ZERO;

            Integer scale = tapNumber.getScale();
            Boolean fixed = tapNumber.getFixed();
            Boolean unsigned = tapNumber.getUnsigned();

            BigDecimal comingMaxValue = tapNumber.getMaxValue();
            if(comingMaxValue.compareTo(valueValue) > 0) {
                comingMaxValue = theMaxValue;
            }
            BigDecimal comingMinValue = tapNumber.getMinValue();
            if(comingMinValue.compareTo(valueValue.negate()) < 0) {
                comingMinValue = theMaxValue.negate();
            }

//            final BigDecimal unsignedValue = realMaxValue.multiply(BigDecimal.TEN);
//            final BigDecimal fixedValue =   unsignedValue.multiply(BigDecimal.TEN);
//            final BigDecimal scaleValue =  fixedValue.multiply(BigDecimal.TEN);
//            final BigDecimal valueValue = scaleValue.multiply(BigDecimal.TEN);

            //scale is minus, still consider as scaled, not as an integer. so use scale != 0 instead of scale > 0
            if((scale != null && scale != 0 && isScale()) ||
                    (scale == null && !isScale())) {
//                score += scaleValue;
                score = score.add(scaleValue);
            } else {
//                score -= scaleValue;
                score = score.subtract(scaleValue);
            }

            if(((fixed != null && fixed) && (this.fixed != null && this.fixed)) ||
                    ((fixed == null || !fixed) && (this.fixed == null || !this.fixed))) {
//                score += fixedValue;
                score = score.add(fixedValue);
            } else {
//                score -= fixedValue;
                score = score.subtract(fixedValue);
            }

            if(((unsigned != null && unsigned) && (this.unsigned != null)) ||
                    ((unsigned == null || !unsigned)/* && (this.unsigned == null)*/)) {
//                score += unsignedValue;
                score = score.add(unsignedValue);
            } else {
//                score -= unsignedValue;
                score = score.subtract(unsignedValue);
            }

            if(unsigned != null && unsigned) {
                //unsigned number
                if(unsignedMinValue != null && unsignedMaxValue != null) {
                    score = score.add(calculateScoreForValue(comingMinValue, comingMaxValue, unsignedMinValue, unsignedMaxValue, valueValue));
                } else {
                    score = score.add(calculateScoreForValue(comingMinValue, comingMaxValue, minValue, maxValue, valueValue));
                }
            } else {
                //singed number
                score = score.add(calculateScoreForValue(comingMinValue, comingMaxValue, minValue, maxValue, valueValue));
            }

//            Integer precision = tapNumber.getPrecision();
//            if(precision != null && scale != null && minPrecision != null && minScale != null && maxPrecision != null && maxScale != null) {
//                if(minPrecision <= precision && precision <= maxPrecision) {
//                    score += 1000L - (maxPrecision - precision); // The closest to maxPrecision the better.
//                } else {
//                    if(precision > maxPrecision) {
//                        return (maxPrecision - precision);
//                    }
//                    return Long.MIN_VALUE; //if precision didn't match, it is not acceptable
//                }
//                if(minScale <= scale && scale <= maxScale) {
//                    score += 500;
//                } else {
//                    score += 1; //loss scale, somehow is acceptable as lowest priority
//                }
//            }
//
//            Integer bit = tapNumber.getBit();
//            if(bit != null && this.bit != null) {
//                int theBit = this.bit * bitRatio;
//                if(0 < bit && bit <= theBit) {
//                    score = 1000L - (theBit - bit); //The closest to max bit, the better
//                } else {
////                    if(bit > theBit) {
////                        return theBit - bit;
////                    }
//                    return Long.MIN_VALUE; //if bit didn't match, it is not acceptable
//                }
//            }
//
//            if(tapNumber.getUnsigned() != null && tapNumber.getUnsigned() && unsigned != null) {
//                //number is unsigned, current mapping support unsigned, closer.
//                score += 10;
//            }
//
//            if(tapNumber.getZerofill() != null && tapNumber.getZerofill() && zerofill != null) {
//                //number is zerofill, current mapping support zerofill, closer.
//                score += 1;
//            }
            return score;
        }

        return TapMapping.MIN_SCORE;
    }

    private BigDecimal calculateScoreForValue(BigDecimal comingMinValue, BigDecimal comingMaxValue, BigDecimal minValue, BigDecimal maxValue, BigDecimal valueValue) {
        BigDecimal minDistance = comingMinValue.subtract(minValue);
        BigDecimal maxDistance = maxValue.subtract(comingMaxValue);

        if (minDistance.compareTo(BigDecimal.ZERO) < 0 || maxDistance.compareTo(BigDecimal.ZERO) < 0) {
            BigDecimal theDistance = minDistance.add(maxDistance).abs();
            if(theDistance.compareTo(valueValue.pow(2)) > 0) {
                return valueValue.add(valueValue.pow(2)).negate();//-valueValue - valueValue;
            } else {
                return valueValue.add(theDistance).negate();//-valueValue - theDistance.negate().doubleValue();
            }
        } else {
            BigDecimal valueDistance = valueValue.subtract(minDistance.add(maxDistance));
            if(valueDistance.compareTo(BigDecimal.ZERO) < 0) {
                return BigDecimal.ZERO;
            }
            return valueDistance;
        }
    }

    private boolean isScale() {
        return this.defaultScale != null || (this.minScale != null && this.maxScale != null);
    }

    @Override
    public TapResult<String> fromTapType(String typeExpression, TapType tapType) {
        String theFinalExpression = null;
        TapResult<String> tapResult = new TapResult<>();
        if (tapType instanceof TapNumber) {
            TapNumber tapNumber = (TapNumber) tapType;
            theFinalExpression = typeExpression;
            if (tapNumber.getUnsigned() != null && tapNumber.getUnsigned()) {
                theFinalExpression = clearBrackets(theFinalExpression, unsigned);
            }
            if (tapNumber.getZerofill() != null && tapNumber.getZerofill()) {
                theFinalExpression = clearBrackets(theFinalExpression, zerofill);
            }

            if (tapNumber.getBit() != null) {
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_BIT, false);
                int bit = getFromTapTypeBytes(tapNumber.getBit());
                if(this.bit != null && bit > this.bit) {
                    tapResult.addItem(new ResultItem("TapNumberMapping BIT", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Bit " + bit + " from source exceeded the maximum of target bit " + this.bit + ", bit before ratio " + tapNumber.getBit() + ", expression " + typeExpression));
                    bit = this.bit;
                }
                theFinalExpression = theFinalExpression.replace("$" + KEY_BIT, String.valueOf(bit));
            }

            boolean precisionExceeded = false;
            Integer precision = tapNumber.getPrecision();
            if(precision != null && this.maxPrecision != null && this.minPrecision != null) {
                if(maxPrecision < precision) {
                    precisionExceeded = true;
                }
            }

            int precisionFromNegativeScale = 0;

            Integer scale = tapNumber.getScale();
            if(tapNumber.getPrecision() != null && scale == null)
                scale = 0;
            else if(precisionExceeded) //if precision exceeded, remove scale to keep the number as larger as possible.
                scale = 0;

            if (scale != null) {
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_SCALE, false);

                if(minScale != null && maxScale != null) {
                    if(minScale > scale) {
                        tapResult.addItem(new ResultItem("TapNumberMapping MIN_SCALE", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Scale " + scale + " from source exceeded the minimum of target scale " + this.minScale + ", expression " + typeExpression));
                        if(scale < 0) {
                            //handle scale is negative case.
                            int theMinScale = minScale >= 0 ? 0 : minScale;
                            precisionFromNegativeScale = theMinScale - scale;
                        }
                        scale = minScale;
                    } else if(maxScale < scale) {
                        tapResult.addItem(new ResultItem("TapNumberMapping MAX_SCALE", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Scale " + scale + " from source exceeded the maxiumu of target scale " + this.maxScale + ", expression " + typeExpression));
                        scale = maxScale;
                    }
                }
                theFinalExpression = theFinalExpression.replace("$" + KEY_SCALE, String.valueOf(scale));
            }

            if (precision != null) {
                precision += precisionFromNegativeScale;

                //if scale larger than precision, force precision equal to scale.
                if(scale != null && scale > precision) {
                    precision = scale;
                }

                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_PRECISION, false);

                if(this.maxPrecision != null && this.minPrecision != null) {
                    if(minPrecision > precision) {
                        tapResult.addItem(new ResultItem("TapNumberMapping MIN_PRECISION", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Precision " + precision + " from source exceeded the minimum of target precision " + this.minPrecision + ", expression " + typeExpression));
                        precision = minPrecision;
                    } else if(maxPrecision < precision) {
                        tapResult.addItem(new ResultItem("TapNumberMapping MAX_PRECISION", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Precision " + precision + " from source exceeded the maximum of target precision " + this.maxPrecision + ", expression " + typeExpression));
                        precision = maxPrecision;
                    }
                }
                theFinalExpression = theFinalExpression.replace("$" + KEY_PRECISION, String.valueOf(precision));
            }

            theFinalExpression = removeBracketVariables(theFinalExpression, 0);
        }
        if(tapResult.getResultItems() != null && !tapResult.getResultItems().isEmpty())
            tapResult.result(TapResult.RESULT_SUCCESSFULLY_WITH_WARN);
        else
            tapResult.result(TapResult.RESULT_SUCCESSFULLY);
        return tapResult.data(theFinalExpression);
    }

    public Integer getMinPrecision() {
        return minPrecision;
    }

    public void setMinPrecision(Integer minPrecision) {
        this.minPrecision = minPrecision;
    }

    public Integer getMaxPrecision() {
        return maxPrecision;
    }

    public void setMaxPrecision(Integer maxPrecision) {
        this.maxPrecision = maxPrecision;
    }

    public Integer getMinScale() {
        return minScale;
    }

    public void setMinScale(Integer minScale) {
        this.minScale = minScale;
    }

    public Integer getMaxScale() {
        return maxScale;
    }

    public void setMaxScale(Integer maxScale) {
        this.maxScale = maxScale;
    }

    public String getUnsigned() {
        return unsigned;
    }

    public void setUnsigned(String unsigned) {
        this.unsigned = unsigned;
    }

    public String getZerofill() {
        return zerofill;
    }

    public void setZerofill(String zerofill) {
        this.zerofill = zerofill;
    }

    public Integer getBit() {
        return bit;
    }

    public void setBit(Integer bit) {
        this.bit = bit;
    }

    public Integer getDefaultBit() {
        return defaultBit;
    }

    public void setDefaultBit(Integer defaultBit) {
        this.defaultBit = defaultBit;
    }

    public Integer getDefaultPrecision() {
        return defaultPrecision;
    }

    public void setDefaultPrecision(Integer defaultPrecision) {
        this.defaultPrecision = defaultPrecision;
    }

    public Integer getDefaultScale() {
        return defaultScale;
    }

    public void setDefaultScale(Integer defaultScale) {
        this.defaultScale = defaultScale;
    }

    public Boolean getFixed() {
        return fixed;
    }

    public void setFixed(Boolean fixed) {
        this.fixed = fixed;
    }

    public int getBitRatio() {
        return bitRatio;
    }

    public void setBitRatio(int bitRatio) {
        this.bitRatio = bitRatio;
    }

    public BigDecimal getMinValue() {
        return minValue;
    }

    public void setMinValue(BigDecimal minValue) {
        this.minValue = minValue;
    }

    public BigDecimal getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(BigDecimal maxValue) {
        this.maxValue = maxValue;
    }

    public BigDecimal getUnsignedMinValue() {
        return unsignedMinValue;
    }

    public void setUnsignedMinValue(BigDecimal unsignedMinValue) {
        this.unsignedMinValue = unsignedMinValue;
    }

    public BigDecimal getUnsignedMaxValue() {
        return unsignedMaxValue;
    }

    public void setUnsignedMaxValue(BigDecimal unsignedMaxValue) {
        this.unsignedMaxValue = unsignedMaxValue;
    }

    public Integer getPreferBit() {
        return preferBit;
    }

    public void setPreferBit(Integer preferBit) {
        this.preferBit = preferBit;
    }

    public Integer getPreferPrecision() {
        return preferPrecision;
    }

    public void setPreferPrecision(Integer preferPrecision) {
        this.preferPrecision = preferPrecision;
    }

    public Integer getPreferScale() {
        return preferScale;
    }

    public void setPreferScale(Integer preferScale) {
        this.preferScale = preferScale;
    }
}
