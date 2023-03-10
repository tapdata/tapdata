package io.tapdata.entity.mapping.type;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.result.ResultItem;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapType;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * "datetime": {"range": ["1000-01-01 00:00:00", "9999-12-31 23:59:59"], "to": "typeDateTime"},
 */
public class TapDateTimeMapping extends TapDateBase {
    private static final String TAG = TapDateTimeMapping.class.getSimpleName();

    @Override
    protected String pattern() {
        return "yyyy-MM-dd HH:mm:ss SSSSSSSSS";
    }

    @Override
    protected Instant parse(String dateTimeString, String thePattern) {
        try {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(thePattern);
            LocalDateTime localDateTime = LocalDateTime.parse(dateTimeString, dateTimeFormatter);
            return localDateTime.atZone(ZoneId.of("GMT-0")).toInstant();
        } catch (Throwable e) {
//            e.printStackTrace();
            TapLogger.debug(TAG, "Parse date time {} pattern {}, failed, {}", dateTimeString, thePattern, e.getMessage());
        }
        return null;
    }


    @Override
    public TapType toTapType(String dataType, Map<String, String> params) {
        String fractionStr = getParam(params, KEY_FRACTION);
        Integer fraction = null;
        if (fractionStr != null) {
            fractionStr = fractionStr.trim();
            try {
                fraction = Integer.parseInt(fractionStr);
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }
        if(fraction == null)
            fraction = defaultFraction;
        if(fraction == null)
            fraction = maxFraction;

        return new TapDateTime()
                .fraction(fraction)
                .min(min)
                .max(max)
                .withTimeZone(withTimeZone)
                .fraction(fraction)
                .defaultFraction(defaultFraction)
                .bytes(bytes);
    }

    @Override
    public TapResult<String> fromTapType(String typeExpression, TapType tapType) {
//        if (tapType instanceof TapDateTime) {
//            return TapResult.successfully(removeBracketVariables(typeExpression, 0));
//        }
//        return null;

        String theFinalExpression = null;
        TapResult<String> tapResult = new TapResult<>();
        if (tapType instanceof TapDateTime) {
            TapDateTime tapDateTime = (TapDateTime) tapType;
            theFinalExpression = typeExpression;

            Integer fraction = tapDateTime.getFraction();
            if (fraction != null) {
                theFinalExpression = clearBrackets(theFinalExpression, "$" + KEY_FRACTION, false);

                if(this.maxFraction != null && this.minFraction != null) {
                    if(minFraction > fraction) {
                        tapResult.addItem(new ResultItem("TapDateTimeMapping MIN_FRACTION", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Fraction " + fraction + " from source exceeded the minimum of target fraction " + this.minFraction + ", expression " + typeExpression));
                        fraction = minFraction;
                    } else if(maxFraction < fraction) {
                        tapResult.addItem(new ResultItem("TapDateTimeMapping MAX_FRACTION", TapResult.RESULT_SUCCESSFULLY_WITH_WARN, "Precision " + fraction + " from source exceeded the maximum of target fraction " + this.maxFraction + ", expression " + typeExpression));
                        fraction = maxFraction;
                    }
                }
                theFinalExpression = theFinalExpression.replace("$" + KEY_FRACTION, String.valueOf(fraction));
            }

            theFinalExpression = removeBracketVariables(theFinalExpression, 0);
        }
        if(tapResult.getResultItems() != null && !tapResult.getResultItems().isEmpty())
            tapResult.result(TapResult.RESULT_SUCCESSFULLY_WITH_WARN);
        else
            tapResult.result(TapResult.RESULT_SUCCESSFULLY);
        return tapResult.data(theFinalExpression);
    }

    final BigDecimal rangeValue = BigDecimal.valueOf(10).pow(19);
    final BigDecimal fractionValue = BigDecimal.valueOf(10).pow(18);
    final BigDecimal timeZoneValue = BigDecimal.valueOf(10).pow(17);
    final BigDecimal defaultFractionValue = BigDecimal.valueOf(10).pow(16);

    final BigDecimal minMaxFractionValue = BigDecimal.valueOf(10).pow(16);
    @Override
    public BigDecimal matchingScore(TapField field) {
        if (field.getTapType() instanceof TapDateTime) {
            TapDateTime tapDateTime = (TapDateTime) field.getTapType();

            //field is primary key, but this type is not able to be primary type.
            if(field.getPrimaryKey() != null && field.getPrimaryKey() && pkEnablement != null && !pkEnablement) {
                return TapMapping.MIN_SCORE;
            }

            BigDecimal score = BigDecimal.ZERO;

            Integer fraction = tapDateTime.getFraction();
            Integer defaultFraction = tapDateTime.getDefaultFraction();
            Boolean withTimeZone = tapDateTime.getWithTimeZone();

            Instant max = tapDateTime.getMax();
            Instant min = tapDateTime.getMin();

            if((fraction != null && isFraction()) ||
                    (fraction == null && !isFraction())) {
                score = score.add(fractionValue);
            } else {
                score = score.subtract(fractionValue);
            }

            if(isFraction() && this.defaultFraction != null) {
                score = score.add(defaultFractionValue);
            }

            if(fraction != null && this.minFraction != null && this.maxFraction != null && fraction >= this.minFraction && fraction <= this.maxFraction) {
                score = score.add(minMaxFractionValue);
            }

            if((withTimeZone != null && withTimeZone && this.withTimeZone != null && this.withTimeZone) ||
                    ((withTimeZone == null || !withTimeZone) && (this.withTimeZone == null || !this.withTimeZone))) {
                score = score.add(timeZoneValue);
            } else {
                score = score.subtract(timeZoneValue);
            }

            if(min != null && max != null && this.min != null && this.max != null)
                score = score.add(calculateScoreForValue(min, max, this.min, this.max, rangeValue));

            return score;
        }

        return TapMapping.MIN_SCORE;
    }

}
