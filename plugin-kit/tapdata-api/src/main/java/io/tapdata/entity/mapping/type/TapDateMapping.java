package io.tapdata.entity.mapping.type;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapType;

import java.math.BigDecimal;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * "date": {"range": ["1000-01-01", "9999-12-31"], "gmt" : 0, "to": "typeDate"},
 */
public class TapDateMapping extends TapDateBase {

    private static final String TAG = TapDateMapping.class.getSimpleName();

    @Override
    protected String pattern() {
        return "yyyy-MM-dd";
    }

    @Override
    protected Instant parse(String dateString, String thePattern) {
        try {
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(thePattern);
            LocalDate localDate = LocalDate.parse(dateString, dateTimeFormatter);
            return localDate.atStartOfDay(ZoneId.of("GMT-0")).toInstant();
        } catch(Throwable throwable) {
//            throwable.printStackTrace();
            TapLogger.debug(TAG, "Parse date {} pattern {}, failed {}", dateString, thePattern, throwable.getMessage());
        }
        return null;
    }

    @Override
    public TapType toTapType(String dataType, Map<String, String> params) {
        return new TapDate().withTimeZone(withTimeZone).min(min).max(max).bytes(bytes);
    }

    @Override
    public TapResult<String> fromTapType(String typeExpression, TapType tapType) {
        if (tapType instanceof TapDate) {
            return TapResult.successfully(removeBracketVariables(typeExpression, 0));
        }
        return null;
    }

    final BigDecimal rangeValue = BigDecimal.valueOf(10).pow(19);
    final BigDecimal timeZoneValue = BigDecimal.valueOf(10).pow(17);

    @Override
    public BigDecimal matchingScore(TapField field) {
        if (field.getTapType() instanceof TapDate) {
            TapDate tapDate = (TapDate) field.getTapType();

            //field is primary key, but this type is not able to be primary type.
            if(field.getPrimaryKey() != null && field.getPrimaryKey() && pkEnablement != null && !pkEnablement) {
                return TapMapping.MIN_SCORE;
            }

            BigDecimal score = BigDecimal.ZERO;

            Boolean withTimeZone = tapDate.getWithTimeZone();

            Instant max = tapDate.getMax();
            Instant min = tapDate.getMin();

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
