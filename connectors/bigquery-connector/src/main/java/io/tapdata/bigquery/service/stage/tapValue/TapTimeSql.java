package io.tapdata.bigquery.service.stage.tapValue;

public class TapTimeSql implements TapValueForBigQuery{
    @Override
    public String value(Object value) {
        return TapValueForBigQuery.simpleStringValue(value);
    }
}
