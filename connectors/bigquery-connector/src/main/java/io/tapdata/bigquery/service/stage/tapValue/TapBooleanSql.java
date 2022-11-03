package io.tapdata.bigquery.service.stage.tapValue;

public class TapBooleanSql implements TapValueForBigQuery{
    @Override
    public String value(Object value) {
        return TapValueForBigQuery.simpleValue(value);
    }
}

