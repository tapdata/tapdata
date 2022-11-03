package io.tapdata.bigquery.service.stage.tapValue;

public class TapNumberSql implements TapValueForBigQuery{
    @Override
    public String value(Object value) {
        return TapValueForBigQuery.simpleValue(value);
    }
}

