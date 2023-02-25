package io.tapdata.bigquery.service.stage.tapvalue;

public class TapStringSql implements TapValueForBigQuery{
    @Override
    public String value(Object value) {
        return TapValueForBigQuery.simpleStringValue(value);
    }
}

