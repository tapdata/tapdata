package io.tapdata.bigquery.service.stage.tapvalue;

public class TapMapSql implements TapValueForBigQuery{
    @Override
    public String value(Object value) {
        return TapValueForBigQuery.toJsonValue(value);
    }
}

