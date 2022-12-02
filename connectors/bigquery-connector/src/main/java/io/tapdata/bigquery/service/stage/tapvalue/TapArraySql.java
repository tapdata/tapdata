package io.tapdata.bigquery.service.stage.tapvalue;

public class TapArraySql implements TapValueForBigQuery{
    @Override
    public String value(Object value) {
        return TapValueForBigQuery.toJsonValue(value);
    }
}
