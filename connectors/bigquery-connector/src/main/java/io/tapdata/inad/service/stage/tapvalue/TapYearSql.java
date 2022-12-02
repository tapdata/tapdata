package io.tapdata.inad.service.stage.tapvalue;

public class TapYearSql implements TapValueForBigQuery{
    @Override
    public String value(Object value) {
        return TapValueForBigQuery.simpleStringValue(value);
    }
}
