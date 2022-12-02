package io.tapdata.inad.service.stage.tapvalue;

public class TapNumberSql implements TapValueForBigQuery{
    @Override
    public String value(Object value) {
        return TapValueForBigQuery.simpleValue(value);
    }
}

