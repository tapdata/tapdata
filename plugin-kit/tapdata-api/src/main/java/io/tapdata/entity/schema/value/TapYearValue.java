package io.tapdata.entity.schema.value;

import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.type.TapYear;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TapYearValue extends TapValue<DateTime, TapYear> {
    public TapYearValue() {
    }

    public TapYearValue(DateTime dateTime) {
        value = dateTime;
    }

    public TapYearValue(Integer year) {
        try {
            value = new DateTime(new SimpleDateFormat("yyyy").parse(String.valueOf(year)).getTime());
        } catch (ParseException e) {
            value = new DateTime(Long.valueOf(year));
        }
    }

    @Override
    public TapType createDefaultTapType() {
        return new TapYear();
    }

    @Override
    public Class<? extends TapType> tapTypeClass() {
        return TapYear.class;
    }
}
