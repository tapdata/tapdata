package com.tapdata.tm.commons.metrics;

import lombok.Data;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Data
public class SampleVO {
    public static final String FIELD_DATE = "date";
    public static final String FIELD_VALUES = "vs";

    private Map<String, Number> vs;
    private Date date;


    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(FIELD_DATE, date);
        map.put(FIELD_VALUES, vs);
        return map;
    }

    @Override
    public String toString() {
        return "Sample{" +
                "values=" + vs +
                ", date=" + date +
                '}';
    }

    public Map initVsValue() {
        Map<String, Number> vsMap = this.getVs();
        vsMap.forEach((key, value) -> {
            vsMap.put(key, 0);
        });
        return vsMap;
    }
}
