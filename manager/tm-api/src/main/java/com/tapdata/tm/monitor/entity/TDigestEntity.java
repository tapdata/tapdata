package com.tapdata.tm.monitor.entity;

import lombok.Data;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Data
public class TDigestEntity {
    public static final String FIELD_DATE = "date";
    public static final String FIELD_VALUES = "digest";

    private Map<String, byte[]> digest;
    private Date date;

    public Map<String, Object> toMap() {
        HashMap<String, Object> map = new HashMap<>();
        map.put(FIELD_DATE, date);
        map.put(FIELD_VALUES, digest);
        return map;
    }
}
