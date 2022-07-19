package io.tapdata.common.sample.request;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Sample {
    public static final String FIELD_DATE = "date";
    public static final String FIELD_VALUES = "vs";

    private Map<String, Number> vs;
    private Date date;

    public Map<String, Number> getVs() {
        return vs;
    }

    public void setVs(Map<String, Number> vs) {
        this.vs = vs;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

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
        Map<String, Number> vs = this.getVs();
        vs.forEach((key, value) -> {
            vs.put(key, 0);
        });
        return vs;
    }

}
