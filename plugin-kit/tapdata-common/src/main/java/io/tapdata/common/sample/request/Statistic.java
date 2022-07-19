package io.tapdata.common.sample.request;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Dexter
 */
public class Statistic {
    public static final String FIELD_DATE = "date";
    public static final String FIELD_VALUES = "vs";
    private Map<String, Number> values;

    /**
     * fields that should use incr to update values
     */
    private List<String> incFields;
    private Date date;

    public Map<String, Number> getValues() {
        return values;
    }

    public void setValues(Map<String, Number> values) {
        this.values = values;
    }

    public List<String> getIncFields() {
        return incFields;
    }

    public void setIncFields(List<String> incFields) {
        this.incFields = incFields;
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
        map.put(FIELD_VALUES, values);
        return map;
    }

    @Override
    public String toString() {
        return "Statistic{" +
                "values=" + values +
                ", incFields=" + incFields +
                ", date=" + date +
                '}';
    }
}
