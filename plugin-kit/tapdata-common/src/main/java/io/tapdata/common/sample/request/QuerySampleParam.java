package io.tapdata.common.sample.request;

import java.util.List;
import java.util.Map;

public class QuerySampleParam {
    private Map<String, String> tags;
    private List<String> fields;
    private Long start;
    private Long end;
    private Long limit;
    private String guanluary;
    private String type;

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    public Long getLimit() {
        return limit;
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public String getGuanluary() {
        return guanluary;
    }

    public void setGuanluary(String guanluary) {
        this.guanluary = guanluary;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
