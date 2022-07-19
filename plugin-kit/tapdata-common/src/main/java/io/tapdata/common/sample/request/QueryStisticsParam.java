package io.tapdata.common.sample.request;

import java.util.List;
import java.util.Map;

public class QueryStisticsParam {
    private Map<String, String> tags;
    private List<String> fields;

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
}
