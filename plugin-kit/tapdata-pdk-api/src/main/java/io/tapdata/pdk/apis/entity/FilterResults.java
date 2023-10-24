package io.tapdata.pdk.apis.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FilterResults {
    private TapAdvanceFilter filter;

    private Throwable error;
    private List<Map<String, Object>> results;

    public FilterResults add(Map<String, Object> value) {
        if(results == null) {
            results = new ArrayList<>();
        }
        results.add(value);
        return this;
    }

    public int resultSize() {
        if(results != null)
            return results.size();
        return 0;
    }

    public TapAdvanceFilter getFilter() {
        return filter;
    }

    public void setFilter(TapAdvanceFilter filter) {
        this.filter = filter;
    }

    public List<Map<String, Object>> getResults() {
        return results;
    }

    public void setResults(List<Map<String, Object>> results) {
        this.results = results;
    }

    public Throwable getError() {
        return error;
    }

    public void setError(Throwable error) {
        this.error = error;
    }
}
