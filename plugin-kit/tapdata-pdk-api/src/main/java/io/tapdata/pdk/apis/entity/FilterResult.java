package io.tapdata.pdk.apis.entity;

import java.util.Map;

public class FilterResult {
    private TapFilter filter;
    public FilterResult filter(TapFilter filter) {
        this.filter = filter;
        return this;
    }

    private Map<String, Object> result;
    public FilterResult result(Map<String, Object> result) {
        this.result = result;
        return this;
    }

    private Throwable error;
    public FilterResult error(Throwable error) {
        this.error = error;
        return this;
    }

    public Throwable getError() {
        return error;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    public TapFilter getFilter() {
        return filter;
    }

    public void setFilter(TapFilter filter) {
        this.filter = filter;
    }

    public Map<String, Object> getResult() {
        return result;
    }

    public void setResult(Map<String, Object> result) {
        this.result = result;
    }
}
