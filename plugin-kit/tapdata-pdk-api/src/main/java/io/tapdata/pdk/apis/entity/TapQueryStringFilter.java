package io.tapdata.pdk.apis.entity;

import io.tapdata.entity.utils.DataMap;

import java.util.ArrayList;
import java.util.List;

public class TapQueryStringFilter {
    private Integer top;
    public TapQueryStringFilter top(Integer top) {
        this.top = top;
        return this;
    }
    private String queryString;
    public TapQueryStringFilter queryString(String queryString) {
        this.queryString = queryString;
        return this;
    }
    private Integer batchSize;
    public TapQueryStringFilter batchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public static TapQueryStringFilter create() {
        return new TapQueryStringFilter();
    }

    public Integer getTop() {
        return top;
    }

    public void setTop(Integer top) {
        this.top = top;
    }

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }
}