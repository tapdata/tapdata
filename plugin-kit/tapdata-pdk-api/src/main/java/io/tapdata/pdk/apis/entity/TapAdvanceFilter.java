package io.tapdata.pdk.apis.entity;

import io.tapdata.entity.utils.DataMap;

import java.util.ArrayList;
import java.util.List;

/**
 * >, >=, <, <=
 * or, and
 */
public class TapAdvanceFilter extends TapFilter {
    private Integer batchSize;
    private Integer skip;
    private Integer limit;
    private List<QueryOperator> operators;
    private List<SortOn> sortOnList;
    private Projection projection;

    public static TapAdvanceFilter create() {
        return new TapAdvanceFilter();
    }

    public TapAdvanceFilter batchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public TapAdvanceFilter projection(Projection projection) {
        this.projection = projection;
        return this;
    }
    public TapAdvanceFilter limit(int limit) {
        this.limit = limit;
        return this;
    }

    public TapAdvanceFilter skip(int skip) {
        this.skip = skip;
        return this;
    }

    public TapAdvanceFilter op(QueryOperator operator) {
        if(operator == null)
            return this;
        if(operators == null) {
            operators = new ArrayList<>();
        }
        operators.add(operator);
        return this;
    }

    public TapAdvanceFilter match(DataMap match) {
        this.match = match;
        return this;
    }

    public TapAdvanceFilter sort(SortOn sortOn) {
        if(sortOnList == null) {
            sortOnList = new ArrayList<>();
        }
        sortOnList.add(sortOn);
        return this;
    }

    public List<QueryOperator> getOperators() {
        return operators;
    }

    public void setOperators(List<QueryOperator> operators) {
        this.operators = operators;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public List<SortOn> getSortOnList() {
        return sortOnList;
    }

    public void setSortOnList(List<SortOn> sortOnList) {
        this.sortOnList = sortOnList;
    }

    public Integer getSkip() {
        return skip;
    }

    public void setSkip(Integer skip) {
        this.skip = skip;
    }

    public Projection getProjection() {
        return projection;
    }

    public void setProjection(Projection projection) {
        this.projection = projection;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }
    @Override
    public String toString() {
        return "TapAdvanceFilter{" +
                "skip=" + skip +
                ", limit=" + limit +
                ", batchSize=" + batchSize +
                ", operators=" + operators +
                ", sortOnList=" + sortOnList +
                ", projection=" + projection +
                ", match=" + match +
                "} " + super.toString();
    }
}
