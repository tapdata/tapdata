package io.tapdata.pdk.cli.support;

import io.tapdata.entity.schema.TapTable;

public class TableExpressionWrapper {
    public TableExpressionWrapper(TapTable table, String expression) {
        this.table = table;
        this.expression = expression;
    }

    private final TapTable table;
    private final String expression;

    public TapTable table(){
        return this.table;
    }
    public String expression(){
        return this.expression;
    }
}