package io.tapdata.customsql.util;

import io.tapdata.pdk.apis.entity.QueryOperator;

public enum QueryOpertorEnum {
    GT(QueryOperator.GT, ">"),
    GTE(QueryOperator.GTE, ">="),
    LE(QueryOperator.LT, "<"),
    LTE(QueryOperator.LTE, "<="),
    EQL(5, "="),
    ;

    private int op;
    private String opStr;
    QueryOpertorEnum(int op, String opStr) {
        this.op = op;
        this.opStr = opStr;
    }


    public String getOpStr() {
        return opStr;
    }

    public int getOp() {
        return op;
    }

    public void setOp(int op) {
        this.op = op;
    }
}

