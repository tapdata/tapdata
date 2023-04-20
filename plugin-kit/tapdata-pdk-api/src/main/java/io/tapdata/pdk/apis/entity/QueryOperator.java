package io.tapdata.pdk.apis.entity;

import java.io.Serializable;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

public class QueryOperator implements Serializable {
    public static final int GT = 1;
    public static final int GTE = 2;
    public static final int LT = 3;
    public static final int LTE = 4;

    private String key;
    private Object value;
    private int operator;

    public QueryOperator() {

    }

    public QueryOperator(String key, Object value, int operator) {
        this.key = key;
        this.value = value;
        this.operator = operator;
    }

    public static QueryOperator gt(String key, Object value) {
        return new QueryOperator(key, value, GT);
    }

    public static QueryOperator gte(String key, Object value) {
        return new QueryOperator(key, value, GTE);
    }

    public static QueryOperator lt(String key, Object value) {
        return new QueryOperator(key, value, LT);
    }

    public static QueryOperator lte(String key, Object value) {
        return new QueryOperator(key, value, LTE);
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public int getOperator() {
        return operator;
    }

    public void setOperator(int operator) {
        this.operator = operator;
    }

    public String toString() {
        return toString("");
    }

    public String toString(String quote) {
        String operatorStr;
        switch (operator) {
            case GT:
                operatorStr = ">";
                break;
            case GTE:
                operatorStr = ">=";
                break;
            case LT:
                operatorStr = "<";
                break;
            case LTE:
                operatorStr = "<=";
                break;
            default:
                operatorStr = "";
                break;
        }
        StringBuilder sb = new StringBuilder(quote + key + quote + operatorStr);
        if(value instanceof Number) {
            sb.append(value);
        } else {
            sb.append("'").append(value).append("'");
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        QueryOperator queryOperator = new QueryOperator("a", map(entry("aa", "aa")), LT);
        System.out.println(queryOperator);
    }
}
