package io.tapdata.entity.mapping;

import java.util.Map;

public class TypeExprResult<T> {
    private String expression;
    private T value;
    private Map<String, String> params;

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }
}
