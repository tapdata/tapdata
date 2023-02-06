package io.tapdata.pdk.tdd.core;

import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;

public class SupportFunction {
    private Class<? extends TapFunction> function;
    private List<Class<? extends TapFunction>> anyOfFunctions;
    private String errorMessage;
    public static final int TYPE_ONE = 1;
    public static final int TYPE_ANY = 2;
    private int type;

    public SupportFunction(List<Class<? extends TapFunction>> anyOfFunctions, String errorMessage) {
        this.anyOfFunctions = anyOfFunctions;
        this.errorMessage = errorMessage;
        type = TYPE_ANY;
    }

    public SupportFunction(Class<? extends TapFunction> function, String errorMessage) {
        this.function = function;
        this.errorMessage = errorMessage;
        type = TYPE_ONE;
    }

    public Class<? extends TapFunction> getFunction() {
        return function;
    }

    public void setFunction(Class<? extends TapFunction> function) {
        this.function = function;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public SupportFunction type(int type) {
        this.type = type;
        return this;
    }

    public List<Class<? extends TapFunction>> getAnyOfFunctions() {
        return anyOfFunctions;
    }

    public void setAnyOfFunctions(List<Class<? extends TapFunction>> anyOfFunctions) {
        this.anyOfFunctions = anyOfFunctions;
    }
}
