package io.tapdata.js.connector.server.function;

import io.tapdata.js.connector.iengine.LoadJavaScripter;

import java.util.Objects;

public class FunctionBase {
    protected JSFunctionNames functionName;
    protected LoadJavaScripter javaScripter;
    public boolean hasNotSupport(LoadJavaScripter javaScripter){
        this.javaScripter = javaScripter;
        return Objects.isNull(javaScripter.supportFunctions(functionName.jsName()));
    }
    public FunctionBase javaScripter(LoadJavaScripter javaScripter){
        this.javaScripter = javaScripter;
        return this;
    }
    public LoadJavaScripter javaScripter(){
        return this.javaScripter;
    }
}
