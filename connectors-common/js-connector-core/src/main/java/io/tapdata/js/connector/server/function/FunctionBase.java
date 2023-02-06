package io.tapdata.js.connector.server.function;

import io.tapdata.js.connector.iengine.LoadJavaScripter;

public class FunctionBase {
    protected JSFunctionNames functionName;
    protected LoadJavaScripter javaScripter;

    public boolean hasNotSupport(LoadJavaScripter javaScripter) {
        return !this.hasSupport(javaScripter);
    }

    public boolean hasSupport(LoadJavaScripter javaScripter) {
        this.javaScripter = javaScripter;
        return this.javaScripter.functioned(functionName.jsName());
    }

    public FunctionBase javaScripter(LoadJavaScripter javaScripter) {
        this.javaScripter = javaScripter;
        return this;
    }

    public LoadJavaScripter javaScripter() {
        return this.javaScripter;
    }
}
