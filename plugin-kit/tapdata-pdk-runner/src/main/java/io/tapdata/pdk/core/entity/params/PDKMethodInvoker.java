package io.tapdata.pdk.core.entity.params;

import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.function.Consumer;

public class PDKMethodInvoker {

    public static PDKMethodInvoker create(){
        return new PDKMethodInvoker();
    }
    private PDKMethodInvoker(){}

    private CommonUtils.AnyError runnable;
    private String message;
    private String logTag;
    private Consumer<CoreException> errorConsumer;
    private boolean async;
    private ClassLoader contextClassLoader;
    private long retryTimes;
    private long retryPeriodSeconds;

    public PDKMethodInvoker setRunnable(CommonUtils.AnyError anyError) {
        this.runnable = anyError;
        return this;
    }

    public PDKMethodInvoker setMessage(String message) {
        this.message = message;
        return this;
    }

    public PDKMethodInvoker setLogTag(String logTag) {
        this.logTag = logTag;
        return this;
    }

    public PDKMethodInvoker setErrorConsumer(Consumer<CoreException> errorConsumer) {
        this.errorConsumer = errorConsumer;
        return this;
    }

    public PDKMethodInvoker setAsync(boolean async) {
        this.async = async;
        return this;
    }

    public PDKMethodInvoker setContextClassLoader(ClassLoader contextClassLoader) {
        this.contextClassLoader = contextClassLoader;
        return this;
    }

    public PDKMethodInvoker setRetryTimes(long retryTimes) {
        this.retryTimes = retryTimes;
        return this;
    }

    public PDKMethodInvoker setRetryPeriodSeconds(long retryPeriodSeconds) {
        this.retryPeriodSeconds = retryPeriodSeconds;
        return this;
    }


    public CommonUtils.AnyError getR() {
        return runnable;
    }

    public String getMessage() {
        return message;
    }

    public Consumer<CoreException> getErrorConsumer() {
        return errorConsumer;
    }

    public boolean isAsync() {
        return async;
    }

    public ClassLoader getContextClassLoader() {
        return contextClassLoader;
    }

    public long getRetryTimes() {
        return retryTimes;
    }

    public long getRetryPeriodSeconds() {
        return retryPeriodSeconds;
    }

    public String getLogTag() {
        return logTag;
    }
}
