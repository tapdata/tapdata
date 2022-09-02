package io.tapdata.pdk.apis.functions.connection;

public class RetryOptions {
    public static RetryOptions create(){
        return new RetryOptions();
    }
    private boolean needRetry;
    public RetryOptions needRetry(boolean needRetry) {
        this.needRetry = needRetry;
        return this;
    }
    private Runnable beforeRetryMethod;
    public RetryOptions beforeRetryMethod(Runnable beforeRetryMethod) {
        this.beforeRetryMethod = beforeRetryMethod;
        return this;
    }

    public boolean isNeedRetry() {
        return needRetry;
    }

    public void setNeedRetry(boolean needRetry) {
        this.needRetry = needRetry;
    }

    public Runnable getBeforeRetryMethod() {
        return beforeRetryMethod;
    }

    public void setBeforeRetryMethod(Runnable beforeRetryMethod) {
        this.beforeRetryMethod = beforeRetryMethod;
    }
}
