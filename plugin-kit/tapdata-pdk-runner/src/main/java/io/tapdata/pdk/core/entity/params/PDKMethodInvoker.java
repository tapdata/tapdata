package io.tapdata.pdk.core.entity.params;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.function.Consumer;

public class PDKMethodInvoker {

    public static PDKMethodInvoker create(){
        return new PDKMethodInvoker();
    }
    private PDKMethodInvoker(){}

    public void cancelRetry() {
        synchronized (this) {
            retryTimes = 0;
            maxRetryTimeMinute = 0;
            this.notifyAll();
        }
    }
    private CommonUtils.AnyError runnable;
    @Deprecated
    private String message;
    private String logTag;
    private Consumer<RuntimeException> errorConsumer;
    private boolean async;
    private ClassLoader contextClassLoader;
    private long retryTimes;
    private long retryPeriodSeconds;
    private long maxRetryTimeMinute; //util:seconds
    private TapLogger.LogListener logListener;
    private Runnable startRetry;
	private Runnable resetRetry;
    private boolean enableSkipErrorEvent;
    private Runnable signFunctionRetry;
    private Runnable clearFunctionRetry;

    public PDKMethodInvoker clearFunctionRetry(Runnable clearFunctionRetry) {
        this.clearFunctionRetry = clearFunctionRetry;
        return this;
    }

    public PDKMethodInvoker signFunctionRetry(Runnable signFunctionRetry) {
        this.signFunctionRetry = signFunctionRetry;
        return this;
    }

    public PDKMethodInvoker startRetry(Runnable startRetry) {
        this.startRetry = startRetry;
        return this;
    }

    public PDKMethodInvoker resetRetry(Runnable resetRetry) {
        this.resetRetry = resetRetry;
        return this;
    }

    public PDKMethodInvoker logListener(TapLogger.LogListener logListener) {
        this.logListener = logListener;
        return this;
    }

    public PDKMethodInvoker maxRetryTimeMinute(long maxRetryTimeMinute){
        this.maxRetryTimeMinute = maxRetryTimeMinute;
        return this;
    }

    public PDKMethodInvoker runnable(CommonUtils.AnyError anyError) {
        this.runnable = anyError;
        return this;
    }

    public PDKMethodInvoker message(String message) {
        this.message = message;
        return this;
    }

    public PDKMethodInvoker logTag(String logTag) {
        this.logTag = logTag;
        return this;
    }

    public PDKMethodInvoker errorConsumer(Consumer<RuntimeException> errorConsumer) {
        this.errorConsumer = errorConsumer;
        return this;
    }

    public PDKMethodInvoker async(boolean async) {
        this.async = async;
        return this;
    }

    public PDKMethodInvoker contextClassLoader(ClassLoader contextClassLoader) {
        this.contextClassLoader = contextClassLoader;
        return this;
    }

    public PDKMethodInvoker retryTimes(long retryTimes) {
        this.retryTimes = retryTimes;
        return this;
    }

    public PDKMethodInvoker retryPeriodSeconds(long retryPeriodSeconds) {
        this.retryPeriodSeconds = retryPeriodSeconds;
        return this;
    }


    public CommonUtils.AnyError getR() {
        return runnable;
    }

    public String getMessage() {
        return message;
    }

    public Consumer<RuntimeException> getErrorConsumer() {
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

    public void setRunnable(CommonUtils.AnyError runnable) {
        this.runnable = runnable;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setLogTag(String logTag) {
        this.logTag = logTag;
    }

    public void setErrorConsumer(Consumer<RuntimeException> errorConsumer) {
        this.errorConsumer = errorConsumer;
    }

    public void setAsync(boolean async) {
        this.async = async;
    }

    public void setContextClassLoader(ClassLoader contextClassLoader) {
        this.contextClassLoader = contextClassLoader;
    }

    public void setRetryTimes(long retryTimes) {
        this.retryTimes = retryTimes;
    }

    public void setRetryPeriodSeconds(long retryPeriodSeconds) {
        this.retryPeriodSeconds = retryPeriodSeconds;
    }

    public CommonUtils.AnyError getRunnable() {
        return runnable;
    }

    public long getMaxRetryTimeMinute() {
        return maxRetryTimeMinute;
    }

    public void setMaxRetryTimeMinute(long maxRetryTimeMinute) {
        this.maxRetryTimeMinute = maxRetryTimeMinute;
    }

    public TapLogger.LogListener getLogListener() {
        return logListener;
    }

    public Runnable getStartRetry() {
        return startRetry;
    }

	public Runnable getResetRetry() {
		return resetRetry;
	}

    public boolean isEnableSkipErrorEvent() {
        return enableSkipErrorEvent;
    }

    public void setEnableSkipErrorEvent(boolean enableSkipErrorEvent) {
        this.enableSkipErrorEvent = enableSkipErrorEvent;
    }

    public Runnable getSignFunctionRetry() {
        return signFunctionRetry;
    }

    public Runnable getClearFunctionRetry() {
        return clearFunctionRetry;
    }
}
