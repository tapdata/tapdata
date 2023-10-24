package io.tapdata.entity.aspect;

public class AspectInterceptResult {
    private boolean intercepted = false;
    public AspectInterceptResult intercepted(boolean intercepted) {
        this.intercepted = intercepted;
        return this;
    }
    private String interceptReason;
    public AspectInterceptResult interceptReason(String interceptReason) {
        this.interceptReason = interceptReason;
        return this;
    }
    private AspectInterceptor<? extends Aspect> interceptor;
    public AspectInterceptResult interceptor(AspectInterceptor<? extends Aspect> interceptor) {
        this.interceptor = interceptor;
        return this;
    }

    public static AspectInterceptResult create() {
        return new AspectInterceptResult();
    }

    public boolean isIntercepted() {
        return intercepted;
    }

    public void setIntercepted(boolean intercepted) {
        this.intercepted = intercepted;
    }

    @Override
    public String toString() {
        return AspectInterceptResult.class.getSimpleName() + ": " + (interceptor != null ? interceptor.toString() : "") + " " + (intercepted ? "intercepted" : "not intercepted") + " interceptReason " + (interceptReason != null ? interceptReason : "");
    }
}
