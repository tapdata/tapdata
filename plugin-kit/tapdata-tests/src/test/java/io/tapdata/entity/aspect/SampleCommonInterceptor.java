package io.tapdata.entity.aspect;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class SampleCommonInterceptor implements AspectInterceptor<SampleAspect> {
    private final List<SampleAspect> aspects = new CopyOnWriteArrayList<>();
    private boolean intercept;

    @Override
    public AspectInterceptResult intercept(SampleAspect aspect) {
        if(intercept) {
            return AspectInterceptResult.create().intercepted(true).interceptor(this).interceptReason("Test intercept");
        }
        aspects.add(aspect);
        return null;
    }

    public List<SampleAspect> getAspects() {
        return aspects;
    }

    public boolean isIntercept() {
        return intercept;
    }

    public void setIntercept(boolean intercept) {
        this.intercept = intercept;
    }
}
