package io.tapdata.entity.aspect;

import io.tapdata.entity.aspect.annotations.AspectObserverClass;

@AspectObserverClass(PerformanceAspect.class)
public class PerformanceObserver implements AspectObserver<PerformanceAspect> {
    @Override
    public void observe(PerformanceAspect aspect) {
    }
}
