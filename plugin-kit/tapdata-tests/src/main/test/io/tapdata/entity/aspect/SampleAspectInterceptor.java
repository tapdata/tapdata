package io.tapdata.entity.aspect;

import io.tapdata.entity.aspect.annotations.AspectInterceptorClass;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@AspectInterceptorClass(value = SampleAspect.class,order = 1)
public class SampleAspectInterceptor extends SampleCommonInterceptor {

    @Override
    public AspectInterceptResult intercept(SampleAspect aspect) {
        return super.intercept(aspect);
    }
}

