package io.tapdata.entity.aspect;

import io.tapdata.entity.aspect.annotations.AspectInterceptorClass;

@AspectInterceptorClass(value = SampleAspect.class,order = 1)
public class SampleAspectInterceptor extends SampleCommonInterceptor {

    @Override
    public AspectInterceptResult intercept(SampleAspect aspect) {
        return super.intercept(aspect);
    }
}

