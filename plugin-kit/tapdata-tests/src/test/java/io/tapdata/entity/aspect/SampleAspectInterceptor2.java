package io.tapdata.entity.aspect;

import io.tapdata.entity.aspect.annotations.AspectInterceptorClass;

@AspectInterceptorClass(value = SampleAspect.class, order = 2)
public class SampleAspectInterceptor2 extends SampleCommonInterceptor {

    @Override
    public AspectInterceptResult intercept(SampleAspect aspect) {
        return super.intercept(aspect);
    }
}
