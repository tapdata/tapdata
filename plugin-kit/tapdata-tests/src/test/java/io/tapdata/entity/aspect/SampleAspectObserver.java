package io.tapdata.entity.aspect;

import io.tapdata.entity.aspect.annotations.AspectObserverClass;

@AspectObserverClass(SampleAspect.class)
public class SampleAspectObserver extends SampleCommonObserver {
    @Override
    public void observe(SampleAspect aspect) {
        super.observe(aspect);
    }

}
