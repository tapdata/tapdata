package io.tapdata.entity.aspect;

import io.tapdata.entity.aspect.annotations.AspectObserverClass;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

@AspectObserverClass(SampleAspect.class)
public class SampleAspectObserver extends SampleCommonObserver {
    @Override
    public void observe(SampleAspect aspect) {
        super.observe(aspect);
    }

}
