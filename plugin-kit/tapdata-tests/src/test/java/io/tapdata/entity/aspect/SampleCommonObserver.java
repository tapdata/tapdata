package io.tapdata.entity.aspect;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class SampleCommonObserver implements AspectObserver<SampleAspect> {
    private final List<SampleAspect> aspects = new CopyOnWriteArrayList<>();

    @Override
    public void observe(SampleAspect aspect) {
        aspects.add(aspect);
    }

    public List<SampleAspect> getAspects() {
        return aspects;
    }
}
