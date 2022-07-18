package io.tapdata.observable;

import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.flow.engine.V2.aspect.task.AspectTask;
import io.tapdata.flow.engine.V2.aspect.task.AspectTaskSession;

import java.util.List;

@AspectTaskSession
public class ObservableAspectTask extends AspectTask {
    @Override
    public void onStart() {

    }

    @Override
    public void onStop() {

    }

    @Override
    public List<Class<? extends Aspect>> observeAspects() {
        return null;
    }

    @Override
    public List<Class<? extends Aspect>> interceptAspects() {
        return null;
    }

    @Override
    public void onObserveAspect(Aspect aspect) {

    }

    @Override
    public AspectInterceptResult onInterceptAspect(Aspect aspect) {
        return null;
    }
}
