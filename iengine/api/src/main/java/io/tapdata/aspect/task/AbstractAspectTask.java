package io.tapdata.aspect.task;

import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.simplify.pretty.TypeHandlers;

import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/29 19:21 Create
 */
public abstract class AbstractAspectTask extends AspectTask {
    protected final TypeHandlers<Aspect, Void> observerHandlers = TypeHandlers.create();
    protected final TypeHandlers<Aspect, AspectInterceptResult> interceptHandlers = TypeHandlers.create();

    @Override
    public void onStart() {

    }

    @Override
    public void onStop() {

    }

    public List<Class<? extends Aspect>> observeAspects() {
        return observerHandlers.keyList();
    }

    public List<Class<? extends Aspect>> interceptAspects() {
        return interceptHandlers.keyList();
    }

    public void onObserveAspect(Aspect aspect) {
        observerHandlers.handle(aspect);
    }

    public AspectInterceptResult onInterceptAspect(Aspect aspect) {
        return interceptHandlers.handle(aspect);
    }
}
