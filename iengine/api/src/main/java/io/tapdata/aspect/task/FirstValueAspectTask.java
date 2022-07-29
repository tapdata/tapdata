package io.tapdata.aspect.task;

import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.simplify.pretty.FirstValueClassHandlers;

import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/29 19:21 Create
 */
public abstract class FirstValueAspectTask extends AspectTask {
    protected final FirstValueClassHandlers<Aspect, Void> observerHandlers = FirstValueClassHandlers.ins();
    protected final FirstValueClassHandlers<Aspect, AspectInterceptResult> interceptHandlers = FirstValueClassHandlers.ins();

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
