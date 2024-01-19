package io.tapdata.aspect;

import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;

@AspectObserverClass(ApplicationStartAspect.class)
public class ApplicationStartAspectHandler implements AspectObserver<ApplicationStartAspect> {
	@Override
	public void observe(ApplicationStartAspect aspect) {

	}
}
