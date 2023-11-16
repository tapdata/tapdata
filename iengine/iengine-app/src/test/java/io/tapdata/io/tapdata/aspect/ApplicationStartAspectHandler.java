package io.tapdata.io.tapdata.aspect;

import io.tapdata.aspect.ApplicationStartAspect;
import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;

@AspectObserverClass(ApplicationStartAspect.class)
public class ApplicationStartAspectHandler implements AspectObserver<ApplicationStartAspect> {
	@Override
	public void observe(ApplicationStartAspect aspect) {

	}
}
