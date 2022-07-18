package aspect;

import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;
import io.tapdata.aspect.ApplicationStartAspect;

@AspectObserverClass(ApplicationStartAspect.class)
public class ApplicationStartAspectHandler implements AspectObserver<ApplicationStartAspect> {
	@Override
	public void observe(ApplicationStartAspect aspect) {

	}
}
