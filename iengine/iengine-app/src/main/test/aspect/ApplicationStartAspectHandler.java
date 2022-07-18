package aspect;

import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;
import io.tapdata.flow.engine.V2.aspect.ApplicationStartAspect;

@AspectObserverClass(ApplicationStartAspect.class)
public class ApplicationStartAspectHandler implements AspectObserver<ApplicationStartAspect> {
	@Override
	public void observe(ApplicationStartAspect aspect) {

	}
}
