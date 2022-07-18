package io.tapdata.aspect;

import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;

import java.util.concurrent.Callable;

public class AspectUtils {
	private static final AspectManager aspectManager = InstanceFactory.instance(AspectManager.class);
	private static final String TAG = AspectUtils.class.getSimpleName();

	static {
		if (aspectManager == null)
			TapLogger.error(TAG, "AspectManager is null, no aspects can be reported, the features implemented by modules won't work. ");
	}

	/**
	 * Execute Aspect for both interceptors and observers.
	 * This method is recommended, best efficiency
	 *
	 * @param aspectClass Aspect class.
	 * @param aspectCallable only create the Aspect when there is any interceptor or observer exists.
	 * @return
	 * @param <T>
	 */
	public static <T extends Aspect> AspectInterceptResult executeAspect(Class<T> aspectClass, Callable<T> aspectCallable) {
		if(aspectManager != null)
			return aspectManager.executeAspect(aspectClass, aspectCallable);
		return null;
	}

	/**
	 * Execute Aspect for both interceptors and observers.
	 *
	 * @param aspect
	 * @return
	 * @param <T>
	 */
	public static <T extends Aspect> AspectInterceptResult executeAspect(T aspect) {
		if(aspectManager != null)
			return aspectManager.executeAspect(aspect);
		return null;
	}
}
