package io.tapdata.aspect.utils;

import io.tapdata.aspect.DataFunctionAspect;
import io.tapdata.aspect.ProcessorFunctionAspect;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

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

	public static <T extends DataFunctionAspect<T>> AspectInterceptResult executeDataFuncAspect(Class<T> aspectClass, Callable<T> aspectCallable, Consumer<T> consumer) {
		T aspect = null;
		if(aspectManager != null && aspectManager.hasInterceptorOrObserver(aspectClass)) {
			try {
				aspect = aspectCallable.call();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		if(aspect != null) {
			AspectInterceptResult interceptResult = aspectManager.executeAspect(aspect);
			if(interceptResult == null || !interceptResult.isIntercepted()) {
				try {
					consumer.accept(aspect);
					aspect.state(DataFunctionAspect.STATE_END);
					aspectManager.executeAspect(aspect);
				} catch(Throwable throwable) {
					aspect.throwable(throwable).state(DataFunctionAspect.STATE_END);
					aspectManager.executeAspect(aspect);
				}
			} else {
				return interceptResult;
			}
		} else {
			consumer.accept(null);
		}

		return null;
	}

	public static <T extends ProcessorFunctionAspect<T>> AspectInterceptResult executeProcessorFuncAspect(Class<T> aspectClass, Callable<T> aspectCallable, Consumer<T> consumer) {
		T aspect = null;
		if(aspectManager != null && aspectManager.hasInterceptorOrObserver(aspectClass)) {
			try {
				aspect = aspectCallable.call();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		if(aspect != null) {
			AspectInterceptResult interceptResult = aspectManager.executeAspect(aspect);
			if(interceptResult == null || !interceptResult.isIntercepted()) {
				try {
					consumer.accept(aspect);
					aspect.state(DataFunctionAspect.STATE_END);
					aspectManager.executeAspect(aspect);
				} catch(Throwable throwable) {
					aspect.throwable(throwable).state(DataFunctionAspect.STATE_END);
					aspectManager.executeAspect(aspect);
				}
			} else {
				return interceptResult;
			}
		} else {
			consumer.accept(null);
		}
		return null;
	}
}
