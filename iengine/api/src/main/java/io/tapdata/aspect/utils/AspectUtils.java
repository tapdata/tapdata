package io.tapdata.aspect.utils;

import io.tapdata.aspect.DataFunctionAspect;
import io.tapdata.aspect.ProcessorFunctionAspect;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.aspect.AspectManager;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
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

	public static <T extends DataFunctionAspect<T>> AspectInterceptResult executeDataFuncAspect(Class<T> aspectClass, Callable<T> aspectCallable, CommonUtils.AnyErrorConsumer<T> consumer) {
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
					return aspectManager.executeAspect(aspect);
				} catch(Throwable throwable) {
					aspect.throwable(throwable).state(DataFunctionAspect.STATE_END);
					aspectManager.executeAspect(aspect);
					throw new RuntimeException(throwable);
				}
			} else {
				return interceptResult;
			}
		} else {
			try {
				consumer.accept(null);
			} catch (Throwable e) {
				throw new RuntimeException(e);
			}
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
					return aspectManager.executeAspect(aspect);
				} catch(Throwable throwable) {
					aspect.throwable(throwable).state(DataFunctionAspect.STATE_END);
					aspectManager.executeAspect(aspect);
					throw throwable;
				}
			} else {
				return interceptResult;
			}
		} else {
			consumer.accept(null);
		}
		return null;
	}

	public static <T>  void accept(List<Consumer<T>> consumers, T t) {
		if(consumers != null) {
			for(Consumer<T> consumer : consumers) {
				CommonUtils.ignoreAnyError(() -> consumer.accept(t), TAG);
			}
		}
	}

	public static <T, P>  void accept(List<BiConsumer<T, P>> consumers, T t, P p) {
		if(consumers != null) {
			for(BiConsumer<T, P> consumer : consumers) {
				CommonUtils.ignoreAnyError(() -> consumer.accept(t, p), TAG);
			}
		}
	}
}
