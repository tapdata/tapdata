package io.tapdata.entity.aspect;

/**
 * AspectInterceptor will be shared for multiple Aspect instances.
 *
 * @param <T>
 */
public interface AspectInterceptor<T extends Aspect> {
    /**
     * Intercept an Aspect.
     *
     * @param aspect
     * @return if null, then will not intercept,
     *          return AspectInterceptResult with intercepted = true, then the aspect will be intercepted,
     *          the interceptors and observers behind will be ignored.
     */
    AspectInterceptResult intercept(T aspect);
}
