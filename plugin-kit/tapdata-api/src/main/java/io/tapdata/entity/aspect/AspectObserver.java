package io.tapdata.entity.aspect;

/**
 * AspectObserver will be shared for multiple Aspect instances.
 *
 * @param <T>
 */
public interface AspectObserver<T extends Aspect> {
    /**
     * Observe an Aspect
     *
     * @param aspect
     */
    void observe(T aspect);
}
