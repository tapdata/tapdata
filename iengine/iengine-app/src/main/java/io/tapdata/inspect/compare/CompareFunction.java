package io.tapdata.inspect.compare;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/8/13 7:23 下午
 * @description
 */
@FunctionalInterface
public interface CompareFunction<T, R> {
	R apply(T t1, T t2, String sourceId, String targetId);
}
