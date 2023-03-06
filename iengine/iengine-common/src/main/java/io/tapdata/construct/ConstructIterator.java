package io.tapdata.construct;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * @author samuel
 * @Description
 * @create 2022-02-17 20:30
 **/
public interface ConstructIterator<E> extends Iterator<E> {

	E tryNext();

	E peek();

	E peek(long timeout, TimeUnit timeUnit);

	long getSequence();

	default List<E> tryNextMany(int maxCount, Predicate<Void> stop) {
		throw new UnsupportedOperationException();
	}
}
