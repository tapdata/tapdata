package io.tapdata.construct;

import java.util.Iterator;

/**
 * @author samuel
 * @Description
 * @create 2022-02-17 20:30
 **/
public interface ConstructIterator<E> extends Iterator<E> {

	E tryNext();

	E peek();

	long getSequence();
}
