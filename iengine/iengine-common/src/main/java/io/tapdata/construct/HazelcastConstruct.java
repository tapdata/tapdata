package io.tapdata.construct;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-11-23 17:05
 **/
public interface HazelcastConstruct<T> {

	default int insert(T data) throws Exception {
		throw new UnsupportedOperationException();
	}

	default int insert(String key, T data) throws Exception {
		throw new UnsupportedOperationException();
	}

	default int update(String key, T data) throws Exception {
		throw new UnsupportedOperationException();
	}

	default int upsert(String key, T data) throws Exception {
		throw new UnsupportedOperationException();
	}

	default int delete(String key) throws Exception {
		throw new UnsupportedOperationException();
	}

	default void clear() throws Exception {
		throw new UnsupportedOperationException();
	}

	default void destroy() throws Exception {
	}

	default boolean exists(String key) throws Exception {
		throw new UnsupportedOperationException();
	}

	default T find(String key) throws Exception {
		throw new UnsupportedOperationException();
	}

	/**
	 * For ringbuffer
	 *
	 * @param timestamp
	 * @return
	 * @throws Exception
	 */
	default long findSequence(long timestamp) throws Exception {
		throw new UnsupportedOperationException();
	}

	default ConstructIterator<T> find() throws Exception {
		throw new UnsupportedOperationException();
	}

	default ConstructIterator<T> find(Map<String, Object> filter) throws Exception {
		throw new UnsupportedOperationException();
	}

	default boolean isEmpty() {
		throw new UnsupportedOperationException();
	}

	default String getType() {
		throw new UnsupportedOperationException();
	}

	default String getName() {
		throw new UnsupportedOperationException();
	}
}
