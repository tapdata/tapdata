package com.tapdata.tm.autoinspect.connector;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/9 10:43 Create
 */
public interface IDataCursor<T> extends AutoCloseable {

    T next() throws Exception;
}
