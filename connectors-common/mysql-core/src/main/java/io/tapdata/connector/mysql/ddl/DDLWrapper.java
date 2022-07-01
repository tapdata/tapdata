package io.tapdata.connector.mysql.ddl;

import io.tapdata.entity.event.ddl.TapDDLEvent;

import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-06-29 20:51
 **/
public interface DDLWrapper<T> {
	void wrap(T ddl, Consumer<TapDDLEvent> consumer) throws Throwable;
}
