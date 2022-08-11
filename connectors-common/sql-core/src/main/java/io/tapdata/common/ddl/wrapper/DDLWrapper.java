package io.tapdata.common.ddl.wrapper;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.entity.Capability;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-06-29 20:51
 **/
public interface DDLWrapper<T> {
	void init(DDLWrapperConfig config);
	List<Capability> getCapabilities();
	void wrap(T ddl, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) throws Throwable;
}
