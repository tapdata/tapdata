package com.tapdata.cache;

import java.util.List;
import java.util.Map;

/**
 * 从数据源获取数据接口
 */
public interface IDataSourceRowsGetter {

	List<Map<String, Object>> getRows(Object[] keys);

	default void close() {
	}

}
