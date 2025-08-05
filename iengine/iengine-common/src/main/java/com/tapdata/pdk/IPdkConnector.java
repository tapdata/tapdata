package com.tapdata.pdk;

import io.tapdata.entity.schema.TapTable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Predicate;

/**
 * PDK 连接器，用于任务外调用 PDK 能力
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/27 11:44 Create
 */
public interface IPdkConnector extends AutoCloseable {

    void init() throws Exception;

    String getNodeId();

    TapTable getTapTable(String tableName);

    void eachAllTable(Predicate<TapTable> predicate);

    LinkedHashMap<String, Object> findOneByKeys(String tableName, LinkedHashMap<String, Object> keys, List<String> fields);

    String getDatabaseType();
}
