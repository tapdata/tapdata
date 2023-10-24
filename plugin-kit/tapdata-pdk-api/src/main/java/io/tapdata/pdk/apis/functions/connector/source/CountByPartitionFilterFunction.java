package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

/**
 * filter为空时， 查所有数据。
 * filter里只用关心里面的operators， 作为条件count数据
 */
public interface CountByPartitionFilterFunction extends TapConnectorFunction {
    long countByPartitionFilter(TapConnectorContext connectorContext, TapTable table, TapAdvanceFilter filter) throws Throwable;
}