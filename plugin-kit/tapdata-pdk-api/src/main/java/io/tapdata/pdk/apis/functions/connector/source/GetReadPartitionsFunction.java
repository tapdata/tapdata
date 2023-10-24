package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapConnectorFunction;

/**
 * @author aplomb
 */
public interface GetReadPartitionsFunction extends TapConnectorFunction {

	/**
	 * numberOfPartition可以为空， 默认16， partition数量 x maxRecordInPartition可能会小于总数，
	 此时， 一个partition的worker做完自己的同步之后， 再开始做剩余的Partition数据的同步
	 *
	 * existingPartitions是用于任务恢复执行时， 引擎会存储已经生成的ReadPartition列表， 交给此方法， 计算其余的ReadPartition
	 *
	 * maxRecordInPartition是一个ReadPartition的数据阈值
	 */
	void getReadPartitions(TapConnectorContext connectorContext, TapTable table, GetReadPartitionOptions options) throws Throwable;
}
