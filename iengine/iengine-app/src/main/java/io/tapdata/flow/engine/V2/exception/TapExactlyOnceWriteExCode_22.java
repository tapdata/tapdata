package io.tapdata.flow.engine.V2.exception;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-05-15 18:57
 **/
@TapExClass(code = 22, module = "Exactly Once Write", prefix = "EOW")
public interface TapExactlyOnceWriteExCode_22 {

	@TapExCode
	String UNKNOWN_ERROR = "22001";

	@TapExCode(
			describe = "When the target initiates incremental exact-once writes, the engine generates a exact-once cache table (which records incremental events that have been written to the target) and creates it in the target database. Failed to generate an accurate cache table structure according to the target database",
			describeCN = "当目标开启增量精确一次写入时，引擎会生成一张精准一次缓存表（用于记录已经写入目标的增量事件）并在目标数据库创建。根据目标数据库生成精准一次缓存表结构失败",
			dynamicDescription = "Failed to generate the target database table based on the target database type with the fields: {}",
			dynamicDescriptionCN = "根据目标数据库类型生成目标数据库表时失败，表的字段为: {}"
	)
	String TARGET_TYPES_GENERATOR_FAILED = "22002";

	@TapExCode(
			describe = "When the target initiates incremental exact-once write, each incremental event generates a corresponding precision-once record event written to the exact-once cache table, and they are written to the database in the same transaction. Failed to write an accurate record event to the target database. And rolls back the whole batch of events",
			describeCN = "当目标开启增量精确一次写入时，每条增量事件会生成对应的一条精准一次写的记录事件写入精确一次缓存表，并且它们是在同一个事务写入数据库。精准一次记录事件写入目标数据库时失败，并且会回滚整批事件",
			dynamicDescription = "1. The target database is not available \n" +
					"2. The cache table (named _TAP_EXACTLY_ONCE_CACHE) is deleted exactly once\n",
			dynamicDescriptionCN = "1. 目标数据库无法提供正常服务\n" +
					"2. 精确一次缓存表(表名为_TAP_EXACTLY_ONCE_CACHE)被删除\n",
			solution = "1. Check whether it is the target database problem according to the cause of failure. If there is an outage, you need to restore the target database and restart the synchronization task\n" +
					"2. Check the database's exact one-time cache table (the table name is _TAP_EXACTLY_ONCE_CACHE) to see if it is deleted by mistake. If it is deleted by mistake, restart the synchronization task",
			solutionCN = "1. 根据失败原因排查是否为目标数据库问题。如是否为目标数据库宕机，如出现宕机，则需要恢复目标数据库并重新启动同步任务\n" +
					"2. 查看数据库精确一次缓存表(表名为_TAP_EXACTLY_ONCE_CACHE)是否被误删，如被误删，则需要重新启动同步任务"
	)
	String WRITE_CACHE_FAILED = "22003";

	@TapExCode(
			describeCN = "使用json序列化增量断点信息失败",
			describe = "Failed to use json serialization incremental breakpoint information"
	)
	String STREAM_OFFSET_TO_JSON_FAILED = "22004";

	@TapExCode(
			describeCN = "压缩json字符串失败",
			describe = "Failed to compress json string"
	)
	String COMPRESS_OFFSET_FAILED = "22005";

	@TapExCode(
			describe = "When the target enables incremental precision-one-shot write, it takes the time when the source library increment event was generated in the source library as one of the required fields for the precision-one-shot record event. The timestamp is detected to be empty",
			describeCN = "当目标开启增量精确一次写入时，会取源库增量事件在源库生成时的时间作为精确一次写的记录事件的其中一个必要字段。检测出时间戳为空",
			dynamicDescription = "The source library's incremental event table is named: {}, and the exact primary ID of the incremental event is: {}",
			dynamicDescriptionCN = "源库的增量事件的表名为：{},增量事件的精确一次ID为：{}"
	)
	String WRITE_CACHE_FAILED_TIMESTAMP_IS_NULL = "22006";

	@TapExCode(
			describe = "When the target initiates the incremental exact write, it will query the exact cache table for whether the incremental event has been written according to the record event of the exact write before the incremental write. Failed to query the cache table exactly once",
			describeCN = "当目标开启增量精确一次写入时，在增量写入前，会根据精确一次写的记录事件去精确一次缓存表中查询是否增量事件已经被写入。查询精确一次缓存表时失败",
			dynamicDescription = "The query for the exact cache table failed, the query condition is: {}, the exact table name is: {}",
			dynamicDescriptionCN = "查询精确一次缓存表失败，查询条件为：{}，查询精确一次的表名为：{}"
	)
	String CHECK_CACHE_FAILED = "22007";
	@TapExCode(
			describe = "When the target enables incremental exact write, it takes the exact unique ID of the source library incremental event (which consists of different values depending on the database type: Oracle uses SCN as the exact unique ID) as one of the fields in the record event of the exact write (which identifies whether the event has been written to the target library). The exact unique ID is detected to be empty",
			describeCN = "当目标开启增量精确一次写入时，会取源库增量事件的精确唯一ID（根据数据库类型不同由不同的值组成如：Oracle使用SCN作为精确唯一ID）作为精确一次写的记录事件的其中一个字段（用于标识事件是否已经写入到目标库中）。检测出精确唯一ID为空",
			dynamicDescription = "The source cdc event tableId is {}",
			dynamicDescriptionCN = "源库的增量事件表名为：{}"
	)
	String EXACTLY_ONCE_ID_IS_BLANK = "22008";
}
