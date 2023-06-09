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
			describeCN = "创建精确一次写入缓存表时，生成目标数据库类型错误",
			describe = "When creating an exactly-once cache table, an error occur when generate target database types"
	)
	String TARGET_TYPES_GENERATOR_FAILED = "22002";

	@TapExCode(
			describeCN = "记录精确一次缓存表失败，将会回滚整批写入数据",
			describe = "Record exactly one cache table failure, and the entire batch of written data will be rolled back"
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
			describeCN = "写入精确一次缓存表时，时间戳为空",
			describe = "When writing to the exactly-once cache table, the timestamp is empty"
	)
	String WRITE_CACHE_FAILED_TIMESTAMP_IS_NULL = "22006";

	@TapExCode(
			describeCN = "检查精确一次缓存表失败",
			describe = "Failed to check exactly-once cache table"
	)
	String CHECK_CACHE_FAILED = "22007";
	@TapExCode(
			describeCN = "增量事件的精确一次唯一ID为空",
			describe = "The exactly-once unique ID of the incremental event is empty"
	)
	String EXACTLY_ONCE_ID_IS_BLANK = "22008";
}
