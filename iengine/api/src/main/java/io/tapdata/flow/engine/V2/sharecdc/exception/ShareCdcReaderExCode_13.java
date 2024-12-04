package io.tapdata.flow.engine.V2.sharecdc.exception;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-23 11:11
 **/
@TapExClass(code = 13, module = "Share CDC Reader", prefix = "SCR", describe = "Read share cdc log data")
public interface ShareCdcReaderExCode_13 {
	@TapExCode
	String UNKNOWN_ERROR = "13001";
	@TapExCode(
			describe = "Unable to get correct operation type from log data",
			dynamicDescription = "The current log operation type is: {}",
			describeCN = "无法从日志数据获取正确的操作类型",
			dynamicDescriptionCN = "当前日志的操作类型为：{}"
	)
	String GET_OP_TYPE_ERROR = "13002";
	@TapExCode(
			describe = "Update data log event, the after data is empty or does not exist",
			describeCN = "更新数据日志事件，更新后数据是空值或不存在",
			dynamicDescription = "Log content: {}",
			dynamicDescriptionCN = "日志内容：{}"
	)
	String UPDATE_MISSING_AFTER = "13003";
	@TapExCode(
			describe = "In the log event, the source table name is empty or does not exist",
			describeCN = "日志事件中，源表名为空或不存在",
			dynamicDescription = "Log content: {}",
			dynamicDescriptionCN = "日志内容：{}"
	)
	String MISSING_TABLE_NAME = "13004";
	@TapExCode(
			describe = "DDL log event, the specific content of the operation is empty or does not exist",
			describeCN = "元数据操作日志事件，操作的具体内容为空或不存在",
			dynamicDescription = "Log content: {}",
			dynamicDescriptionCN = "日志内容：{}"
	)
	String DDL_MISSING_BYTES = "13005";
	@TapExCode(
			describe = "log event is null",
			describeCN = "日志事件为空"
	)
	String LOG_DATA_NULL = "13006";
	@TapExCode(
			describe = "In the log event, the operation type attribute is missing",
			describeCN = "日志事件中，缺失操作类型属性",
			dynamicDescription = "Log content: {}",
			dynamicDescriptionCN = "日志内容：{}"
	)
	String MISSING_OPERATION = "13007";
	@TapExCode(
			describe = "In the log event, the breakpoint information is missing",
			describeCN = "日志事件中，缺失断点信息"
	)
	String MISSING_OFFSET = "13008";
	@TapExCode(
			describe = "Delete data log event, the before data is empty or does not exist",
			describeCN = "删除数据日志事件，删除数据是空值或不存在",
			dynamicDescription = "Log content: {}",
			dynamicDescriptionCN = "日志内容：{}"
	)
	String DELETE_MISSING_BEFORE = "13009";
	@TapExCode(
			describe = "Insert data log event, the after data is empty or does not exist",
			describeCN = "写入数据日志事件，写入数据是空值或不存在",
			dynamicDescription = "Log content: {}",
			dynamicDescriptionCN = "日志内容：{}"
	)
	String INSERT_MISSING_AFTER = "13010";
	@TapExCode(
			recoverable = true
	)
	String FIND_NEXT_FAILED = "13011";
}
