package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 15, module = "Task Target Processor", prefix = "TTP", describe = "Task target processor")
public interface TaskTargetProcessorExCode_15 {
	@TapExCode
	String UNKNOWN_ERROR = "15001";

	@TapExCode(
			describe = "The target model does not exist, there may be a problem with the model calculation",
			describeCN = "目标模型不存在，可能是模型推算出现问题"
	)
	String INIT_TARGET_TABLE_TAP_TABLE_NULL = "15002";

	@TapExCode(
			describe = "Failed to create target table index",
			describeCN = "创建目标表索引失败"
	)
	String CREATE_INDEX_FAILED = "15003";

	@TapExCode(
			describe = "Failed to drop target table",
			describeCN = "删除目标表失败"
	)
	String DROP_TABLE_FAILED = "15004";

	@TapExCode(
			describe = "Automatic creation of target table failed",
			describeCN = "自动创建目标表失败"
	)
	String CREATE_TABLE_FAILED = "15005";

	@TapExCode(
			describe = "Failed to clear target table data",
			describeCN = "清空目标表数据失败"
	)
	String CLEAR_TABLE_FAILED = "15006";

	@TapExCode(
			describe = "When writing data, the target table name cannot be obtained from the table name mapping relationship through the source table name",
			describeCN = "写入数据时，通过源端表名无法从表名映射关系中，获取到目标表名"
	)
	String WRITE_RECORD_GET_TARGET_TABLE_NAME_FAILED = "15007";
	@TapExCode(
			describe = "The data source does not support write operations",
			describeCN = "数据源不支持写入操作"
	)
	String WRITE_RECORD_PDK_NONSUPPORT = "15008";
	@TapExCode(
			describe = "Failed to perform the operation of adding a new field, and the target table model could not be obtained",
			describeCN = "执行新增字段操作时失败，无法获取到目标表模型"
	)
	String ADD_NEW_FIELD_GET_TAP_TABLE_FAILED = "15009";
	@TapExCode(
			describe = "The operation of adding a new field failed, and the description of the new field in the message body is empty",
			describeCN = "执行新增字段操作失败，消息体中，新增字段的描述为空"
	)
	String ADD_NEW_FIELD_IS_NULL = "15010";
	@TapExCode(
			describe = "Failed to perform add field operation",
			describeCN = "执行新增字段操作失败"
	)
	String ADD_NEW_FIELD_EXECUTE_FAILED = "15011";
	@TapExCode(
			describe = "Failed to modify the field name operation",
			describeCN = "执行修改字段名操作失败"
	)
	String ALTER_FIELD_NAME_EXECUTE_FAILED = "15012";
	@TapExCode(
			describe = "Failed to modify the field properties, and the target table model could not be obtained",
			describeCN = "执行修改字段属性失败，无法获取到目标表模型"
	)
	String ALTER_FIELD_ATTR_CANNOT_GET_TAP_TABLE = "15013";
	@TapExCode(
			describe = "Failed to modify field properties",
			describeCN = "执行修改字段属性失败"
	)
	String ALTER_FIELD_ATTR_EXECUTE_FAILED = "15014";
	@TapExCode(
			describe = "Failed to drop field",
			describeCN = "执行删除字段操作失败"
	)
	String DROP_FIELD_EXECUTE_FAILED = "15015";
	@TapExCode(
			describe = "Failed to create target table index, the target table schema does not exist",
			describeCN = "创建目标表索引失败，目标表模型不存在"
	)
	String CREATE_INDEX_TABLE_NOT_FOUND = "15016";
	@TapExCode(
			describe = "Failed to create target table index",
			describeCN = "创建目标表索引失败"
	)
	String CREATE_INDEX_EXECUTE_FAILED = "15017";
	@TapExCode(
			describe = "Failed to handle event",
			describeCN = "处理事件失败"
	)
	String HANDLE_EVENTS_FAILED = "15018";
}
