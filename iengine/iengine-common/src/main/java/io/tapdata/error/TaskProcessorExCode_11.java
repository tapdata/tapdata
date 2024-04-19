package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 11, module = "Task Processor", prefix = "TPR", describe = "Task running processor, include read, write, process node")
public interface TaskProcessorExCode_11 {
	@TapExCode
	String UNKNOWN_ERROR = "11001";

	@TapExCode(
			describe = "Log mining task, failed to write a single log data into the external persistence cache.\n" +
					"Reason\n1. The database configured by the external cache cannot provide normal services",
			solution = "1. On the mining task monitoring interface, find the external cache configuration. Check whether the configuration database is normal",
			describeCN = "日志挖掘任务，将单条日志数据写入外部持久缓存失败。\n" +
					"原因\n1. 外部缓存配置的数据库无法提供正常的服务",
			solutionCN = "1. 在挖掘任务监控界面，找到外部缓存的配置。检查该配置的数据库是否正常"
	)
	String WRITE_ONE_SHARE_LOG_FAILED = "11002";

	@TapExCode
	String CREATE_PROCESSOR_FAILED = "11003";

	@TapExCode(
			describe = "JavaScript node processing error.\n" +
					"Reason\n1. The JavaScript code has an error",
			solution = "1. Review the code and fix errors",
			describeCN = "JavaScript节点处理出错。\n" +
					"原因\n1. JavaScript代码存在错误",
			solutionCN = "1. 检查代码并修正错误"
	)
	String JAVA_SCRIPT_PROCESS_FAILED = "11004";

	@TapExCode(
			describe = "Python node processing error.\n" +
					"Reason\n1. The Python code has an error",
			solution = "1. Review the code and fix errors",
			describeCN = "Python节点处理出错。\n" +
					"原因\n1. Python代码存在错误",
			solutionCN = "1. 检查代码并修正错误"
	)
	String PYTHON_PROCESS_FAILED = "11005";

	@TapExCode
	String CUSTOM_NODE_NOT_FOUND = "11006";

	@TapExCode
	String SCRIPT_INIT_FAILED = "11007";

	@TapExCode
	String INIT_DATA_FLOW_PROCESSOR_FAILED = "11008";

	@TapExCode(
			describe = "Cannot get metadata cause qualified name is empty",
			describeCN = "模型的唯一编码为空，无法获取模型"
	)
	String UPDATE_TAP_TABLE_QUALIFIED_NAME_EMPTY = "11009";
	@TapExCode(
			describe = "Offer data to next node failed cause outbox is null"
	)
	String OUTBOX_IS_NULL_WHEN_OFFER = "11010";
	@TapExCode(
			describe = "Cannot get node's metadata by node id and table name",
			describeCN = "无法根据节点id和表名获取对应节点的模型"
	)
	String GET_NODE_METADATA_BY_TABLE_NAME_FAILED = "11011";
	@TapExCode(
			describe = "Update memory dag failed",
			describeCN = "更新内存DAG失败"
	)
	String UPDATE_MEMORY_DAG_FAILED = "11012";
	@TapExCode(
			describe = "Update memory node config failed",
			describeCN = "更新内存节点配置失败"
	)
	String UPDATE_MEMORY_NODE_CONFIG_FAILED = "11013";
	@TapExCode(
			describe = "Update memory schema failed",
			describeCN = "更新内存模型失败"
	)
	String UPDATE_MEMORY_TAP_TABLE_FAILED = "11014";
	@TapExCode(
			describe = "The result metadata after transform is invalid, id is null",
			describeCN = "推演后的模型id是空"
	)
	String TRANSFORM_METADATA_ID_NULL = "11015";

	@TapExCode
	String START_JET_JOB_STATUS_MONITOR_FAILED = "11016";
	@TapExCode(
			describe = "Init base node failed, configuration center is null"
	)
	String INIT_CONFIGURATION_CENTER_CANNOT_BE_NULL = "11017";
	@TapExCode(
			describe = "Init setting service failed, because client mongo operator is null"
	)
	String INIT_SETTING_SERVICE_FAILED_CLIENT_MONGO_OPERATOR_IS_NULL = "11018";
	@TapExCode
	String INIT_SCRIPT_ENGINE_FAILED = "11019";
	@TapExCode
	String ERROR_HANDLE_FAILED = "11020";

	@TapExCode(
			describe = "Unable to handle DDL events.\n" +
					"Reason\n1. DDL synchronization on the source node is configured so that the task will report an error and stop when encountering a DDL event.",
			solution = "1. Optional other synchronization configurations, such as ignoring all DDL events or synchronizing DDL events.",
			describeCN = "无法处理DDL事件\n" +
					"原因\n1. 在源节点的DDL同步配置为遇到DDL事件任务将报错停止",
			solutionCN = "1. 可选其他同步配置，如忽略所有DDL事件或者同步DDL事件"
	)
	String ENCOUNTERED_DDL_EVENT_REPORT_ERROR = "11021";

	@TapExCode(
			describe = "This DDL event is not recognized.\n" +
					"Reason\n1. Tapdata does not currently support this type of DDL event",
			solution = "1. You can choose to ignore all DDL events in the source node DDL synchronization configuration to prevent the task from reporting an error and stopping.",
			describeCN = "无法识别此DDL事件\n" +
					"原因\n1. Tapdata 暂不支持此类型的DDL事件",
			solutionCN = "1. 可在源节点DDL同步配置选择忽略所有DDL事件，防止任务报错停止"
	)
	String UNABLE_TO_SYNCHRONIZE_DDL_EVENT = "11022";

	@TapExCode(
			describe = "Unable to read incremental breakpoint information because an unknown task type was encountered",
			describeCN = "无法读取增量断点信息，因为遇到了未知的任务类型"
	)
	String READ_STREAM_OFFSET_UNKNOWN_TASK_TYPE = "11023";
	@TapExCode(
			describe = "Unable to initialize incremental breakpoint information because an unrecognized start time type was encountered",
			describeCN = "无法初始化增量断点信息，因为遇到了无法识别的起始时间类型"
	)
	String INIT_STREAM_OFFSET_UNKNOWN_POINT_TYPE = "11024";
	@TapExCode(
			describe = "Unable to initialize incremental breakpoint information because start time is empty",
			describeCN = "无法初始化增量断点信息，因为起始时间为空值"
	)
	String INIT_STREAM_OFFSET_EMPTY_START_TIME = "11025";
	@TapExCode
	String SHARE_CDC_SWITCH_TO_NORMAL_TASK_FAILED = "11026";
	@TapExCode(
			describe = "Run cdc task failed, sync point type cannot be empty",
			describeCN = "运行cdc任务失败，同步起始点类型不能为空"
	)
	String INIT_STREAM_OFFSET_SYNC_POINT_TYPE_IS_EMPTY = "11027";

	@TapExCode(
			describe = "Source node clone batch offset failed when wrap tapdata event",
			describeCN = "在封装tapdata事件时，源节点克隆批量断点信息失败"
	)
	String SOURCE_CLONE_BATCH_OFFSET_FAILED = "11028";
}
