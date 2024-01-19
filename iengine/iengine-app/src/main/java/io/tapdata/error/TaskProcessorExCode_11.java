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
}
