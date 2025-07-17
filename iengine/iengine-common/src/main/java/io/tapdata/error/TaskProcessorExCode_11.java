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
	@TapExCode(
			describe = "In the task startup phase, it fails to create the corresponding node object based on the node configuration information of the task. This error will occur",
			describeCN = "任务启动阶段，基于任务的节点配置信息创建对应的节点对象时失败。则会出现此错误"
	)
	String CREATE_PROCESSOR_FAILED = "11003";


	String JAVA_SCRIPT_PROCESS_FAILED = "11004";


	String PYTHON_PROCESS_FAILED = "11005";

	@TapExCode(
			describe = "When the task starts, the engine initializes the custom node based on the custom node template created. Failed to get custom template information when initializing a custom node",
			describeCN = "任务启动时，引擎会根据创建的自定义节点模版初始化自定义节点。当初始化时自定义节点时获取自定义的模版信息失败",
			dynamicDescription = "The template for the custom node is removed before the task is started",
			dynamicDescriptionCN = "自定义节点的模版在任务启动前被删除",
			solution = "Click the Custom node of the Advanced functions sub-menu on the left side of the system to view the currently created custom template. If the custom template is deleted, you'll need to recreate the custom template and reconfigure the task",
			solutionCN = "点击系统左侧菜单高级功能的子菜单自定义节点查看当前创建的自定义模版。若自定义模版被删除，则需要重新创建自定义模版并重新配置任务"
	)
	String CUSTOM_NODE_NOT_FOUND = "11006";

	@TapExCode
	String SCRIPT_INIT_FAILED = "11007";

	@TapExCode(
			describe = "Failed to initialize a processing node according to its configuration information",
			describeCN = "根据处理节点的配置信息初始化处理节点失败",
			dynamicDescription = "The name of the failed node is {} and the node type is {}.",
			dynamicDescriptionCN = "初始化失败的节点名称为{}，节点类型为{}"
	)
	String INIT_DATA_FLOW_PROCESSOR_FAILED = "11008";

	@TapExCode(
			describe = "Cannot get metadata cause qualified name is empty",
			describeCN = "目标节点或处理节点识别到有DDL事件时，会根据DDL事件更新内存中维护的表模型。根据DDL事件中的表名获取不到模型的唯一编码",
			dynamicDescription = "When getting a unique model code for the table name {}, the model code is empty",
			dynamicDescriptionCN = "获取表名为 {} 的唯一模型编码时，模型编码为空"
	)
	String UPDATE_TAP_TABLE_QUALIFIED_NAME_EMPTY = "11009";
	@TapExCode(
			describe = "When transmitting data to the next node, the Outbox is detected to be empty, so the data cannot be transmitted to the next node",
			describeCN = "往一下个节点传输数据时，检测出Outbox为空，导致数据无法往下一节点传输"
	)
	String OUTBOX_IS_NULL_WHEN_OFFER = "11010";
	@TapExCode(
			describe = "Cannot get node's metadata by node id and table name",
			describeCN = "当更新节点模型时，无法根据节点id和表名获取对应节点的模型"
	)
	String GET_NODE_METADATA_BY_TABLE_NAME_FAILED = "11011";
	@TapExCode(
			describe = "Updating the memory DAG in the node fails when the processing node or the target node recognizes that there is a DDL event",
			describeCN = "当处理节点或目标节点识别到有DDL事件时，更新节点中的内存DAG失败"
	)
	String UPDATE_MEMORY_DAG_FAILED = "11012";
	@TapExCode(
			describe = "Updating the memory node configuration fails when a DDL event is recognized by the processing node or the destination node",
			describeCN = "当处理节点或目标节点识别到有DDL事件时，更新内存节点配置失败"
	)
	String UPDATE_MEMORY_NODE_CONFIG_FAILED = "11013";
	@TapExCode(
			describe = "Updating the memory model fails when a DDL event is recognized by the processing node or the target node",
			describeCN = "当处理节点或目标节点识别到有DDL事件时，更新内存模型失败"
	)
	String UPDATE_MEMORY_TAP_TABLE_FAILED = "11014";
	@TapExCode(
			describe = "The result metadata after transform is invalid, id is null",
			describeCN = "推演后的模型id是空"
	)
	String TRANSFORM_METADATA_ID_NULL = "11015";

	@TapExCode(
			describe = "Failed to enable JET mission status monitor",
			describeCN = "开启JET任务状态监视器失败"
	)
	String START_JET_JOB_STATUS_MONITOR_FAILED = "11016";
	@TapExCode(
			describe = "Init base node failed, configuration center is null"
	)
	String INIT_CONFIGURATION_CENTER_CANNOT_BE_NULL = "11017";
	@TapExCode(
			describe = "Init setting service failed, because client mongo operator is null",
			describeCN = "由于ClientMongoOperator为空，导致初始化SettingService 失败"
	)
	String INIT_SETTING_SERVICE_FAILED_CLIENT_MONGO_OPERATOR_IS_NULL = "11018";
	@TapExCode
	String INIT_SCRIPT_ENGINE_FAILED = "11019";
	@TapExCode(
			describe = "The task cannot be stopped in an error state because it failed to handle an exception",
			describeCN = "处理异常时失败，导致任务不能以错误状态停止",
			dynamicDescription = "Failed while handling the {} exception",
			dynamicDescriptionCN = "处理{}异常时出现失败"
	)
	String ERROR_HANDLE_FAILED = "11020";

	@TapExCode(
			describe = "When a DDL event is encountered, the task stops with an error",
			describeCN = "遇到DDL事件时，任务停止报错",
			dynamicDescription = "Since the DDL synchronization in the advanced configuration of the source node of the task is configured as \"Stop Task Upon Encountering DDL\", the source node will report error to stop the task when it recognizes the DDL event",
			dynamicDescriptionCN = "由于任务的源节点的高级配置中的DDL同步配置为 “遇到DDL时任务报错停止” ，所以源节点在识别到DDL事件时，任务会报错停止",
			solution = "This error can be avoided by setting other DDL synchronization configurations, such as automatically ignoring all DDL events or synchronizing DDL events",
			solutionCN = "可以通过设置其他DDL同步配置来避免此报错，如自动忽略所有DDL事件或同步DDL事件"
	)
	String ENCOUNTERED_DDL_EVENT_REPORT_ERROR = "11021";

	@TapExCode(
			describe = "This exception occurs when a task encounters an unrecognized DDL event",
			describeCN = "任务遇到无法识别的DDL事件时，会出现此异常",
			dynamicDescription = "This error occurs when the source database uses DDL statements to do DDL operations on the database that the system has not yet recognized",
			dynamicDescriptionCN = "当源端数据库使用了系统还未能识别的DDL语句在数据库做DDL操作时，则会出现此报错",
			solution = "The \"Ignore all DDL\" option can be selected in the source node DDL synchronization configuration, and the task needs to be restarted after manually performing the same DDL operation on the target database to ensure the source and target models",
			solutionCN = "可在源节点DDL同步配置选择“忽略所有DDL”选项，并且需要在目标数据库手动执行相同的DDL操作以保证源端与目标端模型后重新启动任务"
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
	@TapExCode(
			describe = "Tasks cannot be started properly when switching from shared mining mode to normal mode",
			describeCN = "任务由使用共享挖掘模式切换为普通模式时无法正常启动",
			dynamicDescription = "When the task switches from shared mining mode to normal task mode, the task can not be started normally due to the loss of task breakpoint information",
			dynamicDescriptionCN = "任务由共享挖掘模式切换为普通任务模式时，由于任务断点信息丢失，导致任务无法正常启动",
			solution = "Reset this task and run it",
			solutionCN = "将此任务重置后重新启动"
	)
	String SHARE_CDC_SWITCH_TO_NORMAL_TASK_FAILED = "11026";
	@TapExCode(
			describe = "When the incremental task is started, since the start time of the incremental acquisition is empty. The task could not be started",
			describeCN = "启动增量任务时，由于增量采集开始时刻为空。导致任务无法启动",
			solution = "Restart the task after setting the \"incremental acquisition start time\" as \"at this moment\" or \"user-specified time\" in the task",
			solutionCN = "在任务中设置“增量采集开始时刻”为“此刻”或“用户指定时间”后重新启动任务"
	)
	String INIT_STREAM_OFFSET_SYNC_POINT_TYPE_IS_EMPTY = "11027";

	@TapExCode(
			describe = "Source node clone batch offset failed when wrap tapdata event",
			describeCN = "在封装tapdata事件时，源节点克隆批量断点信息失败"
	)
	String SOURCE_CLONE_BATCH_OFFSET_FAILED = "11028";

	@TapExCode(
			describe = "Source node can not support countByPartitionFilterFunction",
			describeCN = "源节点在不支持过滤count的方法"
	)
	String SOURCE_NOT_SUPPORT_COUNT_BY_PARTITION_FILTER_FUNCTION = "11029";


	@TapExCode(
			describe = "Data convert failed",
			describeCN = "数据转换失败"
	)
	String DATA_COVERT_FAILED = "11030";

	@TapExCode(
			describe = "When starting an incremental task, parsing the incremental Offset format failed",
			describeCN = "启动增量任务时，解析增量Offset格式失败",
			solution = "Please refer to the corresponding data source document and correctly fill in the data source Offset format.",
			solutionCN = "请您参考对应数据源文档，正确填写数据源Offset格式"
	)
	String INIT_STREAM_OFFSET_FAILED = "11031";
}
