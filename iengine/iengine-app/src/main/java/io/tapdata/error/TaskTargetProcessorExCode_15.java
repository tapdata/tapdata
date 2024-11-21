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
			describe = "After the task starts, the engine will get the target table model pushed by the management side and create the table in the target database according to the target table model. When the target table model is not obtained, the engine will report this error",
			describeCN = "任务启动后，引擎会获取管理端推演出的目标表模型并根据目标表模型在目标数据库中创建表，获取不到目标表模型",
			dynamicDescription = "Failed to get table model with table name {}",
			dynamicDescriptionCN = "获取表名为 {} 的表模型失败",
			solution = "On the management side, reset the task and run the task again to see if the task can be started normally. If it cannot be started normally, you need to raise a work order to troubleshose the problem",
			solutionCN = "在管理端重置任务并重新运行任务，看任务能否正常启动，如不能正常启动则需要提工单排查问题"
	)
	String INIT_TARGET_TABLE_TAP_TABLE_NULL = "15002";

	@TapExCode(
			describe = "In the synchronization task, if there is no table for the target node table model in the target database, the engine automatically creates a table in the target database. If the automatic table creation is successful, the engine fails to create an index for the update condition field or the index for the synchronized source table",
			describeCN = "在同步任务中，如果目标数据库中没有目标节点表模型的表，引擎会在目标数据库自动创建表。如果自动创建表成功后，引擎为更新条件字段创建索引或同步源端表的索引创建索引失败\n",
			dynamicDescription = "1. When enabling target node configuration advanced Settings > When creating the synchronized index option, the target side will synchronize the source side index, and when the source side index applies the function, this error will be thrown\n" +
								"2. This error will also occur when the update condition field has a type that does not support index creation, such as the TEXT type of some structural databases\n" +
								"3. The account connected to the destination node lacks permission to create an index" +
					"Index creation failed for table:{},Index:{}",
			dynamicDescriptionCN = "1. 当开启目标节点配置中高级设置->建表同步索引选项时，目标端会同步源端的索引，当源端索引应用函数时，会报此错误\n" +
								"2. 当更新条件字段存在不支持创建索引的类型时如某些结构型数据库的TEXT类型，执行创建索引时也会报此错误\n" +
								"3. 目标节点使用的连接的账号缺少创建索引的权限\n" +
					"创建索引失败，表名为：{},索引为：{}",
			solution = "1, close the target node configuration advanced Settings -> Create table synchronization index option or delete the index of the reference function in the source database table \n"+
					"2. Eliminate the fields that do not support index creation in the update condition field or the target node. Modify the field type that cannot create index to the type that can create index in the target data and ensure that the data is compatible when writing.\n"+
					"3. Create the corresponding index creation permission for the connected account used by the target node",
			solutionCN = "1. 关闭目标节点配置中高级设置->建表同步索引选项或删除源端数据库表中的引用函数的索引\n"+
						"2. 在更新条件字段中剔除不支持创建索引的字段或目标节点中将不能创建索引的字段类型修改为可以在目标数据中创建索引并且保证数据写入时兼容的类型\n"+
					"3. 给目标节点使用的连接的账号创建相应的创建索引权限"

	)
	String CREATE_INDEX_FAILED = "15003";

	@TapExCode(
			describe = "An error occurred when the destination node was executing the delete table structure",
			describeCN = "当目标节点在执行删除表结构时出错\n",
			dynamicDescription = "1. The account used for the connection to the destination node does not have permission to drop the table\n" +
					"2. The table cannot be deleted because it is referenced by a foreign key of another table\n" +
					"Failed to delete table, table name {}",
			dynamicDescriptionCN = "1.目标节点的连接使用的账号没有删除表的权限\n" +
					"2.目标表被其他表的外键引用，导致无法删除表\n" +
					"删除表失败，表名为：{}",
			solution = "1. the target node connection to use user delete table permissions\n"+
						"2. Remove the foreign key constraint from the associated table and restart the task",
			solutionCN = "1. 授予目标节点连接使用用户删除表权限\n"+
					"2. 先删除关联表的外键约束后重新启动任务"
	)
	String DROP_TABLE_FAILED = "15004";

	@TapExCode(
			describe = "The target table node failed to automatically create the target table\n",
			describeCN = "目标表节点自动创建目标表失败\n",
			dynamicDescription = "Failed to create table {}",
			dynamicDescriptionCN = "创建表失败，表名为：{}"
	)
	String CREATE_TABLE_FAILED = "15005";

	@TapExCode(
			describe = "When the policy of \"if the target table exists\" in the target node configuration is \"keep the original table structure of the target side and clear the data\", the target table data cleaning fails",
			describeCN = "当目标节点配置中“如果目标表存在”的策略为“保持目标端原有表结构，清除数据”时，清除目标表数据失败",
			dynamicDescription = "1. The target table is referenced by a foreign key of another table\n" +
					"2. The connection account permissions of the destination node are insufficient\n" +
					"table Name:{}",
			dynamicDescriptionCN = "1. 目标表被其他表的外键引用\n" +
					"2. 目标节点的连接账号权限不足\n" +
					"表名为:{}",
			solution = "1. Remove foreign key constraints that refer to the target table\n"+
						"2. Clear the table for connection account permissions granted to the destination node",
			solutionCN = "1. 删除引用目标表的外键约束\n" +
					"2. 给授予目标节点的连接账号权限清空表"
	)
	String CLEAR_TABLE_FAILED = "15006";

	@TapExCode(
			describe = "When writing data, the target table name cannot be retrieved from the source table name in the table name mapping relationship",
			describeCN = "写入数据时，无法通过源端表名在表名映射关系中获取到目标表名",
			dynamicDescription = "According to the source table name: {}",
			dynamicDescriptionCN = "源表名为：{}"
	)
	String WRITE_RECORD_GET_TARGET_TABLE_NAME_FAILED = "15007";
	@TapExCode(
			describe = "The data source does not support write operations",
			describeCN = "数据源暂不支持写入"
	)
	String WRITE_RECORD_PDK_NONSUPPORT = "15008";
	@TapExCode(
			describe = "When the target node performs the operation of adding new fields, it fails to obtain the target table model",
			describeCN = "目标节点执行新增字段操作时失败，无法获取目标表模型",
			dynamicDescription = "The name of the table for the new field event {}",
			dynamicDescriptionCN = "新增字段事件的表名为：{}"
	)
	String ADD_NEW_FIELD_GET_TAP_TABLE_FAILED = "15009";
	@TapExCode(
			describe = "The operation of adding a new field failed, and the description of the new field in the message body is empty",
			describeCN = "目标节点执行新增字段操作时失败，无法在目标表模型中获取新增字段定义",
			dynamicDescription = "Table name: {}, field name: {}",
			dynamicDescriptionCN = "表名为：{}，字段名为：{}"
	)
	String ADD_NEW_FIELD_IS_NULL = "15010";
	@TapExCode(
			describe = "The target node failed to perform the new field operation",
			describeCN = "目标节点在执行新增字段操作时失败",
			dynamicDescription = "1. The new field event is repeated by the target node. For example, after the target has performed the new field operation, the task is started in the \"incremental\" synchronous way after stopping the task and the incremental acquisition starts before the new field event, which leads to the target node repeatedly executing the new field operation and generating an error\n" +
					"2. The account connected by the target node does not have the corresponding permission, which causes the execution of the new field to fail",
			dynamicDescriptionCN = "1. 新增字段事件被目标节点重复执行。如目标已经执行了新增字段操作后，停止任务后以“增量”的同步方式启动任务并且增量采集开始时刻在新增字段事件之前，导致目标节点重复执行新增字段操作而发生报错\n" +
					"2. 目标节点使用连接的账号没有相应的权限，导致执行新增字段失败",
			solution = "1. When the task is started in the \"incremental\" synchronous way, after the specified time point of the field event needs to be added at the beginning of the incremental acquisition, the target node can avoid the error caused by the repeated execution of the new field operation\n"+
						"2. Grant the target node the appropriate permission to use the connected account",
			solutionCN = "1. 以“增量”的同步方式启动任务时，指定的增量采集开始时刻需要新增字段事件时间点之后，避免目标节点重复执行新增字段操作发生报错\n" +
					"2.授予目标节点使用连接的账号相应的权限"
	)
	String ADD_NEW_FIELD_EXECUTE_FAILED = "15011";
	@TapExCode(
			describe = "The target node fails to modify the field name",
			describeCN = "目标节点在执行修改字段名操作时失败",
			dynamicDescription = "1. The change field name event is repeated by the target node. If the target has executed the field name modification operation, the task is started in the \"incremental\" synchronous way after stopping the task and the incremental acquisition starts before the field name modification event, the target node will repeatedly execute the field name renaming operation and an error will occur\n" +
					"2. The account that the destination node is connecting to does not have the appropriate Alter permission, resulting in the failure to modify the field name\n" +
					" name change :{}",
			dynamicDescriptionCN = "1.修改字段名事件被目标节点重复执行。如果目标已经执行了修改字段名操作后，停止任务后以“增量”的同步方式启动任务并且增量采集开始时刻在修改字段名事件之前，导致目标节点重复执行字段改名操作而发生报错\n" +
					"2. 目标节点使用连接的账号没有相应的权限，导致修改字段名失败\n" +
					"字段名的变化:{}",
			solution = "1. When the task is started in the \"incremental\" synchronous way, the specified incremental acquisition start time needs to modify the time point of the field name event, so as to avoid the error caused by the repeated execution of the field renaming operation by the target node \n"+
						"2. Grant the target node the appropriate permission to use the connected account",
			solutionCN = "1. 以“增量”的同步方式启动任务时，指定的增量采集开始时刻需要再修改字段名事件的时间点后，避免目标节点重复执行字段改名操作发生报错\n" +
					"2. 授予目标节点使用连接的账号相应的权限"
	)
	String ALTER_FIELD_NAME_EXECUTE_FAILED = "15012";
	@TapExCode(
			describe = "Failed to modify the field properties, and the target table model could not be obtained",
			describeCN = "目标执行修改字段属性时失败，无法获取到目标表模型",
			dynamicDescription = "Unable to retrieve models for table name :{}",
			dynamicDescriptionCN = "无法获取表名为:{} 的模型"
	)
	String ALTER_FIELD_ATTR_CANNOT_GET_TAP_TABLE = "15013";
	@TapExCode(
			describe = "The target node failed to perform the operation to modify the field attribute",
			describeCN = "目标节点执行修改字段属性操作时失败",
			dynamicDescription = "1. The change field property event is repeatedly executed by the destination node. If the target has executed the field attribute modification operation, the task is started in the \"incremental\" synchronous way after stopping the task and the incremental acquisition starts before the field attribute modification event, the target node executes the field repeatedly and an error is reported\n" +
					"2. The account that the destination node uses to connect does not have the corresponding permission, resulting in the failure to modify the field attribute\n" +
					"The name of the table is :{}, and the name of the field that modifies the field attribute is :{}",
			dynamicDescriptionCN = "1. 修改字段属性事件被目标节点重复执行。如果目标已经执行了修改字段属性操作后，停止任务后以“增量”的同步方式启动任务并且增量采集开始时刻在修改字段属性事件之前，导致目标节点重复执行字段而发生报错\n" +
					"2. 目标节点使用连接的账号没有相应的权限，导致修改字段属性失败\n" +
					"表名为:{}，修改字段属性的字段名为:{}",
			solution = "1. When the task is started in the \"incremental\" synchronous way, the specified incremental acquisition start time needs to modify the time point of the field name event, so as to avoid the error caused by the repeated execution of the field renaming operation by the target node"+
						"2. Grant the target node the appropriate permission to use the connected account",
			solutionCN = "1. 以“增量”的同步方式启动任务时，指定的增量采集开始时刻需要再修改字段名事件的时间点后，避免目标节点重复执行字段改名操作发生报错\n" +
					"2. 授予目标节点使用连接的账号相应的权限"
	)
	String ALTER_FIELD_ATTR_EXECUTE_FAILED = "15014";
	@TapExCode(
			describe = "The target node fails to delete the field operation",
			describeCN = "目标节点执行删除字段操作时失败",
			dynamicDescription = "1. The deletion event is repeatedly executed by the destination node. If the target has executed the deletion operation, the task is started in the \"incremental\" synchronous way after stopping the task and the incremental acquisition starts before the deletion field attribute event, the target node will repeatedly execute the deletion field operation and an error will be reported\n" +
					"2. The account that the destination node uses to connect does not have the corresponding permission, resulting in the failure to delete the field\n" +
					"Table name for deletion event :{}, field name for deletion field :{}",
			dynamicDescriptionCN = "1. 删除事件被目标节点重复执行。如果目标已经执行了删除性操作后，停止任务后以“增量”的同步方式启动任务并且增量采集开始时刻在删除字段属性事件之前，导致目标节点重复执行删除字段操作而发生报错\n" +
					"2. 目标节点使用连接的账号没有相应的权限，导致删除字段失败\n" +
					"删除事件的表名:{}，删除字段的字段名:{}",
			solution = "1. When the task is started in the \"incremental\" synchronous way, the specified time point of deleting the field event needs to be repaired at the beginning of the specified incremental collection time, so as to avoid the error caused by the target node repeatedly executing deleting the field operation\n" +
					"2. Grant the destination node the appropriate permissions to use the connected account",
			solutionCN = "1. 以“增量”的同步方式启动任务时，指定的增量采集开始时刻需要在删除字段事件的时间点后，避免目标节点重复执行删除字段操作发生报错\n"+
					"2. 授予目标节点使用连接的账号相应的权限"
	)
	String DROP_FIELD_EXECUTE_FAILED = "15015";
	@TapExCode(
			describe = "Failed to create target table index, the target table schema does not exist",
			describeCN = "创建目标表索引时失败，无法获取目标表模型",
			dynamicDescription = "The model of table {} cannot be obtained when creating the target table index",
			dynamicDescriptionCN = "创建目标表索引时，无法获取表 {} 的模型"
	)
	String CREATE_INDEX_TABLE_NOT_FOUND = "15016";
	@TapExCode(
			describe = "Failed to handle event",
			describeCN = "处理事件失败"
	)
	String HANDLE_EVENTS_FAILED = "15018";
	@TapExCode(
			describe = "Failed to write data",
			describeCN = "数据源写入失败",
			recoverable = true
	)
	String WRITE_RECORD_COMMON_FAILED = "15019";
	@TapExCode(
			describe = "When the target node executes the index creation event, it detects that the table name attribute in the index creation event is empty, so the index cannot be created",
			describeCN = "目标节点在执行创建索引事件时，检测出创建索引事件中的表名属性为空，导致无法创建索引"
	)
	String CREATE_INDEX_EVENT_TABLE_ID_EMPTY = "15020";
	@TapExCode(
			describe = "When creating an index, checking if the index exists fails",
			describeCN = "创建索引时，检查索引是否存在失败"
	)
	String CREATE_INDEX_QUERY_EXISTS_INDEX_FAILED = "15021";
	@TapExCode(
			describe = "Failed to process events",
			describeCN = "数据源处理失败"
	)
	String PROCESS_EVENTS_FAILED = "15022";
}
