package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 16, module = "Task Merge Processor", prefix = "TMP", describe = "Task merge processor")
public interface TaskMergeProcessorExCode_16 {
	@TapExCode
	String UNKNOWN_ERROR = "16001";
	@TapExCode(
			describe = "Failed to look up data from the cache based on the federated key when looking up the data in the subtable",
			describeCN = "反查子表数据时，根据联合键从缓存中查找数据失败",
			dynamicDescription = "The configured external cache database is unable to provide normal service\n" +
					"Main table node name :{}, reverse lookup sub-table node name :{}, union key value :{}, encoded union key value :{}, configured external storage database type :{}, external storage name :{}, storage structure table name :{} ",
			dynamicDescriptionCN = "配置的外存缓存数据库无法正常提供服务\n" +
					"主表节点名称:{}，反查子表的节点名称:{}，联合键值:{}，编码后的联合键值:{}，配置的外存数据库类型为:{}，外存名称为:{}，存储结构表名：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String LOOK_UP_FIND_BY_JOIN_KEY_FAILED = "16002";

	@TapExCode(
			describe = "When the subtable data is unlooked up, the list of node ids passed by the acquisition event is empty, so the configuration of the master-slave merge cannot be obtained",
			describeCN = "反查子表数据时，获取事件经过的节点ID列表为空,导致无法获取主从合并的配置"
	)
	String LOOK_UP_MISSING_FROM_NODE_ID = "16003";

	@TapExCode(
			describe = "The previous node of the master-slave merged node cannot be obtained by the node ID",
			describeCN = "无法通过节点ID获取主从合并节点的上一个节点"
	)
	String CANNOT_FOUND_PRE_NODE = "16004";

	@TapExCode(
			describe = "When initializing the master-slave merge node, the unexpected node type is detected",
			describeCN = "初始化主从合并节点时，检测出非预期节点类型",
			dynamicDescription = "Expect MergeTableNode, but got: {}",
			dynamicDescriptionCN = "预期是MergeTableNode，但是检测出{}"
	)
	String WRONG_NODE_TYPE = "16005";
	//TODO 节点ID 减少重复信息
	@TapExCode(
			describe = "The merged source table does not have a primary key or a unique index, the data cannot be cached, and the merge cannot be performed normally",
			describeCN = "合并的源表没有主键或唯一索引，无法对数据缓存，合并无法正常进行",
			dynamicDescription = "Node ID: {},Table name: {}, node name: {}, merge operation: {}",
			dynamicDescriptionCN = "节点ID：{}，表名：{}，节点名：{}，合并操作：{}",
			solution = "1. Add a primary key or unique index to the table in the database of the source table, based on the table name suggested\n" +
					"2. Add the enhanced JS node after the node name indicating the missing primary key. In the enhanced JS basic Settings, click Use Model Declaration and add the " +
					"<pre><code>TapModelDeclare.setPk(tapTable, 'fieldName')" +
					"</code></pre>\n" +
					"script to the script to set the logical primary key for the source table. Restart tasks",
			solutionCN = "1. 根据提示的表名，在源表的数据库中为表添加主键或唯一索引。并在同步工具中重新刷新模型后重新启动任务\n" +
					"2. 根据提示缺少主键的的节点名，在对应的节点后面添加增强JS节点。在增强JS的基础设置中点击使用模型声明，并在脚本中添加\n" +
					"<pre><code>TapModelDeclare.setPk(tapTable, 'fieldName')" +
					"</code></pre>\n" +
					"为源表设置逻辑主键后。重新启动任务"
	)
	String TAP_MERGE_TABLE_NO_PRIMARY_KEY = "16006";

	@TapExCode(
			describe = "When the data write mode of the child table is \"update into embedded data\", the embedded array matching condition is not filled",
			describeCN = "当子表的数据写入模式为“更新进内嵌数据”时,内嵌数组匹配条件没有填写",
			dynamicDescription = "Node ID: {},Table name: {} Node name: {}",
			dynamicDescriptionCN = "节点ID：{}，表名：{}，节点名：{}",
			solution = "1. According to the table name and node name indicated in the error message, find the corresponding master-slave configuration in the master-slave merge and set the matching condition of the embedded array to restart the task",
			solutionCN = "1. 根据报错信息中提示的表名与节点名，找到主从合并中对应的主从配置并设置内嵌数组匹配条件后重新启动任务"
	)
	String TAP_MERGE_TABLE_NO_ARRAY_KEY = "16007";

	@TapExCode(
			describe = "Updating or writing to the cache, finding data from the cache based on the federated key failed",
			describeCN = "更新或写入缓存时，根据联合键从缓存中查找原有数据失败",
			dynamicDescription = "node name: {}, Table name: {},  association key: {}, encoded association key: {}, primary key or unique index key: {}, encoded primary key or unique index key: {}, configured external storage database type: {}, external storage name: {}, storage structure name: {}",
			dynamicDescriptionCN = "节点名称：{}，表名：{}，关联键值：{}，编码后的关联键值：{}，主键或唯一键值：{}，编码后的主键或唯一键值：{}，更新或写入的数据：{}，配置的外存数据库类型为：{}，外存名称为：{}，存储结构名称：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String UPSERT_CACHE_FIND_BY_JOIN_KEY_FAILED = "16008";
	//表名、节点、字段名原值、拼起来后的joinkey,外存类型，外存的表名

	@TapExCode(
			describe = "Update or write cache failed",
			describeCN = "更新或写入缓存失败",
			dynamicDescription = "The configured external cache database is unable to provide normal service\n" +
					"Node name: {}, table name: {}, association key: {}, encoded association key: {}, primary or unique key: {}, encoded primary or unique key: {}, updated or written data: {}, configured external storage database type: {}, external storage name: {}, storage structure name: {}",
			dynamicDescriptionCN = "配置的外存缓存数据库无法正常提供服务\n" +
					"节点名称：{}，表名：{}，关联键值：{}，编码后的关联键值：{}，主键或唯一键值：{}，编码后的主键或唯一键值：{}，更新或写入的数据：{}，配置的外存数据库类型为：{}，外存名称为：{}，存储结构名称：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String UPSERT_CACHE_FAILED = "16009";


	@TapExCode(
			describe = "Deleting cache, finding data from the cache based on the federated key failed",
			describeCN = "删除缓存时，根据联合键从缓存中查找数据失败",
			dynamicDescription = "The configured external cache database is unable to provide normal service\n" +
					"Node name: {}, table name: {}, association key value: {}, encoded association key value: {}, primary or unique key value: {}, encoded primary or unique key value: {}, configured external storage database type: {}, external storage name: {}, storage structure name: {}",
			dynamicDescriptionCN = "配置的外存缓存数据库无法正常提供服务\n" +
					"节点名称：{}，表名：{}，关联键值：{}，编码后的关联键值：{}，主键或唯一键值：{}，编码后的主键或唯一键值：{}，配置的外存数据库类型为：{}，外存名称为：{}，存储结构名称：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String DELETE_CACHE_FIND_BY_JOIN_KEY_FAILED = "16010";

	@TapExCode(
			describe = "Delete cache failed",
			describeCN = "删除缓存失败",
			dynamicDescription = "The configured external cache database is unable to provide normal service\n" +
					"Node name: {}, table name: {}, association key value: {}, encoded association key value: {}, primary or unique key value: {}, encoded primary or unique key value: {}, configured external database type: {}, external memory name: {}, storage structure name: {}",
			dynamicDescriptionCN = "配置的外存缓存数据库无法正常提供服务\n" +
					"节点名称：{}，表名：{}，关联键值：{}，编码后的关联键值：{}，主键或唯一键值：{}，编码后的主键或唯一键值：{}，配置的外存数据库类型为：{}，外存名称为：{}，存储结构名称：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String DELETE_CACHE_FAILED = "16011";
	@TapExCode(
			describe = "Failed to initialize source node cache, unexpected node type",
			describeCN = "初始化源端节点缓存失败，非预期的节点类型",
			dynamicDescription = "Actual node type: {}",
			dynamicDescriptionCN = "实际节点类型：{}"
	)
	String INIT_SOURCE_NODE_MAP_WRONG_NODE_TYPE = "16012";
	@TapExCode(
			describe = "Failed to initialize the merge cache data resource, unable to get the cache name",
			describeCN = "初始化合并缓存数据资源失败，无法获取缓存名称",
			dynamicDescription = "Node ID:{}，table name:{}",
			dynamicDescriptionCN = "节点ID：{}，表名：{}"
	)
	String INIT_MERGE_CACHE_GET_CACHE_NAME_FAILED = "16013";

	@TapExCode(
			describe = "Failed to clean merged cache data, unable to get cache name",
			describeCN = "清理合并缓存数据失败，无法获取缓存名称"
	)
	String CLEAR_MERGE_CACHE_GET_CACHE_NAME_FAILED = "16014";

	@TapExCode(
			describe = "Cache merging data failed, unable to find external cache resources, which should have been loaded in memory at startup initialization",
			describeCN = "缓存合并数据失败，无法找到外部缓存资源，该资源应该在启动初始化时，已经被加载在内存中"
	)
	String NOT_FOUND_CACHE_IN_MEMORY_MAP = "16015";

	@TapExCode(
			describe = "Failed to update the cache, unknown error",
			describeCN = "更新缓存失败，未知错误"
	)
	String UPSERT_CACHE_UNKNOWN_ERROR = "16016";

	@TapExCode(
			describe = "Failed to delete the cache, unknown error",
			describeCN = "删除缓存失败，未知错误"
	)
	String DELETE_CACHE_UNKNOWN_ERROR = "16017";
	@TapExCode(
			describe = "In the master-slave merge configuration, the source join key configuration is missing",
			describeCN = "主从合并节点的配置中，缺少源端关联条件配置",
			dynamicDescription = "master-slave Node ID:{},Master Slave Node name: {}",
			dynamicDescriptionCN = "主从节点ID:{},主从节点名称: {}",
			solution = "Please find the master-slave merge node according to the node name prompted, and configure the association condition in the corresponding table and fill in the association condition",
			solutionCN = "请根据提示的节点名称找到主从合并节点，并在找到对应的表配置关联条件并填写关联条件"

	)
	String MISSING_SOURCE_JOIN_KEY_CONFIG = "16018";
	@TapExCode(
			describe = "When the data passes through the master-slave merge node, the value of the associated condition field set in the master-slave merge node is missing in the data",
			describeCN = "数据经过主从合并节点时，数据中缺少主从合并节点中设置的关联条件字段的值",
			dynamicDescription = "The data flowing through a node named \"{}\" is missing an associated key. Associated key: {}, data content: {}",
			dynamicDescriptionCN = "经过节点名为“{}”的节点流出的数据中缺少关联键。关联键：{}，数据内容：{}",
			solution = "1. If the source database of the master-slave merge node is an unstructured database such as MongoDB. The value of the associated field is missing from the source node data. Other processing nodes such as JS nodes can be added after the source node and before the master-slave merge node, and the script code that filters the missing associated key data can be added to the script, so as not to transmit to the master-slave merge node to ensure the normal operation of the task\n"+
					"2. If there are other processing nodes such as JS nodes before the master-slave merge node, check and remove the script that removes the associated keys from the data",
			solutionCN = "1. 如果主从合并节点的源节点的数据库为非结构化数据库，如MongoDB。源节点数据中缺少关联字段的值。可以在源节点的后面、主从合并节点前加入其他处理节点如JS节点，并在脚本中加入将缺少关联键数据过滤的脚本代码，不往主从合并节点传输，保证任务正常运行\n" +
					"2. 如果主从合并节点前存在其他处理节点如JS节点，请检查并删除将数据中的关联键值移除的脚本"
	)
	String JOIN_KEY_VALUE_NOT_EXISTS = "16019";

	@TapExCode(
			describe = "In the master-slave merge configuration, the target join key configuration is missing",
			describeCN = "主从合并配置中，缺少目标关联条件配置",
			dynamicDescription = "Node ID: {},Node name: \"{}\" master-slave merge node is missing the associated key configuration.",
			dynamicDescriptionCN = "节点ID：{}，节点名为：\"{}\"的主从合并节点缺少关联键配置。",
			solution = "Please find the master-slave merge node according to the node name prompted, and fill in the target association condition configuration in the master-slave merge node",
			solutionCN = "请根据提示的节点名称找到主从合并节点，并填写主从合并节点中目标关联条件配置"
	)
	String MISSING_TARGET_JOIN_KEY_CONFIG = "16020";

	@TapExCode(
			describe = "When data passes through a master-slave merge node, there is no primary key or unique index value in the data",
			describeCN = "数据经过主从合并节点时，数据中不存在主键或唯一索引的值",
			dynamicDescription = "Data content: {}, primary or unique key Field: {}",
			dynamicDescriptionCN = "数据内容：{}，主键或唯一键字段：{}",
			solution = "Before merging nodes, process the nodes to see if there are any operations that remove values from primary key fields or unique keys in the data, and remove those operations",
			solutionCN = "请主从合并节点前处理节点中是否存在将数据中的主键字段或唯一键的值移除的操作，并且将这些操作移除\n"
	)
	String PK_OR_UNIQUE_VALUE_NOT_EXISTS = "16021";

	@TapExCode(
			describe = "",
			describeCN = ""
	)
	String NOT_FOUND_SOURCE_NODE = "16022";

	@TapExCode(
			describe = "The node name of a node of type TableNode is detected as null",
			describeCN = "检测出TableNode类型节点的表名为空"
	)
	String TABLE_NAME_CANNOT_BE_BLANK = "16023";
	//TODO 不应该把所有节点信息打上
	@TapExCode(
			describe = "The connection id cannot be blank",
			describeCN = "检测出TableNode类型节点连接id为空",
			dynamicDescription = "Node ID:{}，Node name:{}",
			dynamicDescriptionCN = "节点ID：{}，节点名称：{}"
	)
	String CONNECTION_ID_CANNOT_BE_BLANK = "16024";
	//TODO 下面的不能合并也应该封装错误码

	@TapExCode(
			describe = "It failed to obtain the pre-node information of the master-slave merge based on the node ID",
			describeCN = "根据节点ID获取主从合并的前置节点信息失败",
			dynamicDescription = "Node ID:{}",
			dynamicDescriptionCN = "节点ID:{}"
	)
	String TAP_MERGE_TABLE_NODE_NOT_FOUND = "16025";

	@TapExCode(
			describe = "Batch update or write to the cache, lookup data from the cache based on the union key failed",
			describeCN = "批量更新或写入缓存，根据联合键从缓存中查找数据失败",
			dynamicDescription = "The configured external cache database is unable to provide normal service\n" +
					"Node name: {}, table name: {}, association key value: {}, external database type configured: {}, external memory name: {}, storage structure name: {}",
			dynamicDescriptionCN = "配置的外存缓存数据库无法正常提供服务\n" +
					"节点名称：{}，表名：{}，关联键值：{}，配置的外存数据库类型为：{}，外存名称为：{}，存储结构名称：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String UPSERT_CACHE_FIND_BY_JOIN_KEYS_FAILED = "16026";

	@TapExCode(
			describe = "Update or write cache failed",
			describeCN = "批量更新或写入缓存失败",
			dynamicDescription = "The configured external cache database is unable to provide normal service\n" +
					"Node name: {}, table name: {}, encoded association key value: {}, configured external storage database type: {}, external storage name: {}, storage structure name: {}",
			dynamicDescriptionCN = "配置的外存缓存数据库无法正常提供服务\n" +
					"节点名称：{}，表名：{}，编码后的关联键值：{}，配置的外存数据库类型为：{}，外存名称为：{}，存储结构名称：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String UPSERT_CACHES_FAILED = "16027";
	@TapExCode(
			describe = "Batch update cache, operation can only be insert",
			describeCN = "批量更新缓存，操作只能是写入"
	)
	String INVALID_OPERATION = "16028";
	@TapExCode(
			describe = "The initialization of the associated key sharing relationship failed, and the merge configuration dictionary is empty, possibly because the calling order is wrong",
			describeCN = "初始化关联键共用关系失败，合并配置字典为空"
	)
	String INIT_SHARE_JOIN_KEYS_FAILED_TABLE_MERGE_MAP_EMPTY = "16029";

	@TapExCode(
			describe = "Checking whether it is necessary to create a cache to update the join key value failed. The merge configuration dictionary is empty, possibly because the calling order is wrong",
			describeCN = "检查是否需要建立更新关联键值的缓存失败，合并配置字典为空"
	)
	String CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_FAILED_TABLE_MERGE_MAP_EMPTY = "16030";
	@TapExCode(
			describe = "Checking whether it is necessary to establish a cache to update the join key value failed, and the merged configuration could not be obtained based on the ID",
			describeCN = "检查是否需要建立更新关联键值的缓存失败，无法根据id获取到合并配置"
	)
	String CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_FAILED_CANT_GET_MERGE_TABLE_PROPERTIES_BY_ID = "16031";
	@TapExCode(
			describe = "Checking whether it is necessary to establish a cache to update the join key value failed. The connection information dictionary is empty, possibly because the calling order is wrong",
			describeCN = "检查是否需要建立更新关联键值的缓存失败，连接信息字典为空"
	)
	String CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_FAILED_SOURCE_CONNECTION_MAP_EMPTY = "16032";
	@TapExCode(
			describe = "Checking whether it is necessary to establish a cache to update the join key value failed, and the source connection configuration information could not be obtained based on the ID",
			describeCN = "检查是否需要建立更新关联键值的缓存失败，无法根据id获取到源端连接配置信息"
	)
	String CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_FAILED_CANT_GET_SOURCE_CONNECTION_BY_ID = "16033";

	@TapExCode(
			describe = "The establishment of the cache to detect whether the associated key value changes fails, and the node id is empty, and the name of the cache cannot be constructed according to the id",
			describeCN = "建立检测关联键值是否变更缓存失败，检查出节点id为空，无法根据id构建缓存的名称"
	)
	String GET_CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_NAME_FAILED_NODE_ID_CANNOT_NULL = "16034";

	@TapExCode(
			describe = "Failed to establish a write signature that checks whether the associated key value changes the cache",
			describeCN = "建立检测关联键值变更缓存时，写入签名失败",
			dynamicDescription = "The configured external cache database is unable to provide normal service\n" +
					"Cache name: {}, signature content: {}, external storage database type configured: {}, external storage name: {}, storage structure name: {}",
			dynamicDescriptionCN = "配置的外存缓存数据库无法正常提供服务\n" +
					"缓存名称：{}，签名内容：{}，配置的外存数据库类型为：{}，外存名称为：{}，存储结构名称：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String INIT_CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_WRITE_SIGN_FAILED = "16035";
	@TapExCode(
			describe = "Failed to copy external storage, original dto is null",
			describeCN = "复制外部存储失败，检测出原始外部存储对象为空"
	)
	String COPY_EXTERNAL_STORAGE_FAILED_SOURCE_IS_NULL = "16036";
	@TapExCode(
			describe = "Failed to analyze the reference relationship, and the merged configuration could not be obtained based on the ID",
			describeCN = "分析引用关系失败，无法根据id获取到合并配置"
	)
	String ANALYZE_REFERENCE_FAILED_CANT_GET_MERGE_TABLE_PROPERTIES_BY_ID = "16037";
	@TapExCode(
			describe = "Failed to analyze the reference relationship, the merge configuration dictionary is empty, possibly because the calling order is wrong",
			describeCN = "分析引用关系失败，合并配置字典为空，可能是因为调用顺序错误"
	)
	String ANALYZE_REFERENCE_FAILED_TABLE_MERGE_MAP_EMPTY = "16038";
	//TODO
	@TapExCode(
			describe = "Failed to get and update the join key cache, unable to get the IMAP",
			describeCN = "根据节点id获取关联键值变更缓存失败",
			dynamicDescription = "Node ID: {}, Node Name: {}",
			dynamicDescriptionCN = "节点ID：{}，Node Name：{}"
	)
	String GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP = "16039";
	@TapExCode(
			describe = "Failed to get and update the join key cache, unable to get after data",
			describeCN = "获取并更新关联键缓存失败，无法获取after数据"
	)
	String GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_AFTER_IS_EMPTY = "16040";
	@TapExCode(
			describe = "Failed to get and update the join key cache, failed to find by PK",
			describeCN = "根据主键或唯一键查找关联键缓存失败",
			dynamicDescription = "The configured external cache database is unable to provide normal service\n" +
					"Node name: {}, primary key or unique key value: {}, external storage database type configured: {}, external storage name: {}, storage structure name: {}",
			dynamicDescriptionCN = "配置的外存缓存数据库无法正常提供服务\n" +
					"节点名称：{}，主键或唯一键值：{}，配置的外存数据库类型为：{}，外存名称为：{}，存储结构名称：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_FIND_BY_PK_FAILED = "16041";
	@TapExCode(
			describe = "Failed to get and update the join key cache, upsert failed",
			describeCN = "更新关联键缓存中数据失败",
			dynamicDescription = "The configured external cache database is unable to provide normal service\n" +
					"Node name: {}, primary key or unique key value: {}, after content: {}, external storage database type configured: {}, external storage name: {}, storage structure name: {}",
			dynamicDescriptionCN = "配置的外存缓存数据库无法正常提供服务\n" +
					"节点名称：{}，主键或唯一键值：{}，after的内容：{}，配置的外存数据库类型为：{}，外存名称为：{}，存储结构名称：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_UPSERT_FAILED = "16042";
	@TapExCode(
			describe = "Failed to update the join key, unable to get after data",
			describeCN = "更新关联键失败，无法获取after数据"
	)
	String HANDLE_UPDATE_JOIN_KEY_FAILED_AFTER_IS_EMPTY = "16043";
	@TapExCode(
			describe = "Failed to merge properties, merge properties map is null",
			describeCN = "合并属性失败，合并属性字典为空"
	)
	String INIT_MERGE_PROPERTY_RREFERENCE_FAILED_MERGE_PROPERTIES_MAP_IS_NULL = "16044";
	@TapExCode(
			describe = "Failed to analyze the reference relationship, and the merged configuration could not be obtained based on the ID",
			describeCN = "分析引用关系失败，无法根据id获取到合并配置",
			dynamicDescription = "Current ID: {}, the IDs included in the reference relationship are: {}",
			dynamicDescriptionCN = "当前ID：{}，引用关系中包含的ID有：{}"
	)
	String ANALYZE_CHILD_REFERENCE_FAILED_CANT_GET_MERGE_TABLE_PROPERTIES_BY_ID = "16045";
	@TapExCode(
			describe = "Failed to analyze the reference relationship, the merge configuration dictionary is empty, possibly because the calling order is wrong",
			describeCN = "分析引用关系失败，合并配置字典为空，可能是因为调用顺序错误"
	)
	String ANALYZE_CHILD_REFERENCE_FAILED_TABLE_MERGE_MAP_EMPTY = "16046";
	@TapExCode(
			describe = "Failed to get the pre node, unable to get the pre node by id",
			describeCN = "获取上个节点失败，无法根据id获取上个节点",
			dynamicDescription = "Node ID: {}",
			dynamicDescriptionCN = "节点ID：{}"
	)
	String CANNOT_GET_PRENODE_BY_ID = "16047";
	@TapExCode(
			describe = "When enabling the update of association condition changes, it is not supported for the association condition field to be both an association key and a primary key",
			describeCN = "当开启更新关联条件变更时，不支持关联条件字段同时是关联键和主键",
			dynamicDescription = "Join key include pk, node: {}[{}], both join key and pk: {}",
			dynamicDescriptionCN = "关联键包含主键，节点：{}[{}]，同时是关联键和主键：{}",
			solution = "Choose any of the following ways to resolve: \n" +
					"1. Disable the update association condition function+\n" +
					"2. Modify the associated field to a non primary key field",
			solutionCN = "选择以下任意方式解决：\n" +
					"1. 关闭更新关联条件功能\n" +
					"2. 修改关联字段为非主键字段"
	)
	String BUILD_CHECK_UPDATE_JOIN_KEY_CACHE_FAILED_JOIN_KEY_INCLUDE_PK = "16048";
	@TapExCode(
			describe = "Failed to get and update the join key cache, the source must have before data",
			describeCN = "获取并更新关联键缓存失败，源端必须有before数据",
			dynamicDescription = "Node Name: {}, Node ID: {}, Event: {}",
			dynamicDescriptionCN = "节点名称：{}，节点ID：{}，事件：{}",
			solution = "1. If MongoDB is the source and the version is 6.0 or above, you need to turn on the 'Document Preimages' function",
			solutionCN = "1. 如果是MongoDB作为源头，并且版本是6.0及以上，则需要打开'文档原像'功能"
	)
	String GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_SOURCE_MUST_HAVE_BEFORE = "16049";
	@TapExCode(
			describe = "Failed to get and update the join key cache, unable to find before data",
			describeCN = "获取并更新关联键缓存失败，无法找到before数据",
			dynamicDescription = "The configured external cache database is unable to provide normal service\n" +
					"Node name: {}, primary key or unique key value: {}, external storage database type configured: {}, external storage name: {}, storage structure name: {}",
			dynamicDescriptionCN = "配置的外存缓存数据库无法正常提供服务\n" +
					"节点名称：{}，主键或唯一键值：{}，配置的外存数据库类型为：{}，外存名称为：{}，存储结构名称：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_CANNOT_FIND_BEFORE = "16050";
	@TapExCode(
			describe = "Failed to insert the join key cache, unable to get cache objects",
			describeCN = "插入关联键缓存失败，无法获取缓存对象",
			dynamicDescription = "Node Name: {}, Node id: {}",
			dynamicDescriptionCN = "节点名称：{}，节点id：{}"
	)
	String INSERT_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP = "16051";
	@TapExCode(
			describe = "Failed to insert the join key cache",
			describeCN = "插入关联键缓存失败",
			dynamicDescription = "External storage name: {}, pk or unique value key: {}, data: {}",
			dynamicDescriptionCN = "外存名称：{}，主键或唯一键：{}，数据：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String INSERT_JOIN_KEY_CACHE_FAILED_UPSERT_FAILED = "16052";
	@TapExCode(
			describe = "Failed to delete the join key cache, unable to get cache objects",
			describeCN = "删除关联键缓存失败，无法获取缓存对象",
			dynamicDescription = "Node name: {}, Node id: {}, Event: {}",
			dynamicDescriptionCN = "节点名称：{}，节点id：{}，事件：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String DELETE_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP = "16053";
	@TapExCode(
			describe = "Failed to delete the join key cache",
			describeCN = "删除关联键缓存失败",
			dynamicDescription = "External storage name: {}, pk or unique value key: {}",
			dynamicDescriptionCN = "外存名称：{}，主键或唯一键：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String DELETE_JOIN_KEY_CACHE_FAILED_UPSERT_FAILED = "16054";
	@TapExCode(
			describe = "The association condition has changed and failed to correct the sub table cache due to an error in finding the cache",
			describeCN = "关联条件发生了变更，在纠正子表缓存时失败，由于查找缓存时发生错误",
			dynamicDescription = "External storage name: {}, join value key: {}",
			dynamicDescriptionCN = "外存名称：{}，关联键：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_FIND_CACHE_ERROR = "16055";
	@TapExCode(
			describe = "Failed to delete merge cache while updating associated key processing",
			describeCN = "在更新关联键处理时，删除合并缓存失败",
			dynamicDescription = "External storage name: {}, join value key: {}, data: {}",
			dynamicDescriptionCN = "外存名称：{}，关联键：{}，数据：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_DELETE_CACHE_ERROR = "16056";
	@TapExCode(
			describe = "Failed to update merge cache while updating associated key processing",
			describeCN = "在更新关联键处理时，更新合并缓存失败",
			dynamicDescription = "External storage name: {}, join value key: {}, data: {}",
			dynamicDescriptionCN = "外存名称：{}，关联键：{}，数据：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_UPDATE_CACHE_ERROR = "16057";
	@TapExCode(
			describe = "Clear and destroy cache failed",
			describeCN = "清理并销毁缓存失败",
			dynamicDescription = "Cache name: {}, cache type: {}",
			dynamicDescriptionCN = "缓存名称：{}，缓存类型：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String CLEAR_AND_DESTROY_CACHE_FAILED = "16058";
	@TapExCode(
			describe = "Conversion of associated key value digits to string failed",
			describeCN = "关联键值数字转换为字符串失败",
			dynamicDescription = "Merge table:{}, Map name: {}, Join key: {}, Data: {}({})",
			dynamicDescriptionCN = "合并表名：{}，映射名称：{}，关联键：{}，数据：{}({})",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String JOIN_KEY_VALUE_CONVERT_NUMBER_FAILED = "16059";
	@TapExCode(
			describe = "Look up cache data, unknown error occurred",
			describeCN = "反查缓存数据，发生未知错误",
			dynamicDescription = "Event: {}",
			dynamicDescriptionCN = "事件: {}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. Find the external storage configuration configured by the master-slave merge node, and conduct a connection test to see if it is available. If not, you need to troubleshose according to the test failure information. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String LOOK_UP_UNKNOWN_ERROR = "16060";
	@TapExCode(
			describe = "Lookup processing thread list cannot be null",
			describeCN = "反查数据的处理线程不能为空"
	)
	String LOOKUP_COMPLETABLE_FUTURE_LIST_IS_NULL = "16061";


	@TapExCode(
			describe = "Init Cache failed",
			describeCN = "由于主从合并功能需要使用缓存，启动带有主从合并节点的任务时，根据主从合并节点配置中的外存配置初始化缓存失败",
			dynamicDescription = "The configured external cache database is unable to provide normal service\n" +
					"The external storage database type configured is :{}, the external storage name is :{}",
			dynamicDescriptionCN = "配置的外存缓存数据库无法正常提供服务\n" +
					"配置的外存数据库类型为:{}，外存名称为:{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. The external memory configuration corresponding to the shared mining task is found, and the connection test is carried out to see if it is available. If it is not available, the test failure information is used to troubleshoate. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String INIT_CACHE_FAILED = "16062";

	@TapExCode(
			describe = "When the update association condition is enabled, the incremental event of the source node needs to support get before data, which is not supported by the current node",
			describeCN = "当开启更新关联条件时，源节点增量事件需要支持获取before数据，当前节点不支持该能力",
			dynamicDescription = "Node: {}[{}], connection name: {}, connection type: {}",
			dynamicDescriptionCN = "节点：{}[{}]，连接名：{}，连接类型：{}",
			solution = "Restart the task after disabling the association condition change function",
			solutionCN = "关闭关联条件变更功能后重新启动任务"
	)
	String GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_SOURCE_MUST_SUPPORT_HAVA_BEFORE_CAPABILITY = "16063";
}
