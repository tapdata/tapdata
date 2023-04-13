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
			describeCN= "反查子表数据时，根据联合键从缓存中查找数据失败"
	)
	String LOOK_UP_FIND_BY_JOIN_KEY_FAILED = "16002";

	@TapExCode(
			describe = "Merge table node lookup failed, from node id list is empty",
			describeCN = "合并表数据反查失败，from节点id列表为空"
	)
	String LOOK_UP_MISSING_FROM_NODE_ID = "16003";

	@TapExCode(
			describe = "Cannot found pre node",
			describeCN = "无法找到上个节点"
	)
	String CANNOT_FOUND_PRE_NODE = "16004";

	@TapExCode(
			describe = "Wrong node type",
			describeCN = "节点类型错误"
	)
	String WRONG_NODE_TYPE = "16005";

	@TapExCode(
			describe = "The merged source table does not have a primary key or a unique index, the data cannot be cached, and the merge cannot be performed normally",
			describeCN = "合并的源表没有主键或唯一索引，无法对数据缓存，合并无法正常进行"
	)
	String TAP_MERGE_TABLE_NO_PRIMARY_KEY = "16006";

	@TapExCode(
			describe = "The mode of merging into the array lacks the associated key of the array, please set it in the interface",
			describeCN = "合并进数组的模式，缺少数组关联键，请在界面设置"
	)
	String TAP_MERGE_TABLE_NO_ARRAY_KEY = "16007";

	@TapExCode(
			describe = "Updating or writing to the cache, finding data from the cache based on the federated key failed",
			describeCN= "更新或写入缓存，根据联合键从缓存中查找数据失败"
	)
	String UPSERT_CACHE_FIND_BY_JOIN_KEY_FAILED = "16008";

	@TapExCode(
			describe = "Update or write cache failed",
			describeCN = "更新或写入缓存失败"
	)
	String UPSERT_CACHE_FAILED = "16009";

	@TapExCode(
			describe = "Deleting cache, finding data from the cache based on the federated key failed",
			describeCN= "删除缓存，根据联合键从缓存中查找数据失败"
	)
	String DELETE_CACHE_FIND_BY_JOIN_KEY_FAILED = "16010";

	@TapExCode(
			describe = "Delete cache failed",
			describeCN = "删除缓存失败"
	)
	String DELETE_CACHE_FAILED = "16011";

	@TapExCode(
			describe = "Failed to initialize source node cache, unexpected node type",
			describeCN = "初始化源端节点缓存失败，非预期的节点类型"
	)
	String INIT_SOURCE_NODE_MAP_WRONG_NODE_TYPE = "16012";

	@TapExCode(
			describe = "Failed to initialize the merge cache data resource, unable to get the cache name",
			describeCN = "初始化合并缓存数据资源失败，无法获取缓存名称"
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

	@TapExCode
	String UPSERT_CACHE_UNKNOWN_ERROR = "16016";

	@TapExCode
	String DELETE_CACHE_UNKNOWN_ERROR = "16017";

	@TapExCode(
			describe = "In the master-slave merge configuration, the source join key configuration is missing",
			describeCN = "主从合并配置中，缺少源端关联键配置"
	)
	String MISSING_SOURCE_JOIN_KEY_CONFIG = "16018";

	@TapExCode(
			describe = "The value for the join key does not exist in the data",
			describeCN = "数据中不存在关联键的值"
	)
	String JOIN_KEY_VALUE_NOT_EXISTS = "16019";

	@TapExCode(
			describe = "In the master-slave merge configuration, the target join key configuration is missing",
			describeCN = "主从合并配置中，缺少目标关联键配置"
	)
	String MISSING_TARGET_JOIN_KEY_CONFIG = "16020";

	@TapExCode(
			describe = "The value of the primary key or unique index does not exist in the data",
			describeCN = "数据中不存在主键或唯一索引的值"
	)
	String PK_OR_UNIQUE_VALUE_NOT_EXISTS = "16021";

	@TapExCode(
			describe = "",
			describeCN = ""
	)
	String NOT_FOUND_SOURCE_NODE = "16022";

	@TapExCode(
			describe = "The table name cannot be blank",
			describeCN = "表名不能为空"
	)
	String TABLE_NAME_CANNOT_BE_BLANK = "16023";

	@TapExCode(
			describe = "The connection id cannot be blank",
			describeCN = "连接id不能为空"
	)
	String CONNECTION_ID_CANNOT_BE_BLANK = "16024";
}
