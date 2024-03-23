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
			describeCN = "反查子表数据时，根据联合键从缓存中查找数据失败"
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
			describeCN = "更新或写入缓存，根据联合键从缓存中查找数据失败"
	)
	String UPSERT_CACHE_FIND_BY_JOIN_KEY_FAILED = "16008";

	@TapExCode(
			describe = "Update or write cache failed",
			describeCN = "更新或写入缓存失败"
	)
	String UPSERT_CACHE_FAILED = "16009";

	@TapExCode(
			describe = "Deleting cache, finding data from the cache based on the federated key failed",
			describeCN = "删除缓存，根据联合键从缓存中查找数据失败"
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

	@TapExCode(
			describe = "The merged source table node not found, please check you config",
			describeCN = "合并的源表节点不存在，请检查你的配置"
	)
	String TAP_MERGE_TABLE_NODE_NOT_FOUND = "16025";

	@TapExCode(
			describe = "Updating or writing to the cache, finding data from the cache based on the federated key failed",
			describeCN = "批量更新或写入缓存，根据联合键从缓存中查找数据失败"
	)
	String UPSERT_CACHE_FIND_BY_JOIN_KEYS_FAILED = "16026";

	@TapExCode(
			describe = "Update or write cache failed",
			describeCN = "批量更新或写入缓存失败"
	)
	String UPSERT_CACHES_FAILED = "16027";
	@TapExCode(
			describe = "Batch update cache, operation can only be insert",
			describeCN = "批量更新缓存，操作只能是写入"
	)
	String INVALID_OPERATION = "16028";
	@TapExCode(
			describe = "The initialization of the associated key sharing relationship failed, and the merge configuration dictionary is empty, possibly because the calling order is wrong",
			describeCN = "初始化关联键共用关系失败，合并配置字典为空，可能是因为调用顺序错误"
	)
	String INIT_SHARE_JOIN_KEYS_FAILED_TABLE_MERGE_MAP_EMPTY = "16029";

	@TapExCode(
			describe = "Checking whether it is necessary to create a cache to update the join key value failed. The merge configuration dictionary is empty, possibly because the calling order is wrong",
			describeCN = "检查是否需要建立更新关联键值的缓存失败，合并配置字典为空，可能是因为调用顺序错误"
	)
	String CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_FAILED_TABLE_MERGE_MAP_EMPTY = "16030";
	@TapExCode(
			describe = "Checking whether it is necessary to establish a cache to update the join key value failed, and the merged configuration could not be obtained based on the ID",
			describeCN = "检查是否需要建立更新关联键值的缓存失败，无法根据id获取到合并配置"
	)
	String CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_FAILED_CANT_GET_MERGE_TABLE_PROPERTIES_BY_ID = "16031";
	@TapExCode(
			describe = "Checking whether it is necessary to establish a cache to update the join key value failed. The connection information dictionary is empty, possibly because the calling order is wrong",
			describeCN = "检查是否需要建立更新关联键值的缓存失败，连接信息字典为空，可能因为调用顺序错误"
	)
	String CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_FAILED_SOURCE_CONNECTION_MAP_EMPTY = "16032";
	@TapExCode(
			describe = "Checking whether it is necessary to establish a cache to update the join key value failed, and the source connection configuration information could not be obtained based on the ID",
			describeCN = "检查是否需要建立更新关联键值的缓存失败，无法根据id获取到源端连接配置信息"
	)
	String CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_FAILED_CANT_GET_SOURCE_CONNECTION_BY_ID = "16033";

	@TapExCode(
			describe = "Failed to establish a cache to detect whether the join key value has changed. The node id is empty and the name cannot be constructed",
			describeCN = "建立检测关联键值是否变更缓存失败，节点id为空，无法构建名称"
	)
	String GET_CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_NAME_FAILED_NODE_ID_CANNOT_NULL = "16034";

	@TapExCode(
			describe = "Failed to establish a cache to detect whether the join key value has changed, and the write signature failed",
			describeCN = "建立检测关联键值是否变更缓存失败，写入签名失败"
	)
	String INIT_CHECK_UPDATE_JOIN_KEY_VALUE_CACHE_WRITE_SIGN_FAILED = "16035";
	@TapExCode(
			describe = "Failed to copy external storage, original dto is null",
			describeCN = "复制外部存储失败，原始对象为空"
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
	@TapExCode(
			describe = "Failed to get and update the join key cache, unable to get the IMAP",
			describeCN = "获取并更新关联键缓存失败，无法获取IMAP"
	)
	String GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP = "16039";
	@TapExCode(
			describe = "Failed to get and update the join key cache, unable to get after data",
			describeCN = "获取并更新关联键缓存失败，无法获取after数据"
	)
	String GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_AFTER_IS_EMPTY = "16040";
	@TapExCode(
			describe = "Failed to get and update the join key cache, failed to find by PK",
			describeCN = "获取并更新关联键缓存失败，根据PK查找失败"
	)
	String GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_FIND_BY_PK_FAILED = "16041";
	@TapExCode(
			describe = "Failed to get and update the join key cache, upsert failed",
			describeCN = "获取并更新关联键缓存失败，更新after数据失败"
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
			describeCN = "分析引用关系失败，无法根据id获取到合并配置"
	)
	String ANALYZE_CHILD_REFERENCE_FAILED_CANT_GET_MERGE_TABLE_PROPERTIES_BY_ID = "16045";
	@TapExCode(
			describe = "Failed to analyze the reference relationship, the merge configuration dictionary is empty, possibly because the calling order is wrong",
			describeCN = "分析引用关系失败，合并配置字典为空，可能是因为调用顺序错误"
	)
	String ANALYZE_CHILD_REFERENCE_FAILED_TABLE_MERGE_MAP_EMPTY = "16046";
	@TapExCode(
			describe = "Failed to get the pre node, unable to get the pre node by id",
			describeCN = "获取上个节点失败，无法根据id获取上个节点"
	)
	String CANNOT_GET_PRENODE_BY_ID = "16047";
	@TapExCode(
			describe = "Building and updating the join key cache failed because there is a field that is both an join key and a primary key",
			describeCN = "构建更新关联键缓存失败，因为有字段同时是关联键和主键，无法构建缓存名称"
	)
	String BUILD_CHECK_UPDATE_JOIN_KEY_CACHE_FAILED_JOIN_KEY_INCLUDE_PK = "16048";
	@TapExCode(
			describe = "Failed to get and update the join key cache, the source must have before data",
			describeCN = "获取并更新关联键缓存失败，源端必须有before数据",
			solution = "1. If MongoDB is the source and the version is 6.0 or above, you need to turn on the 'Document Preimages' function",
			solutionCN = "1. 如果是MongoDB作为源头，并且版本是6.0及以上，则需要打开'文档原像'功能"
	)
	String GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_SOURCE_MUST_HAVE_BEFORE = "16049";
	@TapExCode(
			describe = "Failed to get and update the join key cache, unable to find before data",
			describeCN = "获取并更新关联键缓存失败，无法找到before数据"
	)
	String GET_AND_UPDATE_JOIN_KEY_CACHE_FAILED_CANNOT_FIND_BEFORE = "16050";
	@TapExCode(
			describe = "Failed to insert the join key cache, unable to get IMAP",
			describeCN = "插入关联键缓存失败，无法获取IMAP"
	)
	String INSERT_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP = "16051";
	@TapExCode(
			describe = "Failed to insert the join key cache, insert failed",
			describeCN = "插入关联键缓存失败，写入失败"
	)
	String INSERT_JOIN_KEY_CACHE_FAILED_UPSERT_FAILED = "16052";
	@TapExCode(
			describe = "Failed to delete the join key cache, unable to get IMAP",
			describeCN = "删除关联键缓存失败，无法获取IMAP"
	)
	String DELETE_JOIN_KEY_CACHE_FAILED_CANNOT_GET_IMAP = "16053";
	@TapExCode(
			describe = "Failed to delete the join key cache, delete failed",
			describeCN = "删除关联键缓存失败，删除失败"
	)
	String DELETE_JOIN_KEY_CACHE_FAILED_UPSERT_FAILED = "16054";
	@TapExCode(
			describe = "Delete merge cache failed, lookup cache failed",
			describeCN = "删除合并缓存失败，查找缓存失败"
	)
	String REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_FIND_CACHE_ERROR = "16055";
	@TapExCode(
			describe = "Delete merge cache failed, delete cache failed",
			describeCN = "删除合并缓存失败，删除缓存失败"
	)
	String REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_DELETE_CACHE_ERROR = "16056";
	@TapExCode(
			describe = "Delete merge cache failed, update cache failed",
			describeCN = "删除合并缓存失败，更新缓存失败"
	)
	String REMOVE_MERGE_CACHE_IF_UPDATE_JOIN_KEY_FAILED_UPDATE_CACHE_ERROR = "16057";
	@TapExCode(
			describe = "Clear and destroy cache failed",
			describeCN = "清理并销毁缓存失败"
	)
	String CLEAR_AND_DESTROY_CACHE_FAILED = "16058";
	@TapExCode(
			describe = "Lookup CompletableFuture list cannot be null",
			describeCN = "反查数据的CompletableFuture列表不能为空"
	)
	String LOOKUP_COMPLETABLE_FUTURE_LIST_IS_NULL = "16059";
}
