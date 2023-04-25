package com.tapdata.entity.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.DataFlowUtil;
import com.tapdata.constant.FileProperty;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.FieldProcess;
import com.tapdata.entity.FieldScript;
import com.tapdata.entity.JoinTable;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author jackin
 */
public class Stage implements Serializable {

	private static final long serialVersionUID = 4715756135163716920L;

	public static final String TYPE_FILED = "type";
	public static final String SOURCE_STAGE = "source";
	public static final String TARGET_STAGE = "target";

	public static final String DROP_TYPE_DROP_SCHEMA = "drop_schema";
	public static final String DROP_TYPE_DROP_DATA = "drop_data";
	public static final String DROP_TYPE_NO_DROP = "no_drop";

	private String id;

	private String name;

	private String type;

	private String sourceOrTarget;

	private String connectionId;

	private String tableName;

	private String sql;

	private String filter;

	/**
	 * 是否开启数据过滤
	 */
	private boolean isFilter;

	private String initialOffset;

	private boolean dataQualityTag;

	private boolean dropTable;

	private String script;

	private String primaryKeys;

	private String databaseType;

	private List<FieldProcess> operations;

	private List<FieldScript> scripts;

	private List<JoinTable> joinTables;

	private List<String> inputLanes = new ArrayList<>();

	private List<String> outputLanes = new ArrayList<>();

	private List<Aggregation> aggregations;

	private String expression;

	private String action;

	private List<String> includeTables;

	private String fieldFilter;

	private String fieldFilterType;

	@JsonProperty("table_prefix")
	private String tablePrefix;

	@JsonProperty("table_suffix")
	private String tableSuffix;

	private int initialSyncOrder;

	private boolean enableInitialOrder;

	private boolean collectionAggregate;

	private String collectionAggrPipeline;

	/**
	 * memory cache
	 */
	private String cacheKeys;

	private String cacheName;

	private String cacheType;

	private long maxRows;

	private long maxSize;

	/**
	 * 失效时间
	 */
	private long ttl;

	/**
	 * 缓存字段
	 */
	private Set<String> fields;

	private List<SyncObjects> syncObjects;

	/**
	 * log collect
	 */
	private List<LogCollectorSetting> logCollectorSettings;
	private long logTtl;
	private SyncPoint syncPoint;
	private boolean disabled;

	private Double maxTransactionLength = 12d;

	/**
	 * redis hmset key and prefix
	 **/
	private String redisKey;
	private String redisKeyPrefix;

	/**
	 * 目标数据删除操作类型
	 * drop_schema: 删除模型,
	 * drop_data:  删除数据,
	 * no_drop: 不做删除操作，保留模型保留数据
	 */
	private String dropType;

	private String statsStatus;

	private int aggCacheMaxSize = 10000;

	private FileProperty fileProperty;

	private long aggregateProcessorInitialInterval = 180 * 1000;

	private boolean keepAggRet;

	private long aggrCleanSecond = TimeUnit.HOURS.toSeconds(1L);
	private long aggrFullSyncSecond = TimeUnit.HOURS.toSeconds(1L);

	private String kafkaPartitionKey;

	/**
	 * kafka的分区id
	 */
	private String partitionId;

	/**
	 * kafka作为源是否开启高性能模式
	 */
	private Boolean performanceMode;

	/**
	 * kafka高性能模式的分区编号
	 */
	private Set<Integer> partitionIdSet;

	/**
	 * kafka是否开启自定义消息体
	 */
	private boolean customMessage;

	/**
	 * es的分片数量
	 */
	private int chunkSize;

	/**
	 * es的index
	 */
	private String index;

	private String table_type;

	/**
	 * pb格式的配置
	 */
	private Map<String, Object> pbProcessorConfig;

	/**
	 * cluster clone字段处理器
	 */
	private List<CloneFieldProcess> field_process;

	/**
	 * 全部表名大小写转换, optional: {@link com.tapdata.entity.dataflow.Capitalized}
	 */
	private String tableNameTransform = Capitalized.NOOPERATION.getValue();

	/**
	 * 全部字段名大小写转换, optional: {@link com.tapdata.entity.dataflow.Capitalized}
	 */
	private String fieldsNameTransform = Capitalized.NOOPERATION.getValue();

	/**
	 * 使用的js引擎名称（支持nashorn、graal.js）
	 */
	private String jsEngineName;

	public Stage() {

	}

	public Stage(String id, String sourceOrTarget) {
		this.id = id;
		this.sourceOrTarget = sourceOrTarget;
	}

	/**
	 * 生成ony many临时表节点
	 *
	 * @param tableStage
	 * @param inputStage
	 * @param targetStage
	 * @param dataFlowId
	 * @return
	 */
	public static Stage oneManyInvisibleStage(Stage tableStage, Stage inputStage, Stage targetStage, String dataFlowId) {
		Stage stage = new Stage();
		stage.setId(tableStage.getId() + ConnectorConstant.LOOKUP_TABLE_SUFFIX);
		stage.setConnectionId(tableStage.getConnectionId());
		stage.setDatabaseType(tableStage.getDatabaseType());
		stage.setType(StageTypeEnum.INVISIBLE_COLLECTION_TYPE.type);
		stage.setDropTable(targetStage.getDropTable());

		List<JoinTable> joinTables = new ArrayList<>();

		List<JoinTable> targetJoinTables = targetStage.getJoinTables();
		if (CollectionUtils.isNotEmpty(targetJoinTables)) {
			for (JoinTable targetJoinTable : targetJoinTables) {
				String stageId = targetJoinTable.getStageId();
				if (tableStage.getId().equals(stageId)) {
					JoinTable joinTable = new JoinTable(targetJoinTable, tableStage, "upsert");

					joinTables.add(joinTable);

					break;
				}
			}

			if (CollectionUtils.isEmpty(joinTables)) {
				joinTables.add(targetJoinTables.get(0));
			}
		}
		stage.setTableName(
				DataFlowUtil.getOneManyTporigTableName(tableStage.getTableName(), dataFlowId, tableStage.getId())
		);

		stage.setJoinTables(joinTables);
		stage.getInputLanes().add(inputStage.getId());

		return stage;
	}

	public enum StageTypeEnum {
		TABLE_TYPE("data", "table"),
		COLLECTION_TYPE("data", "collection"),
		FILES_TYPE("data", "file"),
		SCRIPT_TYPE("data", "script"),
		GRIDFS_TYPE("data", "gridfs"),
		DATABASE("data", "database"),
		DUMMY("data", "dummy db"),
		API("data", "rest api"),
		ELASTICSEARCH("data", "elasticsearch"),
		CUSTOM("data", "custom_connection"),
		MEM_CACHE("data", "mem_cache"),
		LOG_COLLECT("data", "log_collect"),
		LOG_COLLECTOR("data", DatabaseTypeEnum.LOG_COLLECT_V2.getType()),
		REDIS("data", "redis"),
		CSV("data", "csv"),
		EXCEL("data", "excel"),
		JSON("data", "json"),
		XML("data", "xml"),
		KAFKA("data", "kafka", true),
		UDP("data", "udp"),
		MQ("data", "mq", true),
		HIVE("data", "hive"),
		HBASE("data", "hbase"),
		KUDU("data", "kudu"),
		GREENPLUM("data", "greenplum"),
		HANA("data", "hana"),
		CLICKHOUSE("data", "clickhouse"),
		DAMENG("data", "dameng"),
		KUNDB("data", DatabaseTypeEnum.KUNDB.getType()),
		ADB_MYSQL("data", DatabaseTypeEnum.ADB_MYSQL.getType()),
		ALIYUN_MYSQL("data", DatabaseTypeEnum.ALIYUN_MYSQL.getType()),
		ADB_POSTGRESQL("data", DatabaseTypeEnum.ADB_POSTGRESQL.getType()),
		HAZELCASTIMDG("data", DatabaseTypeEnum.HAZELCAST_IMDG.getType()),
		VIKA("data", "Vika"),

		INVISIBLE_COLLECTION_TYPE("data", "invisible_collection"),
		TCP_UDP("data", "tcp_udp", true),

		JS_PROCESSOR("processor", "js_processor"),
		FIELD_PROCESSOR("processor", "field_processor"),
		AGGREGATION_PROCESSOR("processor", "aggregation_processor"),
		ROW_FILTER_PROCESSOR("processor", "row_filter_processor"),
		DATA_RULES_PROCESSOR("processor", "data_rules_processor"),
		CUSTOM_PROCESSOR("processor", "custom_processor"),
		CACHE_LOOKUP_PROCESSOR("processor", "cache_lookup_processor"),
		PROTOBUF_CONVERT_PROCESSOR("processor", "protobuf_convert_processor"),
		FIELD_NAME_TRANSFORM_PROCESSOR("processor", "fieldNameTransform_processor"),
		MERGETABLE("processor", "merge_table_processor"),
		;

		public String type;

		public String parentType;

		public boolean onlyAppend;

		StageTypeEnum(String parentType, String type) {
			this.parentType = parentType;
			this.type = type;
			this.onlyAppend = false;
		}

		StageTypeEnum(String parentType, String type, boolean onlyAppend) {
			this.parentType = parentType;
			this.type = type;
			this.onlyAppend = onlyAppend;
		}


		private static final Map<String, StageTypeEnum> map = new HashMap<>();

		static {
			for (StageTypeEnum stageType : StageTypeEnum.values()) {
				map.put(stageType.type, stageType);
			}
		}

		public static StageTypeEnum fromString(String stageType) {
			return map.get(stageType);
		}

		public String getType() {
			return type;
		}

		public String getParentType() {
			return parentType;
		}
	}

	public List<String> getIncludeTables() {
		return includeTables;
	}

	public void setIncludeTables(List<String> includeTables) {
		this.includeTables = includeTables;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Stage stage = (Stage) o;
		return Objects.equals(id, stage.getId());
	}

	public String getFilter() {
		return filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getInitialOffset() {
		return initialOffset;
	}

	public void setInitialOffset(String initialOffset) {
		this.initialOffset = initialOffset;
	}

	public boolean isDataQualityTag() {
		return dataQualityTag;
	}

	public void setDataQualityTag(boolean dataQualityTag) {
		this.dataQualityTag = dataQualityTag;
	}

	public boolean getDropTable() {
		return dropTable;
	}

	public void setDropTable(boolean dropTable) {
		this.dropTable = dropTable;
	}

	public List<String> getInputLanes() {
		return inputLanes;
	}

	public void setInputLanes(List<String> inputLanes) {
		this.inputLanes = inputLanes;
	}

	public List<String> getOutputLanes() {
		return outputLanes;
	}

	public void setOutputLanes(List<String> outputLanes) {
		this.outputLanes = outputLanes;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public String getPrimaryKeys() {
		return primaryKeys;
	}

	public void setPrimaryKeys(String primaryKeys) {
		this.primaryKeys = primaryKeys;
	}

	public String getDatabaseType() {
		return databaseType;
	}

	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}

	public List<FieldProcess> getOperations() {
		return operations;
	}

	public void setOperations(List<FieldProcess> operations) {
		this.operations = operations;
	}

	public String getSourceOrTarget() {
		return sourceOrTarget;
	}

	public void setSourceOrTarget(String sourceOrTarget) {
		this.sourceOrTarget = sourceOrTarget;
	}

	public List<Aggregation> getAggregations() {
		return aggregations;
	}

	public void setAggregations(List<Aggregation> aggregations) {
		this.aggregations = aggregations;
	}

	public List<FieldScript> getScripts() {
		return scripts;
	}

	public void setScripts(List<FieldScript> scripts) {
		this.scripts = scripts;
	}

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getTablePrefix() {
		return tablePrefix;
	}

	public void setTablePrefix(String tablePrefix) {
		this.tablePrefix = tablePrefix;
	}

	public String getTableSuffix() {
		return tableSuffix;
	}

	public void setTableSuffix(String tableSuffix) {
		this.tableSuffix = tableSuffix;
	}

	public int getInitialSyncOrder() {
		return initialSyncOrder;
	}

	public void setInitialSyncOrder(int initialSyncOrder) {
		this.initialSyncOrder = initialSyncOrder;
	}

	public String getCacheKeys() {
		return cacheKeys;
	}

	public void setCacheKeys(String cacheKeys) {
		this.cacheKeys = cacheKeys;
	}

	public String getCacheName() {
		return cacheName;
	}

	public void setCacheName(String cacheName) {
		this.cacheName = cacheName;
	}

	public String getCacheType() {
		return cacheType;
	}

	public void setCacheType(String cacheType) {
		this.cacheType = cacheType;
	}

	public long getMaxRows() {
		return maxRows;
	}

	public void setMaxRows(long maxRows) {
		this.maxRows = maxRows;
	}

	public long getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(long maxSize) {
		this.maxSize = maxSize;
	}

	public boolean getEnableInitialOrder() {
		return enableInitialOrder;
	}

	public void setEnableInitialOrder(boolean enableInitialOrder) {
		this.enableInitialOrder = enableInitialOrder;
	}

	public String getFieldFilter() {
		return fieldFilter;
	}

	public void setFieldFilter(String fieldFilter) {
		this.fieldFilter = fieldFilter;
	}

	public String getFieldFilterType() {
		return fieldFilterType;
	}

	public void setFieldFilterType(String fieldFilterType) {
		this.fieldFilterType = fieldFilterType;
	}

	public List<LogCollectorSetting> getLogCollectorSettings() {
		return logCollectorSettings;
	}

	public void setLogCollectorSettings(List<LogCollectorSetting> logCollectorSettings) {
		this.logCollectorSettings = logCollectorSettings;
	}

	public long getLogTtl() {
		return logTtl;
	}

	public void setLogTtl(long logTtl) {
		this.logTtl = logTtl;
	}

	public SyncPoint getSyncPoint() {
		return syncPoint;
	}

	public void setSyncPoint(SyncPoint syncPoint) {
		this.syncPoint = syncPoint;
	}

	public List<JoinTable> getJoinTables() {
		return joinTables;
	}

	public void setJoinTables(List<JoinTable> joinTables) {
		this.joinTables = joinTables;
	}

	public boolean getDisabled() {
		return disabled;
	}

	public void setDisabled(boolean disabled) {
		this.disabled = disabled;
	}

	public boolean getIsFilter() {
		return this.isFilter;
	}

	public void setIsFilter(boolean isFilter) {
		this.isFilter = isFilter;
	}

	public Double getMaxTransactionLength() {
		return maxTransactionLength;
	}

	public void setMaxTransactionLength(Double maxTransactionLength) {
		this.maxTransactionLength = maxTransactionLength;
	}

	public String getRedisKey() {
		return redisKey;
	}

	public String getRedisKeyPrefix() {
		return redisKeyPrefix;
	}

	public void setRedisKey(String redisKey) {
		this.redisKey = redisKey;
	}

	public void setRedisKeyPrefix(String redisKeyPrefix) {
		this.redisKeyPrefix = redisKeyPrefix;
	}

	public String getDropType() {
		return dropType;
	}

	public void setDropType(String dropType) {
		this.dropType = dropType;
	}

	public String getStatsStatus() {
		return statsStatus;
	}

	public void setStatsStatus(String statsStatus) {
		this.statsStatus = statsStatus;
	}

	public List<SyncObjects> getSyncObjects() {
		return syncObjects;
	}

	public void setSyncObjects(List<SyncObjects> syncObjects) {
		this.syncObjects = syncObjects;
	}

	public boolean isCollectionAggregate() {
		return collectionAggregate;
	}

	public void setCollectionAggregate(boolean collectionAggregate) {
		this.collectionAggregate = collectionAggregate;
	}

	public String getCollectionAggrPipeline() {
		return collectionAggrPipeline;
	}

	public void setCollectionAggrPipeline(String collectionAggrPipeline) {
		this.collectionAggrPipeline = collectionAggrPipeline;
	}

	public int getAggCacheMaxSize() {
		return aggCacheMaxSize;
	}

	public void setAggCacheMaxSize(int aggCacheMaxSize) {
		this.aggCacheMaxSize = aggCacheMaxSize;
	}

	public FileProperty getFileProperty() {
		return fileProperty;
	}

	public void setFileProperty(FileProperty fileProperty) {
		this.fileProperty = fileProperty;
	}

	public long getAggregateProcessorInitialInterval() {
		return aggregateProcessorInitialInterval;
	}

	public void setAggregateProcessorInitialInterval(long aggregateProcessorInitialInterval) {
		this.aggregateProcessorInitialInterval = aggregateProcessorInitialInterval;
	}

	public boolean getKeepAggRet() {
		return keepAggRet;
	}

	public void setKeepAggRet(boolean keepAggRet) {
		this.keepAggRet = keepAggRet;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getTable_type() {
		return table_type;
	}

	public void setTable_type(String table_type) {
		this.table_type = table_type;
	}

	public Map<String, Object> getPbProcessorConfig() {
		return pbProcessorConfig;
	}

	public void setPbProcessorConfig(Map<String, Object> pbProcessorConfig) {
		this.pbProcessorConfig = pbProcessorConfig;
	}

	public boolean isCustomMessage() {
		return customMessage;
	}

	public void setCustomMessage(boolean customMessage) {
		this.customMessage = customMessage;
	}

	@Override
	public int hashCode() {

		return Objects.hash(id);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(id);
		return sb.toString();
	}

	public long getAggrCleanSecond() {
		return aggrCleanSecond;
	}

	public void setAggrCleanSecond(long aggrCleanSecond) {
		this.aggrCleanSecond = aggrCleanSecond;
	}

	public long getAggrFullSyncSecond() {
		return aggrFullSyncSecond;
	}

	public void setAggrFullSyncSecond(long aggrFullSyncSecond) {
		this.aggrFullSyncSecond = aggrFullSyncSecond;
	}

	public List<CloneFieldProcess> getField_process() {
		return field_process;
	}

	public void setField_process(List<CloneFieldProcess> field_process) {
		this.field_process = field_process;
	}

	public String getKafkaPartitionKey() {
		return kafkaPartitionKey;
	}

	public String getVertexName() {
		return vertexNameHandler((name == null ? "" : name) + "_" + (id == null ? "" : id));
	}

	private String vertexNameHandler(String vertexName) {
		return vertexName.replaceAll("(-| |\\.)", "_");
	}

	public String getPartitionId() {
		return partitionId;
	}

	public void setPartitionId(String partitionId) {
		this.partitionId = partitionId;
	}

	public void setKafkaPartitionKey(String kafkaPartitionKey) {
		this.kafkaPartitionKey = kafkaPartitionKey;
	}

	public Boolean getPerformanceMode() {
		return performanceMode;
	}

	public void setPerformanceMode(Boolean performanceMode) {
		this.performanceMode = performanceMode;
	}

	public Set<Integer> getPartitionIdSet() {
		return partitionIdSet;
	}

	public void setPartitionIdSet(Set<Integer> partitionIdSet) {
		this.partitionIdSet = partitionIdSet;
	}

	public String getTableNameTransform() {
		return tableNameTransform;
	}

	public void setTableNameTransform(String tableNameTransform) {
		this.tableNameTransform = tableNameTransform;
	}

	public String getFieldsNameTransform() {
		return fieldsNameTransform;
	}

	public void setFieldsNameTransform(String fieldsNameTransform) {
		this.fieldsNameTransform = fieldsNameTransform;
	}

	public String getJsEngineName() {
		return jsEngineName;
	}

	public void setJsEngineName(String jsEngineName) {
		this.jsEngineName = jsEngineName;
	}

	public long getTtl() {
		return ttl;
	}

	public void setTtl(long ttl) {
		this.ttl = ttl;
	}

	public Set<String> getFields() {
		return fields;
	}

	public void setFields(Set<String> fields) {
		this.fields = fields;
	}
}
