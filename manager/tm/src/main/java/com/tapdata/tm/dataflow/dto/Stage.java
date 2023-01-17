/**
 * @title: Stage
 * @description:
 * @author lk
 * @date 2021/9/6
 */
package com.tapdata.tm.dataflow.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.dag.vo.FieldProcess;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.dag.vo.TableOperation;
import com.tapdata.tm.commons.task.dto.JoinTable;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class Stage {

	private String id;

	private String name;

	private String type;

	private String sourceOrTarget;

	private String connectionId;

	private String tableName;

	private String sql;

	private String filter;

	private Boolean isFilter;

	private String initialOffset;

	private Boolean dataQualityTag;

	private Boolean dropTable;

	private String script;

	private String primaryKeys;

	@JsonProperty("database_type")
	private String databaseType;

	private List<FieldProcess> operations;

	private List<FieldScript> scripts;

	private List<JoinTable> joinTables;

	private List<String> inputLanes;

	private List<String> outputLanes;

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

	private Integer initialSyncOrder;

	private Boolean enableInitialOrder;

	private Boolean collectionAggregate;

	private String collectionAggrPipeline;

	private String cacheKeys;

	/** ""表示不变  toUpperCase 表示变大写  toLowerCase 表示转小写 */
	private String tableNameTransform;
	/** ""表示不变  toUpperCase 表示变大写  toLowerCase 表示转小写 */
	private String fieldsNameTransform;
	private List<TableOperation> tableOperations;

	private String cacheName;

	private Long maxRows;

	private Long maxSize;

	private List<SyncObjects> syncObjects;

	private List<LogCollectorSetting> logCollectorSettings;

	private Long logTtl;

	private SyncPoint syncPoint;

	private Boolean disabled;

	private Double maxTransactionLength;

	private String redisKey;

	private String redisKeyPrefix;

	private String dropType;

	private String statsStatus;

	private Integer aggCacheMaxSize;

	private FileProperty fileProperty;

	private Long aggregateProcessorInitialInterval;

	private Boolean keepAggRet;

	private Long aggrCleanSecond;

	private Long aggrFullSyncSecond;

	private String kafkaPartitionKey;

	private String partitionId;

	private int chunkSize;

	private String index;

	private String table_type;

	private Map<String, Object> pbProcessorConfig;

	@JsonProperty("field_process")
	private List<FieldProcess> fieldProcess;

	private Integer distance;

	private Boolean freeTransform;

	private Integer readBatchSize;

	private Integer readCdcInterval;

}
