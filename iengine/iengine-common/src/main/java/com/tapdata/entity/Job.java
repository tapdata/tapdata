package com.tapdata.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.DataFlowStageUtil;
import com.tapdata.constant.DataFlowUtil;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.constant.OffsetUtil;
import com.tapdata.constant.ReflectUtil;
import com.tapdata.constant.TapdataOffset;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.entity.dataflow.Capitalized;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.DataFlowEmailWarning;
import com.tapdata.entity.dataflow.DataFlowSetting;
import com.tapdata.entity.dataflow.ErrorEvent;
import com.tapdata.entity.dataflow.ReadShareLogMode;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.entity.dataflow.StageRuntimeStats;
import com.tapdata.entity.dataflow.SyncObjects;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.exception.ConnectionException;
import io.tapdata.exception.DataFlowException;
import io.tapdata.schema.SchemaList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tapdata.entity.dataflow.Stage.StageTypeEnum.COLLECTION_TYPE;
import static org.springframework.data.mongodb.core.query.Criteria.where;

/**
 * @author jackin
 * @date 2017/5/16
 */
public class Job implements Serializable {

	private static final long serialVersionUID = -1372730810079434907L;
	private static final Logger logger = LogManager.getLogger(Job.class);

	public static int CACHE_JOB_DEFAULT_READ_BATCH_SIZE = 25000;

	public final static String TRIGGER_LOG_REMAIN_TIME_FIELD = "trigger_log_remain_time";
	public final static String TRIGGER_START_HOUR_FIELD = "trigger_start_hour";
	public final static String SYNC_TYPE_FIELD = "sync_type";
	public final static String EDIT_DEBUG = "editing_debug";
	public final static String RUNNING_DEBUG = "running_debug";
	public static final int ERROR_RETRY = 3;
	// unit millis
	public static final long ERROR_RETRY_INTERVAL = 3000;

	public long startupTime = System.currentTimeMillis();

//    private String _id;

	// loopback api id
	private String id;

	private String name;

	private String priority;

	private Long first_ts;

	private Long last_ts;

	private String user_id;

	private JobConnection connections;

	@Deprecated
	private List<Mapping> mappings;

	private Map<String, Object> deployment;

	private String mapping_template = ConnectorConstant.MAPPING_TEMPLATE_CUSTOM;

	private volatile String status;

	private Object source;

	private Object offset;

	private boolean fullSyncSucc;

	private boolean event_job_editted;

	private boolean event_job_error;

	private boolean event_job_started;

	private boolean event_job_stopped;

	private String dataFlowId;

	private Map<String, Object> warning;

	private ProgressRateStats progressRateStats;

	private Stats stats;

	private Long connector_ping_time;

	private Long ping_time;

	private List<String> dbhistory;
	private String dbhistoryStr;

	private ObjectId process_offset;

	private boolean is_validate;

	private Object validate_offset;

	private Map<String, List<Mapping>> tableMappings;

	private Map<String, List<Mapping>> testTableMappings;

	private List<SyncObjects> syncObjects;

	private boolean keepSchema = true;

	/**
	 * 写入目标时的线程数
	 */
	private int transformerConcurrency = 8;

	/**
	 * 处理器处理时的线程数
	 */
	private int processorConcurrency = 1;

	/**
	 * oracle增量日志，解析sql的线程数
	 */
	private int oracleLogSqlParseConcurrency = 1;

	private String sync_type = ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC;

	private List<OplogFilter> op_filters;

	private String running_mode = "normal";

	private double sampleRate = 10d;

	private boolean is_test_write;

	private TestWrite test_write;

	private boolean is_null_write;

	private long lastStatsTimestamp;

	private boolean drop_target;

	private boolean increment;

	private Boolean connectorStopped = false;

	private Boolean transformerStopped = false;

	private boolean needToCreateIndex;

	private long notification_window = 0;

	private long notification_interval = 300;

	private long lastNotificationTimestamp;

	private boolean isCatchUpLag;

	private int lagCount;

	private long stopWaitintMills = -1;

	public final static int TEN = 10;

	private final static long DEFAULT_STOP_WAITING_MILLS = 15000;

	private int progressFailCount = 0;

	private long nextProgressStatTS = 0;

	private int trigger_log_remain_time = 0;

	private int trigger_start_hour = 0;

	private boolean is_changeStream_mode = false;

	private int readBatchSize = 25000;

	private Integer readCdcInterval = 500;

	private Boolean stopOnError = false;

	private Map<String, Object> row_count;

	private Map<String, Object> ts;

	private String dataQualityTag;

	private String executeMode;

	private int limit;

	@JsonProperty("debug_order")
	private Integer debugOrder;

	@JsonProperty("previous_job")
	private Job previousJob;

	private boolean copyManagerOpen = false;

	private boolean isOpenAutoDDL;

	private RuntimeInfo runtimeInfo;

	private List<Stage> stages;

	private boolean isSchedule;

	private String cronExpression;

	private Long nextSyncTime = 0L;

	private int cdcCommitOffsetInterval = 3000;

	private List<String> includeTables;

	/**
	 * jdbc目标，定时器处理的间隔(ms)
	 * default: 5 minutes
	 */
	private Long timingTargetOffsetInterval = 5L * 60 * 1000;

	private boolean reset;

	/**
	 * 同一data flow下的任务，是否允许分布式执行
	 * 存在cache节点的编排任务，必须在同一data engine下运行
	 */
	private boolean isDistribute = true;

	private String process_id;

	private String discardDDL;

	private Long cdcLagWarnSendMailLastTime;

	private String distinctWriteType;

	private Double maxTransactionLength = 12d;

	private boolean isSerialMode;

	private List<ErrorEvent> connectorErrorEvents;
	private List<ErrorEvent> transformerErrorEvents;

	private String connectorLastSyncStage = this.sync_type.equals(ConnectorConstant.SYNC_TYPE_CDC) ? TapdataOffset.SYNC_STAGE_CDC : TapdataOffset.SYNC_STAGE_SNAPSHOT;
	private String transformerLastSyncStage = this.sync_type.equals(ConnectorConstant.SYNC_TYPE_CDC) ? TapdataOffset.SYNC_STAGE_CDC : TapdataOffset.SYNC_STAGE_SNAPSHOT;

	private int cdcFetchSize = 1;

	private List<Milestone> milestones;

	private ReadShareLogMode readShareLogMode;

	private boolean cdcConcurrency = false;

	private int manuallyMinerConcurrency = 1;

	private boolean cdcShareFilterOnServer = false; // 是否在服务端过滤（增量共享挖掘日志）

	private boolean timeoutToStop = false; // 如果因任务超时停止的任务，标记为true，DataFlow启动时需要重置所有子任务的状态为false

	/**
	 * es的分片数量
	 */
	private int chunkSize;

	/**
	 * 是否开启无主键同步
	 */
	private boolean noPrimaryKey = true;

	private boolean onlyInitialAddMapping = false;

	/**
	 * 分区id，作为子任务标识
	 */
	private String partitionId;

	private List<List<Stage>> sameJobStages;

	private ClientMongoOperator clientMongoOperator;

	private String oracleLogminer = "automatically";

	/**
	 * 使用自定义SQL解析，默认：关闭
	 */
	private boolean useCustomSQLParser = false;

	private String subTaskId;
	private String taskId;

	private ErrorNotifier<Throwable, String> jobErrorNotifier;

	private String transformModelVersion;

	private Map<String, Long> lastDdlTimes;

	private String offsetStr;

	private EngineVersion engineVersion = EngineVersion.V1;

	public Job() {

	}

	public Job(
			List<List<Stage>> sameJobStages,
			String mappingTemplate,
			ClientMongoOperator clientMongoOperator,
			DataFlowSetting setting
	) {
		this.connections = new JobConnection();
		this.mappings = new ArrayList<>();
		this.deployment = new HashMap<>();
		deployment.put("sync_point", "current");
		deployment.put("sync_time", "");
		this.stats = new Stats();
		this.status = ConnectorConstant.DRAFT;
		this.stages = new ArrayList<>();
		this.mapping_template = mappingTemplate;
		this.noPrimaryKey = setting.getNoPrimaryKey();
		this.sameJobStages = sameJobStages;

		Set<String> addedStages = new HashSet<>();
		for (List<Stage> sameJobStage : sameJobStages) {
			for (Stage stage : sameJobStage) {
				if (!addedStages.contains(stage.getId())) {
					addedStages.add(stage.getId());
					this.stages.add(stage);
				}
			}
		}

		boolean hasCacheStage = false;
		boolean hasAggregationProcessor = false;
		for (List<Stage> mappingStages : sameJobStages) {

			if (!hasCacheStage) {
				hasCacheStage = DataFlowUtil.hasCacheStage(mappingStages);
			}

			int size = mappingStages.size();

			String script = null;
			List<FieldProcess> fieldProcesses = null;
			Stage sourceStage = null;
			boolean hasMultiSourceProcessor = false;
			Connections sourceConn = null;
			for (int i = 0; i < size; i++) {
				Stage stage = mappingStages.get(i);

				String type = stage.getType();
				Stage.StageTypeEnum stageTypeEnum = Stage.StageTypeEnum.fromString(type);
				// source stage config
				if (i == 0) {
					if (StringUtils.isNotBlank(stage.getConnectionId())) {
						Query query = new Query(where("_id").is(stage.getConnectionId()));
						query.fields().exclude("schema");
						List<Connections> sourceConns = MongodbUtil.getConnections(query, null, clientMongoOperator, true);
						if (CollectionUtils.isEmpty(sourceConns)) {
							throw new ConnectionException("Unable to find source connection.");
						}
						sourceConn = sourceConns.get(0);
						this.adaptSourceStageConfig(stage, stageTypeEnum, sourceConn);
					}
					sourceStage = stage;
					continue;
				}

				if (DataFlowStageUtil.isProcessorStage(type)) {

					if (!hasMultiSourceProcessor) {
						List<String> inputLanes = stage.getInputLanes();
						hasMultiSourceProcessor = CollectionUtils.isNotEmpty(inputLanes) && inputLanes.size() > 1;
					}

					if (Stage.StageTypeEnum.AGGREGATION_PROCESSOR.getType().equals(type)) {
						hasAggregationProcessor = true;
					}
					continue;
				}

				// target stage config
				if (DataFlowStageUtil.isDataStage(type)) {
					this.adaptTargetStageConfig(
							stage,
							stageTypeEnum,
							hasMultiSourceProcessor,
							mappingTemplate,
							mappingStages,
							sourceConn,
							script,
							fieldProcesses,
							sourceStage
					);
				}
			}
		}
		if (hasAggregationProcessor) {
			logger.warn(
					"Transformer concurrency only can be 1 and De-rewrite mode only can be 'Force de-rewrite' when use aggregation processor."
			);
			this.transformerConcurrency = 1;
			this.distinctWriteType = ConnectorConstant.DISTINCT_WRITE_TYPE_COMPEL;
		}

		if (hasCacheStage) {
			this.setReadBatchSize(CACHE_JOB_DEFAULT_READ_BATCH_SIZE);
		}
		this.setDataFlowSetting(setting);
		this.stats.initStats(this.stages);

		String transformModelVersionFromDataFlow = setting.getTransformModelVersion();
		if (transformModelVersionFromDataFlow == null) {
			// use the old version as the default
			transformModelVersionFromDataFlow = "v1";
		}
		this.transformModelVersion = transformModelVersionFromDataFlow;
		fieldsNameTransformModifyCondition(clientMongoOperator);

	}

	private void fieldsNameTransformModifyCondition(ClientMongoOperator clientMongoOperator) {
		if (mappings == null) {
			return;
		}
		if (StringUtils.isNoneBlank(this.connections.getSource(), this.connections.getTarget())) {
			Query query = new Query(Criteria.where("_id").is(this.connections.getSource()));
			query.fields().include("database_type");
			Connections sourceConnection = MongodbUtil.getConnections(query, clientMongoOperator, true);
			query = new Query(Criteria.where("_id").is(this.connections.getTarget()));
			query.fields().include("database_type");
			Connections targetConnection = MongodbUtil.getConnections(query, clientMongoOperator, true);
			if (StringUtils.equalsAnyIgnoreCase(sourceConnection.getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType())
					&& (targetConnection == null || StringUtils.equalsAnyIgnoreCase(targetConnection.getDatabase_type(), DatabaseTypeEnum.MONGODB.getType(), DatabaseTypeEnum.ALIYUN_MONGODB.getType()))) {
				return;
			}
		}

		for (Mapping mapping : mappings) {
			List<Stage> mappingStages = mapping.getStages();
			if (CollectionUtils.isEmpty(mappingStages)) {
				continue;
			}
			Stage fieldsNameTransformProcessor = mappingStages.stream().filter(stage -> stage.getType().equals(Stage.StageTypeEnum.FIELD_NAME_TRANSFORM_PROCESSOR.getType())).findFirst().orElse(null);
			if (fieldsNameTransformProcessor == null) {
				continue;
			}
			List<Map<String, String>> joinCondition = mapping.getJoin_condition();
			if (joinCondition == null) {
				continue;
			}
			for (Map<String, String> condition : joinCondition) {
				if (condition == null || !condition.containsKey("target")) {
					continue;
				}
				condition.put("target", Capitalized.convert(condition.get("target"), fieldsNameTransformProcessor.getFieldsNameTransform()));
			}
		}
	}

	private void adaptTargetStageConfig(
			Stage targetStage,
			Stage.StageTypeEnum stageTypeEnum,
			boolean hasMultiSourceProcessor,
			String mappingTemplate,
			List<Stage> mappingStages,
			Connections sourceConn,
			String script,
			List<FieldProcess> fieldProcesses,
			Stage sourceStage
	) {
		boolean cacheTarget = Stage.StageTypeEnum.fromString(targetStage.getType()) == Stage.StageTypeEnum.MEM_CACHE;
		this.connections.setCacheTarget(
				cacheTarget
		);

		this.connections.setTarget(targetStage.getConnectionId());
		if (COLLECTION_TYPE == stageTypeEnum ||
				Stage.StageTypeEnum.TABLE_TYPE == stageTypeEnum ||
				Stage.StageTypeEnum.DATABASE == stageTypeEnum
		) {
			String dropType = targetStage.getDropType();
			if (Stage.StageTypeEnum.DATABASE == stageTypeEnum) {
				if (Stage.DROP_TYPE_DROP_DATA.equals(dropType)) {
					this.drop_target = true;
				} else if (Stage.DROP_TYPE_DROP_SCHEMA.equals(dropType)) {
					this.keepSchema = false;
				}
			} else if (!this.drop_target) {
				this.drop_target = targetStage.getDropTable();
			}

			if (CollectionUtils.isNotEmpty(targetStage.getSyncObjects())) {
				this.syncObjects = new ArrayList<>(targetStage.getSyncObjects());
				targetStage.getSyncObjects().clear();
			}
		}
		//设置es的分片数量到job
		this.setChunkSize(targetStage.getChunkSize());

		if (ConnectorConstant.MAPPING_TEMPLATE_CLUSTER_CLONE.equals(mappingTemplate)) {

			Map<String, List<RelateDataBaseTable>> schema = sourceConn.getSchema();
			if (MapUtils.isNotEmpty(schema) && CollectionUtils.isNotEmpty(schema.get("tables"))) {
				this.generateCloneMapping(schema, mappingStages);
			}
		} else if (ConnectorConstant.MAPPING_TEMPLATE_CUSTOM.equals(mappingTemplate)) {

			Set<String> sourceDataStages = DataFlowUtil.findSourceDataStagesByInputLane(
					stages,
					mappingStages.get(mappingStages.size() - 2).getId()
			);

			Mapping mapping = null;
			List<JoinTable> joinTables = targetStage.getJoinTables();
			if (CollectionUtils.isEmpty(joinTables)) {
				mapping = Mapping.stagesToMapping(
						mappingStages,
						script,
						fieldProcesses,
						sourceStage,
						targetStage,
						stageTypeEnum
				);

				if (sourceConn != null) {
					final Map<String, List<RelateDataBaseTable>> schema = sourceConn.getSchema();
					if (MapUtils.isNotEmpty(schema) && CollectionUtils.isNotEmpty(schema.get("tables"))) {
						mapping.noPrimaryJoinIfNeed(this.noPrimaryKey, schema.get("tables"));
					}
				}
				mappings.add(mapping);
			} else {
				for (JoinTable joinTable : joinTables) {
					if (joinTable == null) {
						throw new DataFlowException(String.format("Join table cannot be null, join tables %s", joinTables));
					}

					String stageId = joinTable.getStageId();
					if (hasMultiSourceProcessor && sourceDataStages.contains(stageId)) {
						mapping = Mapping.joinTableToMapping(
								mappingStages,
								script,
								fieldProcesses,
								sourceStage,
								targetStage,
								joinTable
						);

						final Map<String, List<RelateDataBaseTable>> schema = sourceConn.getSchema();
						if (MapUtils.isNotEmpty(schema) && CollectionUtils.isNotEmpty(schema.get("tables"))) {
							mapping.noPrimaryJoinIfNeed(this.noPrimaryKey, schema.get("tables"));
						}
						mappings.add(mapping);
						break;
					} else if (stageId.equals(sourceStage.getId())) {
						mapping = Mapping.joinTableToMapping(
								mappingStages,
								script,
								fieldProcesses,
								sourceStage,
								targetStage,
								joinTable
						);

						final Map<String, List<RelateDataBaseTable>> schema = sourceConn.getSchema();
						if (MapUtils.isNotEmpty(schema) && CollectionUtils.isNotEmpty(schema.get("tables"))) {
							mapping.noPrimaryJoinIfNeed(this.noPrimaryKey, schema.get("tables"));
						}
						mappings.add(mapping);
						break;
					}

				}
			}

			if (mapping == null) {
				logger.warn("Cannot find construct mapping, source stage {}, target stage {}.", sourceStage, targetStage);
			}
		}
	}

	public void initMappings(ClientMongoOperator clientMongoOperator) {
		if (mappings == null) {
			mappings = new ArrayList<>();
		}
		boolean hasCacheStage = false;
		boolean hasAggregationProcessor = false;
		for (List<Stage> mappingStages : sameJobStages) {

			if (!hasCacheStage) {
				hasCacheStage = DataFlowUtil.hasCacheStage(mappingStages);
			}

			int size = mappingStages.size();

			String script = null;
			List<FieldProcess> fieldProcesses = null;
			Stage sourceStage = null;
			boolean hasMultiSourceProcessor = false;
			Connections sourceConn = null;
			for (int i = 0; i < size; i++) {
				Stage stage = mappingStages.get(i);

				String type = stage.getType();
				Stage.StageTypeEnum stageTypeEnum = Stage.StageTypeEnum.fromString(type);
				// source stage config
				if (i == 0) {

					Query query = new Query(where("_id").is(stage.getConnectionId()));
					query.fields().exclude("schema");
					List<Connections> sourceConns = MongodbUtil.getConnections(query, null, clientMongoOperator, true);
					if (CollectionUtils.isEmpty(sourceConns)) {
						throw new ConnectionException("Unable to find source connection.");
					}
					sourceConn = sourceConns.get(0);
					this.adaptSourceStageConfig(stage, stageTypeEnum, sourceConn);
					sourceStage = stage;
					continue;
				}

				if (DataFlowStageUtil.isProcessorStage(type)) {

					if (!hasMultiSourceProcessor) {
						List<String> inputLanes = stage.getInputLanes();
						hasMultiSourceProcessor = CollectionUtils.isNotEmpty(inputLanes) && inputLanes.size() > 1;
					}

					if (Stage.StageTypeEnum.AGGREGATION_PROCESSOR.getType().equals(type)) {
						hasAggregationProcessor = true;
					}
					continue;
				}

				// target stage config
				if (DataFlowStageUtil.isDataStage(type)) {
					this.adaptTargetStageConfig(
							stage,
							stageTypeEnum,
							hasMultiSourceProcessor,
							mapping_template,
							mappingStages,
							sourceConn,
							script,
							fieldProcesses,
							sourceStage
					);
				}
			}
		}
		if (hasAggregationProcessor) {
			logger.warn(
					"Transformer concurrency only can be 1 and De-rewrite mode only can be 'Force de-rewrite' when use aggregation processor."
			);
			this.transformerConcurrency = 1;
			this.distinctWriteType = ConnectorConstant.DISTINCT_WRITE_TYPE_COMPEL;
		}

		if (hasCacheStage) {
			this.setReadBatchSize(CACHE_JOB_DEFAULT_READ_BATCH_SIZE);
		}

		if (CollectionUtils.isEmpty(mappings)) {
			Optional<Stage> optionalStage = stages.stream().filter(Stage::getDisabled).findFirst();
			if (optionalStage.isPresent()) {
				logger.info("Stage disable,stage name : {}", optionalStage.get().getName());
				return;
			}
		}
		Set<String> initializedStageId = new HashSet<>();
		DataFlow dataFlow = null;
		if (StringUtils.isNotBlank(dataFlowId)) {
			Query query = new Query(Criteria.where("id").is(dataFlowId));
			dataFlow = clientMongoOperator.findOne(query, ConnectorConstant.DATA_FLOW_COLLECTION, DataFlow.class);
			if (dataFlow.getStats() != null && CollectionUtils.isNotEmpty(dataFlow.getStats().getStagesMetrics())) {
				for (StageRuntimeStats dataFlowStagesMetric : dataFlow.getStats().getStagesMetrics()) {
					if (ConnectorConstant.STATS_STATUS_INITIALIZED.equals(dataFlowStagesMetric.getStatus())) {
						initializedStageId.add(dataFlowStagesMetric.getStageId());
					}
				}
			}
		}
		List<Mapping> newMappings = new ArrayList<>();
		for (Mapping mapping : mappings) {

			if (dataFlow != null && DataFlowUtil.initializedMapping(initializedStageId, this, mapping)
					&& StringUtils.isBlank(dataFlow.getSetting().getCronExpression())
					&& !dataFlow.getSetting().getIncrement()) {
				continue;
			}

			newMappings.add(mapping);

			List<Stage> mappingStages = mapping.getStages();
			// 生成one many隐藏节点
			if (Mapping.need2CreateTporigMapping(this, mapping)) {

				List<Map<String, String>> joinConditions = mapping.getJoin_condition();
				List<Map<String, String>> joinConditionNew = new ArrayList<>();
				for (Map<String, String> joinCondition : joinConditions) {
					Map<String, String> joinKey = new HashMap<>();
					joinKey.put("source", joinCondition.get("source"));
					joinKey.put("target", joinCondition.get("source"));
					joinConditionNew.add(joinKey);
				}

				List<Stage> oneManyMappingStages = new ArrayList<>(mappingStages);
				// 上一个节点
				Stage previousStage = oneManyMappingStages.get(oneManyMappingStages.size() - 2);
				// 目标节点
				Stage targetStage = oneManyMappingStages.get(oneManyMappingStages.size() - 1);
				// 源节点
				Stage srcStage = oneManyMappingStages.get(0);
				Stage oneManyInvisibleStage = Stage.oneManyInvisibleStage(
						srcStage,
						previousStage,
						targetStage,
						dataFlow == null ? "" : dataFlow.getId()
				);

				previousStage.getOutputLanes().add(oneManyInvisibleStage.getId());
				this.stages.add(oneManyInvisibleStage);
				oneManyMappingStages.set(oneManyMappingStages.size() - 1, oneManyInvisibleStage);

				String toTable = DataFlowUtil.getOneManyTporigTableName(mapping.getFrom_table(), dataFlow.getId(), srcStage.getId());

				Mapping mappingNew = new Mapping(
						mapping.getFrom_table(),
						toTable,
						ConnectorConstant.RELATIONSHIP_ONE_ONE,
						joinConditionNew,
						mapping.getFields_process(),
						mapping.getScript(),
						0,           // 中间表优先级最低
						mapping.getFieldFilter(),
						mapping.getFieldFilterType()
				);
				mappingNew.setStages(oneManyMappingStages);
				mappingNew.setDropTarget(targetStage.getDropTable());

				newMappings.add(mappingNew);
			}
		}

		if (!CollectionUtils.isEmpty(newMappings)) {
			// 排序mapping，优先执行one one的场景
			Mapping.sortMapping(newMappings);
			this.mappings = newMappings;
		}
	}

	private void adaptSourceStageConfig(Stage stage, Stage.StageTypeEnum stageTypeEnum, Connections sourceConn) {
		this.connections.setSource(stage.getConnectionId());

		if (Stage.StageTypeEnum.DUMMY == stageTypeEnum || Stage.StageTypeEnum.API == stageTypeEnum) {
			DataFlowStageUtil.fillStageConfig(stage, sourceConn);
		}

		switch (stageTypeEnum) {
			case DUMMY:
				DataFlowStageUtil.fillStageConfig(stage, sourceConn);
				break;
			case COLLECTION_TYPE:
				String dataFliter = stage.getFilter();
				if (StringUtils.isNotBlank(dataFliter)) {
					try {
						Document.parse(dataFliter);
					} catch (Exception e) {
						throw new DataFlowException(
								String.format(
										"Invalid mongodb collection filter %s, message %s.",
										stage.getFilter(),
										e.getMessage())
						);
					}
				}
				break;
			case DATABASE:
				includeTables = stage.getIncludeTables();
				break;
			default:
				break;
		}
	}

	public void mergeWithoutRuntimeState(Job job) {
		this.setExecuteMode(job.getExecuteMode());
		this.setSync_type(job.getSync_type());
		this.setTransformerConcurrency(job.getTransformerConcurrency());
		this.setSampleRate(job.getSampleRate());
		this.setMapping_template(job.getMapping_template());
		this.setUser_id(job.getUser_id());
		this.setLimit(job.getLimit());
		this.setDataFlowId(job.getDataFlowId());

		this.setName(job.getName());
		this.setConnections(job.getConnections());
		this.setReadBatchSize(job.getReadBatchSize());
		this.setCatchUpLag(job.getCatchUpLag());
		this.setCopyManagerOpen(job.isCopyManagerOpen());
		this.setCronExpression(job.getCronExpression());

		List<String> dbhistory = job.getDbhistory();
		if (CollectionUtils.isNotEmpty(dbhistory)) {
			this.setDbhistory(dbhistory);
		}
		this.setDebugOrder(job.getDebugOrder());
		this.setDrop_target(job.getDrop_target());
		this.setEvent_job_editted(job.getEvent_job_editted());
		this.setEvent_job_error(job.getEvent_job_error());
		this.setEvent_job_started(job.getEvent_job_started());
		this.setEvent_job_stopped(job.getEvent_job_stopped());
		this.setFirst_ts(job.getFirst_ts());
		this.setFullSyncSucc(job.getFullSyncSucc());
		this.setIncrement(job.isIncrement());
		this.setIs_changeStream_mode(job.isIs_changeStream_mode());
		this.setIs_null_write(job.is_null_write());
		this.setIs_test_write(job.is_test_write());
		this.setIs_validate(job.getIs_validate());
		this.setIsOpenAutoDDL(job.getIsOpenAutoDDL());
		this.setLastDdlTimes(job.getLastDdlTimes());
		this.setIsSchedule(job.getIsSchedule());
		this.setNeedToCreateIndex(job.getNeedToCreateIndex());
		this.setNotification_interval(job.getNotification_interval());
		this.setNotification_window(job.getNotification_window());
		this.setOp_filters(job.getOp_filters());
		this.setPriority(job.getPriority());
		this.setReadCdcInterval(job.getReadCdcInterval());
		this.setRunning_mode(job.getRunning_mode());
		this.setStages(job.getStages());
		this.setStopOnError(job.getStopOnError());
		this.setStatus(job.getStatus());
		this.setIsDistribute(job.getIsDistribute());
		this.setProcess_id(job.getProcess_id());
		this.setCdcFetchSize(job.getCdcFetchSize());
		this.setReadShareLogMode(job.getReadShareLogMode());
		this.setCdcConcurrency(job.getCdcConcurrency());
		this.setManuallyMinerConcurrency(job.getManuallyMinerConcurrency());
		this.setCdcShareFilterOnServer(job.getCdcShareFilterOnServer());
		this.setSubTaskId(job.getSubTaskId());
		this.setTaskId(job.getTaskId());
		if (StringUtils.isNotBlank(dataFlowId)) {
			this.setMappings(job.getMappings());
		}

		if (this.stats != null && job.getStats() != null && CollectionUtils.isNotEmpty(this.stats.getStageRuntimeStats()) && CollectionUtils.isNotEmpty(job.getStats().getStageRuntimeStats())) {
			for (StageRuntimeStats stageRuntimeStat : job.getStats().getStageRuntimeStats()) {
				boolean isExist = false;
				for (StageRuntimeStats runtimeStat : this.stats.getStageRuntimeStats()) {
					if (runtimeStat.getStageId().equals(stageRuntimeStat.getStageId())) {
						isExist = true;
						break;
					}
				}
				if (!isExist) {
					this.stats.getStageRuntimeStats().add(stageRuntimeStat);
				}
			}
		}
	}

	public boolean getIsSchedule() {
		return isSchedule;
	}

	public void setIsSchedule(boolean schedule) {
		isSchedule = schedule;
	}

	public String getCronExpression() {
		return cronExpression;
	}

	public void setCronExpression(String cronExpression) {
		this.cronExpression = cronExpression;
	}

	public Long getNextSyncTime() {
		return nextSyncTime;
	}

	public void setNextSyncTime(Long nextSyncTime) {
		this.nextSyncTime = nextSyncTime;
	}

	public RuntimeInfo getRuntimeInfo() {
		return runtimeInfo;
	}

	public void setRuntimeInfo(RuntimeInfo runtimeInfo) {
		this.runtimeInfo = runtimeInfo;
	}

	public boolean getIsOpenAutoDDL() {
		return isOpenAutoDDL;
	}

	public void setIsOpenAutoDDL(boolean openAutoDDL) {
		isOpenAutoDDL = openAutoDDL;
	}

	public Map<String, Long> getLastDdlTimes() {
		return lastDdlTimes;
	}

	public void setLastDdlTimes(Map<String, Long> lastDdlTimes) {
		this.lastDdlTimes = lastDdlTimes;
	}

	public Long getFirst_ts() {
		return first_ts;
	}

	public void setFirst_ts(Long first_ts) {
		this.first_ts = first_ts;
	}

	public Long getLast_ts() {
		return last_ts;
	}

	public void setLast_ts(Long last_ts) {
		this.last_ts = last_ts;
	}

	public String getStatus() {
		return status;
	}

	/**
	 * 增加状态逻辑切换控制
	 *
	 * @param status
	 */
	public synchronized void setStatus(String status) {
		if (StringUtils.isBlank(this.status)) {
			this.status = status;
		} else {
			if (ConnectorConstant.RUNNING.equals(status)) {
				runningJob(status);
			} else {
				logger.info(
						"Job {}'s status from {} to {}, method call stack {}.",
						this.name,
						this.status,
						status,
						Thread.currentThread().getStackTrace()
				);
				this.status = status;
			}
		}
	}

	public void runningJob(String status) {
		// 状态为 scheduled或running 才可切换为 running
		if (ConnectorConstant.RUNNING.equals(status) &&
				StringUtils.equalsAny(
						this.status,
						ConnectorConstant.SCHEDULED,
						ConnectorConstant.RUNNING
				)) {
			this.status = status;
		} else {
			logger.warn(
					"Job {} status from {} to {} is illegal, method call stack {}.",
					this.name,
					this.status,
					status,
					Thread.currentThread().getStackTrace()
			);
		}
	}

	public Object getSource() {
		return source;
	}

	public void setSource(Object source) {
		this.source = source;
	}

	public Object getOffset() {
		return offset;
	}

	public void setOffset(Object offset) {
		this.offset = offset;
	}

	public boolean getFullSyncSucc() {
		return fullSyncSucc;
	}

	public void setFullSyncSucc(boolean fullSyncSucc) {
		this.fullSyncSucc = fullSyncSucc;
	}

	public Stats getStats() {
		return stats;
	}

	public void setStats(Stats stats) {
		this.stats = stats;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public JobConnection getConnections() {
		return connections;
	}

	public void setConnections(JobConnection connections) {
		this.connections = connections;
	}

	public List<Mapping> getMappings() {
		if (mappings == null) {
			if (clientMongoOperator == null) {
				throw new RuntimeException("Init mappings failed, client mongo operator is null");
			}
			initMappings(clientMongoOperator);
		}
		return mappings;
	}

	public void setMappings(List<Mapping> mappings) {
		this.mappings = mappings;
	}

	public boolean isFullSyncSucc() {
		return fullSyncSucc;
	}

	public String getMapping_template() {
		return mapping_template;
	}

	public void setMapping_template(String mapping_template) {
		this.mapping_template = mapping_template;
	}

	public Map<String, Object> getDeployment() {
		return deployment;
	}

	public void setDeployment(Map<String, Object> deployment) {
		this.deployment = deployment;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public Long getConnector_ping_time() {
		return connector_ping_time;
	}

	public void setConnector_ping_time(Long connector_ping_time) {
		this.connector_ping_time = connector_ping_time;
	}

	public List<String> getDbhistory() {
		return dbhistory;
	}

	public void setDbhistory(List<String> dbhistory) {
		this.dbhistory = dbhistory;
	}

	public ObjectId getProcess_offset() {
		return process_offset;
	}

	public void setProcess_offset(ObjectId process_offset) {
		this.process_offset = process_offset;
	}

	public boolean getIs_validate() {
		return is_validate;
	}

	public void setIs_validate(boolean is_validate) {
		this.is_validate = is_validate;
	}


	public Map<String, List<Mapping>> getTableMappings() {
		return tableMappings;
	}

	public void setTableMappings(Map<String, List<Mapping>> tableMappings) {
		this.tableMappings = tableMappings;
	}

	public int getTransformerConcurrency() {
		return transformerConcurrency;
	}

	public void setTransformerConcurrency(int transformerConcurrency) {
		this.transformerConcurrency = transformerConcurrency;
	}

	public String getSync_type() {
		return sync_type;
	}

	public void setSync_type(String sync_type) {
		this.sync_type = sync_type;
	}

	public List<OplogFilter> getOp_filters() {
		return op_filters;
	}

	public void setOp_filters(List<OplogFilter> op_filters) {
		this.op_filters = op_filters;
	}

	public Map<String, Object> getWarning() {
		return warning;
	}

	public void setWarning(Map<String, Object> warning) {
		this.warning = warning;
	}

	public boolean getEvent_job_editted() {
		return event_job_editted;
	}

	public void setEvent_job_editted(boolean event_job_editted) {
		this.event_job_editted = event_job_editted;
	}

	public boolean getEvent_job_error() {
		return event_job_error;
	}

	public void setEvent_job_error(boolean event_job_error) {
		this.event_job_error = event_job_error;
	}

	public boolean getEvent_job_started() {
		return event_job_started;
	}

	public void setEvent_job_started(boolean event_job_started) {
		this.event_job_started = event_job_started;
	}

	public boolean getEvent_job_stopped() {
		return event_job_stopped;
	}

	public void setEvent_job_stopped(boolean event_job_stopped) {
		this.event_job_stopped = event_job_stopped;
	}

	public String getRunning_mode() {
		return running_mode;
	}

	public void setRunning_mode(String running_mode) {
		this.running_mode = running_mode;
	}

	public ProgressRateStats getProgressRateStats() {
		return progressRateStats;
	}

	public void setProgressRateStats(ProgressRateStats progressRateStats) {
		this.progressRateStats = progressRateStats;
	}

	public Double getSampleRate() {
		return sampleRate;
	}

	public void setSampleRate(Double sampleRate) {
		this.sampleRate = sampleRate;
	}

	public Object getValidate_offset() {
		return validate_offset;
	}

	public void setValidate_offset(Object validate_offset) {
		this.validate_offset = validate_offset;
	}


	public boolean is_test_write() {
		return is_test_write;
	}

	public void setIs_test_write(boolean is_test_write) {
		this.is_test_write = is_test_write;
	}

	public TestWrite getTestWrite() {
		return test_write;
	}

	public void setTestWrite(TestWrite testWrite) {
		this.test_write = testWrite;
	}

	public Map<String, List<Mapping>> getTestTableMappings() {
		return testTableMappings;
	}

	public void setTestTableMappings(Map<String, List<Mapping>> testTableMappings) {
		this.testTableMappings = testTableMappings;
	}

	public boolean is_null_write() {
		return is_null_write;
	}

	public void setIs_null_write(boolean is_null_write) {
		this.is_null_write = is_null_write;
	}

	public boolean getDrop_target() {
		return drop_target;
	}

	public void setDrop_target(boolean drop_target) {
		this.drop_target = drop_target;
	}

	public Long getPing_time() {
		return ping_time;
	}

	public void setPing_time(Long ping_time) {
		this.ping_time = ping_time;
	}

	public boolean isIncrement() {
		return increment;
	}

	public void setIncrement(boolean increment) {
		this.increment = increment;
	}

	public Boolean getConnectorStopped() {
		return connectorStopped;
	}

	public void setConnectorStopped(Boolean connectorStopped) {
		this.connectorStopped = connectorStopped;
	}

	public Boolean getTransformerStopped() {
		return transformerStopped;
	}

	public void setTransformerStopped(Boolean transformerStopped) {
		this.transformerStopped = transformerStopped;
	}

	public long getNotification_window() {
		return notification_window;
	}

	public void setNotification_window(long notification_window) {
		this.notification_window = notification_window;
	}

	public long getNotification_interval() {
		return notification_interval;
	}

	public void setNotification_interval(long notification_interval) {
		this.notification_interval = notification_interval;
	}

	public boolean getCatchUpLag() {
		return isCatchUpLag;
	}

	public void setCatchUpLag(boolean catchUpLag) {
		isCatchUpLag = catchUpLag;
	}

	public int getLagCount() {
		return lagCount;
	}

	public void setLagCount(int lagCount) {
		this.lagCount = lagCount;
	}

	public int getReadBatchSize() {
		return readBatchSize;
	}

	public void setReadBatchSize(int readBatchSize) {
		this.readBatchSize = readBatchSize;
	}

	public long getStopWaitintMills() {
		if (stopWaitintMills <= DEFAULT_STOP_WAITING_MILLS && stopWaitintMills != -1) {
			stopWaitintMills = DEFAULT_STOP_WAITING_MILLS;
		}
		return stopWaitintMills;
	}

	public void setStopWaitintMills(long stopWaitintMills) {
		this.stopWaitintMills = stopWaitintMills;
	}

	public void setDataFlowSetting(DataFlowSetting setting) {
		if (setting == null) {
			return;
		}

		this.sync_type = setting.getSync_type();
		boolean schedule = setting.getIsSchedule();
		if (schedule) {
			this.isSchedule = schedule;
			String cronExpression = setting.getCronExpression();
			this.cronExpression = cronExpression;
		}

		DataFlowEmailWarning emailWaring = setting.getEmailWaring();
		if (emailWaring != null) {
			this.event_job_editted = emailWaring.getEdited();
			this.event_job_error = emailWaring.getError();
			this.event_job_started = emailWaring.getStarted();
			this.event_job_stopped = emailWaring.getPaused();
		}

		this.needToCreateIndex = setting.getNeedToCreateIndex();
		this.notification_window = setting.getNotificationWindow();
		this.notification_interval = setting.getNotificationInterval();
		this.readBatchSize = (int) setting.getReadBatchSize();
		this.readCdcInterval = (int) setting.getReadCdcInterval();
		this.stopOnError = setting.getStopOnError();
		this.increment = setting.getIncrement();
		this.isOpenAutoDDL = setting.getIsOpenAutoDDL();
		if (ConnectorConstant.SYNC_TYPE_CDC.equals(sync_type)) {
			if (this.deployment == null) {
				this.deployment = new HashMap<>();
			}

			this.deployment.put(ConnectorConstant.SYNC_POINT_FIELD, setting.getSyncPoint());
			this.deployment.put(ConnectorConstant.SYNC_TIME_FIELD, setting.getSyncTime());
			List<SyncPoints> syncPoints = setting.getSyncPoints();
			if (CollectionUtils.isNotEmpty(syncPoints)) {
				for (SyncPoints syncPoint : syncPoints) {
					if (syncPoint != null && connections.getSource().equals(syncPoint.getConnectionId())) {
						this.deployment.put(ConnectorConstant.SYNC_POINTS_FIELD, syncPoint);
						break;
					}
				}
			}
		}

		this.transformerConcurrency = setting.getTransformerConcurrency() > 0 ? setting.getTransformerConcurrency() : 1;
		this.processorConcurrency = setting.getProcessorConcurrency() > 0 ? setting.getProcessorConcurrency() : 1;
		this.discardDDL = setting.getDiscardDDL();
		this.distinctWriteType = setting.getDistinctWriteType();
		this.maxTransactionLength = setting.getMaxTransactionLength();
		this.isSerialMode = setting.getIsSerialMode();
		this.cdcFetchSize = setting.getCdcFetchSize();
		this.readShareLogMode = setting.getReadShareLogMode();
		this.cdcConcurrency = setting.getCdcConcurrency();
		this.manuallyMinerConcurrency = setting.getManuallyMinerConcurrency();
		this.cdcShareFilterOnServer = setting.getCdcShareFilterOnServer();
		this.useCustomSQLParser = setting.getUseCustomSQLParser();
		this.oracleLogminer = setting.getOracleLogminer();
	}

	public void generateTestTableMappings() {
		List<Map<String, String>> joinConditions = new ArrayList<>(test_write.getCol_length());
		Map<String, String> condition = new HashMap<>(2);
		condition.put("source", TestWrite.PK_FEILD_NAME);
		condition.put("target", TestWrite.PK_FEILD_NAME);
		joinConditions.add(condition);

		Mapping mapping = new Mapping(TestWrite.TABLE_NAME, TestWrite.TABLE_NAME, joinConditions);
		mapping.setRelationship(ConnectorConstant.RELATIONSHIP_ONE_ONE);
		if (mappings == null) {
			mappings = new ArrayList<>(1);
		} else {
			mappings.clear();
		}
		mappings.add(mapping);
	}

	public long getLastStatsTimestamp() {
		return lastStatsTimestamp;
	}

	public void setLastStatsTimestamp(long lastStatsTimestamp) {
		this.lastStatsTimestamp = lastStatsTimestamp;
	}

	public long getLastNotificationTimestamp() {
		return lastNotificationTimestamp;
	}

	public void setLastNotificationTimestamp(long lastNotificationTimestamp) {
		this.lastNotificationTimestamp = lastNotificationTimestamp;
	}

	public int getProgressFailCount() {
		return progressFailCount;
	}

	public void setProgressFailCount(int progressFailCount) {
		this.progressFailCount = progressFailCount;
	}

	public long getNextProgressStatTS() {
		return nextProgressStatTS;
	}

	public void setNextProgressStatTS(long nextProgressStatTS) {
		this.nextProgressStatTS = nextProgressStatTS;
	}

	public int getTrigger_log_remain_time() {
		return trigger_log_remain_time;
	}

	public void setTrigger_log_remain_time(int trigger_log_remain_time) {
		this.trigger_log_remain_time = trigger_log_remain_time;
	}

	public int getTrigger_start_hour() {
		return trigger_start_hour;
	}

	public void setTrigger_start_hour(int trigger_start_hour) {
		this.trigger_start_hour = trigger_start_hour;
	}

	public void generateCloneMapping(Map<String, List<RelateDataBaseTable>> schema, List<Stage> stages) {
		logger.info("Starting generate clone mappings");
		List<RelateDataBaseTable> tables = schema.get("tables");
		if (CollectionUtils.isNotEmpty(tables)) {
			if (mappings == null) {
				mappings = new ArrayList<>();
			}
			Stage targetStage = DataFlowStageUtil.findTargetStageFromStages(stages);
			mappings.clear();
			List<String> syncTables = null;
			Map<String, Set<String>> syncMqDestinationMap = new HashMap<>();
			if (CollectionUtils.isNotEmpty(syncObjects)) {
				for (SyncObjects syncObject : syncObjects) {
					if (SyncObjects.TABLE_TYPE.equals(syncObject.getType())) {
						syncTables = syncObject.getObjectNames();
					} else if (SyncObjects.QUEUE.equals(syncObject.getType()) || SyncObjects.TOPIC.equals(syncObject.getType())) {
						syncMqDestinationMap.computeIfAbsent(syncObject.getType(), k -> new HashSet<>());
						syncMqDestinationMap.get(syncObject.getType()).addAll(syncObject.getObjectNames());
					}
				}
			}

			int loopCounter = 0;
			if (CollectionUtils.isNotEmpty(syncTables)) {
				new ExecutorUtil().queueMultithreading(syncTables, null, syncTable -> {
							RelateDataBaseTable table = ((SchemaList<String, RelateDataBaseTable>) tables).get(syncTable);
							Mapping mapping = null;
							if (null != table) {
								mapping = new Mapping(table, this.noPrimaryKey);
								setMappingProperties(stages, targetStage, mapping);
							}
							if (mapping != null) {
								if (targetStage != null) {
									mapping.setTo_table(Capitalized.convert(mapping.getTo_table(), targetStage.getTableNameTransform()));
								}
								mappings.add(mapping);
							} else {
								logger.warn("Cannot found table {} in source connection's schema.", syncTable);
							}
						}, "Generate-Clone-Mapping-" + (StringUtils.isNotBlank(this.getName()) ? this.getName() + "-" : "") + System.currentTimeMillis(),
						this, job -> !isRunning() && "v2".equalsIgnoreCase(this.transformModelVersion), this);
			} else if (MapUtils.isNotEmpty(syncMqDestinationMap)) {
				//组转queue和topic的mapping
				for (Map.Entry<String, Set<String>> entry : syncMqDestinationMap.entrySet()) {
					for (String tableName : entry.getValue()) {
						RelateDataBaseTable table = ((SchemaList<String, RelateDataBaseTable>) tables).get(tableName);
						Mapping mapping = null;
						if (null != table) {
							mapping = new Mapping(table, this.noPrimaryKey);
							//设置类型是topic还是queue
							mapping.setTableType(entry.getKey());
							setMappingProperties(stages, targetStage, mapping);
						}
						if (mapping != null) {
							if (targetStage != null) {
								mapping.setTo_table(Capitalized.convert(mapping.getTo_table(), targetStage.getTableNameTransform()));
							}
							mappings.add(mapping);
						} else {
							logger.warn("Cannot found table {} in source connection's schema.", tableName);
						}
					}
					if ((++loopCounter) % ConnectorConstant.LOOP_BATCH_SIZE == 0) {
						logger.info("Generate clone mappings progress: " + loopCounter + "/" + syncTables.size());
					}
				}

			} else {
				if (null != targetStage && !StringUtils.equalsAny(targetStage.getType(), Stage.StageTypeEnum.FILES_TYPE.getType(), Stage.StageTypeEnum.GRIDFS_TYPE.getType())) {
					throw new DataFlowException("sync objects cannot be empty.");
				}
			}
		}
		logger.info("Finish generate clone mappings: {}", mappings.size());
	}

	private void setMappingProperties(List<Stage> stages, Stage targetStage, Mapping mapping) {
		if (CollectionUtils.isNotEmpty(stages)) {
			mapping.setStages(stages);
		}
		if (targetStage != null) {
			if (StringUtils.isNotBlank(targetStage.getTablePrefix())) {
				mapping.setTo_table(targetStage.getTablePrefix().trim() + mapping.getTo_table());
			}

			if (StringUtils.isNotBlank(targetStage.getTableSuffix())) {
				mapping.setTo_table(mapping.getTo_table() + targetStage.getTableSuffix().trim());
			}

			if (StringUtils.isNotBlank(targetStage.getScript())) {
				//增加js处理节点
				addJsStage(targetStage);
			}

			String dropType = targetStage.getDropType();
			Stage.StageTypeEnum stageTypeEnum = Stage.StageTypeEnum.fromString(targetStage.getType());
			if (Stage.StageTypeEnum.DATABASE == stageTypeEnum && (Stage.DROP_TYPE_DROP_DATA.equals(dropType) || Stage.DROP_TYPE_DROP_SCHEMA.equals(dropType))) {
				mapping.setDropTarget(true);
			}

			mapping.setFieldsNameTransform(targetStage.getFieldsNameTransform());
		}
	}

	/**
	 * 库迁移支持js
	 *
	 * @param targetStage
	 * @return
	 */
	private void addJsStage(Stage targetStage) {
		Stage jsStage = new Stage();
		jsStage.setId(UUIDGenerator.uuid());
		//修改源的outputLanes 和设置script节点的inputLanes
		jsStage.setInputLanes(new ArrayList<>());
		for (Stage stage : this.stages) {
			if (stage.getOutputLanes().contains(targetStage.getId())) {
				stage.getOutputLanes().remove(targetStage.getId());
				stage.getOutputLanes().add(jsStage.getId());
				jsStage.getInputLanes().add(stage.getId());
				targetStage.getInputLanes().remove(stage.getId());
			}
		}
		targetStage.getInputLanes().add(jsStage.getId());
		jsStage.setOutputLanes(Lists.newArrayList(targetStage.getId()));
		jsStage.setName("JavaScript");
		jsStage.setType(Stage.StageTypeEnum.JS_PROCESSOR.getType());
		jsStage.setScript(targetStage.getScript());
		jsStage.setDisabled(false);
		this.stages.add(jsStage);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPriority() {
		return priority;
	}

	public void setPriority(String priority) {
		this.priority = priority;
	}

	public boolean isIs_changeStream_mode() {
		return is_changeStream_mode;
	}

	public void setIs_changeStream_mode(boolean is_changeStream_mode) {
		this.is_changeStream_mode = is_changeStream_mode;
	}

	public Integer getReadCdcInterval() {
		return readCdcInterval;
	}

	public void setReadCdcInterval(Integer readCdcInterval) {
		this.readCdcInterval = readCdcInterval;
	}

	public Boolean getStopOnError() {
		return stopOnError;
	}

	public void setStopOnError(Boolean stopOnError) {
		this.stopOnError = stopOnError;
	}

	public Map<String, Object> getRow_count() {
		return row_count;
	}

	public void setRow_count(Map<String, Object> row_count) {
		this.row_count = row_count;
	}

	public Map<String, Object> getTs() {
		return ts;
	}

	public void setTs(Map<String, Object> ts) {
		this.ts = ts;
	}

	public String getDataFlowId() {
		return dataFlowId;
	}

	public void setDataFlowId(String dataFlowId) {
		this.dataFlowId = dataFlowId;
	}

	public String getExecuteMode() {
		return executeMode;
	}

	public void setExecuteMode(String executeMode) {
		this.executeMode = executeMode;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public Integer getDebugOrder() {
		return debugOrder;
	}

	public void setDebugOrder(Integer debugOrder) {
		this.debugOrder = debugOrder;
	}

	public Job getPreviousJob() {
		return previousJob;
	}

	public void setPreviousJob(Job previousJob) {
		this.previousJob = previousJob;
	}

	public boolean isCopyManagerOpen() {
		return copyManagerOpen;
	}

	public void setCopyManagerOpen(boolean copyManagerOpen) {
		this.copyManagerOpen = copyManagerOpen;
	}

	public boolean getNeedToCreateIndex() {
		return needToCreateIndex;
	}

	public void setNeedToCreateIndex(boolean needToCreateIndex) {
		this.needToCreateIndex = needToCreateIndex;
	}

	public List<Stage> getStages() {
		return stages;
	}

	public void setStages(List<Stage> stages) {
		this.stages = stages;
	}

	public int getCdcCommitOffsetInterval() {
		return cdcCommitOffsetInterval;
	}

	public void setCdcCommitOffsetInterval(int cdcCommitOffsetInterval) {
		this.cdcCommitOffsetInterval = cdcCommitOffsetInterval;
	}

	public boolean isEditDebug() {
		if (executeMode == null) {
			return false;
		}
		return executeMode.equals(EDIT_DEBUG);
	}

	public boolean isRunningDebug() {
		if (executeMode == null) {
			return false;
		}
		return executeMode.equals(RUNNING_DEBUG);
	}

	public boolean isDebug() {
		return isEditDebug() || isRunningDebug();
	}

	public String getDbhistoryStr() {
		return dbhistoryStr;
	}

	public void setDbhistoryStr(String dbhistoryStr) {
		this.dbhistoryStr = dbhistoryStr;
	}

	public Long getTimingTargetOffsetInterval() {
		return timingTargetOffsetInterval;
	}

	public void setTimingTargetOffsetInterval(Long timingTargetOffsetInterval) {
		this.timingTargetOffsetInterval = timingTargetOffsetInterval;
	}

	public boolean isReset() {
		return reset;
	}

	public void setReset(boolean reset) {
		this.reset = reset;
	}

	public int getProcessorConcurrency() {
		return processorConcurrency;
	}

	public void setProcessorConcurrency(int processorConcurrency) {
		this.processorConcurrency = processorConcurrency;
	}

	public boolean getIsDistribute() {
		return isDistribute;
	}

	public void setIsDistribute(boolean isDistribute) {
		this.isDistribute = isDistribute;
	}

	public String getProcess_id() {
		return process_id;
	}

	public void setProcess_id(String process_id) {
		this.process_id = process_id;
	}

	public String getDiscardDDL() {
		return discardDDL;
	}

	public void setDiscardDDL(String discardDDL) {
		this.discardDDL = discardDDL;
	}

	public Long getCdcLagWarnSendMailLastTime() {
		return cdcLagWarnSendMailLastTime;
	}

	public void setCdcLagWarnSendMailLastTime(Long cdcLagWarnSendMailLastTime) {
		this.cdcLagWarnSendMailLastTime = cdcLagWarnSendMailLastTime;
	}

	public String getDistinctWriteType() {
		return distinctWriteType;
	}


	public void setDistinctWriteType(String distinctWriteType) {
		this.distinctWriteType = distinctWriteType;
	}

	public Double getMaxTransactionLength() {
		return maxTransactionLength;
	}

	public void setMaxTransactionLength(Double maxTransactionLength) {
		this.maxTransactionLength = maxTransactionLength;
	}

	public void jobError() {
		this.status = ConnectorConstant.ERROR;
	}

	/**
	 * @param throwable
	 * @param forceError   true: 会忽略任何逻辑，直接停止任务，并且状态是错误，返回值永远为false
	 *                     false: 会判断stop on error，是否跳过等逻辑，返回是否需要停止任务
	 * @param logger
	 * @param syncStage    {@link SyncStageEnum}
	 * @param workerType   {@link ConnectorConstant#WORKER_TYPE_CONNECTOR} {@link ConnectorConstant#WORKER_TYPE_TRANSFORMER}
	 * @param errorMessage
	 * @param warnMessage
	 * @param args
	 * @return true: 该错误可以跳过，外部逻辑应该继续运行
	 * false: 该错误已经导致任务异常停止，外部逻辑应该处理停止和清理的逻辑后退出
	 */
	public synchronized boolean jobError(Throwable throwable, boolean forceError, String syncStage, Logger logger, String workerType,
										 String errorMessage, String warnMessage,
										 Object... args) {

		// init
		if (throwable == null) {
			throw new IllegalArgumentException("Throwable cannot be empty");
		}

		errorMessage = StringUtils.isBlank(errorMessage) ? throwable.getMessage() : errorMessage;
		warnMessage = StringUtils.isBlank(warnMessage) ? errorMessage : warnMessage;
		syncStage = initSyncStage(syncStage);
		if (args.length > 0) {
			try {
				errorMessage = String.format(errorMessage.replaceAll("\\{\\}", "%s"), args);
				warnMessage = String.format(warnMessage.replaceAll("\\{\\}", "%s"), args);
				warnMessage += String.format(", stack: %s", Log4jUtil.getStackString(throwable));
			} catch (Exception ignore) {

			}
		}
		logger = logger == null ? LogManager.getLogger(Job.class) : logger;
		if (!isRunning()) {
			logger.error(errorMessage + "; Will stop job", throwable);
			return false;
		}

		// stop on error
		if (!this.stopOnError && !forceError) {
			logger.warn(warnMessage);
			return true;
		}

		String type = throwable.getClass().getName();
		String loggerName = logger == null ? "" : logger.getName();
		if (StringUtils.isBlank(workerType) && StringUtils.equalsAny(workerType,
				ConnectorConstant.WORKER_TYPE_CONNECTOR, ConnectorConstant.WORKER_TYPE_TRANSFORMER)) {
			logger.error(errorMessage, throwable);
			return false;
		}

		List<ErrorEvent> errorEvents = null;

		if (workerType.equals(ConnectorConstant.WORKER_TYPE_CONNECTOR)) {
			this.connectorErrorEvents = this.connectorErrorEvents == null ? new ArrayList<>() : this.connectorErrorEvents;
			errorEvents = this.connectorErrorEvents;
			syncStage = StringUtils.isBlank(syncStage) && StringUtils.isNotBlank(this.connectorLastSyncStage) ? this.connectorLastSyncStage : syncStage;
			if (StringUtils.isNoneBlank(this.connectorLastSyncStage, syncStage)
					&& !this.connectorLastSyncStage.equals(syncStage)) {
				this.connectorLastSyncStage = syncStage;
			}
		} else if (workerType.equals(ConnectorConstant.WORKER_TYPE_TRANSFORMER)) {
			this.transformerErrorEvents = this.transformerErrorEvents == null ? new ArrayList<>() : this.transformerErrorEvents;
			errorEvents = this.transformerErrorEvents;
			syncStage = StringUtils.isBlank(syncStage) && StringUtils.isNotBlank(this.transformerLastSyncStage) ? this.transformerLastSyncStage : syncStage;
			if (StringUtils.isNoneBlank(this.transformerLastSyncStage, syncStage)
					&& !this.transformerLastSyncStage.equals(syncStage)) {
				this.transformerLastSyncStage = syncStage;
			}
		}
		errorEvents = errorEvents == null ? new ArrayList<>() : errorEvents;

		if (errorEvents.size() > 10) {
			int difference = errorEvents.size() - 10;
			for (int i = 0; i < difference; i++) {
				errorEvents.remove(0);
			}
		}

		if (CollectionUtils.isEmpty(errorEvents) || forceError) {
			// no exists error events or force stop error
			// 1. add error event in list
			// 2. log error & stop job with error status
			addErrorEvent(throwable, type, loggerName, errorMessage, errorEvents);
			logger.error(errorMessage + "; Will stop job", throwable);
			jobError();
			if (jobErrorNotifier != null) {
				jobErrorNotifier.accept(throwable, errorMessage);
			}
			return false;
		} else {
			String finalNewMessage = errorMessage;
			// find if error event is exists
			ErrorEvent hitErrorEvent = errorEvents.stream().filter(event ->
					event.getLoggerName().equals(loggerName) &&
							event.getMessage().equals(finalNewMessage) &&
							event.getType().equals(type)
			).findFirst().orElse(null);

			if (hitErrorEvent != null) {
				// find exists error event
				// skip this error & set hit=true
				logger.warn(warnMessage + "; Will skip it");
				hitErrorEvent.setHit(true);
				return true;
			} else {
				// not find exists error event
				// 1. add error event in list
				// 2. log error & stop job with error status
				logger.error(errorMessage + "; Will stop job", throwable);
				jobError();
				addErrorEvent(throwable, type, loggerName, errorMessage, errorEvents);
				if (jobErrorNotifier != null) {
					jobErrorNotifier.accept(throwable, errorMessage);
				}
			}
		}
		return false;
	}

	private void notifyError(Throwable throwable, String errorMessage) {

	}

	private String initSyncStage(String syncStage) {
		if (StringUtils.isBlank(syncStage)) {
			if (this.offset instanceof TapdataOffset) {
				syncStage = ((TapdataOffset) this.offset).getSyncStage();
			} else {
				syncStage = TapdataOffset.SYNC_STAGE_SNAPSHOT;
			}
		}
		return syncStage;
	}

	private synchronized void addErrorEvent(Throwable throwable, String type, String loggerName, String message, List<ErrorEvent> errorEvents) {
		if (throwable == null || StringUtils.isAnyBlank(type, loggerName, message) || errorEvents == null) {
			return;
		}

		if (errorEvents.stream().filter(event -> event.getType().equals(type)
				&& event.getLoggerName().equals(loggerName)
				&& event.getMessage().equals(message)).findFirst().orElse(null) != null) {
			return;
		}

		String[] stackStrings = Log4jUtil.getStackStrings(throwable);
		ErrorEvent errorEvent = new ErrorEvent(
				message, stackStrings, this.id, type, loggerName
		);

		errorEvents.add(errorEvent);
	}

	public String getJobInfo() {
		List<String> mappingItems = new ArrayList<>();
		for (Mapping mapping : mappings) {
			List<String> items = new ArrayList<>();
			items.add(mapping.getFrom_table());
			items.add(mapping.getTo_table());
			mappingItems.add(StringUtils.join(items, " -> "));
		}
		return StringUtils.join(mappingItems, "\n");
	}


	public boolean isRunning() {
		return ConnectorConstant.RUNNING.equals(this.status)
				&& !Thread.currentThread().isInterrupted();
	}

	public List<SyncObjects> getSyncObjects() {
		return syncObjects;
	}

	public void setSyncObjects(List<SyncObjects> syncObjects) {
		this.syncObjects = syncObjects;
	}

	public boolean getKeepSchema() {
		return keepSchema;
	}

	public void setKeepSchema(boolean keepSchema) {
		this.keepSchema = keepSchema;
	}

	public boolean getIsSerialMode() {
		return isSerialMode;
	}

	public void setIsSerialMode(boolean serialMode) {
		isSerialMode = serialMode;
	}

	public List<ErrorEvent> getConnectorErrorEvents() {
		return connectorErrorEvents;
	}

	public void setConnectorErrorEvents(List<ErrorEvent> connectorErrorEvents) {
		this.connectorErrorEvents = connectorErrorEvents;
	}

	public List<ErrorEvent> getTransformerErrorEvents() {
		return transformerErrorEvents;
	}

	public void setTransformerErrorEvents(List<ErrorEvent> transformerErrorEvents) {
		this.transformerErrorEvents = transformerErrorEvents;
	}

	/**
	 * stats source object count
	 *
	 * @param connection
	 */
	public void setStatsTotalCount(Connections connection, Object instance) {

		if (connection == null || CollectionUtils.isEmpty(getMappings()) || stats == null || instance == null) {
			return;
		}
		logger.info("Start stat table(s) data count");
		List<Map<String, Object>> totalCount = new ArrayList<>();
		ExecutorUtil executorUtil = new ExecutorUtil();
		executorUtil.queueMultithreading(getMappings(),
				mapping -> !CollectionUtils.isEmpty(mapping.getStages()),
				mapping -> {
					try {
						long dataCount = getDataCount(connection, mapping.getFrom_table(), instance, mapping);

						Stage filterStage = mapping.getStages().stream()
								.filter(stage -> DataFlowStageUtil.isDataStage(stage.getType())
										&& StringUtils.isNotBlank(stage.getConnectionId())
										&& stage.getConnectionId().equals(connections.getSource()))
								.findFirst().orElse(null);

						if (filterStage != null) {
							Map<String, Object> filterCountStageId = totalCount.stream().filter(count -> count.get("stageId").equals(filterStage.getId())).findFirst().orElse(null);
							if (filterCountStageId == null) {
								Map<String, Object> map = new HashMap<>();
								map.put("stageId", filterStage.getId());
								map.put("dataCount", dataCount);
								totalCount.add(map);
							} else {
								filterCountStageId.put("dataCount", Long.valueOf(filterCountStageId.get("dataCount") + "") + dataCount);
							}
						}
						stats.getInitialStats().forEach(countStat -> {
							if (countStat.getSourceConnectionId().equals(connection.getId()) && countStat.getSourceTableName().equals(mapping.getFrom_table())) {
								countStat.setSourceRowNum(dataCount);
							}
						});
					} catch (Exception e) {
						if (this.isRunning()) {
							this.jobError(e, true, SyncStageEnum.SNAPSHOT.getSyncStage(), logger, ConnectorConstant.WORKER_TYPE_CONNECTOR,
									"Stat table count failed, connection name: " + connection.getName() + ", table name: " + mapping.getFrom_table() + ": " + e.getMessage(), null);
						} else {
							throw new ExecutorUtil.InterruptExecutorException();
						}
					}
				}, "Stat table count", this, job -> !job.isRunning(), this);
		stats.setTotalCount(totalCount);
	}

	private long getDataCount(Connections connection, String objectName, Object instance, Mapping mapping) {

		long startTs = System.currentTimeMillis();
		try {
			Object count = ReflectUtil.invokeInterfaceMethod(
					instance, "io.tapdata.BaseExtend;io.tapdata.ConnectorExtend;io.tapdata.TargetExtend", "count",
					objectName, connection, mapping
			);

			if (count instanceof Long) {
				if (logger.isDebugEnabled()) {
					logger.debug("Table total count finish, connection name: {}, table name: {}, count: {}, spend: {} ms",
							connection.getName(), objectName, count, (System.currentTimeMillis() - startTs));
				}
				return (long) count;
			} else {
				throw new RuntimeException("Database type " + connection.getDatabase_type() + " does not support count");
			}
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e.getTargetException().getMessage() + "\n  " + Log4jUtil.getStackString(e.getTargetException()), e.getTargetException());
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Total count failed, table name: " + objectName
					+ ", connection name: " + connection.getName()
					+ ", err: " + e.getMessage() + "\n  " + Log4jUtil.getStackString(e));
		}
	}

	/**
	 * @param source
	 * @param clientMongoOperator 中间库访问数据层
	 * @param sourceConn          缓存节点时，需要，其余情况可以传null
	 * @return
	 */
	public Connections getConn(boolean source, ClientMongoOperator clientMongoOperator, Connections sourceConn) {
		if (null == this.connections || null == clientMongoOperator) {
			throw new IllegalArgumentException();
		}
		String connId;
		Connections connections;

		if (source) {
			connId = this.connections.getSource();
		} else {
			connId = this.connections.getTarget();
		}

		Query query = new Query(Criteria.where("_id").is(connId));
		query.fields().exclude("schema");

		if (!source) {
			// 缓存目标节点
			boolean cacheTarget = this.connections.getCacheTarget();
			if (cacheTarget) {
				connections = Connections.cacheConnection(sourceConn, this.stages);
			} else {
				connections = MongodbUtil.getConnections(query, clientMongoOperator, true);
			}
		} else {
			connections = MongodbUtil.getConnections(query, clientMongoOperator, true);
		}

		return connections;
	}

	public Connections getConn(boolean source, ClientMongoOperator clientMongoOperator) {
		return getConn(source, clientMongoOperator, null);
	}

	public String getConnectorLastSyncStage() {
		return connectorLastSyncStage;
	}

	public void setConnectorLastSyncStage(String connectorLastSyncStage) {
		this.connectorLastSyncStage = connectorLastSyncStage;
	}

	public String getTransformerLastSyncStage() {
		return transformerLastSyncStage;
	}

	public void setTransformerLastSyncStage(String transformerLastSyncStage) {
		this.transformerLastSyncStage = transformerLastSyncStage;
	}

	public List<Milestone> getMilestones() {
		return milestones;
	}

	public void setMilestones(List<Milestone> milestones) {
		this.milestones = milestones;
	}

	public boolean needInitTargetDB() {
		return isInitialOffset() || hasAddMapping();
	}

	public boolean needInitial() {
		return checkSyncTypeAndOffsetNeedInitial() || hasAddInitialMapping();
	}

	public boolean onlyInitialAddMapping() {
		return !checkSyncTypeAndOffsetNeedInitial() && hasAddInitialMapping();
	}

	public boolean checkSyncTypeAndOffsetNeedInitial() {
		return isInitialSyncType() && isInitialOffset();
	}

	private boolean isInitialOffset() {
		boolean result = true;
		if (offset != null) {
			if (increment && CollectionUtils.isNotEmpty(mappings) &&
					mappings.stream().filter(mapping -> StringUtils.isNotBlank(mapping.getCustom_sql()) && mapping.getCustom_sql().contains("${OFFSET1}")).findFirst().orElse(null) != null) {

				// custom sql cdc
				result = false;
			} else {
				result = OffsetUtil.getSyncStage(offset).equals(TapdataOffset.SYNC_STAGE_SNAPSHOT);
			}
		}
		return result;
	}

	private boolean isInitialSyncType() {
		return StringUtils.equalsAnyIgnoreCase(sync_type, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC, ConnectorConstant.SYNC_TYPE_INITIAL_SYNC_CDC);
	}

	public boolean hasInitialOffset() {
		boolean result = false;

		if (offset instanceof Map && ((Map<?, ?>) offset).containsKey("offset")) {
			Object jobOffset = ((Map<?, ?>) offset).get("offset");
			if (jobOffset instanceof Map && ((Map<?, ?>) jobOffset).containsKey("snapshotOffset")) {
				Object snapshotOffset = ((Map<?, ?>) jobOffset).getOrDefault("snapshotOffset", null);
				if (snapshotOffset instanceof Boolean) {
					result = (boolean) snapshotOffset;
				}
			}
		}

		return result;
	}

	public boolean hasAddInitialMapping() {
		return getMappings().stream().anyMatch(Mapping::checkAddInitialMapping);
	}

	public boolean hasAddMapping() {
		return mappings.stream().anyMatch(Mapping::isAddMapping);
	}

	public List<Mapping> getAddInitialMapping() {
		return getMappings().stream().filter(Mapping::checkAddInitialMapping).collect(Collectors.toList());
	}

	public void finishAddInitialMapping(ClientMongoOperator clientMongoOperator) {
		getMappings().forEach(mapping -> {
			if (mapping.checkAddInitialMapping()) {
				mapping.setAddInitialMappingFinish(true);
			}
		});

		Query query = new Query(Criteria.where("id").is(id));
		Update update = new Update().set("mappings", getMappings());
		clientMongoOperator.update(query, update, ConnectorConstant.JOB_COLLECTION);
	}

	public int getCdcFetchSize() {
		return cdcFetchSize;
	}

	public void setCdcFetchSize(int cdcFetchSize) {
		this.cdcFetchSize = cdcFetchSize;
	}

	public int getOracleLogSqlParseConcurrency() {
		return oracleLogSqlParseConcurrency;
	}

	public void setOracleLogSqlParseConcurrency(int oracleLogSqlParseConcurrency) {
		this.oracleLogSqlParseConcurrency = oracleLogSqlParseConcurrency;
	}

	public ReadShareLogMode getReadShareLogMode() {
		return readShareLogMode;
	}

	public void setReadShareLogMode(ReadShareLogMode readShareLogMode) {
		this.readShareLogMode = readShareLogMode;
	}

	public boolean isUseCustomSQLParser() {
		return useCustomSQLParser;
	}

	public void setUseCustomSQLParser(boolean useCustomSQLParser) {
		this.useCustomSQLParser = useCustomSQLParser;
	}

	public boolean getCdcConcurrency() {
		return cdcConcurrency;
	}

	public void setCdcConcurrency(boolean cdcConcurrency) {
		this.cdcConcurrency = cdcConcurrency;
	}

	public boolean getCdcShareFilterOnServer() {
		return cdcShareFilterOnServer;
	}

	public void setCdcShareFilterOnServer(boolean cdcShareFilterOnServer) {
		this.cdcShareFilterOnServer = cdcShareFilterOnServer;
	}

	public boolean getTimeoutToStop() {
		return timeoutToStop;
	}

	public void setTimeoutToStop(boolean timeoutToStop) {
		this.timeoutToStop = timeoutToStop;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}

	public boolean getNoPrimaryKey() {
		return noPrimaryKey;
	}

	public void setNoPrimaryKey(boolean noPrimaryKey) {
		this.noPrimaryKey = noPrimaryKey;
	}

	public boolean isOnlyInitialAddMapping() {
		return onlyInitialAddMapping;
	}

	public void setOnlyInitialAddMapping(boolean onlyInitialAddMapping) {
		this.onlyInitialAddMapping = onlyInitialAddMapping;
	}

	public String getPartitionId() {
		return partitionId;
	}

	public void setPartitionId(String partitionId) {
		this.partitionId = partitionId;
	}

	public ClientMongoOperator getClientMongoOperator() {
		return clientMongoOperator;
	}

	public void setClientMongoOperator(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
	}

	public List<List<Stage>> getSameJobStages() {
		return sameJobStages;
	}

	public void setSameJobStages(List<List<Stage>> sameJobStages) {
		this.sameJobStages = sameJobStages;
	}

	public String getOracleLogminer() {
		return oracleLogminer;
	}

	public void setOracleLogminer(String oracleLogminer) {
		this.oracleLogminer = oracleLogminer;
	}

	public String getSubTaskId() {
		return subTaskId;
	}

	public void setSubTaskId(String subTaskId) {
		this.subTaskId = subTaskId;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public void setJobErrorNotifier(ErrorNotifier<Throwable, String> jobErrorNotifier) {
		this.jobErrorNotifier = jobErrorNotifier;
	}

	public String getTransformModelVersion() {
		return transformModelVersion;
	}

	public void setTransformModelVersion(String transformModelVersion) {
		this.transformModelVersion = transformModelVersion;
	}

	public int getManuallyMinerConcurrency() {
		return manuallyMinerConcurrency;
	}

	public void setManuallyMinerConcurrency(int manuallyMinerConcurrency) {
		this.manuallyMinerConcurrency = manuallyMinerConcurrency;
	}

	public String getOffsetStr() {
		return offsetStr;
	}

	public void setOffsetStr(String offsetStr) {
		this.offsetStr = offsetStr;
	}

	@Override
	public String toString() {
		return "Job{" +
				"startupTime=" + startupTime +
				", id='" + id + '\'' +
				", name='" + name + '\'' +
				", priority='" + priority + '\'' +
				", first_ts=" + first_ts +
				", last_ts=" + last_ts +
				", user_id='" + user_id + '\'' +
				", connections=" + connections +
				", deployment=" + deployment +
				", mapping_template='" + mapping_template + '\'' +
				", status='" + status + '\'' +
				", source=" + source +
				", offset=" + offset +
				", fullSyncSucc=" + fullSyncSucc +
				", event_job_editted=" + event_job_editted +
				", event_job_error=" + event_job_error +
				", event_job_started=" + event_job_started +
				", event_job_stopped=" + event_job_stopped +
				", dataFlowId='" + dataFlowId + '\'' +
				", warning=" + warning +
				", progressRateStats=" + progressRateStats +
				", stats=" + stats +
				", connector_ping_time=" + connector_ping_time +
				", ping_time=" + ping_time +
				", dbhistory=" + dbhistory +
				", dbhistoryStr='" + dbhistoryStr + '\'' +
				", process_offset=" + process_offset +
				", is_validate=" + is_validate +
				", validate_offset=" + validate_offset +
				", tableMappings=" + tableMappings +
				", testTableMappings=" + testTableMappings +
				", syncObjects=" + syncObjects +
				", keepSchema=" + keepSchema +
				", transformerConcurrency=" + transformerConcurrency +
				", processorConcurrency=" + processorConcurrency +
				", oracleLogSqlParseConcurrency=" + oracleLogSqlParseConcurrency +
				", sync_type='" + sync_type + '\'' +
				", op_filters=" + op_filters +
				", running_mode='" + running_mode + '\'' +
				", sampleRate=" + sampleRate +
				", is_test_write=" + is_test_write +
				", test_write=" + test_write +
				", is_null_write=" + is_null_write +
				", lastStatsTimestamp=" + lastStatsTimestamp +
				", drop_target=" + drop_target +
				", increment=" + increment +
				", connectorStopped=" + connectorStopped +
				", transformerStopped=" + transformerStopped +
				", needToCreateIndex=" + needToCreateIndex +
				", notification_window=" + notification_window +
				", notification_interval=" + notification_interval +
				", lastNotificationTimestamp=" + lastNotificationTimestamp +
				", isCatchUpLag=" + isCatchUpLag +
				", lagCount=" + lagCount +
				", stopWaitintMills=" + stopWaitintMills +
				", progressFailCount=" + progressFailCount +
				", nextProgressStatTS=" + nextProgressStatTS +
				", trigger_log_remain_time=" + trigger_log_remain_time +
				", trigger_start_hour=" + trigger_start_hour +
				", is_changeStream_mode=" + is_changeStream_mode +
				", readBatchSize=" + readBatchSize +
				", readCdcInterval=" + readCdcInterval +
				", stopOnError=" + stopOnError +
				", row_count=" + row_count +
				", ts=" + ts +
				", dataQualityTag='" + dataQualityTag + '\'' +
				", executeMode='" + executeMode + '\'' +
				", limit=" + limit +
				", debugOrder=" + debugOrder +
				", previousJob=" + previousJob +
				", copyManagerOpen=" + copyManagerOpen +
				", isOpenAutoDDL=" + isOpenAutoDDL +
				", lastDdlTimes=" + lastDdlTimes +
				", runtimeInfo=" + runtimeInfo +
				", isSchedule=" + isSchedule +
				", cronExpression='" + cronExpression + '\'' +
				", nextSyncTime=" + nextSyncTime +
				", cdcCommitOffsetInterval=" + cdcCommitOffsetInterval +
				", includeTables=" + includeTables +
				", timingTargetOffsetInterval=" + timingTargetOffsetInterval +
				", reset=" + reset +
				", isDistribute=" + isDistribute +
				", process_id='" + process_id + '\'' +
				", discardDDL='" + discardDDL + '\'' +
				", cdcLagWarnSendMailLastTime=" + cdcLagWarnSendMailLastTime +
				", distinctWriteType='" + distinctWriteType + '\'' +
				", maxTransactionLength=" + maxTransactionLength +
				", isSerialMode=" + isSerialMode +
				", connectorErrorEvents=" + connectorErrorEvents +
				", transformerErrorEvents=" + transformerErrorEvents +
				", connectorLastSyncStage='" + connectorLastSyncStage + '\'' +
				", transformerLastSyncStage='" + transformerLastSyncStage + '\'' +
				", cdcFetchSize=" + cdcFetchSize +
				", milestones=" + milestones +
				", readShareLogMode=" + readShareLogMode +
				", cdcConcurrency=" + cdcConcurrency +
				", cdcShareFilterOnServer=" + cdcShareFilterOnServer +
				", timeoutToStop=" + timeoutToStop +
				", chunkSize=" + chunkSize +
				", noPrimaryKey=" + noPrimaryKey +
				", onlyInitialAddMapping=" + onlyInitialAddMapping +
				", partitionId='" + partitionId + '\'' +
				", transformModelVersion='" + transformModelVersion + '\'' +
				'}';
	}

	public interface ErrorNotifier<T, N> {
		void accept(T t, N n);
	}

	public enum EngineVersion {
		V1,
		V2
	}

	public EngineVersion getEngineVersion() {
		return engineVersion;
	}

	public void setEngineVersion(EngineVersion engineVersion) {
		this.engineVersion = engineVersion;
	}
}
