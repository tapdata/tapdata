/*
 * Copyright (c) 2016, 2025, HHLY and/or its affiliates. All rights reserved.
 * HHLY PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */
package com.tapdata.constant;

import com.tapdata.mongo.ClientMongoOperator;

import java.text.SimpleDateFormat;

/**
 * @author huangjq
 * @ClassName: ConnectorConstant
 * @Description: 常量
 * @date 2017年5月9日 下午4:26:04
 * @since 1.0
 */
public class ConnectorConstant {

	/**
	 * DML操作的event
	 */
	public static final long DML_EVENT = 2;

	public static final String SCHEDULED = "scheduled";
	public static final String DRAFT = "draft";

	public static final String RUNNING = "running";

	public static final String PAUSED = "paused";

	public static final String ERROR = "error";

	public static final String STOPPING = "stopping";

	public static final String FORCE_STOPPING = "force stopping";

	/**
	 * Manager api collection(endpoint)
	 */
	public static final String JOB_COLLECTION = "Jobs";

	public static final String LOG_COLLECTION = "Logs";

	public static final String CUSTOMER_LOG_COLLECTION = "CustomerJobLogs";

	public static final String EVENT_COLLECTION = "Events";

	public static final String USER_COLLECTION = "user";

	public static final String INCONSISTENT_DATA_COLLECTION = "InconsistentData";

	public static final String CONNECTION_COLLECTION = "Connections";

	public static final String WORKER_COLLECTION = "Workers";

	public static final String SETTING_COLLECTION = "Settings";
	public static final String METADATA_INSTANCE_COLLECTION = "MetadataInstances";
	public static final String DATA_CATALOG_COLLECTION = "DataCatalog";
	public static final String INSIGHTS_COLLECTION = "Insights";
	public static final String MODULES_COLLECTION = "Modules";
	public static final String API_CALL_COLLECTION = "ApiCall";
	public static final String DATA_FLOW_COLLECTION = "DataFlows";
	public static final String DATA_FLOW_INSIGHT_COLLECTION = "DataFlowInsights";
	public static final String VALIDATION_RESULT_COLLECTION = "ValidationResults";
	public static final String DELETE_CACHE_COLLECTION = "DeleteCaches";
	public static final String INSPECT_COLLECTION = "Inspects";
	public static final String INSPECT_RESULT_COLLECTION = "InspectResults";
	public static final String INSPECT_DETAILS_COLLECTION = "InspectDetails";
	public static final String AUTO_INSPECT_RESULTS_COLLECTION = "AutoInspectResults";

	public static final String MESSAGE_COLLECTION = "Messages";
	public static final String JOB_DDL_HISTORIES_COLLECTION = "JobDDLHistories";
	public static final String TYPE_MAPPINGS_COLLECTION = "TypeMappings";
	public static final String TASK_COLLECTION = "Task";
	public static final String TASK_NODE = "node";
	public static final String METRICS_COLLECTION = "Metrics";
	public static final String CUSTOMNODETEMP_COLLECTION = "customNode";

	/**
	 * This is for both statistics and samples report, these two values share one api
	 */
	public static final String SAMPLE_STATISTIC_COLLECTION = "measurement";

	/**
	 * This is for storing agent info, actually in db, it is saved in collection "AgentStatistics"
	 */
	public static final String AGENT_INFO_COLLECTION = "agentEnvironment";

	public static final String JOB_STATUS_FIELD = "status";

	public static final String JOB_CONNECTOR_STOPPED_FIELD = "connectorStopped";

	public static final String JOB_TRANSFORMER_STOPPED_FIELD = "transformerStopped";

	public static final String JOB_CONNECTOR_PING_TIME_FIELD = "connector_ping_time";

	public static final String JOB_PING_TIME_FIELD = "ping_time";

	public static final String DATABASE_TYPE_COLLECTION = "DatabaseTypes";

	public static final String LIB_SUPPORTEDS_COLLECTION = "LibSupporteds";

	public static final String SCHEDULE_TASK_COLLECTION = "ScheduleTasks";

	public static final String TASK_HISTORY_COLLECTION = "TaskHistories";
	public static final String JAVASCRIPT_FUNCTION_COLLECTION = "Javascript_functions";
	public static final String CDC_EVENTS_COLLECTION = "CdcEvents";
	public static final String METADATA_HISTROY_COLLECTION = "metadata/history";
	public static final String SUBTASK_PROGRESS = "TaskProgress";

	public static final String OFFSET_MONGO_SERVER_URI = "offset.mongo.server.uri";

	public static final String DATABASE_HISTORY_MONGO_SERVER_URI = "database.history.mongo.server.uri";

	private static final String CONFIGURATION_FIELD_PREFIX_STRING = "database.history.";

	public static final String DATABASE_HISTORY_BASE_URL = CONFIGURATION_FIELD_PREFIX_STRING + "baseURL";

	public static final String DATABASE_HISTORY_ACCESS_CODE = CONFIGURATION_FIELD_PREFIX_STRING + "accessCode";

	public static final String DATABASE_HISTORY_REST_RETRY_TIME = CONFIGURATION_FIELD_PREFIX_STRING + "restRetryTime";

	public static final String DATABASE_HISTORY_USER_ID = CONFIGURATION_FIELD_PREFIX_STRING + "userId";

	public static final String DATABASE_HISTORY_ROLE_ID = CONFIGURATION_FIELD_PREFIX_STRING + "roleId";

	public final static String LOOKUP_TABLE_SUFFIX = "_TPORIG";

	public final static String LOOKUP_TABLE_AGG_SUFFIX = "_TP_AGG_ORIG";

	public final static String DATA_VERSION_SEQ = "DATA_VERSION_SEQ" + LOOKUP_TABLE_SUFFIX;

	public final static String DATAFLOW_ID = "dataFlowId";

	/**
	 * connection type
	 */
	public static final String CONNECTION_TYPE_SOURCE = "source";
	public static final String CONNECTION_TYPE_TARGET = "target";
	public static final String CONNECTION_TYPE_SOURCE_TARGET = "source_and_target";

	/**
	 * sync point
	 */

	public static final String SYNC_POINT_FIELD = "sync_point";

	public static final String SYNC_TIME_FIELD = "sync_time";
	public static final String SYNC_TIME_TS_FIELD = "sync_time_ts";

	public static final String SYNC_POINT_CURRENT = "current";

	public static final String SYNC_POINT_SYNC_TIME = "sync_time";

	public static final String SYNC_POINT_BEGINNING = "beginning";

	public static final String TIMEZONE = "timezone";

	public static final String SYNC_POINTS_FIELD = "syncPoints";

	public static final String SYNC_POINTS_TYPE_LOCALTZ = "localTZ";

	public static final String SYNC_POINTS_TYPE_CURRENT = "current";

	public static final String SYNC_POINTS_TYPE_CONNTZ = "connTZ";

	/**
	 * =================== relationship =====================
	 */

	public static final String RELATIONSHIP_ONE_ONE = "OneOne";
	public static final String RELATIONSHIP_MANY_ONE = "ManyOne";
	public static final String RELATIONSHIP_ONE_MANY = "OneMany";
	public static final String RELATIONSHIP_APPEND = "Append";

	/**
	 * =================== mapping_template =====================
	 */
	public static final String MAPPING_TEMPLATE_CUSTOM = "custom";

	public static final String MAPPING_TEMPLATE_CLUSTER_CLONE = "cluster-clone";


	/**
	 * =================== worker type =====================
	 */

	public static final String WORKER_TYPE_CONNECTOR = "connector";
	public static final String WORKER_TYPE_TRANSFORMER = "transformer";
	public static final String WORKER_TARGET_MONGODB_STATUS = "MongoDB";
	public static final String WORKER_TYPE_DATAFLOW = "dataflow";


	/**
	 * =================== primary key =====================
	 */
	public static final String SCHEMA_PRIMARY_KEY = "PRI";

	public static final String APP_TRANSFORMER = "transformer";

	public static final String MESSAGE_OPERATION_INSERT = "i";
	public static final String MESSAGE_OPERATION_DELETE = "d";
	public static final String MESSAGE_OPERATION_UPDATE = "u";
	public static final String MESSAGE_OPERATION_DDL = "ddl";
	//	public static final String MESSAGE_OPERATION_COMMIT_OFFSET = "commit_offset";
	public static final String MESSAGE_OPERATION_ABSOLUTE_INSERT = "abi";
	public static final String MESSAGE_OPERATION_SWITCH_CUSTOM_SQL_CDC = "switch_custom_sql_cdc";

	/**
	 * =================== sync way =====================
	 */
	public static final String SYNC_TYPE_INITIAL_SYNC = "initial_sync";
	public static final String SYNC_TYPE_CDC = "cdc";
	public static final String SYNC_TYPE_INITIAL_SYNC_CDC = "initial_sync+cdc";

	public final static SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	/**
	 * =================== fileCollector =====================
	 */
	public final static String GET_FILE_IN_MEMORY = "memory";
	public final static String GET_FILE_IN_STREAM = "stream";

	/**
	 * =================== settings =====================
	 */
	public static final String SETTINGS_JOB_REPLACEMENT = "job.field_replacement";

	/**
	 * ================== Language ==================
	 */
	public static final String CH_SIMPLIFIED_CHINESE = "CH_S";
	public static final String CH_TRADITIONAL_CHINESE = "CH_T";
	public static final String HK_TRADITIONAL_CHINESE = "HK_T";
	public static final String TW_TRADITIONAL_CHINESE = "TW_T";

	/**
	 * ================== Create Index ==================
	 */
	public final static long CREATE_INDEX_COUNT_LIMIT = 5000000L;

	/**
	 * ================== Scheduler Thread Name ==================
	 */
	public final static String START_JOB_THREAD = "%s Start Job Handler[%s]";
	public final static String STOP_JOB_THREAD = "%s Stop Job Handler[%s]";
	public final static String ERROR_JOB_THREAD = "%s Error Job Handler[%s]";
	public final static String PROGRESS_STATS_JOB_THREAD = "%s Progress Stats Handler[%s]";
	public final static String FORCE_STOP_JOB_THREAD = "%s Force Stop Job Handler[%s]";
	public final static String STATS_JOB_THREAD = "%s Stats Handler[%s]";
	public final static String STATS_DATA_SIZE_THREAD = "%s Stats Data Size Handler[%s]";
	public final static String WORKER_HEART_BEAT_THREAD = "%s Worker Heart Beat Handler[%s]";
	public final static String EVENT_THREAD = "%s Event Handler[%s]";
	public final static String STATS_STAGES_THREAD = "%s Stats Stages Handler[%s]";
	public final static String REFREASH_TOKEN_THREAD = "%s Refreash Token Handler[%s]";
	public final static String TEST_CONNECTION_THREAD = "%s Test Connection Handler[%s]";
	public final static String MERAGE_JOB_SETTING_THREAD = "%s Merage Job Setting Handler[%s]";
	public final static String LOAD_SETTINGS_THREAD = "%s Load Settings Handler[%s]";
	public final static String CLEAR_TRIGGER_LOG_THREAD = "%s Clear Trigger Log Handler[%s]";
	public final static String CLEAR_GRIDFS_EXPIRED_FILE_THREAD = "%s Clear Gridfs Expired File Handler[%s]";
	public final static String START_DATAFLOW_THREAD = "Start Dataflow Handler[%s]";
	public final static String STATS_DATAFLOW_THREAD = "Stats Dataflow Handler[%s]";

	/**
	 * ================== stats status ==================
	 */
	public final static String STATS_STATUS_INITIALIZING = "initializing";
	public final static String STATS_STATUS_INITIALIZED = "initialized";
	public final static String STATS_STATUS_CDCING = "cdc";

	/**
	 * ================== distinct write type ==================
	 */
	/**
	 * 强制去重模式
	 */
	public final static String DISTINCT_WRITE_TYPE_COMPEL = "compel";
	/**
	 * 智能去重模式
	 */
	public final static String DISTINCT_WRITE_TYPE_INTELLECT = "intellect";
	/**
	 * 不去重
	 */
	public final static String DISTINCT_WRITE_TYPE_DISABLE = "disable";

	/**
	 * log collect
	 */
	public final static String CONN_STR_FIELD_NAME = "__connStr";

	/**
	 * load schema fields
	 */
	public final static String LOAD_FIELDS = "loadFieldsStatus";
	public final static String LOAD_FIELD_STATUS_LOADING = "loading";
	public final static String LOAD_FIELD_STATUS_FINISHED = "finished";
	public final static String LOAD_FIELD_STATUS_ERROR = "error";

	/**
	 * postgres
	 */
	public static final String PG_SLOT_NAME_PREFFIX = "_io_tapdata_";
	public static final String PG_SLOT_NAMES = "pgSlotNames";

	/**
	 * debezium
	 */
	public static final String START_CDC = "start_cdc";
	public static final String STOP_JOB_TOPIC = "stop_job";
	public static final String FINISH_READ_SNAPSHOT = "finish_read_snapshot";

	public static ClientMongoOperator clientMongoOperator;

	public static final int LOOP_BATCH_SIZE = 1000;

	public static final String TAPDATA_WORKER_DIR = System.getenv("TAPDATA_WORK_DIR");

}
