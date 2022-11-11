package com.tapdata.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.tapdata.constant.*;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.entity.dataflow.Stage;
import io.tapdata.schema.SchemaList;
import io.tapdata.schema.SchemaMap;
import io.tapdata.schema.SchemaProxy;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Query;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Consumer;

/**
 * @author huangjq
 * @ClassName: Connections
 * @Description: TODO
 * @date 17-10-20
 * @since 1.0
 */
public class Connections implements Serializable {

	public final static String GRIDFS_HEADER_TYPE_SPECIFIED_LINE = "specified_line";

	public final static String GRIDFS_HEADER_TYPE_CUSTOM = "custom";
	public final static String EXCEL_HEADER_TYPE_VALUE = "value";
	public final static String EXCEL_HEADER_TYPE_INDEX = "index";

	public final static String JSON_ARRAY_BEGIN = "ArrayBegin";
	public final static String JSON_OBJECT_BEGIN = "ObjectBegin";

	public final static int DEFAULT_BUFFER_SIZE = 261120;

	//    private String _id;
	// loopback api _id
	private String id;
	private String user_id;
	private String name;
	private String connection_type;
	private String database_type;
	private String database_host;
	private String database_username;
	private Integer database_port;
	private String database_password;
	private String database_name;
	private String database_uri;
	private boolean ssl;
	private String server_name;

	private String database_owner;
	private String database_datetype_without_timezone;

	private Map<String, Object> response_body;

	private Integer retry;

	private Long nextRetry;

	/**
	 * @Author liuke5
	 * @Description key : tables
	 * value: table's definition
	 * @Date 2020/7/9 16:24
	 * @Param
	 * @return
	 **/
	@JsonIgnore
	private Map<String, List<RelateDataBaseTable>> schema;

	private String auth_db;

	private boolean is_include_views = true;

	private String additionalString;

	private String seperate;

	private String folder;

	private String prefix;

	private boolean prefixDir = true;

	private int initialReadSize;

	private int increamentalTps;

	private String json_type;

	private String include_filename;

	private String exclude_filename;

	private Integer default_timeout_second;

	private Integer connection_timeout_seconds;

	private Integer data_timeout_seconds;

	private String tags;

	private Integer ttl;

	private String file_source_protocol;

	private String file_type;

	private Boolean ftp_passive;

	private String vc_mode;

	private String tags_filter;

	private String data_content_xpath;

	private String unique_keys;

	private String auth_type;

	private long request_interval;

	private String resp_pre_process;

	private String req_pre_process;

	private List<RestURLInfo> url_info;

	private String collection_name;

	private Integer listen_port;

	private String root_name;

	private String file_schema;

	private String fileDefaultCharset;

	private String table_filter;
	private String tableExcludeFilter;
	private Boolean openTableExcludeFilter;

	/**
	 * byte
	 */
	private int lobMaxSize = 8388608;

	private String thin_type;


	/**
	 * kafka
	 */
	private String acks = "all";
	private int batch_size = 16384;
	private Map<String, Object> addtional_config;


	/**
	 * kafka common
	 */
	private String kafkaBootstrapServers;
	private Set<String> kafkaRawTopics;
	private String kafkaPatternTopics;
	/**
	 * kafka source (Consumer)
	 */
	private Integer kafkaConsumerRequestTimeout = 0;
	private Boolean kafkaConsumerUseTransactional = false;
	private Integer kafkaMaxPollRecords = 0;
	private Integer kafkaPollTimeoutMS = 0;
	private Integer kafkaMaxFetchBytes = 0;
	private Integer kafkaMaxFetchWaitMS = 0;
	private Boolean kafkaIgnoreInvalidRecord = false;
	/**
	 * kafka target (Producer)
	 */
	private Integer kafkaProducerRequestTimeout = 0;
	private Boolean kafkaProducerUseTransactional = false;
	private Integer kafkaRetries = 0;
	private Integer kafkaBatchSize = 0;
	private String kafkaAcks = "-1";
	private Integer kafkaLingerMS = 0;
	private Integer kafkaDeliveryTimeoutMS = 0;
	private Integer kafkaMaxRequestSize = 0;
	private Integer kafkaMaxBlockMS = 0;
	private Integer kafkaBufferMemory = 0;
	private String kafkaCompressionType = "";
	private String kafkaPartitionKey = "";
	private Boolean kafkaIgnorePushError = false;

	/**
	 * mq
	 */
	private byte mqType;
	/**
	 * activemq
	 */
	private String brokerURL;

	private String mqUserName;
	private String mqPassword;

	/**
	 * 队列集合
	 */
	private Set<String> mqQueueSet;
	/**
	 * topic集合
	 */
	private Set<String> mqTopicSet;

	/**
	 * rabbitmq消息路由key在消息map中的字段名
	 */
	private String routeKeyField;

	/**
	 * rocketmq
	 */
	private String nameSrvAddr;

	private String productGroup;

	private String consumerGroup;


	/**
	 * avro
	 */
	private String avro_namespace;
	private String avro_encoder_type = AvroUtil.FILE_WRITER;

	/**
	 * bitsflow
	 */
	private String service;
	private String sql_field_name;
	public final static String AVRO_FORMAT = "avro";
	public final static String XML_FORMAT = "xml";
	private String publish_format;

	/**
	 * data header when gridfs source excel/csv/txt
	 * 1) specified_line: default
	 * 2) custom:
	 */
	private String gridfs_header_type = GRIDFS_HEADER_TYPE_SPECIFIED_LINE;

	/**
	 * 1）line number that header in (default 1), if gridfs_header_type is specified_line
	 * 2) eg: name,age,email..., comma separate if gridfs_header_type is specified_line
	 */
	private String gridfs_header_config = "1";

	private int file_upload_chunk_size = DEFAULT_BUFFER_SIZE;

	private String file_upload_mode = ConnectorConstant.GET_FILE_IN_STREAM;

	private int sampleSize = 100;

	/**
	 * excel addition
	 */
	private String sheet_start;
	private String sheet_end;
	private String excel_header_type = EXCEL_HEADER_TYPE_VALUE;
	private String excel_header_start;
	private String excel_header_end;
	private String excel_value_start;
	private String excel_value_end;
	private String excel_header_concat_char = "-";
	private String excel_password;

	/**
	 * custom source
	 */
	private String custom_type;
	private String custom_initial_script;
	private String custom_cdc_script;
	private String custom_ondata_script;
	private String custom_before_script;
	private String custom_after_script;

	/**
	 * Jira source
	 */
	private String jiraUrl;
	private String jiraUsername;
	private String jiraPassword;
	private int jiraWebhookPort;

	/**
	 * ssl config
	 */
	private boolean sslValidate;

	private String sslCA;

	private String sslCert;

	private String sslKey;

	private String sslPass;

	private String sslCRL;

	private boolean checkServerIdentity;

	/**
	 * kerberos config
	 */
	private boolean krb5; // 是否开启 kerberos 认证
	private String krb5Keytab; // krb5.keytab
	private String krb5Conf; // krb5.conf
	private String krb5Principal; // 格式：kafka/<domain>@EXAMPLE.COM
	private String krb5ServiceName; // 服务名

	/**
	 * kafka sasl.mechanism config
	 */
	private String kafkaSaslMechanism;

	/**
	 * postgres log decorder plugin name
	 */
	private String pgsql_log_decorder_plugin_name;

	private String node_name;

	private long socketReadTimeout = 300 * 1000;

	/**
	 * CSV Config
	 * 文件存放位置、文件名前缀、文件分隔符、是否包括头、文件行数上限
	 */
	private String filePath;
	private String filePrefix;
	private Character delimiter;
	private boolean isContainHeader;
	private Integer maxFileRow;
	private ZoneId zoneId = ZoneId.systemDefault();
	private ZoneId sysZoneId = ZoneId.systemDefault();
	private ZoneId customZoneId;

	/**
	 * elasticsearch
	 */

	private String clusterName;

	/**
	 * cache config
	 * key: cache name
	 * value: cache config
	 */
	private Map<String, DataFlowCacheConfig> cacheConfig;

	private boolean supportUpdatePk;

	private String outputPath;

	private String gridfsReadMode = GridfsReadModeEnum.DATA.getMode();

	private boolean extendSourcePath;

	private boolean schemaAutoUpdate;

	private long schemaAutoUpdateLastTime;

	private String dbCurrentTime;

	private String dbFullVersion;

	private List<String> pgSlotNames;

	private String searchDatabaseType;

	// hive target
	private String hiveConnType;

	// tidbSource
	private String tidbPdServer;

	// hanaSource
	// can be
	private String hanaType;

	/**
	 * 使用的js引擎名称（支持nashorn、graal.js）
	 */
	private String jsEngineName;

	@JsonIgnore
	private boolean loadSchemaField = false;
	@JsonIgnore
	private Consumer<RelateDataBaseTable> tableConsumer;

	@JsonIgnore
	private FileProperty fileProperty;

	private String pdb;

	/**
	 * 数据源为tcp_udp时指定具体时tcp还是udp
	 */
	private String tcpUdpType;

	/**
	 * 数据源插件端动态生成的所有配置都存放在config
	 */
	private Map<String, Object> config;

	private String virtualHost;

	private HazelcastConstructType hazelcastConstructType = HazelcastConstructType.IMap;

	/**
	 * vika属性
	 */
	private String vika_space_name;
	private String vika_space_id;

	private String uniqueName;

	private boolean shareCdcEnable;

	private Integer shareCdcTTL;

	/**
	 * 用户区分是不是pdk数据源类型
	 */
	private String pdkType;
	private String pdkHash;

	/**
	 * 裸日志解析服务配置
	 */
	private boolean redoLogParserEnable;
	private String redoLogParserHost;
	private Integer redoLogParserPort;

	private Map<String, Object> extParam;


	public Connections() {
	}

	public static Connections jdbcConn(String host, int port, String databaseName, String schemaName, String user, String password, DatabaseTypeEnum databaseType) {
		Connections connections = new Connections();
		connections.setDatabase_host(host);
		connections.setDatabase_port(port);
		connections.setDatabase_name(databaseName);
		connections.setDatabase_owner(schemaName);
		connections.setDatabase_username(user);
		connections.setDatabase_password(password);
		connections.setDatabase_type(databaseType.getType());
		return connections;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getConnection_type() {
		return connection_type;
	}

	public void setConnection_type(String connection_type) {
		this.connection_type = connection_type;
	}

	public String getDatabase_type() {
		return database_type;
	}

	public void setDatabase_type(String database_type) {
		this.database_type = database_type;
	}

	public String getDatabase_host() {
		return database_host;
	}

	public void setDatabase_host(String database_host) {
		this.database_host = database_host;
	}

	public String getDatabase_username() {
		return database_username;
	}

	public void setDatabase_username(String database_username) {
		this.database_username = database_username;
	}

	public Integer getDatabase_port() {
		return database_port;
	}

	public void setDatabase_port(Integer database_port) {
		this.database_port = database_port;
	}

	public String getDatabase_password() {
		return database_password;
	}

	public void setDatabase_password(String database_password) {
		this.database_password = database_password;
	}

	public String getDatabase_name() {
		return database_name;
	}

	public void setDatabase_name(String database_name) {
		this.database_name = database_name;
	}

	public Map<String, List<RelateDataBaseTable>> getSchema() {
		SchemaProxy schemaProxy = SchemaProxy.getSchemaProxy();
		SchemaMap schemaMap = schemaProxy.getSchemaMap(id);
		if (schemaMap == null || schemaMap.isEmpty()) {
			synchronized (id.intern()) {
				schemaMap = schemaProxy.getSchemaMap(id);
				if (schemaMap == null || schemaMap.isEmpty()) {
					Query query = new Query();
					List<String> tableNames = ConnectorConstant.clientMongoOperator.find(query, ConnectorConstant.METADATA_INSTANCE_COLLECTION + "/tables?connectionId=" + id, String.class);
					schemaProxy.register(id, tableNames);
					schemaMap = schemaProxy.getSchemaMap(id);
				}
			}
		}
		return schemaMap;
	}

	public SchemaList<String, RelateDataBaseTable> getSchemaList() {
		return ((SchemaList<String, RelateDataBaseTable>) getSchema().get("tables"));
	}

	public void setSchema(Map<String, List<RelateDataBaseTable>> schema) {
		this.schema = schema;
	}

	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public Integer getRetry() {
		return retry;
	}

	public void setRetry(Integer retry) {
		this.retry = retry;
	}

	public Long getNextRetry() {
		return nextRetry;
	}

	public void setNextRetry(Long nextRetry) {
		this.nextRetry = nextRetry;
	}

	public String getDatabase_uri() {
		return database_uri;
	}

	public void setDatabase_uri(String database_uri) {
		this.database_uri = database_uri;
	}

	public void setSsl(boolean ssl) {
		this.ssl = ssl;
	}

	public String getDatabase_owner() {
		return database_owner;
	}

	public void setDatabase_owner(String database_owner) {
		this.database_owner = database_owner;
	}

	public boolean getSsl() {
		return ssl;
	}

	public Map<String, Object> getResponse_body() {
		return response_body;
	}

	public void setResponse_body(Map<String, Object> response_body) {
		this.response_body = response_body;
	}

	public String getAuth_db() {
		return auth_db;
	}

	public void setAuth_db(String auth_db) {
		this.auth_db = auth_db;
	}

	public boolean is_include_views() {
		return is_include_views;
	}

	public void setIs_include_views(boolean is_include_views) {
		this.is_include_views = is_include_views;
	}

	public String getServer_name() {
		return server_name;
	}

	public void setServer_name(String server_name) {
		this.server_name = server_name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPrefix() {
		return prefix;
	}

	public Connections setPrefix(String prefix) {
		this.prefix = prefix;
		return this;
	}

	public boolean isPrefixDir() {
		return prefixDir;
	}

	public Connections setPrefixDir(boolean prefixDir) {
		this.prefixDir = prefixDir;
		return this;
	}

	public void decodeDatabasePassword() {
		if (StringUtils.isNotBlank(database_password)) {
			database_password = AES256Util.Aes256Decode(database_password, AES256Util.getKey());
		}
	}

	public String getAdditionalString() {
		return additionalString;
	}

	public void setAdditionalString(String additionalString) {
		this.additionalString = additionalString;
	}

	public String getFolder() {
		return folder;
	}

	public void setFolder(String folder) {
		this.folder = folder;
	}

	public String getSeperate() {
		return seperate;
	}

	public void setSeperate(String seperate) {
		this.seperate = seperate;
	}

	public String getTableNamesWithoutPK(List<Mapping> mappings) {
		StringBuilder tableNames = new StringBuilder();
		String returnStr = "";

		if (CollectionUtils.isNotEmpty(mappings)) {
			for (int i = 0; i < mappings.size(); i++) {
				Mapping mapping = mappings.get(i);
				if (CollectionUtils.isEmpty(mapping.getJoin_condition())
						&& !ConnectorConstant.RELATIONSHIP_APPEND.equals(mapping.getRelationship())) {
					mappings.remove(i);
					i--;
					tableNames = tableNames.append(mapping.getFrom_table() + ",");
				}
			}
			if (tableNames.toString().endsWith(",")) {
				returnStr = StringUtils.removeEnd(tableNames.toString(), ",");
			}
		}

		return returnStr;
	}

	public static Connections cacheConnection(Connections soureConnection, List<Stage> stages) {
		Connections cacheConnection = null;
		for (Stage stage : stages) {
			if (Stage.StageTypeEnum.MEM_CACHE.type.equals(stage.getType())) {
				if (cacheConnection == null) {
					cacheConnection = new Connections();
					cacheConnection.setDatabase_type(DatabaseTypeEnum.MEM_CACHE.getType());
				}

				String cacheKeys = stage.getCacheKeys();
				String cacheName = stage.getCacheName();
				String cacheType = stage.getCacheType();
				long maxRows = stage.getMaxRows();
				long maxSize = stage.getMaxSize();
				Map<String, DataFlowCacheConfig> cacheConfig = cacheConnection.getCacheConfig();
				if (cacheConfig == null) {
					cacheConfig = new HashMap<>();
					cacheConnection.setCacheConfig(cacheConfig);
				}

				Stage inputStage = null;
				List<String> inputLanes = stage.getInputLanes();
				if (CollectionUtils.isEmpty(inputLanes)) {
					continue;
				}
				String inputStageId = inputLanes.get(0);
				Set<String> sourceDataStageIds = DataFlowUtil.findSourceDataStagesByInputLane(stages, inputStageId);

				for (Stage stage1 : stages) {
					if (sourceDataStageIds.contains(stage1.getId())) {
						inputStage = stage1;
						break;
					}
				}

				List<String> pks = null;
				if (StringUtils.isNotBlank(stage.getPrimaryKeys())) {
					String primaryKeys = stage.getPrimaryKeys();
					pks = Arrays.asList(primaryKeys.split(","));
				}

				String tableName = inputStage.getTableName();
				cacheConfig.put(
						cacheName,
						new DataFlowCacheConfig(
								cacheKeys,
								cacheName,
								cacheType,
								maxRows,
								maxSize,
								stage.getTtl(),
								stage.getFields(),
								soureConnection,
								null,
								tableName,
								inputStage,
								pks
						)
				);
			}
		}

		return cacheConnection;
	}

	public static Connections virtualTargetNodeConnections() {
		return new Connections();
	}

	public int getInitialReadSize() {
		return initialReadSize;
	}

	public void setInitialReadSize(int initialReadSize) {
		this.initialReadSize = initialReadSize;
	}

	public int getIncreamentalTps() {
		return increamentalTps;
	}

	public void setIncreamentalTps(int increamentalTps) {
		this.increamentalTps = increamentalTps;
	}

	public String getJson_type() {
		return json_type;
	}

	public void setJson_type(String json_type) {
		this.json_type = json_type;
	}

	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	public Integer getTtl() {
		return ttl;
	}

	public void setTtl(Integer ttl) {
		this.ttl = ttl;
	}

	public String getFile_source_protocol() {
		return file_source_protocol;
	}

	public void setFile_source_protocol(String file_source_protocol) {
		this.file_source_protocol = file_source_protocol;
	}

	public String getFile_type() {
		return file_type;
	}

	public void setFile_type(String file_type) {
		this.file_type = file_type;
	}

	public Boolean getFtp_passive() {
		return ftp_passive;
	}

	public void setFtp_passive(Boolean ftp_passive) {
		this.ftp_passive = ftp_passive;
	}

	public String getVc_mode() {
		return vc_mode;
	}

	public void setVc_mode(String vc_mode) {
		this.vc_mode = vc_mode;
	}

	public String getTags_filter() {
		return tags_filter;
	}

	public void setTags_filter(String tags_filter) {
		this.tags_filter = tags_filter;
	}

	public String getData_content_xpath() {
		return data_content_xpath;
	}

	public void setData_content_xpath(String data_content_xpath) {
		this.data_content_xpath = data_content_xpath;
	}

	public String getInclude_filename() {
		return include_filename;
	}

	public void setInclude_filename(String include_filename) {
		this.include_filename = include_filename;
	}

	public String getExclude_filename() {
		return exclude_filename;
	}

	public void setExclude_filename(String exclude_filename) {
		this.exclude_filename = exclude_filename;
	}

	public Integer getDefault_timeout_second() {
		return default_timeout_second;
	}

	public void setDefault_timeout_second(Integer default_timeout_second) {
		this.default_timeout_second = default_timeout_second;
	}

	public Integer getConnection_timeout_seconds() {
		return connection_timeout_seconds;
	}

	public void setConnection_timeout_seconds(Integer connection_timeout_seconds) {
		this.connection_timeout_seconds = connection_timeout_seconds;
	}

	public Integer getData_timeout_seconds() {
		return data_timeout_seconds;
	}

	public void setData_timeout_seconds(Integer data_timeout_seconds) {
		this.data_timeout_seconds = data_timeout_seconds;
	}

	public String getUnique_keys() {
		return unique_keys;
	}

	public void setUnique_keys(String unique_keys) {
		this.unique_keys = unique_keys;
	}

	public String getAuth_type() {
		return auth_type;
	}

	public void setAuth_type(String auth_type) {
		this.auth_type = auth_type;
	}

	public long getRequest_interval() {
		return request_interval;
	}

	public void setRequest_interval(long request_interval) {
		this.request_interval = request_interval;
	}

	public String getResp_pre_process() {
		return resp_pre_process;
	}

	public void setResp_pre_process(String resp_pre_process) {
		this.resp_pre_process = resp_pre_process;
	}

	public String getReq_pre_process() {
		return req_pre_process;
	}

	public void setReq_pre_process(String req_pre_process) {
		this.req_pre_process = req_pre_process;
	}

	public List<RestURLInfo> getUrl_info() {
		return url_info;
	}

	public void setUrl_info(List<RestURLInfo> url_info) {
		this.url_info = url_info;
	}

	public String getCollection_name() {
		return collection_name;
	}

	public void setCollection_name(String collection_name) {
		this.collection_name = collection_name;
	}

	public Integer getListen_port() {
		return listen_port;
	}

	public void setListen_port(Integer listen_port) {
		this.listen_port = listen_port;
	}

	public String getRoot_name() {
		return root_name;
	}

	public void setRoot_name(String root_name) {
		this.root_name = root_name;
	}

	public String getFile_schema() {
		return file_schema;
	}

	public void setFile_schema(String file_schema) {
		this.file_schema = file_schema;
	}

	public String getFileDefaultCharset() {
		return fileDefaultCharset;
	}

	public void setFileDefaultCharset(String fileDefaultCharset) {
		this.fileDefaultCharset = fileDefaultCharset;
	}

	public int getLobMaxSize() {
		return lobMaxSize;
	}

	public void setLobMaxSize(int lobMaxSize) {
		this.lobMaxSize = lobMaxSize;
	}

	public String getThin_type() {
		return thin_type;
	}

	public void setThin_type(String thin_type) {
		this.thin_type = thin_type;
	}

	public boolean isIs_include_views() {
		return is_include_views;
	}

	public String getAcks() {
		return acks;
	}

	public void setAcks(String acks) {
		this.acks = acks;
	}

	public int getBatch_size() {
		return batch_size;
	}

	public void setBatch_size(int batch_size) {
		this.batch_size = batch_size;
	}

	public Map<String, Object> getAddtional_config() {
		return addtional_config;
	}

	public void setAddtional_config(Map<String, Object> addtional_config) {
		this.addtional_config = addtional_config;
	}

	public String getAvro_namespace() {
		return avro_namespace;
	}

	public void setAvro_namespace(String avro_namespace) {
		this.avro_namespace = avro_namespace;
	}

	public String getAvro_encoder_type() {
		return avro_encoder_type;
	}

	public void setAvro_encoder_type(String avro_encoder_type) {
		this.avro_encoder_type = avro_encoder_type;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public String getSql_field_name() {
		return sql_field_name;
	}

	public void setSql_field_name(String sql_field_name) {
		this.sql_field_name = sql_field_name;
	}

	public String getPublish_format() {
		return publish_format;
	}

	public void setPublish_format(String publish_format) {
		this.publish_format = publish_format;
	}

	public String getGridfs_header_type() {
		return gridfs_header_type;
	}

	public void setGridfs_header_type(String gridfs_header_type) {
		this.gridfs_header_type = gridfs_header_type;
	}

	public String getGridfs_header_config() {
		return gridfs_header_config;
	}

	public void setGridfs_header_config(String gridfs_header_config) {
		this.gridfs_header_config = gridfs_header_config;
	}

	public int getFile_upload_chunk_size() {
		return file_upload_chunk_size;
	}

	public void setFile_upload_chunk_size(int file_upload_chunk_size) {
		this.file_upload_chunk_size = file_upload_chunk_size;
	}

	public String getFile_upload_mode() {
		return file_upload_mode;
	}

	public void setFile_upload_mode(String file_upload_mode) {
		this.file_upload_mode = file_upload_mode;
	}

	public int getSampleSize() {
		return sampleSize;
	}

	public void setSampleSize(int sampleSize) {
		this.sampleSize = sampleSize;
	}

	public String getTable_filter() {
		if (null == table_filter) {
			return null;
		}
		if (isCaseInSensitive()) {
			return table_filter.toLowerCase();
		}
		return table_filter;
	}

	public void setTable_filter(String table_filter) {
		this.table_filter = table_filter;
	}

	public String getTableExcludeFilter() {
		if (null == tableExcludeFilter) {
			return null;
		}
		if (isCaseInSensitive()) {
			return tableExcludeFilter.toLowerCase();
		}
		return tableExcludeFilter;
	}

	public String getIfOpenTableExcludeFilter() {
		if (Boolean.TRUE.equals(getOpenTableExcludeFilter())) {
			return getTableExcludeFilter();
		}
		return null;
	}

	public void setTableExcludeFilter(String tableExcludeFilter) {
		this.tableExcludeFilter = tableExcludeFilter;
	}

	public Boolean getOpenTableExcludeFilter() {
		return openTableExcludeFilter;
	}

	public void setOpenTableExcludeFilter(Boolean openTableExcludeFilter) {
		this.openTableExcludeFilter = openTableExcludeFilter;
	}

	public String getSheet_start() {
		return sheet_start;
	}

	public void setSheet_start(String sheet_start) {
		this.sheet_start = sheet_start;
	}

	public String getSheet_end() {
		return sheet_end;
	}

	public void setSheet_end(String sheet_end) {
		this.sheet_end = sheet_end;
	}

	public String getExcel_header_type() {
		return excel_header_type;
	}

	public void setExcel_header_type(String excel_header_type) {
		this.excel_header_type = excel_header_type;
	}

	public String getExcel_header_start() {
		return excel_header_start;
	}

	public void setExcel_header_start(String excel_header_start) {
		this.excel_header_start = excel_header_start;
	}

	public String getExcel_header_end() {
		return excel_header_end;
	}

	public void setExcel_header_end(String excel_header_end) {
		this.excel_header_end = excel_header_end;
	}

	public String getExcel_value_start() {
		return excel_value_start;
	}

	public void setExcel_value_start(String excel_value_start) {
		this.excel_value_start = excel_value_start;
	}

	public String getExcel_value_end() {
		return excel_value_end;
	}

	public void setExcel_value_end(String excel_value_end) {
		this.excel_value_end = excel_value_end;
	}

	public String getExcel_header_concat_char() {
		return excel_header_concat_char;
	}

	public void setExcel_header_concat_char(String excel_header_concat_char) {
		this.excel_header_concat_char = excel_header_concat_char;
	}

	public String getCustom_initial_script() {
		return custom_initial_script;
	}

	public void setCustom_initial_script(String custom_initial_script) {
		this.custom_initial_script = custom_initial_script;
	}

	public String getCustom_cdc_script() {
		return custom_cdc_script;
	}

	public void setCustom_cdc_script(String custom_cdc_script) {
		this.custom_cdc_script = custom_cdc_script;
	}

	public String getCustom_type() {
		return custom_type;
	}

	public void setCustom_type(String custom_type) {
		this.custom_type = custom_type;
	}

	public boolean getSslValidate() {
		return sslValidate;
	}

	public void setSslValidate(boolean sslValidate) {
		this.sslValidate = sslValidate;
	}

	public String getSslCA() {
		return sslCA;
	}

	public void setSslCA(String sslCA) {
		this.sslCA = sslCA;
	}

	public String getSslCert() {
		return sslCert;
	}

	public void setSslCert(String sslCert) {
		this.sslCert = sslCert;
	}

	public String getSslKey() {
		return sslKey;
	}

	public void setSslKey(String sslKey) {
		this.sslKey = sslKey;
	}

	public String getSslPass() {
		return sslPass;
	}

	public void setSslPass(String sslPass) {
		this.sslPass = sslPass;
	}

	public String getSslCRL() {
		return sslCRL;
	}

	public void setSslCRL(String sslCRL) {
		this.sslCRL = sslCRL;
	}

	public boolean getCheckServerIdentity() {
		return checkServerIdentity;
	}

	public void setCheckServerIdentity(boolean checkServerIdentity) {
		this.checkServerIdentity = checkServerIdentity;
	}

	public boolean getKrb5() {
		return krb5;
	}

	public void setKrb5(boolean krb5) {
		this.krb5 = krb5;
	}

	public String getKrb5Keytab() {
		return krb5Keytab;
	}

	public void setKrb5Keytab(String krb5Keytab) {
		this.krb5Keytab = krb5Keytab;
	}

	public String getKrb5Conf() {
		return krb5Conf;
	}

	public void setKrb5Conf(String krb5Conf) {
		this.krb5Conf = krb5Conf;
	}

	public String getKrb5Principal() {
		return krb5Principal;
	}

	public void setKrb5Principal(String krb5Principal) {
		this.krb5Principal = krb5Principal;
	}

	public String getKrb5ServiceName() {
		return krb5ServiceName;
	}

	public void setKrb5ServiceName(String krb5ServiceName) {
		this.krb5ServiceName = krb5ServiceName;
	}

	public String getPgsql_log_decorder_plugin_name() {
		return pgsql_log_decorder_plugin_name;
	}

	public void setPgsql_log_decorder_plugin_name(String pgsql_log_decorder_plugin_name) {
		this.pgsql_log_decorder_plugin_name = pgsql_log_decorder_plugin_name;
	}

	public String getNode_name() {
		return node_name;
	}

	public void setNode_name(String node_name) {
		this.node_name = node_name;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public String getFilePrefix() {
		return filePrefix;
	}

	public void setFilePrefix(String filePrefix) {
		this.filePrefix = filePrefix;
	}

	public Character getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(Character delimiter) {
		this.delimiter = delimiter;
	}

	public boolean getIsContainHeader() {
		return isContainHeader;
	}

	public void setIsContainHeader(boolean containHeader) {
		isContainHeader = containHeader;
	}

	public Integer getMaxFileRow() {
		return maxFileRow;
	}

	public void setMaxFileRow(Integer maxFileRow) {
		this.maxFileRow = maxFileRow;
	}

	public long getSocketReadTimeout() {
		return socketReadTimeout;
	}

	public void setSocketReadTimeout(long socketReadTimeout) {
		this.socketReadTimeout = socketReadTimeout;
	}

	public ZoneId getZoneId() {
		return zoneId;
	}

	public void setZoneId(ZoneId zoneId) {
		this.zoneId = zoneId;
	}

	public String getCustom_ondata_script() {
		return custom_ondata_script;
	}

	public void setCustom_ondata_script(String custom_ondata_script) {
		this.custom_ondata_script = custom_ondata_script;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public boolean isSupportUpdatePk() {
		return supportUpdatePk;
	}

	public void setSupportUpdatePk(boolean supportUpdatePk) {
		this.supportUpdatePk = supportUpdatePk;
	}

	public ZoneId getCustomZoneId() {
		return customZoneId;
	}

	public void setCustomZoneId(ZoneId customZoneId) {
		this.customZoneId = customZoneId;
	}

	public String getDatabase_datetype_without_timezone() {
		return database_datetype_without_timezone;
	}

	public void setDatabase_datetype_without_timezone(String database_datetype_without_timezone) {
		this.database_datetype_without_timezone = database_datetype_without_timezone;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getGridfsReadMode() {
		return gridfsReadMode;
	}

	public void setGridfsReadMode(String gridfsReadMode) {
		this.gridfsReadMode = gridfsReadMode;
	}

	public Map<String, DataFlowCacheConfig> getCacheConfig() {
		return cacheConfig;
	}

	public void setCacheConfig(Map<String, DataFlowCacheConfig> cacheConfig) {
		this.cacheConfig = cacheConfig;
	}

	public boolean isExtendSourcePath() {
		return extendSourcePath;
	}

	public void setExtendSourcePath(boolean extendSourcePath) {
		this.extendSourcePath = extendSourcePath;
	}

	public ZoneId getSysZoneId() {
		return sysZoneId;
	}

	public void setSysZoneId(ZoneId sysZoneId) {
		this.sysZoneId = sysZoneId;
	}

	public void initCustomTimeZone() {
		if (StringUtils.isNotBlank(database_datetype_without_timezone)) {
			if (!StringUtils.startsWithIgnoreCase(database_datetype_without_timezone, "gmt")) {
				database_datetype_without_timezone = "GMT" + database_datetype_without_timezone;
			}

			this.customZoneId = ZoneId.of(database_datetype_without_timezone);
		} else if (this.zoneId != null) {
			this.customZoneId = this.zoneId;
		} else {
			this.customZoneId = ZoneId.systemDefault();
		}
	}

	public boolean getSchemaAutoUpdate() {
		return schemaAutoUpdate;
	}

	public void setSchemaAutoUpdate(boolean schemaAutoUpdate) {
		this.schemaAutoUpdate = schemaAutoUpdate;
	}

	public long getSchemaAutoUpdateLastTime() {
		return schemaAutoUpdateLastTime;
	}

	public void setSchemaAutoUpdateLastTime(long schemaAutoUpdateLastTime) {
		this.schemaAutoUpdateLastTime = schemaAutoUpdateLastTime;
	}

	public String getDbCurrentTime() {
		return dbCurrentTime;
	}

	public void setDbCurrentTime(String dbCurrentTime) {
		this.dbCurrentTime = dbCurrentTime;
	}

	public String getDbFullVersion() {
		return dbFullVersion;
	}

	public void setDbFullVersion(String dbFullVersion) {
		this.dbFullVersion = dbFullVersion;
	}

	public List<String> getPgSlotNames() {
		return pgSlotNames;
	}

	public void setPgSlotNames(List<String> pgSlotNames) {
		this.pgSlotNames = pgSlotNames;
	}

	public boolean isLoadSchemaField() {
		return loadSchemaField;
	}

	public void setLoadSchemaField(boolean loadSchemaField) {
		this.loadSchemaField = loadSchemaField;
	}

	public Consumer<RelateDataBaseTable> getTableConsumer() {
		return tableConsumer;
	}

	public void setTableConsumer(Consumer<RelateDataBaseTable> tableConsumer) {
		this.tableConsumer = tableConsumer;
	}

	public String getExcel_password() {
		return excel_password;
	}

	public void setExcel_password(String excel_password) {
		this.excel_password = excel_password;
	}

	public FileProperty getFileProperty() {
		return fileProperty;
	}

	public void setFileProperty(FileProperty fileProperty) {
		this.fileProperty = fileProperty;
	}

	public Integer getKafkaMaxPollRecords() {
		return kafkaMaxPollRecords;
	}

	public void setKafkaMaxPollRecords(Integer kafkaMaxPollRecords) {
		this.kafkaMaxPollRecords = kafkaMaxPollRecords;
	}

	public Integer getKafkaPollTimeoutMS() {
		return kafkaPollTimeoutMS;
	}

	public void setKafkaPollTimeoutMS(Integer kafkaPollTimeoutMS) {
		this.kafkaPollTimeoutMS = kafkaPollTimeoutMS;
	}

	public Integer getKafkaMaxFetchBytes() {
		return kafkaMaxFetchBytes;
	}

	public void setKafkaMaxFetchBytes(Integer kafkaMaxFetchBytes) {
		this.kafkaMaxFetchBytes = kafkaMaxFetchBytes;
	}

	public Integer getKafkaMaxFetchWaitMS() {
		return kafkaMaxFetchWaitMS;
	}

	public void setKafkaMaxFetchWaitMS(Integer kafkaMaxFetchWaitMS) {
		this.kafkaMaxFetchWaitMS = kafkaMaxFetchWaitMS;
	}

	public Boolean getKafkaIgnoreInvalidRecord() {
		return kafkaIgnoreInvalidRecord;
	}

	public void setKafkaIgnoreInvalidRecord(Boolean kafkaIgnoreInvalidRecord) {
		this.kafkaIgnoreInvalidRecord = kafkaIgnoreInvalidRecord;
	}

	public Integer getKafkaRetries() {
		return kafkaRetries;
	}

	public void setKafkaRetries(Integer kafkaRetries) {
		this.kafkaRetries = kafkaRetries;
	}

	public Integer getKafkaBatchSize() {
		return kafkaBatchSize;
	}

	public void setKafkaBatchSize(Integer kafkaBatchSize) {
		this.kafkaBatchSize = kafkaBatchSize;
	}

	public String getKafkaAcks() {
		return kafkaAcks;
	}

	public void setKafkaAcks(String kafkaAcks) {
		this.kafkaAcks = kafkaAcks;
	}

	public Integer getKafkaLingerMS() {
		return kafkaLingerMS;
	}

	public void setKafkaLingerMS(Integer kafkaLingerMS) {
		this.kafkaLingerMS = kafkaLingerMS;
	}

	public Integer getKafkaDeliveryTimeoutMS() {
		return kafkaDeliveryTimeoutMS;
	}

	public void setKafkaDeliveryTimeoutMS(Integer kafkaDeliveryTimeoutMS) {
		this.kafkaDeliveryTimeoutMS = kafkaDeliveryTimeoutMS;
	}

	public Integer getKafkaMaxRequestSize() {
		return kafkaMaxRequestSize;
	}

	public void setKafkaMaxRequestSize(Integer kafkaMaxRequestSize) {
		this.kafkaMaxRequestSize = kafkaMaxRequestSize;
	}

	public Integer getKafkaMaxBlockMS() {
		return kafkaMaxBlockMS;
	}

	public void setKafkaMaxBlockMS(Integer kafkaMaxBlockMS) {
		this.kafkaMaxBlockMS = kafkaMaxBlockMS;
	}

	public Integer getKafkaBufferMemory() {
		return kafkaBufferMemory;
	}

	public void setKafkaBufferMemory(Integer kafkaBufferMemory) {
		this.kafkaBufferMemory = kafkaBufferMemory;
	}

	public String getKafkaCompressionType() {
		return kafkaCompressionType;
	}

	public void setKafkaCompressionType(String kafkaCompressionType) {
		this.kafkaCompressionType = kafkaCompressionType;
	}

	public String getKafkaPartitionKey() {
		return kafkaPartitionKey;
	}

	public void setKafkaPartitionKey(String kafkaPartitionKey) {
		this.kafkaPartitionKey = kafkaPartitionKey;
	}

	public Boolean getKafkaIgnorePushError() {
		return kafkaIgnorePushError;
	}

	public void setKafkaIgnorePushError(Boolean kafkaIgnorePushError) {
		this.kafkaIgnorePushError = kafkaIgnorePushError;
	}

	public String getKafkaBootstrapServers() {
		return kafkaBootstrapServers;
	}

	public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
		this.kafkaBootstrapServers = kafkaBootstrapServers;
	}

	public Integer getKafkaConsumerRequestTimeout() {
		return kafkaConsumerRequestTimeout;
	}

	public void setKafkaConsumerRequestTimeout(Integer kafkaConsumerRequestTimeout) {
		this.kafkaConsumerRequestTimeout = kafkaConsumerRequestTimeout;
	}

	public Boolean getKafkaConsumerUseTransactional() {
		return kafkaConsumerUseTransactional;
	}

	public void setKafkaConsumerUseTransactional(Boolean kafkaConsumerUseTransactional) {
		this.kafkaConsumerUseTransactional = kafkaConsumerUseTransactional;
	}

	public Integer getKafkaProducerRequestTimeout() {
		return kafkaProducerRequestTimeout;
	}

	public void setKafkaProducerRequestTimeout(Integer kafkaProducerRequestTimeout) {
		this.kafkaProducerRequestTimeout = kafkaProducerRequestTimeout;
	}

	public Boolean getKafkaProducerUseTransactional() {
		return kafkaProducerUseTransactional;
	}

	public void setKafkaProducerUseTransactional(Boolean kafkaProducerUseTransactional) {
		this.kafkaProducerUseTransactional = kafkaProducerUseTransactional;
	}

	public Set<String> getKafkaRawTopics() {
		return kafkaRawTopics;
	}

	public void setKafkaRawTopics(Set<String> kafkaRawTopics) {
		this.kafkaRawTopics = kafkaRawTopics;
	}

	public String getKafkaPatternTopics() {
		return kafkaPatternTopics;
	}

	public void setKafkaPatternTopics(String kafkaPatternTopics) {
		this.kafkaPatternTopics = kafkaPatternTopics;
	}

	public String getSearchDatabaseType() {
		return searchDatabaseType;
	}

	public void setSearchDatabaseType(String searchDatabaseType) {
		this.searchDatabaseType = searchDatabaseType;
	}

	public String getJiraUrl() {
		return jiraUrl;
	}

	public void setJiraUrl(String jiraUrl) {
		this.jiraUrl = jiraUrl;
	}

	public String getJiraUsername() {
		return jiraUsername;
	}

	public void setJiraUsername(String jiraUsername) {
		this.jiraUsername = jiraUsername;
	}

	public String getJiraPassword() {
		return jiraPassword;
	}

	public int getJiraWebhookPort() {
		return jiraWebhookPort;
	}

	public void setJiraWebhookPort(int jiraWebhookPort) {
		this.jiraWebhookPort = jiraWebhookPort;
	}

	public void setJiraPassword(String jiraPassword) {
		this.jiraPassword = jiraPassword;
	}

	public String getPdb() {
		return pdb;
	}

	public void setPdb(String pdb) {
		this.pdb = pdb;
	}

	public byte getMqType() {
		return mqType;
	}

	public void setMqType(byte mqType) {
		this.mqType = mqType;
	}

	public String getBrokerURL() {
		return brokerURL;
	}

	public void setBrokerURL(String brokerURL) {
		this.brokerURL = brokerURL;
	}

	public String getNameSrvAddr() {
		return nameSrvAddr;
	}

	public void setNameSrvAddr(String nameSrvAddr) {
		this.nameSrvAddr = nameSrvAddr;
	}

	public String getRouteKeyField() {
		return routeKeyField;
	}

	public void setRouteKeyField(String routeKeyField) {
		this.routeKeyField = routeKeyField;
	}

	public String getMqUserName() {
		return mqUserName;
	}

	public void setMqUserName(String mqUserName) {
		this.mqUserName = mqUserName;
	}

	public String getMqPassword() {
		return mqPassword;
	}

	public void setMqPassword(String mqPassword) {
		this.mqPassword = mqPassword;
	}

	public Set<String> getMqQueueSet() {
		return mqQueueSet;
	}

	public void setMqQueueSet(Set<String> mqQueueSet) {
		this.mqQueueSet = mqQueueSet;
	}

	public Set<String> getMqTopicSet() {
		return mqTopicSet;
	}

	public void setMqTopicSet(Set<String> mqTopicSet) {
		this.mqTopicSet = mqTopicSet;
	}

	public String getTcpUdpType() {
		return tcpUdpType;
	}

	public void setTcpUdpType(String tcpUdpType) {
		this.tcpUdpType = tcpUdpType;
	}

	public Map<String, Object> getConfig() {
		return config;
	}

	public void setConfig(Map<String, Object> config) {
		this.config = config;
	}

	public String getHiveConnType() {
		return hiveConnType;
	}

	public void setHiveConnType(String hiveConnType) {
		this.hiveConnType = hiveConnType;
	}

	public String getCustom_before_script() {
		return custom_before_script;
	}

	public void setCustom_before_script(String custom_before_script) {
		this.custom_before_script = custom_before_script;
	}

	public String getCustom_after_script() {
		return custom_after_script;
	}

	public void setCustom_after_script(String custom_after_script) {
		this.custom_after_script = custom_after_script;
	}

	public String getTidbPdServer() {
		return tidbPdServer;
	}

	public void setTidbPdServer(String tidbPdServer) {
		this.tidbPdServer = tidbPdServer;
	}

	public String getJsEngineName() {
		return jsEngineName;
	}

	public void setJsEngineName(String jsEngineName) {
		this.jsEngineName = jsEngineName;
	}

	public String getKafkaSaslMechanism() {
		return kafkaSaslMechanism;
	}

	public void setKafkaSaslMechanism(String kafkaSaslMechanism) {
		this.kafkaSaslMechanism = kafkaSaslMechanism;
	}

	public List<RelateDatabaseField> getFields(String table) {
		List<RelateDataBaseTable> relateDataBaseTables = getSchema().get("tables");
		return ((SchemaList<String, RelateDataBaseTable>) relateDataBaseTables).getFields(table);
	}

	public void clearFields(String table) {
		List<RelateDataBaseTable> relateDataBaseTables = SchemaProxy.getSchemaProxy().getSchemaMap(id).get("tables");
		((SchemaList<String, RelateDataBaseTable>) relateDataBaseTables).clearFields(table);
	}


	public String getVirtualHost() {
		return virtualHost;
	}

	public void setVirtualHost(String virtualHost) {
		this.virtualHost = virtualHost;
	}

	public HazelcastConstructType getHazelcastConstructType() {
		return hazelcastConstructType;
	}

	public void setHazelcastConstructType(HazelcastConstructType hazelcastConstructType) {
		this.hazelcastConstructType = hazelcastConstructType;
	}

	public String getHanaType() {
		return hanaType;
	}

	public void setHanaType(String hanaType) {
		this.hanaType = hanaType;
	}

	public String getProductGroup() {
		return productGroup;
	}

	public void setProductGroup(String productGroup) {
		this.productGroup = productGroup;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public String getVika_space_name() {
		return vika_space_name;
	}

	public void setVika_space_name(String vika_space_name) {
		this.vika_space_name = vika_space_name;
	}

	public String getVika_space_id() {
		return vika_space_id;
	}

	public void setVika_space_id(String vika_space_id) {
		this.vika_space_id = vika_space_id;
	}

	public String getUniqueName() {
		return uniqueName;
	}

	public void setUniqueName(String uniqueName) {
		this.uniqueName = uniqueName;
	}

	public Integer getShareCdcTTL() {
		return shareCdcTTL;
	}

	public void setShareCdcTTL(Integer shareCdcTTL) {
		this.shareCdcTTL = shareCdcTTL;
	}

	public String getPdkType() {
		return pdkType;
	}

	public void setPdkType(String pdkType) {
		this.pdkType = pdkType;
	}

	public boolean isSsl() {
		return ssl;
	}



	@Override
	public String toString() {
		return "Connections{" +
				"id='" + id + '\'' +
				", user_id='" + user_id + '\'' +
				", name='" + name + '\'' +
				", connection_type='" + connection_type + '\'' +
				", database_type='" + database_type + '\'' +
				", database_host='" + database_host + '\'' +
				", database_username='" + database_username + '\'' +
				", database_port=" + database_port +
				", database_password='" + database_password + '\'' +
				", database_name='" + database_name + '\'' +
				", database_uri='" + database_uri + '\'' +
				", ssl=" + ssl +
				", server_name='" + server_name + '\'' +
				", database_owner='" + database_owner + '\'' +
				", database_datetype_without_timezone='" + database_datetype_without_timezone + '\'' +
				", response_body=" + response_body +
				", retry=" + retry +
				", nextRetry=" + nextRetry +
				", schema=" + schema +
				", auth_db='" + auth_db + '\'' +
				", is_include_views=" + is_include_views +
				", additionalString='" + additionalString + '\'' +
				", seperate='" + seperate + '\'' +
				", folder='" + folder + '\'' +
				", prefix='" + prefix + '\'' +
				", prefixDir=" + prefixDir +
				", initialReadSize=" + initialReadSize +
				", increamentalTps=" + increamentalTps +
				", json_type='" + json_type + '\'' +
				", include_filename='" + include_filename + '\'' +
				", exclude_filename='" + exclude_filename + '\'' +
				", default_timeout_second=" + default_timeout_second +
				", connection_timeout_seconds=" + connection_timeout_seconds +
				", data_timeout_seconds=" + data_timeout_seconds +
				", tags='" + tags + '\'' +
				", ttl=" + ttl +
				", file_source_protocol='" + file_source_protocol + '\'' +
				", file_type='" + file_type + '\'' +
				", ftp_passive=" + ftp_passive +
				", vc_mode='" + vc_mode + '\'' +
				", tags_filter='" + tags_filter + '\'' +
				", data_content_xpath='" + data_content_xpath + '\'' +
				", unique_keys='" + unique_keys + '\'' +
				", auth_type='" + auth_type + '\'' +
				", request_interval=" + request_interval +
				", resp_pre_process='" + resp_pre_process + '\'' +
				", req_pre_process='" + req_pre_process + '\'' +
				", url_info=" + url_info +
				", collection_name='" + collection_name + '\'' +
				", listen_port=" + listen_port +
				", root_name='" + root_name + '\'' +
				", file_schema='" + file_schema + '\'' +
				", fileDefaultCharset='" + fileDefaultCharset + '\'' +
				", table_filter='" + table_filter + '\'' +
				", lobMaxSize=" + lobMaxSize +
				", thin_type='" + thin_type + '\'' +
				", acks='" + acks + '\'' +
				", batch_size=" + batch_size +
				", addtional_config=" + addtional_config +
				", kafkaBootstrapServers='" + kafkaBootstrapServers + '\'' +
				", kafkaRawTopics=" + kafkaRawTopics +
				", kafkaPatternTopics='" + kafkaPatternTopics + '\'' +
				", kafkaConsumerRequestTimeout=" + kafkaConsumerRequestTimeout +
				", kafkaConsumerUseTransactional=" + kafkaConsumerUseTransactional +
				", kafkaMaxPollRecords=" + kafkaMaxPollRecords +
				", kafkaPollTimeoutMS=" + kafkaPollTimeoutMS +
				", kafkaMaxFetchBytes=" + kafkaMaxFetchBytes +
				", kafkaMaxFetchWaitMS=" + kafkaMaxFetchWaitMS +
				", kafkaIgnoreInvalidRecord=" + kafkaIgnoreInvalidRecord +
				", kafkaProducerRequestTimeout=" + kafkaProducerRequestTimeout +
				", kafkaProducerUseTransactional=" + kafkaProducerUseTransactional +
				", kafkaRetries=" + kafkaRetries +
				", kafkaBatchSize=" + kafkaBatchSize +
				", kafkaAcks='" + kafkaAcks + '\'' +
				", kafkaLingerMS=" + kafkaLingerMS +
				", kafkaDeliveryTimeoutMS=" + kafkaDeliveryTimeoutMS +
				", kafkaMaxRequestSize=" + kafkaMaxRequestSize +
				", kafkaMaxBlockMS=" + kafkaMaxBlockMS +
				", kafkaBufferMemory=" + kafkaBufferMemory +
				", kafkaCompressionType='" + kafkaCompressionType + '\'' +
				", kafkaPartitionKey='" + kafkaPartitionKey + '\'' +
				", kafkaIgnorePushError=" + kafkaIgnorePushError +
				", mqType=" + mqType +
				", brokerURL='" + brokerURL + '\'' +
				", mqUserName='" + mqUserName + '\'' +
				", mqPassword='" + mqPassword + '\'' +
				", mqQueueSet=" + mqQueueSet +
				", mqTopicSet=" + mqTopicSet +
				", routeKeyField='" + routeKeyField + '\'' +
				", nameSrvAddr='" + nameSrvAddr + '\'' +
				", productGroup='" + productGroup + '\'' +
				", consumerGroup='" + consumerGroup + '\'' +
				", avro_namespace='" + avro_namespace + '\'' +
				", avro_encoder_type='" + avro_encoder_type + '\'' +
				", service='" + service + '\'' +
				", sql_field_name='" + sql_field_name + '\'' +
				", publish_format='" + publish_format + '\'' +
				", gridfs_header_type='" + gridfs_header_type + '\'' +
				", gridfs_header_config='" + gridfs_header_config + '\'' +
				", file_upload_chunk_size=" + file_upload_chunk_size +
				", file_upload_mode='" + file_upload_mode + '\'' +
				", sampleSize=" + sampleSize +
				", sheet_start='" + sheet_start + '\'' +
				", sheet_end='" + sheet_end + '\'' +
				", excel_header_type='" + excel_header_type + '\'' +
				", excel_header_start='" + excel_header_start + '\'' +
				", excel_header_end='" + excel_header_end + '\'' +
				", excel_value_start='" + excel_value_start + '\'' +
				", excel_value_end='" + excel_value_end + '\'' +
				", excel_header_concat_char='" + excel_header_concat_char + '\'' +
				", excel_password='" + excel_password + '\'' +
				", custom_type='" + custom_type + '\'' +
				", custom_initial_script='" + custom_initial_script + '\'' +
				", custom_cdc_script='" + custom_cdc_script + '\'' +
				", custom_ondata_script='" + custom_ondata_script + '\'' +
				", custom_before_script='" + custom_before_script + '\'' +
				", custom_after_script='" + custom_after_script + '\'' +
				", jiraUrl='" + jiraUrl + '\'' +
				", jiraUsername='" + jiraUsername + '\'' +
				", jiraPassword='" + jiraPassword + '\'' +
				", jiraWebhookPort=" + jiraWebhookPort +
				", sslValidate=" + sslValidate +
				", sslCA='" + sslCA + '\'' +
				", sslCert='" + sslCert + '\'' +
				", sslKey='" + sslKey + '\'' +
				", sslPass='" + sslPass + '\'' +
				", sslCRL='" + sslCRL + '\'' +
				", checkServerIdentity=" + checkServerIdentity +
				", krb5=" + krb5 +
				", krb5Keytab='" + krb5Keytab + '\'' +
				", krb5Conf='" + krb5Conf + '\'' +
				", krb5Principal='" + krb5Principal + '\'' +
				", krb5ServiceName='" + krb5ServiceName + '\'' +
				", kafkaSaslMechanism='" + kafkaSaslMechanism + '\'' +
				", pgsql_log_decorder_plugin_name='" + pgsql_log_decorder_plugin_name + '\'' +
				", node_name='" + node_name + '\'' +
				", socketReadTimeout=" + socketReadTimeout +
				", filePath='" + filePath + '\'' +
				", filePrefix='" + filePrefix + '\'' +
				", delimiter=" + delimiter +
				", isContainHeader=" + isContainHeader +
				", maxFileRow=" + maxFileRow +
				", zoneId=" + zoneId +
				", sysZoneId=" + sysZoneId +
				", customZoneId=" + customZoneId +
				", clusterName='" + clusterName + '\'' +
				", cacheConfig=" + cacheConfig +
				", supportUpdatePk=" + supportUpdatePk +
				", outputPath='" + outputPath + '\'' +
				", gridfsReadMode='" + gridfsReadMode + '\'' +
				", extendSourcePath=" + extendSourcePath +
				", schemaAutoUpdate=" + schemaAutoUpdate +
				", schemaAutoUpdateLastTime=" + schemaAutoUpdateLastTime +
				", dbCurrentTime='" + dbCurrentTime + '\'' +
				", dbFullVersion='" + dbFullVersion + '\'' +
				", pgSlotNames=" + pgSlotNames +
				", searchDatabaseType='" + searchDatabaseType + '\'' +
				", hiveConnType='" + hiveConnType + '\'' +
				", tidbPdServer='" + tidbPdServer + '\'' +
				", hanaType='" + hanaType + '\'' +
				", jsEngineName='" + jsEngineName + '\'' +
				", loadSchemaField=" + loadSchemaField +
				", tableConsumer=" + tableConsumer +
				", fileProperty=" + fileProperty +
				", pdb='" + pdb + '\'' +
				", tcpUdpType='" + tcpUdpType + '\'' +
				", config=" + config +
				", virtualHost='" + virtualHost + '\'' +
				", hazelcastConstructType=" + hazelcastConstructType +
				", vika_space_name='" + vika_space_name + '\'' +
				", vika_space_id='" + vika_space_id + '\'' +
				", uniqueName='" + uniqueName + '\'' +
				", shareCdcEnable=" + shareCdcEnable +
				", shareCdcTTL=" + shareCdcTTL +
				", pdkType='" + pdkType + '\'' +
				", pdkHash='" + pdkHash + '\'' +
				", redoLogParserEnable=" + redoLogParserEnable +
				", redoLogParserHost='" + redoLogParserHost + '\'' +
				", redoLogParserPort=" + redoLogParserPort +
				'}';
	}

	public static void main(String[] args) {
		System.out.println(AES256Util.Aes256Decode("2187c6efbef7341042a17dd7ff8d5a94", AES256Util.getKey()));
	}

	public boolean isCaseInSensitive() {
		boolean flag = false;
		if (database_type.equalsIgnoreCase("hive")) {
			flag = true;
		}
		return flag;
	}

	public String getDataSourceInfo() {
		return String.format("%s [%s]", database_name, name);
	}

	public boolean isShareCdcEnable() {
		return shareCdcEnable;
	}

	public void setShareCdcEnable(boolean shareCdcEnable) {
		this.shareCdcEnable = shareCdcEnable;
	}

	public boolean isRedoLogParserEnable() {
		return redoLogParserEnable;
	}

	public void setRedoLogParserEnable(boolean redoLogParserEnable) {
		this.redoLogParserEnable = redoLogParserEnable;
	}

	public String getRedoLogParserHost() {
		return redoLogParserHost;
	}

	public void setRedoLogParserHost(String redoLogParserHost) {
		this.redoLogParserHost = redoLogParserHost;
	}

	public Integer getRedoLogParserPort() {
		return redoLogParserPort;
	}

	public void setRedoLogParserPort(Integer redoLogParserPort) {
		this.redoLogParserPort = redoLogParserPort;
	}

	public String getPdkHash() {
		return pdkHash;
	}

	public void setPdkHash(String pdkHash) {
		this.pdkHash = pdkHash;
	}

	public Map<String, Object> getExtParam() {
		return extParam;
	}

	public void setExtParam(Map<String, Object> extParam) {
		this.extParam = extParam;
	}
}

