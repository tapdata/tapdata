
package com.tapdata.tm.ds.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.schema.ScheduleTimeEnum;
import com.tapdata.tm.commons.schema.bean.FileSources;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.commons.schema.bean.Schema;
import com.tapdata.tm.commons.schema.bean.UrlInfo;
import com.tapdata.tm.commons.util.CreateTypeEnum;
import com.tapdata.tm.ds.bean.ResponseBody;
import io.tapdata.pdk.apis.entity.Capability;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.TimeZone;

/**
 * 数据源连接
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Connections")
@CompoundIndexes(
        @CompoundIndex(name = "unq_ds_name_create_user", def = "{'createUser':1, 'name':1}", unique = true)
)
public class DataSourceEntity extends BaseEntity {
    public static final String STATUS_INVALID = "invalid";
    public static final String STATUS_READY = "ready";
    public static final String STATUS_TESTING = "testing";

    /** 数据源连接名称 */
    private String name;
    /** 数据源的配置信息 jsonschema */
    private Map<String, Object> config;

    /**
     * Encrypted storage configuration.
     */
    private String encryptConfig;

    /** 创建类型 */
    private CreateTypeEnum createType;
    /** 连接类型 源，目标，源&目标 */
    private String connection_type;
    /** 对应DataSourceDefinition databasetype */
    private String database_type;


    /** definition实体中的scope跟version */
    private String definitionScope;
    private String definitionVersion;
    private String definitionGroup;
    private String definitionPdkId;
    private String definitionBuildNumber;
    private List<String> definitionTags;

    private String pdkType;
    /**
     * used to identify PDKs
     */
    private String pdkHash;

    /** pdk定义目标是否可以连接的一个属性 */
    private List<String> pdkExpansion;

    private List<Capability> capabilities;
    /** */
    private Integer retry;
//    /** 是否删除的标记 */
//    @JsonProperty("is_delete")
//    private Boolean deleted;
    /** 定期加载schema */
    private Boolean everLoadSchema;
    /** schema版本 */
    private String schemaVersion;
    /** 状态  ready invalid */
    private String status;
    private String lastStatus;
    /**  */
    private Long tableCount;
    /** 检测时间 */
    private Long testTime;
    private Integer testCount;

    private String database_host;
    private String database_username;
    private Integer database_port;
    private String database_uri;
    private String database_name;
    private String database_password;
    /**
     * 不确定什么类型
     */
    private Object nextRetry;

    private Boolean ssl;
    private String fill;
    private String plain_password;
    private String table_filter;
    private String tableExcludeFilter;
    private Boolean openTableExcludeFilter;
    private String auth_db;
    private String project;

    /** 测试响应消息 */
    private ResponseBody response_body;
    /** 分类标签列表 */
//    private List<Tag> listtags;
    private List<Map<String,String>> listtags;
    private Boolean transformed;
    //    @JsonProperty("last_updated")
//    private String lastUpdated;
//    private String createTime;
    private String db_version;
    private Schema schema;
    private Boolean checkServerIdentity;
    private String sslCA;
    /**
     * 不确定什么类型
     */
    private String sslCRL;
    private String sslCert;
    private String sslKey;
    private String sslPass;
    private Boolean sslValidate;
    private String additionalString;
    private String dbFullVersion;
    private Integer loadCount;
    private String loadFieldsStatus;
    private Boolean schemaAutoUpdate;
    private String database_owner;
    private String thin_type;
    private Boolean supportUpdatePk;
    private String database_datetype_without_timezone;
    private String database_schema;
    private Boolean isUrl;
    private String node_name;
    private String plugin_name;
    private Boolean submit;
    private Boolean loadSchemaField;
    private Boolean editTest;
    private Object editorData;
    private Boolean updateSchema;
    private String username;
    private Integer connection_timeout_seconds;
    private Integer data_timeout_seconds;
    private Boolean ftp_passive;
    private String pgsql_log_decorder_plugin_name;
    private String search_databaseType;
    private String connectionUrl;
    private String lastUpdateTime;
    private String errorMsg;
    private Integer increamentalTps;
    private Integer initialReadSize;
    private Long schemaAutoUpdateLastTime;
    private String loadFieldErrMsg;
    private Boolean extendSourcePath;
    private String fileDefaultCharset;
    private String file_source_protocol;
    private List<FileSources> file_sources;
    private Integer file_upload_chunk_size;
    private String file_upload_mode;
    private String outputPath;
    private String overwriteSetting;
    private String vc_mode;
    private Boolean multiTenant;
    private String pdb;
    private String custom_cdc_script;
    private String custom_initial_script;
    private String custom_ondata_script;
    private String clusterName;
    private String kafkaBootstrapServers;
    private Set<String> kafkaRawTopics;
    private String kafkaPatternTopics;
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

    private String kafkaSaslMechanism;
    private Boolean krb5;
    private String krb5Keytab;
    private String krb5KeytabName;
    private String krb5Conf;
    private String krb5ConfName;
    private String krb5Principal;
    private String krb5ServiceName;

    private String jiraUrl;
    private String jiraUsername;
    private String jiraPassword;
    private String custom_type;
    private String collection_name;
    private String unique_keys;
    private Integer request_interval;
    private String auth_type;
    private String req_pre_process;
    private String resp_pre_process;
    private String data_sync_mode;
    private List<UrlInfo> url_info;
    private List<String> pgSlotNames;
    private String tcpUdpType;
    private String root_name;
    private String database_password_1;
    private String mqType;
    private String brokerURL;
    private String mqUserName;
    private String mqPassword;
    private List<String> mqQueueSet;
    private List<String> mqTopicSet;
    private String routeKeyField;
    private String virtualHost;
    private String nameSrvAddr;
    private String metadataLayer;
    private String hiveConnType;
    private String tin_type;
    private String model_update_status;

    private PlatformInfo platformInfo;
    private List<String> agentTags;

    private String basePath;
    private String path;
    private String description;
    private String apiVersion;

    private String file_type;

    private String tableMetadataInstanceId;
    private String connMetadataInstanceId;

    @Field("id")
    private String buildModelId;

    private String agentType;

    private String initId;
    private String sourceType;

    private String vika_space_name;
    private String vika_space_id;

    /**
     * QingFlow 属性
     */
    private String qingFlowUserId;
    private String qingFlowTagName;
    private String qingFlowTagId;

    /**
     * Doris 使用 Stream  Load 需要用到 http 接口
     */
    private String dorisHttp;
    private String uniqueName;

    /** 共享缓存开关 */
    private Boolean shareCdcEnable;
    private Integer shareCdcTTL;
    /**
     * 裸日志解析服务配置
     */
    private Boolean redoLogParserEnable;
    private String redoLogParserHost;
    private Integer redoLogParserPort;

    /** mq中的消费者组，生成者组 */
    private String productGroup;
    private String consumerGroup;

    /**
     * 访问节点
     * 类型 默认为“平台自动分配”可选择“用户手动指定” --AccessNodeTypeEnum
     */
    private String accessNodeType;

    /**
     * 后续可能是 flow engine group 选择多个的情况
     */
    private List<String> accessNodeProcessIdList;

    @Transient
    private String accessNodeProcessId;

    private TimeZone timeZone;

    private String connectionString;

    /** 加载表的规则，true为加载全部，false为加载部分 */
    private Boolean loadAllTables;
    private Integer increaseReadSize;
    // Share cdc external storage id
    private String shareCDCExternalStorageId;

    private Date loadSchemaTime;
    /** 数据源大于一万张表的时候， 设置的每天的某个时间点加载一次 可以设置为0-24点直接的值
     * @see ScheduleTimeEnum */
    private String schemaUpdateHour;
    /** 是否开启心跳写入，默认：false */
    private Boolean heartbeatEnable;

    /**
     * 后续 开放可以多选 flow engine 的话，这里一定要删除
     *
     */
    public String getAccessNodeProcessId() {
        return CollectionUtils.isNotEmpty(accessNodeProcessIdList) ? accessNodeProcessIdList.get(0) : "";
    }
}
