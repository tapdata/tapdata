
package com.tapdata.tm.commons.schema.bean;

import com.tapdata.tm.commons.schema.Schema;
import com.tapdata.tm.commons.schema.Tag;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.Date;
import java.util.List;

/**
 * 数据源连接
 */
@Data
public class SourceDto {

    public static final String STATUS_INVALID = "invalid";
    public static final String STATUS_READY = "ready";

    @Field("id")
    @Indexed
    private ObjectId id;
    @Indexed
    @Field("_id")
    private String _id;

    @Indexed
    private String customId;


    @Indexed
    private String createTime;
    @Indexed
    private Date last_updated;
    @Indexed
    private String user_id;
    @Indexed
    private String lastUpdBy;
    @Indexed
    private String createUser;

    /** 数据源连接名称 */
    private String name;
    /** 数据源的配置信息 jsonschema */
    //private Object config;
    /** 连接类型 源，目标，源&目标 */
    private String connection_type;
    /** 对应DataSourceDefinition databasetype */
    private String database_type;
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
    /**  */
    private Long tableCount;
    /** 检测时间 */
    private Long testTime;

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

    private String ssl;
    private String fill;
    private String plain_password;
    private String table_filter;
    private String auth_db;
    private String project;

    /** 测试响应消息 */
    private ResponseBody response_body;
    /** 分类标签列表 */
    private List<Tag> listtags;
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
    private String kafkaPatternTopics;
    private Boolean kafkaIgnoreInvalidRecord;
    private String kafkaAcks;
    private String kafkaCompressionType;
    private Boolean kafkaIgnorePushError;
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
}
