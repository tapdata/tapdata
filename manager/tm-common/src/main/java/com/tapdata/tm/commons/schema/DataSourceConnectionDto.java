package com.tapdata.tm.commons.schema;

import com.google.common.collect.Lists;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.bean.FileSources;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.commons.schema.bean.ResponseBody;
import com.tapdata.tm.commons.schema.bean.UrlInfo;
import com.tapdata.tm.commons.util.CreateTypeEnum;
import io.tapdata.pdk.apis.entity.Capability;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/11/15 上午10:25
 */
@Data
public class DataSourceConnectionDto extends BaseDto {

        public static final String STATUS_INVALID = "invalid";
        public static final String STATUS_READY = "ready";
        public static final String STATUS_TESTING = "testing";

        /** 数据源连接名称 */
        private String name;
        /** 数据源的配置信息 符合connectiondefinition的jsonschema的json  */
        private Map<String, Object> config;
        /** 创建类型 */
        private CreateTypeEnum createType;
        /** 连接类型 源，目标，源&目标 */
        private String connection_type;
        /** 对应DataSourceDefinition type */
        private String database_type;

        /** definition实体中的scope跟version */
        private String definitionScope;
        private String definitionVersion;
        private String definitionGroup;
        private String definitionPdkId;
        private String definitionBuildNumber;
        private List<String> definitionTags;

        /**
         * 用户区分是不是pdk数据源类型
         */
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
        /** 加载表的规则，true为加载全部，false为加载部分 */
        private Boolean loadAllTables;
        /** 自定义的加载表的表名列表  都号隔开*/
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
        private String db_version;
        private com.tapdata.tm.commons.schema.bean.Schema schema;
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
        /** 共享挖掘的数据源唯一名称   如果不同的数据源连接的这个uniqueName是一样的，则表示为相同的共享挖掘数据源  为空的时候，取id作为唯一判断*/
        private String uniqueName;
        /** 是否开启共享挖掘任务 */
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

        private String accessNodeType;

        private String accessNodeProcessId;

        private List<String> accessNodeProcessIdList;

        private boolean accessNodeTypeEmpty;

        private TimeZone timeZone;

        private String connectionString;

        private Map<String, Object> extParam;
        // Share cdc external storage id
        private String shareCDCExternalStorageId;

        private Date loadSchemaTime;

        /** 数据源大于一万张表的时候， 设置的每天的某个时间点加载一次 可以设置为0-24点直接的值
         * @see ScheduleTimeEnum*/
        private String schemaUpdateHour;


        /**
         * 后续 开放可以多选 flow engine 的话，这里一定要删除
         *
         */
        public List<String> getAccessNodeProcessIdList() {
                if (StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), accessNodeType)
                        &&StringUtils.isNotBlank(accessNodeProcessId)) {
                        return Lists.newArrayList(accessNodeProcessId);
                } else {
                        return Lists.newArrayList();
                }
        }
        public List<String> getTrueAccessNodeProcessIdList() {
                if (StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), accessNodeType)
                        &&StringUtils.isNotBlank(accessNodeProcessId)) {
                        return Lists.newArrayList(accessNodeProcessId);
                } else {
                        return null;
                }
        }

        public boolean isAccessNodeTypeEmpty() {
                return StringUtils.isBlank(accessNodeType);
        }

        public String getAlarmInfo() {
                return "DataSourceConnectionDto{" +
                        "name='" + name + '\'' +
                        ", connection_type='" + connection_type + '\'' +
                        ", retry=" + retry +
                        ", status='" + status + '\'' +
                        ", tableCount=" + tableCount +
                        ", loadAllTables=" + loadAllTables +
                        ", loadFieldsStatus=" + loadFieldsStatus +
                        ", loadFieldErrMsg=" + loadFieldErrMsg +
                        '}';
        }
}
