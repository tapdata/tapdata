package com.tapdata.tm.job.entity;

import com.google.gson.annotations.SerializedName;
import com.tapdata.tm.job.dto.JobConnection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * Jobs
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Jobs")
public class JobEntity extends BaseEntity {

    private String name;

    private String priority;

    private Long first_ts;

    private Long last_ts;

    private JobConnection connections;

    private List<Object> mappings;

    private Map<String, Object> deployment;

    private String mapping_template;

    private String status;

    private Object source;

    private Object offset;

    private Boolean fullSyncSucc;

    private Boolean event_job_editted;

    private Boolean event_job_error;

    private Boolean event_job_started;

    private Boolean event_job_stopped;

    private String dataFlowId;

    private Map<String, Object> warning;

    private Object progressRateStats;

    private Object stats;

    private Long connector_ping_time;

    private Long ping_time;

    private List<String> dbhistory;
    private String dbhistoryStr;

    private ObjectId process_offset;

    private Boolean is_validate;

    private Object validate_offset;

    private Map<String, List<Object>> tableMappings;

    private Map<String, List<Object>> testTableMappings;

    private List<Object> syncObjects;

    private Boolean keepSchema = true;

    /**
     * 写入目标时的线程数
     */
    private Integer transformerConcurrency;

    /**
     * 处理器处理时的线程数
     */
    private Integer processorConcurrency;

    /**
     * oracle增量日志，解析sql的线程数
     */
    private Integer oracleLogSqlParseConcurrency;

    private String sync_type;

    private List<Object> op_filters;

    private String running_mode;

    private double sampleRate;

    private Boolean is_test_write;

    private Object test_write;

    private Boolean is_null_write;

    private Long lastStatsTimestamp;

    private Boolean drop_target;

    private Boolean increment;

    private Boolean connectorStopped;

    private Boolean transformerStopped;

    private Boolean needToCreateIndex;

    private Long notification_window;

    private Long notification_interval;

    private Long lastNotificationTimestamp;

    private Boolean isCatchUpLag;

    private Integer lagCount;

    private Long stopWaitintMills;

    private Integer progressFailCount;

    private Long nextProgressStatTS;

    private Integer trigger_log_remain_time;

    private Integer trigger_start_hour;

    private Boolean is_changeStream_mode;

    private Integer readBatchSize;

    private Integer readCdcInterval;

    private Boolean stopOnError = false;

    private Map<String, Object> row_count;

    private Map<String, Object> ts;

    private String dataQualityTag;

    private String executeMode;

    private Integer limit;

    @SerializedName("debug_order")
    private Integer debugOrder;

    @SerializedName("previous_job")
    private Object previousJob;

    private Boolean copyManagerOpen;

    private Boolean isOpenAutoDDL;

    private Object runtimeInfo;

    private List<Object> stages;

    private Boolean isSchedule;

    private String cronExpression;

    private Long nextSyncTime;

    private Integer cdcCommitOffsetInterval;

    private List<String> includeTables;

    /**
     * jdbc目标，定时器处理的间隔(ms)
     * default: 5 minutes
     */
    private Long timingTargetOffsetInterval;

    private Boolean reset;

    /**
     * 同一data flow下的任务，是否允许分布式执行
     * 存在cache节点的编排任务，必须在同一data engine下运行
     */
    private Boolean isDistribute;

    private String process_id;

    private String discardDDL;

    private Long cdcLagWarnSendMailLastTime;

    private String distinctWriteType;

    private Double maxTransactionLength;

    private Boolean isSerialMode;

    private List<Object> connectorErrorEvents;
    private List<Object> transformerErrorEvents;

    private String connectorLastSyncStage;
    private String transformerLastSyncStage;

    private Integer cdcFetchSize;

    private List<Object> milestones;

    private Object readShareLogMode;

    private Boolean cdcConcurrency;

    private Boolean cdcShareFilterOnServer; // 是否在服务端过滤（增量共享挖掘日志）

    private Boolean timeoutToStop; // 如果因任务超时停止的任务，标记为true，DataFlow启动时需要重置所有子任务的状态为false

    /**
     * es的分片数量
     */
    private Integer chunkSize;

    /**
     * 是否开启无主键同步
     */
    private Boolean noPrimaryKey;

    private Boolean onlyInitialAddMapping;

    private String agentId;

    private Boolean editDebug;

    /**
     * 用于标记任务中已经被删除，不需要同步的字段
     * key: 源表表名
     * value: 源表字段名
     */
    private Map<String, Set<String>> deletedFields;

}