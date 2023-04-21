package com.tapdata.tm.task.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.SchemaTransformerResult;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.task.dto.*;
import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleVO;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.data.annotation.Transient;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


/**
 * Task
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Task")
public class TaskEntity extends BaseEntity {
    /** 扩展参数*/
    private Map<String, Object> attrs;

    /** crontab表达式  */
    private String crontabExpression;
    private DAG dag;
    /**去重写入机制   intelligent 智能去重， force 强制去重 */
    private String deduplicWriteMode;

    /** 描述*/
    private String desc;

    /**错误策略 */
    private List<ErrorStrategy> errorStrategy;

    /** 增量之后判断时间间隔 ms */
    private Long hysteresisInterval;

    /**增量滞后判断时间设置  默认关闭 */
    private Boolean increHysteresis;

    /** 增量数据处理模式，支持批量false  跟逐行true */
    private Boolean increOperationMode;

    /** 共享增量读取模式, 支持流式读取STREAMING和轮询读取POLLING两种模式 */
    private String increShareReadMode;

    /** 增量同步并发写入 默认关闭 */
    private Boolean increSyncConcurrency;

    /** 处理器线程数 */
    private Integer processorThreadNum;

    /** 增量读取条数 */
    private Integer increaseReadSize;
    /** 全量一批读取条数 */
    private Integer readBatchSize;
    /** 写入批量条数 */
    private Integer writeBatchSize;
    /** 写入每批最大等待时间 */
    private Long writeBatchWaitMs;

    /** 增量同步间隔*/
    private Integer increaseSyncInterval;

    /** 自动创建索引 */
    private Boolean isAutoCreateIndex;

    /** 过滤设置*/
    private Boolean isFilter;

    /** 自动处理ddl */
    private Boolean isOpenAutoDDL;

    /** 定时调度任务 */
    private Boolean isSchedule;

    /** 遇到错误时停止*/
    private Boolean isStopOnError;

    /** */
    private List<Message> message;

    /** 名称*/
    private String name;
    private String deleteName;

    /** 通知设置 多选  当任务停止（默认选中），当任务出错（默认选中），当任务被编辑，当任务开启*/
    private List<String> notifyTypes;

    /** 共享挖掘 */
    private Boolean shareCdcEnable = false;

    /** 子任务状态*/
    private List<Status> statuses;

    /** 状态*/
    private String status;

    /** 类型 [{label: '全量+增量', value: 'initial_sync+cdc'}, {label: '全量', value: 'initial_sync'}, {label: '增量', value: 'cdc'} ]*/
    private String type;

    /** 目标写入线程数*/
    private Integer writeThreadSize;

    /** 删除标记*/
    private boolean is_deleted;

    /** 编辑版本，用于前端页面的多端编辑区分 */
    private String editVersion;

    /** 增量其实时间点*/
    private List<TaskDto.SyncPoint> syncPoints;
    //private DAG temp;

    /**
     * 同步任务类型
     * sync 同步任务  migrate迁移  logCollector 挖掘任务
     */
    private String syncType;


    private Boolean shareCache=false;
    /**
     * 模型推演结果
     */
    private List<SchemaTransformerResult> metadataMappings;

    private List<Tag> listtags;

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

    //是否开启数据校验 （true：开启校验；false：关闭校验）
    private Boolean isAutoInspect = false;
    private Boolean canOpenInspect = false;

    private SkipErrorEventEntity skipErrorEvent;

    /**
     * 计划开始时间
     */
    private Long planStartDate;
    private boolean planStartDateFlag;

    /**
     * 界面展示的任务开始时间
     */
    private Date startTime;
    private Long lastStartDate;
    private Date stopTime;
    private Long scheduleDate;
    private Long stopedDate;

    private HashSet<String> heartbeatTasks;

    private String taskRecordId;

    /** 里程碑相关数据 */
    private List<Milestone> milestones;
    /** 报错信息 */
    private List<Message> messages;

    /** 需要用到的共享挖掘的task id, 每个数据源对应一个共享挖掘的任务id */
    private Map<String, String> shareCdcTaskId;
    /** 是否编辑中 */
    private Boolean isEdit;

    //private Date startTime;
    private Date scheduledTime;
    private Date schedulingTime;
    private Date stoppingTime;
    private Date runningTime;
    private Date errorTime;
    private Date pausedTime;
    private Date finishTime;
    private Long pingTime;

    //需要重启标识
    private Boolean restartFlag;
    //重启需要的用户id
    private String restartUserId;


    /** 自动处理ddl */
    //private Boolean isOpenAutoDDL = true;
    //todo 这个参数可能要删除掉
    private String parentSyncType;

    private Long tmCurrentTime;

    //用户对接pdk重置删除的标记
    private Boolean resetFlag;
    private Boolean deleteFlag;
    private Long version;

    private String agentId; //调度到指定的实例上去

    private String transformUuid;
    private Boolean transformed;

    private List<AlarmSettingVO> alarmSettings;
    private List<AlarmRuleVO> alarmRules;

    private Map<String, Object> logSetting;
    private Integer resetTimes;

    private Long currentEventTimestamp;
    private Long snapshotDoneAt;

    private boolean needCreateRecord;

    private Boolean crontabExpressionFlag;

    private String testTaskId;
    private String transformTaskId;
    private int stopRetryTimes;
    private Boolean enforceShareCdc = true;

    /** ldp 类型， fdm, mdm   为空或者其他为其他任务*/
    private String ldpType;

    /** ldp需要新增的表名列表 */
    private List<String> ldpNewTables;

    private String pageVersion;

    public String getAccessNodeProcessId() {
        return CollectionUtils.isNotEmpty(accessNodeProcessIdList) ? accessNodeProcessIdList.get(0) : "";
    }
}
