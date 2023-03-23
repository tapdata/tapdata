package com.tapdata.tm.commons.task.dto;

import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.EqField;
import com.tapdata.tm.commons.dag.SchemaTransformerResult;
import com.tapdata.tm.commons.schema.Tag;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

import java.util.*;


/**
 * Task
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ParentTaskDto extends SchedulableDto {

    public final static String TYPE_INITIAL_SYNC = "initial_sync";
    public final static String TYPE_INITIAL_SYNC_CDC = "initial_sync+cdc";
    public final static String TYPE_CDC = "cdc";

    /** 扩展参数*/
    private Map<String, Object> attrs;

    /** crontab表达式  */
    @EqField
    private String crontabExpression;

    /** crontab表达式开关 */
    @EqField
    private Boolean crontabExpressionFlag;

    /**去重写入机制   intelligent 智能去重， force 强制去重 */
    @EqField
    private String deduplicWriteMode;

    /** 描述*/
    private String desc;

    /**错误策略 */
    private List<ErrorStrategy> errorStrategy;

    /** 增量之后判断时间间隔 ms */
    @EqField
    private Long hysteresisInterval;

    /**增量滞后判断时间设置  默认关闭 */
    @EqField
    private Boolean increHysteresis;

    /** 增量数据处理模式，支持批量false  跟逐行true */
    @EqField
    private Boolean increOperationMode;

    /** 共享增量读取模式, 支持流式读取STREAMING和轮询读取POLLING两种模式 */
    @EqField
    private String increShareReadMode;

    /** 增量同步并发写入 默认关闭 */
    @EqField
    private Boolean increSyncConcurrency;

    /** 处理器线程数 */
    @EqField
    private Integer processorThreadNum;

    /** 增量读取条数 */
    @EqField
    private Integer increaseReadSize;
    /** 全量一批读取条数 */
    @EqField
    private Integer readBatchSize;

    /** 写入批量条数 */
    @EqField
    private Integer writeBatchSize;

    /** 写入每批最大等待时间 */
    @EqField
    private Long writeBatchWaitMs;

    /** 增量同步间隔*/
    @EqField
    private Integer increaseSyncInterval;

    /** 自动创建索引 */
    @EqField
    private Boolean isAutoCreateIndex;

    /** 过滤设置*/
    @EqField
    private Boolean isFilter;

    /** 自动处理ddl */
    @EqField
    private Boolean isOpenAutoDDL;

    /** 定时调度任务 */
    @EqField
    private Boolean isSchedule;

    /** 遇到错误时停止*/
    @EqField
    private Boolean isStopOnError;

    /** */
    private List<Message> message;

    /** 名称*/
    private String name;
    private String deleteName;

    /** 通知设置 多选  当任务停止（默认选中），当任务出错（默认选中），当任务被编辑，当任务开启*/
    @EqField
    private List<String> notifyTypes;

    /** 共享挖掘 */
    @EqField
    private Boolean shareCdcEnable;

    /** 子任务状态*/
    private List<Status> statuses;

    /** 状态*/
    private String status;

    /** 类型 [{label: '全量+增量', value: 'initial_sync+cdc'}, {label: '全量', value: 'initial_sync'}, {label: '增量', value: 'cdc'} ]*/
    @EqField
    private String type;

    /** 目标写入线程数*/
    @EqField
    private Integer writeThreadSize;

    /** 删除标记*/
    private boolean is_deleted;

    /** 编辑版本，用于前端页面的多端编辑区分 */
    private String editVersion;

    /** 增量时间点*/
    @EqField
    private List<TaskDto.SyncPoint> syncPoints;


    //数据迁移相关的渲染配置

    /** 源数据源id*/
    private String sourceId;
    /** 源数据源名称 */
    private String sourceName;
    /** 目标数据源id*/
    private String targetId;
    /** 目标数据源名称*/
    private String targetName;
    /** 源数据源类型   all为全部   这个为重新渲染用到的    */
    private String sourceType;
    /** 目标数据源类型   all为全部    */
    private String targetType;

    /**
     * 同步任务类型
     * sync 同步任务  migrate迁移  logCollector 挖掘任务 connHeartbeat 打点任务
     */
    private String syncType;

    private String rollback; //: "table"/"all"
    private String rollbackTable; //: "Leon_CAR_CUSTOMER";
    /**
     * 模型推演结果
     */
    private List<SchemaTransformerResult> metadataMappings;

    private double transformProcess;
    private String transformStatus;
    private List<Tag> listtags;

    /**
     * 计划开始事件开关
     */
    private boolean planStartDateFlag;
    /**
     * 计划开始时间
     */
    private Long planStartDate;

    /**
     * 界面展示的任务开始时间
     */
    private Date startTime;
    private Long lastStartDate;
    private Date stopTime;

    /**
     * 访问节点
     * 类型 默认为“平台自动分配”可选择“用户手动指定” --AccessNodeTypeEnum
     */
    private String accessNodeType;

    /**
     * 后续可能是 flow engine group 选择多个的情况
     */
    private List<String> accessNodeProcessIdList;

    private String accessNodeProcessId;

    private HashSet<String> heartbeatTasks;


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

    private String transformUuid;
    private Boolean transformed;

    private int transformDagHash;

    // 1分钟内不能强制停止（不存库，根据 stoppingTime 来判断）
    private Boolean canForceStopping;

    /**
     * 是否强制使用共享挖掘
     * true: 当共享挖掘不可用时，任务会报错停止
     * false: 当共享挖掘不可用时，任务会自动尝试正常的增量方式
     */
    private Boolean enforceShareCdc = true;

    public Integer getWriteBatchSize() {
        return Objects.isNull(writeBatchSize) ? 0 : writeBatchSize;
    }

    public Long getWriteBatchWaitMs() {
        return Objects.isNull(writeBatchWaitMs) ? 0L : writeBatchWaitMs;
    }

    public Boolean getCanForceStopping() {
        if (null == stoppingTime) {
            return null;
        }
        return System.currentTimeMillis() - stoppingTime.getTime() > 60 * 1000L;
    }

    public List<String> getAccessNodeProcessIdList() {
        accessNodeProcessIdList = new ArrayList<>();
        if (StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), accessNodeType)
                &&StringUtils.isNotBlank(accessNodeProcessId)) {
            accessNodeProcessIdList.add(accessNodeProcessId);
        } else {
            accessNodeType = AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name();
        }

        return accessNodeProcessIdList;
    }
}
