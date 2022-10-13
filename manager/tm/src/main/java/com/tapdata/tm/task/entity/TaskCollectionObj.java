package com.tapdata.tm.task.entity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.base.convert.DagDeserialize;
import com.tapdata.tm.commons.base.convert.DagSerialize;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.commons.task.dto.*;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.HashSet;
import java.util.List;


@EqualsAndHashCode(callSuper = true)
@Document("TaskCollectionObj")
@Data
public class TaskCollectionObj extends BaseEntity {

    private String agentId; //调度到指定的实例上去
    private String hostName;
    private List<String> agentTags; // 标签

    private Integer scheduleTimes;  // 调度次数
    private Long scheduleTime;  // 上次调度时间
    private String desc;

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

    /** 通知设置 多选  当任务停止（默认选中），当任务出错（默认选中），当任务被编辑，当任务开启*/
    private List<String> notifyTypes;

    /** 共享挖掘 */
    private Boolean shareCdcEnable;

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

    /** 增量时间点*/
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


    /** 是否编辑中 */
    private Boolean isEdit;

    //private Date startTime;
    private Date scheduledTime;
    private Date stoppingTime;
    private Date runningTime;
    private Date errorTime;
    private Date pausedTime;
    private Date finishTime;
    private Date pingTime;

    //需要重启标识
    private Boolean restartFlag;
    //重启需要的用户id
    private String restartUserId;


    /** 自动处理ddl */
    private String parentSyncType;

    private Long tmCurrentTime;

    private String transformUuid;
    private Boolean transformed;

    private int transformDagHash;


    @JsonSerialize( using = DagSerialize.class)
    @JsonDeserialize( using = DagDeserialize.class)
    private DAG dag;

    private Boolean shareCache=false;

    // 需要根据数据源是否支持 数据校验功能来判断
    private boolean canOpenInspect;
    //是否开启数据校验
    private Boolean isAutoInspect;

    private String creator;

    private boolean showInspectTips;

    private String inspectId;



    //用户对接pdk重置删除的标记
    private Boolean resetFlag;
    private Boolean deleteFlag;
    private Long version;

    private String taskRecordId;


}
