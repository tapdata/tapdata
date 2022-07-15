package com.tapdata.tm.task.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.SchemaTransformerResult;
import com.tapdata.tm.commons.task.dto.ErrorStrategy;
import com.tapdata.tm.commons.task.dto.Message;
import com.tapdata.tm.commons.task.dto.SubStatus;
import com.tapdata.tm.commons.task.dto.TaskDto;
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
    private Boolean shareCdcEnable = false;

    /** 子任务状态*/
    private List<SubStatus> statuses;

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

    private List<Map<String,String>> listtags;

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
    private Boolean isAutoInspect =true;

    /**
     * 计划开始时间
     */
    private Long planStartDate;
    private boolean planStartDateFlag;

    /**
     * 界面展示的任务开始时间
     */
    private Date startTime;

    private HashSet<String> heartbeatTasks;

    private String migrateModelStatus;

    public String getAccessNodeProcessId() {
        return CollectionUtils.isNotEmpty(accessNodeProcessIdList) ? accessNodeProcessIdList.get(0) : "";
    }
}
