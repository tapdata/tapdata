//package com.tapdata.tm.commons.task.dto;
//
//import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
//import com.fasterxml.jackson.databind.annotation.JsonSerialize;
//import com.tapdata.tm.commons.base.convert.DagDeserialize;
//import com.tapdata.tm.commons.base.convert.DagSerialize;
//import com.tapdata.tm.commons.dag.DAG;
//import lombok.Data;
//import lombok.EqualsAndHashCode;
//
//
///**
// * SubTask
// */
//@EqualsAndHashCode(callSuper = true)
//@Data
//public class SubTaskDto extends ParentSubTaskDto {
//    /** 编辑中 待启动 */
//    public static final String STATUS_EDIT = "edit";
//    /** 准备中 */
//    public static final String STATUS_PREPARING = "preparing";
//    /** 调度中 */
//    public static final String STATUS_SCHEDULING = "scheduling";
//    /** 调度失败 */
//    public static final String STATUS_SCHEDULE_FAILED = "schedule_failed";
//    /** 待运行 */
//    public static final String STATUS_WAIT_RUN = "wait_run";
//    /** 运行中 */
//    public static final String STATUS_RUNNING = "running";
//    /** 停止中 */
//    public static final String STATUS_STOPPING = "stopping";
//    /** 暂停中 */
//    //public static final String STATUS_PAUSING = "pausing";
//    /** 错误 */
//    public static final String STATUS_ERROR = "error";
//    /** 完成 */
//    public static final String STATUS_COMPLETE = "complete";
//    /** 已停止 */
//    public static final String STATUS_STOP = "stop";
//    /** 已暂停 */
//    //public static final String STATUS_PAUSE = "pause";
//
//    /** 结构同任务中的dag相同 */
//    @JsonSerialize( using = DagSerialize.class)
//    @JsonDeserialize( using = DagDeserialize.class)
//    private DAG dag;
//
//    /**这个不需要入库，只需要查询返回的时候 给填充上task的属性信息，不需要taskdto中的dag信息 */
//    private TaskDto parentTask;
//
//    @JsonSerialize( using = DagSerialize.class)
//    @JsonDeserialize( using = DagDeserialize.class)
//    private DAG tempDag;
//
//    //用户对接pdk重置删除的标记
//    private Boolean resetFlag;
//    private Boolean deleteFlag;
//    private Long version;
//
//}
