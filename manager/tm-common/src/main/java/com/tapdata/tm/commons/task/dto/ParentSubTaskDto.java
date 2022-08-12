//package com.tapdata.tm.commons.task.dto;
//
//import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
//import com.fasterxml.jackson.databind.annotation.JsonSerialize;
//import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
//import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
//import com.tapdata.tm.commons.base.dto.SchedulableDto;
//import com.tapdata.tm.commons.dag.EqField;
//import lombok.Data;
//import lombok.EqualsAndHashCode;
//import org.bson.types.ObjectId;
//
//import java.util.Date;
//import java.util.List;
//import java.util.Map;
//
//
///**
// * SubTask
// */
//@EqualsAndHashCode(callSuper = true)
//@Data
//public class ParentSubTaskDto extends SchedulableDto {
//
//    /** 任务id */
//    @JsonSerialize( using = ObjectIdSerialize.class)
//    @JsonDeserialize( using = ObjectIdDeserialize.class)
//    private ObjectId parentId;
//    /** 子任务名称， 默认任务名称 + （序号）*/
//    private String name;
//    /** 每个子任务包含以下的状态： 调度中，调度失败，带运行，运行中，停止中，已停止，已完成，错误。 */
//    private String status;
//    /** 扩展参数 */
//    private Map<String, Object> attrs;
//
//    /** 里程碑相关数据 */
//    private List<Milestone> milestones;
//    /** 报错信息 */
//    private List<Message> messages;
//
//    /** 需要用到的共享挖掘的task id, 每个数据源对应一个共享挖掘的任务id */
//    private Map<String, String> shareCdcTaskId;
//    /** 是否编辑中 */
//    private Boolean isEdit;
//
//    private Date startTime;
//    private Date scheduledTime;
//    private Date stoppingTime;
//    private Date runningTime;
//    private Date errorTime;
//    private Date pausedTime;
//    private Date finishTime;
//    private Date pingTime;
//
//    //需要重启标识
//    private Boolean restartFlag;
//    //重启需要的用户id
//    private String restartUserId;
//
//
//    /** 自动处理ddl */
//    private Boolean isOpenAutoDDL = true;
//
//
//    private String parentSyncType;
//
//    private Long tmCurrentTime;
//
//}
