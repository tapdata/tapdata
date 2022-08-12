//package com.tapdata.tm.task.entity;
//
//import java.util.Date;
//
//import com.tapdata.tm.commons.dag.DAG;
//import com.tapdata.tm.commons.task.dto.Message;
//import com.tapdata.tm.commons.task.dto.Milestone;
//import lombok.Data;
//import lombok.EqualsAndHashCode;
//import com.tapdata.tm.base.entity.BaseEntity;
//import org.bson.types.ObjectId;
//import org.springframework.data.mongodb.core.mapping.Document;
//
//import java.util.List;
//import java.util.Map;
//
//
///**
// * SubTask
// * millstone
// *  init，结构迁移: structureMigration, 全量同步: fullSync，增量同步: increSync
// *
// */
//@EqualsAndHashCode(callSuper = true)
//@Data
//@Document("SubTask")
//public class SubTaskEntity extends BaseEntity {
//    /** 任务id */
//    private ObjectId parentId;
//    /** 子任务名称， 默认任务名称 + （序号）*/
//    private String name;
//    /** 每个子任务包含以下的状态： 调度中，调度失败，带运行，运行中，停止中，已停止，已完成，错误。 */
//    private String status;
//    /** 结构同任务中的dag相同 */
//    private DAG dag;
//    /** 调度的标签，包括地域这些东西*/
//    private List<String> tags;
//    /** 报错信息 */
//    private List<Message> messages;
//    /** */
//    private String agentId;
//    /** 扩展参数 */
//    private Map<String, Object> attrs;
//    /** 里程碑相关数据 */
//    private List<Milestone> milestones;
//    /** 调度结果 */
//    private Object scheduleResult;
//
//    /** 是否编辑中 */
//    private Boolean isEdit;
//
//    /** 需要用到的共享挖掘的task id, 每个数据源对应一个共享挖掘的任务id */
//    private Map<String, String> shareCdcTaskId;
//
//    private Date startTime;
//    private Date scheduledTime;
//    private Date stoppingTime;
//    private Date runningTime;
//    private Date errorTime;
//    private Date pausedTime;
//    private Date finishTime;
//    private Date pingTime;
//    private DAG tempDag;
//
//    //需要重启标识
//    private Boolean restartFlag;
//    //重启需要的用户id
//    private String restartUserId;    //需要重启标识
//
//    /** 自动处理ddl */
//    private Boolean isOpenAutoDDL;
//
////    是否有差异数据  （true：有差异数据；false：无差异数据）
//    private  Boolean  hasInspectDiffData=false;
//
//    private Boolean resetFlag;
//    private Boolean deleteFlag;
//
//    private Long tmCurrentTime;
//
//}