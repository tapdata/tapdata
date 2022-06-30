package com.tapdata.tm.commons.task.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.util.Map;

/**
 * SubTask
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class TaskNodeRuntimeInfoDto extends BaseDto {
    /** 任务id */
    private ObjectId taskId;
    /** nodeID */
    private String nodeId;
    /** 结构迁移 */
    private StructureMigration structureMigration;
    /** 全量同步 */
    private FullSync fullSync;
    /** 子任务名称 */
    private Map<String, Object> attrs;

    @Data
    public static class StructureMigration implements Serializable {
        /** 状态 待迁移 pending， 迁移中executing， 已迁移 finished */
        private String status;
        /** 进度 0-100 页面显示 百分之0到百分之100 */
        private Integer progress;
    }

    @Data
    public static class FullSync implements Serializable {
        /** 开始时间 */
        private Long startTime;
        /** 结束时间 */
        private Long endTime;
        /** 总行数 */
        private Integer totalNum;
        /** 已完成行数 */
        private Integer successNum;
        /** 状态 待同步 pending， 同步中executing， 已同步 finished */
        private String status;
    }
}
