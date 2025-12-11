package com.tapdata.tm.skiperrortable.entity;

import com.tapdata.tm.base.entity.Entity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * 任务-错误表跳过-实体
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/21 15:23 Create
 */
@Data
@Document("TaskSkipErrorTable")
@EqualsAndHashCode(callSuper = true)
public class TaskSkipErrorTableEntity extends Entity {
    private String status;       // 状态
    private Date created;        // 创建时间
    private Date updated;        // 更新时间

    private String taskId;       // 任务编号
    private String sourceTable;  // 源表名
    private String targetTable;  // 目标表名
    private String skipStage;    // 标记在哪个阶段跳过的。参考：TapdataOffset
    private Date skipDate;       // 跳过时间
    private Date cdcDate;        // 增量时间，如果未进增量阶段，则该字段为 null
    private String errorCode;    // 错误信息
    private String errorMessage; // 错误信息


}
