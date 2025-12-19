package com.tapdata.tm.skiperrortable.dto;

import com.tapdata.tm.skiperrortable.SkipErrorTableStatusEnum;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * 任务-错误表跳过-实体
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/21 15:23 Create
 */
@Data
public class TaskSkipErrorTableDto implements Serializable {
    public static final String FIELD_CREATED = "created";
    public static final String FIELD_UPDATED = "updated";

    public static final String FIELD_TASK_ID = "taskId";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_SOURCE_TABLE = "sourceTable";
    public static final String FIELD_TARGET_TABLE = "targetTable";
    public static final String FIELD_SKIP_STAGE = "skipStage";
    public static final String FIELD_SKIP_DATE = "skipDate";
    public static final String FIELD_CDC_DATE = "cdcDate";
    public static final String FIELD_ERROR_CODE = "errorCode";
    public static final String FIELD_ERROR_MESSAGE = "errorMessage";

    private String id;                       // 编号
    private SkipErrorTableStatusEnum status; // 状态
    private Date created;                    // 创建时间
    private Date updated;                    // 更新时间

    private String taskId;        // 任务编号
    private String sourceTable;   // 源表名
    private String targetTable;   // 目标表名
    private String skipStage;     // 标记在哪个阶段跳过的。参考：TapdataOffset
    private Date skipDate;        // 跳过时间
    private Date cdcDate;         // 增量时间，如果未进增量阶段，则该字段为 null
    private String errorCode;     // 错误代码，可能为空
    private String errorMessage;  // 错误信息

}
