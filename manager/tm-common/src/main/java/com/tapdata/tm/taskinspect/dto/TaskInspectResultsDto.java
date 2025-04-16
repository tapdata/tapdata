package com.tapdata.tm.taskinspect.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.taskinspect.cons.DiffTypeEnum;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;
import java.util.Map;

/**
 * 任务内校验-差异详情
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/9 21:58 Create
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class TaskInspectResultsDto extends BaseDto {

    public static final String FIELD_TASK_ID = "taskId";
    public static final String FIELD_HISTORY_ID = "historyId";
    public static final String FIELD_ROW_ID = "rowId";
    public static final String FIELD_KEYS = "keys";
    public static final String FIELD_SOURCE_TABLE = "sourceTable";
    public static final String FIELD_SOURCE_FIELDS = "sourceFields";
    public static final String FIELD_SOURCE = "source";
    public static final String FIELD_TARGET_TABLE = "targetTable";
    public static final String FIELD_TARGET_FIELDS = "targetFields";
    public static final String FIELD_TARGET = "target";
    public static final String FIELD_DIFF_TYPE = "diffType";
    public static final String FIELD_DIFF_FIELDS = "diffFields";

    private String taskId;
    private String historyId;
    private String rowId;
    private Map<String, Object> keys;
    private String sourceTable;
    private List<String> sourceFields;
    private Map<String, Object> source;
    private String targetTable;
    private List<String> targetFields;
    private Map<String, Object> target;
    private DiffTypeEnum diffType;
    private List<String> diffFields;
}
