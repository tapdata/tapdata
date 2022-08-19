package com.tapdata.tm.autoinspect.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.autoinspect.constants.ResultStatus;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class TaskAutoInspectResultDto extends BaseDto {

    private String taskId;
    private ResultStatus status;
    private String originalTableName;
    private LinkedHashMap<String, Object> originalKeymap;

    @JsonSerialize(using = ObjectIdSerialize.class)
    @JsonDeserialize(using = ObjectIdDeserialize.class)
    private ObjectId sourceConnId;
    private String sourceConnName;
    private Map<String, Object> sourceData;

    @JsonSerialize(using = ObjectIdSerialize.class)
    @JsonDeserialize(using = ObjectIdDeserialize.class)
    private ObjectId targetConnId;
    private String targetConnName;
    private String targetTableName;
    private Map<String, Object> targetData;

    public TaskAutoInspectResultDto() {
    }

    public TaskAutoInspectResultDto(
            @NonNull String taskId,
            @NonNull ObjectId sourceConnId,
            @NonNull String originalTableName,
            @NonNull LinkedHashMap<String, Object> originalKeymap,
            Map<String, Object> sourceData,
            @NonNull ObjectId targetConnId,
            @NonNull String targetTableName,
            Map<String, Object> targetData) {
        this.status = ResultStatus.Completed;
        this.taskId = taskId;
        this.originalTableName = originalTableName;
        this.originalKeymap = originalKeymap;
        this.sourceConnId = sourceConnId;
        this.sourceData = sourceData;
        this.targetConnId = targetConnId;
        this.targetTableName = targetTableName;
        this.targetData = targetData;
    }

}
