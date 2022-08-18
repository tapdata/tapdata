package com.tapdata.tm.autoinspect.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.autoinspect.constants.AutoInspectResultStatusEnums;
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
    private AutoInspectResultStatusEnums status;
    private String originalTableName;
    private LinkedHashMap<String, Object> originalKeymap;

    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId sourceConnId;
    private String sourceConnName; // 不存库
    private Map<String, Object> sourceData;

    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId targetConnId;
    private String targetConnName; // 不存库
    private String targetTableName;
    private Map<String, Object> targetData;

    public TaskAutoInspectResultDto() {
    }

    public TaskAutoInspectResultDto(
            @NonNull String taskId
            , @NonNull ObjectId sourceConnId
            , @NonNull ObjectId targetConnId
            , @NonNull String originalTableName
            , @NonNull LinkedHashMap<String, Object> originalKeymap
            , Map<String, Object> sourceData
            , Map<String, Object> targetData) {
        this.taskId = taskId;
        this.sourceConnId = sourceConnId;
        this.targetConnId = targetConnId;
        this.status = AutoInspectResultStatusEnums.Completed;
        this.originalTableName = originalTableName;
        this.originalKeymap = originalKeymap;
        this.sourceData = sourceData;
        this.targetData = targetData;
    }

}
