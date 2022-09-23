package com.tapdata.tm.autoinspect.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.constants.ResultStatus;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class TaskAutoInspectResultDto extends BaseDto {

    private @NonNull String taskId;
    private @NonNull String checkAgainSN; //再次校验处理序号
    private @NonNull ResultStatus status;
    private @NonNull String originalTableName;
    private @NonNull LinkedHashMap<String, Object> originalKeymap;

    @JsonSerialize(using = ObjectIdSerialize.class)
    @JsonDeserialize(using = ObjectIdDeserialize.class)
    private @NonNull ObjectId sourceConnId;
    private String sourceConnName;
    private @NonNull Map<String, Object> sourceData;

    @JsonSerialize(using = ObjectIdSerialize.class)
    @JsonDeserialize(using = ObjectIdDeserialize.class)
    private @NonNull ObjectId targetConnId;
    private String targetConnName;
    private @NonNull String targetTableName;
    private @NonNull LinkedHashMap<String, Object> targetKeymap;
    private Map<String, Object> targetData;

    public TaskAutoInspectResultDto() {
    }

    public TaskAutoInspectResultDto(@NonNull String taskId, @NonNull CompareRecord sourceRecord) {
        setCheckAgainSN(AutoInspectConstants.CHECK_AGAIN_DEFAULT_SN);
        setStatus(ResultStatus.Completed);
        setCreateAt(new Date());
        setLastUpdAt(new Date());
        setTaskId(taskId);
        fillSource(sourceRecord);
    }

    public CompareRecord toSourceRecord() {
        CompareRecord record = new CompareRecord(originalTableName, sourceConnId);
        record.setOriginalKey(getOriginalKeymap());
        record.setData(getSourceData());
        return record;
    }

    public void fillSource(@NonNull CompareRecord sourceRecord) {
        setSourceConnId(sourceRecord.getConnectionId());
        setOriginalTableName(sourceRecord.getTableName());
        setOriginalKeymap(sourceRecord.getOriginalKey());
        setSourceData(sourceRecord.getData());
    }

    public void fillTarget(@NonNull CompareRecord targetRecord) {
        setTargetConnId(targetRecord.getConnectionId());
        setTargetTableName(targetRecord.getTableName());
        setTargetKeymap(targetRecord.getOriginalKey());
        setTargetData(targetRecord.getData());
    }

    /**
     * @param taskId       task id
     * @param sourceRecord source record info, the sourceRecord.data is not allowed to be null
     * @param targetRecord target record info
     * @return difference record
     */
    public static TaskAutoInspectResultDto parse(@NonNull String taskId, @NonNull CompareRecord sourceRecord, @NonNull CompareRecord targetRecord) {
        TaskAutoInspectResultDto dto = new TaskAutoInspectResultDto(taskId, sourceRecord);
        dto.fillTarget(targetRecord);
        return dto;
    }

    /**
     * @param taskId          task id
     * @param sourceRecord    source record info, the sourceRecord.data is not allowed to be null
     * @param targetTableName target table name
     * @param targetConnId    target connections id
     * @return difference record
     */
    public static TaskAutoInspectResultDto parseNoneTarget(@NonNull String taskId, @NonNull CompareRecord sourceRecord, @NonNull ObjectId targetConnId, @NonNull String targetTableName) {
        TaskAutoInspectResultDto dto = new TaskAutoInspectResultDto(taskId, sourceRecord);
        dto.setTargetConnId(targetConnId);
        dto.setTargetTableName(targetTableName);
        dto.setTargetKeymap(new LinkedHashMap<>());
        sourceRecord.getKeyNames().forEach(k -> {
            dto.getTargetKeymap().put(k, sourceRecord.getDataValue(k));
        });
        dto.setTargetData(null);
        return dto;
    }

}
