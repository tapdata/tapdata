package com.tapdata.tm.task.entity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.autoinspect.constants.ResultStatus;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
@Document("TaskAutoInspectResults")
public class TaskAutoInspectResultEntity extends BaseEntity {

    private String taskId;
    private ResultStatus status;
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

}
