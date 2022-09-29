package com.tapdata.tm.task.entity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.autoinspect.constants.ResultStatus;
import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
@Document("TaskAutoInspectResults")
public class TaskAutoInspectResultEntity extends BaseEntity {

    private String taskId; //任务编号
    private ResultStatus status; //状态
    private String checkAgainSN; //再次校验处理序号
    private String originalTableName; //源表名
    private LinkedHashMap<String, Object> originalKeymap; //源主键值

    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId sourceConnId; //源连接编号
    private String sourceConnName; // 不存库
    private Map<String, Object> sourceData; //目标数据

    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId targetConnId; //目标连接编号
    private String targetConnName; // 不存库
    private String targetTableName; //目标表名
    private LinkedHashMap<String, Object> targetKeymap;; //目标主键值
    private Map<String, Object> targetData; //目标数据

    public TaskAutoInspectResultEntity() {
    }
}
