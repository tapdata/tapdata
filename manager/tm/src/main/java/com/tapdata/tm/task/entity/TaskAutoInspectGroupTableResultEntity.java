package com.tapdata.tm.task.entity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import lombok.Data;
import org.bson.types.ObjectId;

import java.io.Serializable;

@Data
public class TaskAutoInspectGroupTableResultEntity implements Serializable {

    private String taskId;
    private String originalTableName;
    private Integer toBeCompared;

    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId sourceConnId;
    private String sourceConnName;

    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId targetConnId;
    private String targetConnName;
    private String targetTableName;

    private Long counts;

}
