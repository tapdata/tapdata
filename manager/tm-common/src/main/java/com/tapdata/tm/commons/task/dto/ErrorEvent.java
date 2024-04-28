package com.tapdata.tm.commons.task.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bson.types.ObjectId;

import java.io.Serializable;

@Data
@NoArgsConstructor
public class ErrorEvent implements Serializable {
    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId id;
    private String message;

    private String code;

    private String stacks;

    private Boolean skip = false;

    public ErrorEvent(String message,String code,String stacks) {
        this.id = new ObjectId();
        this.message = message;
        this.code = code;
        this.stacks = stacks;
    }
}
