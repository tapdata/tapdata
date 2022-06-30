package com.tapdata.tm.commons.schema;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import lombok.Data;
import org.bson.types.ObjectId;

import java.io.Serializable;

/**
 * @Author: Zed
 * @Date: 2021/9/2
 * @Description: connect内嵌tag文档
 */
@Data
public class Tag implements Serializable {

    /**
     * 分类id
     */
    @JsonSerialize(using = ObjectIdSerialize.class)
    @JsonDeserialize(using = ObjectIdDeserialize.class)
    private ObjectId id;
    /**
     * 分类名称
     */
    private String value;

}
