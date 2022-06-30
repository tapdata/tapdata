package com.tapdata.tm.ds.bean;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.index.Indexed;

/**
 * @Author: Zed
 * @Date: 2021/9/2
 * @Description: connect内嵌tag文档
 */
@AllArgsConstructor
@Getter
@Setter
public class Tag {

    /**
     * 分类id
     */
    @JsonSerialize(using = ObjectIdSerialize.class)
    @JsonDeserialize(using = ObjectIdDeserialize.class)
    @Indexed
    private ObjectId id;
    /**
     * 分类名称
     */
    @Indexed
    private String value;

}
