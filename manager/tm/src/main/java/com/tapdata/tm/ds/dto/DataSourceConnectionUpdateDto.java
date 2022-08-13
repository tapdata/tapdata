
package com.tapdata.tm.ds.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import com.tapdata.tm.commons.schema.Tag;
import lombok.Data;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;

/**
 * 数据源连接
 */
@Data
public class DataSourceConnectionUpdateDto {
    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId id;
    /** 数据源的配置信息 jsonschema */
    private Map<String, Object> config;
    /** 连接类型 源，目标，源&目标 */
    private String connectionType;
    /** 定期加载schema */
    private Boolean everLoadSchema;
    /** 分类标签列表 */
    private List<Tag> tagList;
    /** 数据源连接名称 */
    private String name;

}
