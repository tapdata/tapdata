
package com.tapdata.tm.classification.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * 资源分类
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ClassificationDto extends BaseDto {

    /** 名称 */
    private String value;
    /** 父id */
    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId parentId;
    /**
     * 分类适用资源：dataSource、view、table、collection、api、job、dag
     */
    @JsonProperty("item_type")
    private List<String> itemType;
}
