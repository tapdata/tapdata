package com.tapdata.tm.metadatadefinition.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;

import java.util.List;


/**
 * MetadataDefinition
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class MetadataDefinitionDto extends BaseDto {
    private String value;
    @JsonProperty("parent_id")
    private String parent_id;
    @JsonProperty("item_type")
    private List<String> itemType;
    private String desc;
    private long objCount;
    private String linkId;
    private Boolean readOnly;

    private String desc;

    private long objCount;

}
