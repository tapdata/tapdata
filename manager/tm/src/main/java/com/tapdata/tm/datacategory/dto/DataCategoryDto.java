package com.tapdata.tm.datacategory.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;
import org.bson.types.ObjectId;


/**
 * DataCategory
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DataCategoryDto extends BaseDto {
    private String name;
    @JsonProperty("connection_id")
    private ObjectId connectionId;
    private DataSourceConnectionDto source;

}
