package com.tapdata.tm.DataCatalogs.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;


/**
 * DataCatalogs
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class DataCatalogsDto extends BaseDto {
    private String name;

}
