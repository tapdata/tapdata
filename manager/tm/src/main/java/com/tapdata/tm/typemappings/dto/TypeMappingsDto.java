package com.tapdata.tm.typemappings.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.commons.base.dto.BaseDto;


/**
 * TypeMappings
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class TypeMappingsDto extends BaseDto {

    private String databaseType;

    private String version;

    private String dbType;

    private Long minPrecision;

    private Long maxPrecision;

    private Long minScale;

    private Long maxScale;

    private String tapType;

    private boolean dbTypeDefault;
    private boolean tapTypeDefault;

    private String direction;
    private String getter;
    private String minValue;
    private String maxValue;
    private int code;

}
