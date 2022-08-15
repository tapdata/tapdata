package com.tapdata.tm.verison.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;


/**
 * Version of the tapdata
 */
@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VersionDto extends BaseDto {

    public static final String SCRIPT_VERSION_KEY = "script_version";

    private String key;
    private String version;

}