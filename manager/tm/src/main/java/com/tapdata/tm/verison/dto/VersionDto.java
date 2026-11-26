package com.tapdata.tm.verison.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;


/**
 * Version of the tapdata
 */
@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VersionDto extends BaseDto {

    public static final String SCRIPT_VERSION_KEY = "script_version";
    public static final String DAAS_SCRIPT_VERSION_KEY = "daas_script_version";
    public static final String DFS_SCRIPT_VERSION_KEY = "dfs_script_version";
    public static final String DRS_SCRIPT_VERSION_KEY = "drs_script_version";

    private String type;
    private String version;
    private List<String> executedPatches;

    public VersionDto(String type, String version) {
        this.type = type;
        this.version = version;
    }

}