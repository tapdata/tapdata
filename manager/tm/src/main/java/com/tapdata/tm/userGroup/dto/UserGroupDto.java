package com.tapdata.tm.userGroup.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.base.IDataPermissionDto;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;


/**
 * UserGroup
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class UserGroupDto extends BaseDto implements IDataPermissionDto {

    private String name;

    @JsonProperty("parent_id")
    private String parentId;

    @JsonProperty("parent_gid")
    private String parentGid;

    private String gid;
}
