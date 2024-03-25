package com.tapdata.tm.roleMapping.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.commons.base.convert.ObjectIdSerialize;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.role.dto.RoleDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;

@Data
@EqualsAndHashCode(callSuper=false)
public class RoleMappingDto extends BaseDto {
    private String principalType;
    private String principalId;

    @JsonSerialize( using = ObjectIdSerialize.class)
    @JsonDeserialize( using = ObjectIdDeserialize.class)
    private ObjectId roleId;

    private RoleDto role;

    @JsonProperty("self_only")
    private Boolean selfOnly;

    public RoleMappingDto() {
    }

    public RoleMappingDto(String principalType, String principalId, ObjectId roleId) {
        this.principalType = principalType;
        this.principalId = principalId;
        this.roleId = roleId;
    }
}
