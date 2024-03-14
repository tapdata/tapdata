package com.tapdata.tm.roleMapping.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

@Document("RoleMapping")
@Data
@EqualsAndHashCode(callSuper=false)
public class RoleMappingEntity extends BaseEntity {

    private String principalId;
    private String principalType;
    private ObjectId roleId;

    @Field("self_only")
    private Boolean selfOnly;

}
