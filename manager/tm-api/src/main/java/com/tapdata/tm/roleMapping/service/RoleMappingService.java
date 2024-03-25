package com.tapdata.tm.roleMapping.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.entity.RoleMappingEntity;
import com.tapdata.tm.roleMapping.repository.RoleMappingRepository;
import com.tapdata.tm.user.entity.User;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public abstract class RoleMappingService extends BaseService<RoleMappingDto, RoleMappingEntity, ObjectId, RoleMappingRepository> {
    public RoleMappingService(@NonNull RoleMappingRepository repository) {
        super(repository, RoleMappingDto.class, RoleMappingEntity.class);
    }
    public abstract void initUserDefaultRole(User user, UserDetail userDetail);

    public abstract List<RoleMappingDto> getUser(PrincipleType principleType, String principleId);

    public abstract List<RoleMappingDto> getByPrincipleTypeAndPrincipleId(PrincipleType principleType, String principleId);

    public abstract List<RoleMappingDto> getByRoleIdsAndPrincipleType(PrincipleType principleType, List<ObjectId> roleIds);

    @Transactional
    public abstract List<RoleMappingDto> saveAll(List<RoleMappingDto> roleDtos, UserDetail userDetail);
}
