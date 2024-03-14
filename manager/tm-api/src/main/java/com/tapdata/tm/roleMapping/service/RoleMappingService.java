package com.tapdata.tm.roleMapping.service;

import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.entity.RoleMappingEntity;
import com.tapdata.tm.roleMapping.repository.RoleMappingRepository;
import com.tapdata.tm.user.entity.User;
import org.bson.types.ObjectId;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface RoleMappingService extends IBaseService<RoleMappingDto, RoleMappingEntity, ObjectId, RoleMappingRepository> {
    void initUserDefaultRole(User user, UserDetail userDetail);

    List<RoleMappingDto> getUser(PrincipleType principleType, String principleId);

    List<RoleMappingDto> getByPrincipleTypeAndPrincipleId(PrincipleType principleType, String principleId);

    List<RoleMappingDto> getByRoleIdsAndPrincipleType(PrincipleType principleType, List<ObjectId> roleIds);

    @Transactional
    List<RoleMappingDto> saveAll(List<RoleMappingDto> roleDtos, UserDetail userDetail);
}
