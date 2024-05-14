package com.tapdata.tm.roleMapping.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.service.RoleService;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.entity.RoleMappingEntity;
import com.tapdata.tm.roleMapping.repository.RoleMappingRepository;
import com.tapdata.tm.user.entity.User;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class RoleMappingServiceImpl extends RoleMappingService{
    public RoleMappingServiceImpl(@NonNull RoleMappingRepository repository) {
        super(repository);
    }

    @Override
    protected void beforeSave(RoleMappingDto dto, UserDetail userDetail) {
//        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public void initUserDefaultRole(User user, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<RoleMappingDto> getUser(PrincipleType principleType, String principleId) {
        return findAll(new Query());
    }

    @Override
    public List<RoleMappingDto> getByPrincipleTypeAndPrincipleId(PrincipleType principleType, String principleId) {
        return findAll(new Query());
    }

    @Override
    public List<RoleMappingDto> getByRoleIdsAndPrincipleType(PrincipleType principleType, List<ObjectId> roleIds) {
        List<RoleMappingDto> roleMappingDtoList = findAll(new Query());
        return roleMappingDtoList;
    }

    @Override
    public List<RoleMappingDto> saveAll(List<RoleMappingDto> roleDtos, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }
}
