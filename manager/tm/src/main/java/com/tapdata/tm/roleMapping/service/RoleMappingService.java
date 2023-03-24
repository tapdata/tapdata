package com.tapdata.tm.roleMapping.service;

import com.tapdata.tm.base.service.BaseService;
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
public class RoleMappingService extends BaseService<RoleMappingDto, RoleMappingEntity, ObjectId, RoleMappingRepository> {

    @Autowired
    RoleService roleService;

    public RoleMappingService(@NonNull RoleMappingRepository repository) {
        super(repository, RoleMappingDto.class, RoleMappingEntity.class);
    }

    @Override
    protected void beforeSave(RoleMappingDto dto, UserDetail userDetail) {

    }

    /**
     * 初始化用户默认角色
     *
     * @param user
     * @param userDetail
     */
    public void initUserDefaultRole(User user, UserDetail userDetail) {
        List<RoleMappingDto> roleMappings = roleService.findAll(Query.query(Criteria.where("register_user_default").is(true)))
                .stream().map(role -> {
                    RoleMappingDto roleMapping = new RoleMappingDto();
                    roleMapping.setPrincipalType("USER");
                    roleMapping.setRoleId(role.getId());
                    roleMapping.setPrincipalId(user.getId().toHexString());
                    return roleMapping;
                }).collect(Collectors.toList());

        save(roleMappings, userDetail);
    }

    public List<RoleMappingDto> getUser(PrincipleType principleType, String principleId) {
        Query query = Query.query(Criteria.where("principalId").is(principleId).and("principalType").is(principleType.getValue()));
        List<RoleMappingDto> roleMappingDtoList = findAll(query);
        if (CollectionUtils.isNotEmpty(roleMappingDtoList)) {
            for (RoleMappingDto roleMappingDto : roleMappingDtoList) {
                ObjectId roleId = roleMappingDto.getRoleId();
                RoleDto roleDto = roleService.findById(roleId);
                roleMappingDto.setRole(roleDto);
            }
        }
        return roleMappingDtoList;
    }

    public List<RoleMappingDto> getByPrincipleTypeAndPrincipleId(PrincipleType principleType, String principleId) {
        Query query = Query.query(Criteria.where("principalId").is(principleId).and("principalType").is(principleType.getValue()));
        List<RoleMappingDto> roleMappingDtoList = findAll(query);
        return roleMappingDtoList;
    }


    public List<RoleMappingDto> getByRoleIdsAndPrincipleType(PrincipleType principleType, List<ObjectId> roleIds) {
        Query query = Query.query(Criteria.where("roleId").in(roleIds).and("principalType").is(principleType.getValue()));
        List<RoleMappingDto> roleMappingDtoList = findAll(query);
        return roleMappingDtoList;
    }

    /**
     * 编辑用户，修改关联角色时调用，
     * 先删除原来principalType =USER &&principalId =userId的数据
     * 再保存前端传过来最新的
     *
     * @param roleDtos
     */
    @Transactional
    public List<RoleMappingDto> saveAll(List<RoleMappingDto> roleDtos, UserDetail userDetail) {
        for (RoleMappingDto roleMappingDto : roleDtos) {
            String uerId = roleMappingDto.getPrincipalId();
            Query query = Query.query(Criteria.where("principalType").is(PrincipleType.USER.getValue()).and("principalId").is(uerId)
                    .and("roleId").is(roleMappingDto.getRoleId()));
            deleteAll(query);
        }
        save(roleDtos, userDetail);
        return roleDtos;
    }


}
