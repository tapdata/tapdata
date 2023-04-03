package com.tapdata.tm.role.service;

import com.tapdata.tm.Permission.entity.PermissionEntity;
import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.entity.RoleEntity;
import com.tapdata.tm.role.repository.RoleRepository;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class RoleService extends BaseService<RoleDto, RoleEntity, ObjectId, RoleRepository> {

    @Autowired
    private RoleMappingService roleMappingService;

    @Autowired
    private PermissionService permissionService;

    public RoleService(@NonNull RoleRepository repository) {
        super(repository, RoleDto.class, RoleEntity.class);
    }

    @Override
    protected void beforeSave(RoleDto dto, UserDetail userDetail) {

    }

    @Override
    public Page<RoleDto> find(Filter filter, UserDetail userDetail) {
        if (userDetail.isRoot()) {
            return super.find(filter, userDetail);
        }
        //default role
        filter.getWhere().and("_id", "5d31ae1ab953565ded04badd");
        return find(filter);
    }

    @Override
    public boolean deleteById(ObjectId objectId, UserDetail userDetail) {

        long count = roleMappingService.count(Query.query(Criteria.where("principalType").is("USER").and("roleId").is(objectId)));
        if (count > 0) {
            throw new BizException("Role.Unable.Delete");
        }

        return super.deleteById(objectId, userDetail);
    }

    @Override
    public <T extends BaseDto> RoleDto save(RoleDto dto, UserDetail userDetail) {
        boolean isCreate = false;
        if (dto.getId() == null) {
            isCreate = true;
        }
        RoleDto roleDto = super.save(dto, userDetail);
        //mapping default permission
        if (isCreate) {
            Filter filter = new Filter(Where.where("version", new HashMap<String, String>() {{
                put("$ne", "v2");
            }}));
            List<PermissionEntity> permissionEntities = permissionService.find(filter);
            List<RoleMappingDto> roleMappingDtos = permissionEntities.stream().map(p -> {
                RoleMappingDto roleMappingDto = new RoleMappingDto();
                roleMappingDto.setRoleId(roleDto.getId());
                roleMappingDto.setPrincipalId(p.getName());
                roleMappingDto.setPrincipalType(PrincipleType.PERMISSION.getValue());
                return roleMappingDto;
            }).collect(Collectors.toList());
            roleMappingService.save(roleMappingDtos, userDetail);
        }

        return roleDto;
    }
}
