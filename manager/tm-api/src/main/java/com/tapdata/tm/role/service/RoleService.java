package com.tapdata.tm.role.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.entity.RoleEntity;
import com.tapdata.tm.role.repository.RoleRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;

public abstract class RoleService extends BaseService<RoleDto, RoleEntity, ObjectId, RoleRepository> {
    public RoleService(@NonNull RoleRepository repository) {
        super(repository, RoleDto.class, RoleEntity.class);
    }
    public Page<RoleDto> find(Filter filter, UserDetail userDetail){
        return super.find(filter, userDetail);
    }

    public boolean deleteById(ObjectId objectId, UserDetail userDetail){
        return super.deleteById(objectId, userDetail);
    }

    public <T extends BaseDto> RoleDto save(RoleDto dto, UserDetail userDetail){
        return super.save(dto, userDetail);
    }
}
