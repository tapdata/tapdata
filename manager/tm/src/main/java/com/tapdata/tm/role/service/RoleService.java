package com.tapdata.tm.role.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.entity.RoleEntity;
import com.tapdata.tm.role.repository.RoleRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

@Service
public class RoleService extends BaseService<RoleDto, RoleEntity, ObjectId, RoleRepository> {

    public RoleService(@NonNull RoleRepository repository) {
        super(repository, RoleDto.class, RoleEntity.class);
    }

    @Override
    protected void beforeSave(RoleDto dto, UserDetail userDetail) {

    }

}
