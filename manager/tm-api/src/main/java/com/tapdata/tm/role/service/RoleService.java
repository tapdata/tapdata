package com.tapdata.tm.role.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.entity.RoleEntity;
import com.tapdata.tm.role.repository.RoleRepository;
import org.bson.types.ObjectId;

public interface RoleService extends IBaseService<RoleDto, RoleEntity, ObjectId, RoleRepository> {
    Page<RoleDto> find(Filter filter, UserDetail userDetail);

    boolean deleteById(ObjectId objectId, UserDetail userDetail);

    <T extends BaseDto> RoleDto save(RoleDto dto, UserDetail userDetail);
}
