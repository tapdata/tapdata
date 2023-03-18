package com.tapdata.tm.role.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.entity.RoleEntity;
import com.tapdata.tm.role.repository.RoleRepository;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

@Service
public class RoleService extends BaseService<RoleDto, RoleEntity, ObjectId, RoleRepository> {

    @Autowired
    private RoleMappingService roleMappingService;

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
}
