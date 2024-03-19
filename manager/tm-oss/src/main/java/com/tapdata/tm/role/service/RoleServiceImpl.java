package com.tapdata.tm.role.service;

import com.tapdata.tm.Permission.entity.PermissionEntity;
import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.dto.RoleDto;
import com.tapdata.tm.role.repository.RoleRepository;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.repository.RoleMappingRepository;
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
public class RoleServiceImpl extends RoleService{

    public RoleServiceImpl(@NonNull RoleRepository repository) {
        super(repository);
    }

    @Override
    protected void beforeSave(RoleDto dto, UserDetail userDetail) {
        throw new BizException("TapOssNonSupportFunctionException");
    }
    public Page<RoleDto> find(Filter filter, UserDetail userDetail){
        throw new BizException("TapOssNonSupportFunctionException");
    }

    public boolean deleteById(ObjectId objectId, UserDetail userDetail){
        throw new BizException("TapOssNonSupportFunctionException");
    }

    public <T extends BaseDto> RoleDto save(RoleDto dto, UserDetail userDetail){
        throw new BizException("TapOssNonSupportFunctionException");
    }
}
