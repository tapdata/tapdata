package com.tapdata.tm.Permission.service;

import com.tapdata.tm.Permission.dto.PermissionDto;
import com.tapdata.tm.Permission.dto.Status;
import com.tapdata.tm.Permission.entity.PermissionEntity;
import com.tapdata.tm.Permission.repository.PermissionRepository;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.utils.CollectionsUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/10 8:41 下午
 * @description
 */
@Service
@Slf4j
public class PermissionServiceImpl implements PermissionService {
    @Autowired
    RoleMappingService roleMappingService;

    @Autowired
    PermissionRepository permissionRepository;
    @Override
    public List<PermissionDto> getCurrentPermission(String userId) {
        List<PermissionDto> permissionDtoList = new ArrayList<>();
        List<RoleMappingDto> roleMappingDtoList = roleMappingService.getByPrincipleTypeAndPrincipleId(PrincipleType.USER, userId);
        if (CollectionUtils.isNotEmpty(roleMappingDtoList)) {
            List<ObjectId> roleIdList = roleMappingDtoList.stream().map(RoleMappingDto::getRoleId).collect(Collectors.toList());

            List<RoleMappingDto> permissionRoleMappingDtoList = roleMappingService.getByRoleIdsAndPrincipleType(PrincipleType.PERMISSION, roleIdList);

            if (CollectionUtils.isNotEmpty(permissionRoleMappingDtoList)) {
                List<String> principleIds = permissionRoleMappingDtoList.stream().map(RoleMappingDto::getPrincipalId).collect(Collectors.toList());
                permissionDtoList = getByNames(principleIds,Status.ENABLE);
            }
        }
        return permissionDtoList;
    }

    @Override
    public List<PermissionDto> getByNames(List<String> names, Status status) {
        List<PermissionEntity> permissionEntities = permissionRepository.getMongoOperations().find(new Query(), PermissionEntity.class);

        List<PermissionDto> permissionDtoList = CollectionsUtils.deepCloneList(permissionEntities, PermissionDto.class);

        return permissionDtoList;
    }

    @Override
    public List<PermissionEntity> find(Filter filter) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public long count(Where where) {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<String> getAllParentIds() {
        throw new BizException("TapOssNonSupportFunctionException");
    }

    @Override
    public List<PermissionEntity> getTopPermissionAndNoChild(Set<String> codes) {
        throw new BizException("TapOssNonSupportFunctionException");
    }
}
