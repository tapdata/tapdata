package com.tapdata.tm.Permission.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.collection.ListUtil;
import com.tapdata.tm.Permission.dto.PermissionDto;
import com.tapdata.tm.Permission.dto.Status;
import com.tapdata.tm.Permission.entity.PermissionEntity;
import com.tapdata.tm.Permission.repository.PermissionRepository;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.service.RoleMappingService;
import com.tapdata.tm.utils.CollectionsUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators;
import org.springframework.data.mongodb.core.aggregation.ConvertOperators;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/10 8:41 下午
 * @description
 */
@Service
@Slf4j
public class PermissionService {

    @Autowired
    RoleMappingService roleMappingService;

    @Autowired
    PermissionRepository permissionRepository;

    /**
     * 先根据userId 到roleMapping 查询 principalType 为User的，roleId
     * 还是到roleMapping 查询 principalType 为  Permission 的，roleId 等于上面查出来roleId 的  principalId
     * 最后到permission表查询name 等于上面查出来的principalId 的记录
     *
     * @return
     */
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

    public List<PermissionDto> getByNames(List<String> names, Status status) {
        Query query = Query.query(Criteria.where("name").in(names).and("status").is(status.getValue()));
        List<PermissionEntity> permissionEntities = permissionRepository.getMongoOperations().find(query, PermissionEntity.class);

        List<PermissionDto> permissionDtoList = CollectionsUtils.deepCloneList(permissionEntities, PermissionDto.class);

         return permissionDtoList;
    }


    public List<PermissionEntity> find(Filter filter) {
        if (filter == null){
            filter = new Filter();
        }
        filter.getWhere().and("status", "enable");
        Document doc = new Document(filter.getWhere());
        return permissionRepository.getMongoOperations().find(Query.query(Criteria.matchingDocumentStructure(() -> doc)), PermissionEntity.class, "Permission");
    }

    public long count(Where where) {
        where.and("status", "enable");
        Document doc = new Document(where);
        return permissionRepository.getMongoOperations().count(Query.query(Criteria.matchingDocumentStructure(() -> doc)), PermissionEntity.class, "Permission");
    }
}
