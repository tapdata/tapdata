package com.tapdata.tm.roleMapping.service;

import cn.hutool.core.collection.CollUtil;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.roleMapping.dto.PrincipleType;
import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import com.tapdata.tm.roleMapping.entity.RoleMappingEntity;
import com.tapdata.tm.roleMapping.repository.RoleMappingRepository;
import com.tapdata.tm.user.entity.User;
import com.tapdata.tm.utils.Lists;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public abstract class RoleMappingService extends BaseService<RoleMappingDto, RoleMappingEntity, ObjectId, RoleMappingRepository> {
    public RoleMappingService(@NonNull RoleMappingRepository repository) {
        super(repository, RoleMappingDto.class, RoleMappingEntity.class);
    }
    public abstract void initUserDefaultRole(User user, UserDetail userDetail);

    public abstract List<RoleMappingDto> getUser(PrincipleType principleType, String principleId);

    public abstract List<RoleMappingDto> getByPrincipleTypeAndPrincipleId(PrincipleType principleType, String principleId);

    public abstract List<RoleMappingDto> getByRoleIdsAndPrincipleType(PrincipleType principleType, List<ObjectId> roleIds);

    @Transactional
    public abstract List<RoleMappingDto> saveAll(List<RoleMappingDto> roleDtos, UserDetail userDetail);

    public abstract void removeRoleFromUser(String roleMappingId);

    public List<RoleMappingDto> updateUserRoleMapping(List<RoleMappingDto> roleDto, UserDetail userDetail) {
        if (CollUtil.isEmpty(roleDto)) return Lists.newArrayList();
        Criteria c = new Criteria();
        List<Criteria> or = Lists.newArrayList();
        roleDto.stream()
                .filter(Objects::nonNull)
                .forEach(role -> {
                    Criteria criteria = Criteria.where("roleId").is(role.getRoleId())
                            .and("principalId").is(role.getPrincipalId())
                            .and("principalType").is(role.getPrincipalType());
                    upsert(Query.query(criteria), role, userDetail);
                    or.add(criteria);

                });
        c.orOperator(or);
        return Optional.ofNullable(findAll(Query.query(c))).orElse(Lists.newArrayList());
    }

    public abstract void addUserLogIfNeed(List<RoleMappingDto> dtos, UserDetail userDetail);

    public abstract boolean checkHasPermission(PrincipleType principleType, List<ObjectId> roleIdList, String permissionName);
}
