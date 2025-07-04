package com.tapdata.tm.application.service;

import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.application.repository.ApplicationRepository;
import com.tapdata.tm.application.vo.ModulePermissionVo;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.modules.entity.Path;
import com.tapdata.tm.modules.repository.ModulesRepository;
import com.tapdata.tm.role.entity.RoleEntity;
import com.tapdata.tm.role.repository.RoleRepository;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Service
@Slf4j
public class ApplicationService extends BaseService<ApplicationDto, ApplicationEntity, ObjectId, ApplicationRepository> {
    public ApplicationService(@NonNull ApplicationRepository repository) {
        super(repository, ApplicationDto.class, ApplicationEntity.class);
    }

    protected void beforeSave(ApplicationDto metadataDefinition, UserDetail user) {

    }


    public Page find(Filter filter, UserDetail userDetail) {
        RoleRepository roleRepository = SpringContextHelper.getBean(RoleRepository.class);
        Map<ObjectId, String> roleMap = roleRepository.findAll(Query.query(Criteria.where("_id").ne(null))).stream()
                .collect(Collectors.toMap(RoleEntity::getId,RoleEntity::getName,(key1, key2) -> key2));
        Page<ApplicationDto> pageApplications = super.find(filter, userDetail);
        pageApplications.getItems().stream().forEach(applicationDto -> {
            List scopeNames = new ArrayList<>();
            applicationDto.getScopes().forEach(s->{
                scopeNames.add(roleMap.get(ObjectIdDeserialize.toObjectId(s.toString())));
            });
            applicationDto.setScopeNames(scopeNames);
        });
        return pageApplications;
    }

    public ApplicationDto updateById(ApplicationDto applicationDto, UserDetail userDetail) {
        String id = applicationDto.getId().toString();
        Query query = Query.query(Criteria.where("id").is(id));
        if (userDetail.isRoot() && !userDetail.isFreeAuth()) {
            update(query, applicationDto);
        } else {
            updateByWhere(query, applicationDto, userDetail);
        }
        return applicationDto;
    }

    public List<ApplicationDto> findByIds(List<String> idList){
        Query query=Query.query(Criteria.where("id").in(idList));
        List<ApplicationDto> applicationDtoList=findAll(query);
        return applicationDtoList;
    }

    /**
     * Get modules that the application has permission to access
     * @param applicationId Application ID
     * @param userDetail User detail
     * @return List of modules with id and name only
     */
    public List<ModulePermissionVo> getAccessibleModules(String applicationId, UserDetail userDetail) {
        // 1. Query application by ID to get scopes (role IDs)
        ApplicationDto application = findById(MongoUtils.toObjectId(applicationId), userDetail);
        if (application == null || CollectionUtils.isEmpty(application.getScopes())) {
            return new ArrayList<>();
        }

        // 2. Convert scope IDs to ObjectIds and query roles to get role names
        RoleRepository roleRepository = SpringContextHelper.getBean(RoleRepository.class);
        @SuppressWarnings("unchecked")
        List<Object> scopes = (List<Object>) application.getScopes();
        List<ObjectId> roleIds = scopes.stream()
                .map(scope -> ObjectIdDeserialize.toObjectId(scope.toString()))
                .collect(Collectors.toList());

        Query roleQuery = Query.query(Criteria.where("_id").in(roleIds));
        List<RoleEntity> roles = roleRepository.findAll(roleQuery);
        Set<String> roleNames = roles.stream()
                .map(RoleEntity::getName)
                .collect(Collectors.toSet());

        if (roleNames.isEmpty()) {
            return new ArrayList<>();
        }

        // 3. Query modules where paths.acl contains any of the role names
        ModulesRepository modulesRepository = SpringContextHelper.getBean(ModulesRepository.class);
        Query moduleQuery = Query.query(
                Criteria.where("is_deleted").ne(true)
                        .and("status").is("active")
                        .and("paths.acl").in(roleNames)
        );
        moduleQuery.fields().include("_id", "name");

        List<ModulesEntity> modules = modulesRepository.findAll(moduleQuery);

        // 4. Convert to ModulePermissionVo and return
        return modules.stream()
                .map(module -> new ModulePermissionVo(module.getId().toHexString(), module.getName(), module.getListtags()))
                .collect(Collectors.toList());
    }

}