package com.tapdata.tm.application.service;

import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.entity.ApplicationEntity;
import com.tapdata.tm.application.repository.ApplicationRepository;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.entity.RoleEntity;
import com.tapdata.tm.role.repository.RoleRepository;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

}