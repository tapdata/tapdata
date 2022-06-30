package com.tapdata.tm.apiServer.service;

import com.tapdata.tm.apiServer.dto.ApiServerDto;
import com.tapdata.tm.apiServer.entity.ApiServerEntity;
import com.tapdata.tm.apiServer.repository.ApiServerRepository;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Service
@Slf4j
public class ApiServerService extends BaseService<ApiServerDto, ApiServerEntity, ObjectId, ApiServerRepository> {
    public ApiServerService(@NonNull ApiServerRepository repository) {
        super(repository, ApiServerDto.class, ApiServerEntity.class);
    }

    protected void beforeSave(ApiServerDto metadataDefinition, UserDetail user) {

    }


    public Page find(Filter filter, UserDetail userDetail) {
        Where where = filter.getWhere();
        Map notDeleteMap = new HashMap();
        notDeleteMap.put("$ne", true);
        where.put("is_deleted", notDeleteMap);
        return super.find(filter, userDetail);
    }

    public ApiServerDto updateById(ApiServerDto applicationDto, UserDetail userDetail) {
        String id = applicationDto.getId().toString();
        Query query = Query.query(Criteria.where("id").is(id));
        updateByWhere(query, applicationDto, userDetail);
        return applicationDto;
    }


}