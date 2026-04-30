package com.tapdata.tm.apiServer.service;

import com.tapdata.tm.apiServer.dto.ApiServerDto;
import com.tapdata.tm.apiServer.entity.ApiServerEntity;
import com.tapdata.tm.apiServer.repository.ApiServerRepository;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Field;
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
import java.util.function.Supplier;

/**
 * @Author:
 * @Date: 2021/10/15
 * @Description:
 */
@Service
@Slf4j
public class ApiServerService extends BaseService<ApiServerDto, ApiServerEntity, ObjectId, ApiServerRepository> {
    public static final String USER_ID = "user_id";

    public ApiServerService(@NonNull ApiServerRepository repository) {
        super(repository, ApiServerDto.class, ApiServerEntity.class);
    }

    protected void beforeSave(ApiServerDto metadataDefinition, UserDetail user) {

    }

    public Supplier<ApiServerDto> dataPermissionFindById(ObjectId apiServerId, Field fields) {
        return () -> {
            if (null != fields) {
                fields.put(USER_ID, true);
                fields.put(DataPermissionHelper.FIELD_NAME, true);
            }
            return findById(apiServerId, fields);
        };
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