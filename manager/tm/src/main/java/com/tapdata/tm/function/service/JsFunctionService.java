package com.tapdata.tm.function.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.function.dto.JsFunctionDto;
import com.tapdata.tm.function.entity.JsFunctionEntity;
import com.tapdata.tm.function.repository.JsFunctionRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2022/04/07
 * @Description:
 */
@Service
@Slf4j
public class JsFunctionService extends BaseService<JsFunctionDto, JsFunctionEntity, ObjectId, JsFunctionRepository> {
    public JsFunctionService(@NonNull JsFunctionRepository repository) {
        super(repository, JsFunctionDto.class, JsFunctionEntity.class);
    }

    protected void beforeSave(JsFunctionDto jsFunction, UserDetail user) {
        String function_name = jsFunction.getFunction_name();
        if (StringUtils.isNotBlank(function_name)) {
            checkName(jsFunction.getId(), function_name, user);
        }
    }


    public void checkName(ObjectId id, String name, UserDetail user) {
        Criteria criteriaSystem = Criteria.where("function_name").is(name).and("type").is("system");

        long count = count(new Query(criteriaSystem));
        if (count != 0) {
            throw new BizException("Function.Name.Exist");
        }


        Criteria criteriaCustom = Criteria.where("function_name").is(name).and("type").ne("system");
        if (id != null) {
            criteriaCustom.and("_id").ne(id);
        }

        Query query = new Query(criteriaCustom);
        count = count(query, user);
        if (count != 0) {
            throw new BizException("Function.Name.Exist");
        }
    }
}