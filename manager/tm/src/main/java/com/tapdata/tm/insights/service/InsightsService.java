package com.tapdata.tm.insights.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.insights.dto.InsightsDto;
import com.tapdata.tm.insights.entity.InsightsEntity;
import com.tapdata.tm.insights.repository.InsightsRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2021/10/14
 * @Description:
 */
@Service
@Slf4j
public class InsightsService extends BaseService<InsightsDto, InsightsEntity, ObjectId, InsightsRepository> {
    public InsightsService(@NonNull InsightsRepository repository) {
        super(repository, InsightsDto.class, InsightsEntity.class);
    }

    protected void beforeSave(InsightsDto modules, UserDetail user) {

    }


    public Page<InsightsDto> findInsightList(Filter filter) {
        Query query = new Query();
//        List<InsightsEntity> insightsEntityList = repository.getMongoOperations().find(query, InsightsEntity.class, "Insights");
       Page page= find(filter);
        return page;
    }


}