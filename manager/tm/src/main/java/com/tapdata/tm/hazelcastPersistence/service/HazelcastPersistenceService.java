package com.tapdata.tm.hazelcastPersistence.service;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.hazelcastPersistence.dto.HazelcastPersistenceDto;
import com.tapdata.tm.hazelcastPersistence.entity.HazelcastPersistenceEntity;
import com.tapdata.tm.hazelcastPersistence.repository.HazelcastPersistenceRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * @Author:
 * @Date: 2022/10/18
 * @Description:
 */
@Service
@Slf4j
public class HazelcastPersistenceService extends BaseService<HazelcastPersistenceDto, HazelcastPersistenceEntity, ObjectId, HazelcastPersistenceRepository> {
    public HazelcastPersistenceService(@NonNull HazelcastPersistenceRepository repository) {
        super(repository, HazelcastPersistenceDto.class, HazelcastPersistenceEntity.class);
    }

    protected void beforeSave(HazelcastPersistenceDto hazelcastPersistence, UserDetail user) {

    }

    public void deleteAll(Where where, UserDetail userDetail) {
        Filter filter = new Filter(where);
        filter.setLimit(0);
        filter.setSkip(0);
        Query query = repository.filterToQuery(filter);
        repository.deleteAll(query, userDetail);
    }
}