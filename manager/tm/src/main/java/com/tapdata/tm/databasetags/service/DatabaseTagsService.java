package com.tapdata.tm.databasetags.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.databasetags.dto.DatabaseTagsDto;
import com.tapdata.tm.databasetags.entity.DatabaseTagsEntity;
import com.tapdata.tm.databasetags.repository.DatabaseTagsRepository;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2022/03/15
 * @Description:
 */
@Service
@Slf4j
public class DatabaseTagsService extends BaseService<DatabaseTagsDto, DatabaseTagsEntity, ObjectId, DatabaseTagsRepository> {
    public DatabaseTagsService(@NonNull DatabaseTagsRepository repository) {
        super(repository, DatabaseTagsDto.class, DatabaseTagsEntity.class);
    }

    protected void beforeSave(DatabaseTagsDto databaseTags, UserDetail user) {

    }

    @Cacheable(cacheNames = "databaseTags", cacheManager = "memoryCache", unless = "#result == null || #result.size() == 0")
    public List<DatabaseTagsDto> findAvailableTags(){
        Query query = Query.query(Criteria.where("enable").is(true));
        query.fields().exclude("id","enable");
        return findAll(query);
    }

    @CacheEvict(cacheNames = "databaseTags", cacheManager = "memoryCache")
    public void cacheEvict() {
        log.info("DatabaseTags cache evicted");
    }
}