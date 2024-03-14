package com.tapdata.tm.externalStorage.service;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.externalStorage.entity.ExternalStorageEntity;
import com.tapdata.tm.externalStorage.repository.ExternalStorageRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

public interface ExternalStorageService extends IBaseService<ExternalStorageDto, ExternalStorageEntity, ObjectId, ExternalStorageRepository> {
    int DEFAULT_TTL_DAY = 3;

    <T extends BaseDto> ExternalStorageDto save(ExternalStorageDto externalStorage, UserDetail userDetail);

    ExternalStorageDto update(ExternalStorageDto externalStorageDto, UserDetail userDetail);

    List<TaskDto> findUsingTasks(String id);

    Page<ExternalStorageDto> find(Filter filter, UserDetail userDetail);

    ExternalStorageDto findById(ObjectId objectId);

    ExternalStorageDto findById(ObjectId objectId, Field field, UserDetail userDetail);

    ExternalStorageDto findById(ObjectId objectId, UserDetail userDetail);

    ExternalStorageDto findById(ObjectId objectId, Field field);

    ExternalStorageDto findOne(Query query, UserDetail userDetail);

    ExternalStorageDto findOne(Query query);

    ExternalStorageDto findOne(Query query, String excludeField);

    ExternalStorageDto findOne(Filter filter, UserDetail userDetail);

    ExternalStorageDto findNotCheckById(String id);

    boolean deleteById(ObjectId objectId, UserDetail userDetail);

    <T extends BaseDto> T convertToDto(ExternalStorageEntity entity, Class<T> dtoClass, String... ignoreProperties);

    <T extends BaseDto> ExternalStorageEntity convertToEntity(Class<ExternalStorageEntity> externalStorageEntityClass, T dto, String... ignoreProperties);

    void sendTestConnection(ExternalStorageDto externalStorageDto, UserDetail user);
}
