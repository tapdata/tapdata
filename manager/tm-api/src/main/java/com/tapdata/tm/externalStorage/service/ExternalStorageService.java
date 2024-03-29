package com.tapdata.tm.externalStorage.service;

import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.externalStorage.entity.ExternalStorageEntity;
import com.tapdata.tm.externalStorage.repository.ExternalStorageRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;

public abstract class ExternalStorageService extends BaseService<ExternalStorageDto, ExternalStorageEntity, ObjectId, ExternalStorageRepository> {
    public ExternalStorageService(@NonNull ExternalStorageRepository repository) {
        super(repository, ExternalStorageDto.class, ExternalStorageEntity.class);
    }

    public <T extends BaseDto> ExternalStorageDto save(ExternalStorageDto externalStorage, UserDetail userDetail){
        return super.save(externalStorage, userDetail);
    }

    public abstract ExternalStorageDto update(ExternalStorageDto externalStorageDto, UserDetail userDetail);

    public abstract List<TaskDto> findUsingTasks(String id);

    public Page<ExternalStorageDto> find(Filter filter, UserDetail userDetail){
        return super.find(filter, userDetail);
    }

    public ExternalStorageDto findById(ObjectId objectId){
        return super.findById(objectId);
    }

    public ExternalStorageDto findById(ObjectId objectId, Field field, UserDetail userDetail){
        return super.findById(objectId, field, userDetail);
    }

    public ExternalStorageDto findById(ObjectId objectId, UserDetail userDetail){
        return super.findById(objectId, userDetail);
    }

    public ExternalStorageDto findById(ObjectId objectId, Field field){
        return super.findById(objectId, field);
    }

    public ExternalStorageDto findOne(Query query, UserDetail userDetail){
        return super.findOne(query, userDetail);
    }

    public ExternalStorageDto findOne(Query query){
        return super.findOne(query);
    }

    public ExternalStorageDto findOne(Query query, String excludeField){
        return super.findOne(query, excludeField);
    }

    public ExternalStorageDto findOne(Filter filter, UserDetail userDetail){
        return super.findOne(filter, userDetail);
    }

    public abstract ExternalStorageDto findNotCheckById(String id);

    public boolean deleteById(ObjectId objectId, UserDetail userDetail){
        return super.deleteById(objectId, userDetail);
    }

    public <T extends BaseDto> T convertToDto(ExternalStorageEntity entity, Class<T> dtoClass, String... ignoreProperties){
        return super.convertToDto(entity, dtoClass, ignoreProperties);
    }

    public <T extends BaseDto> ExternalStorageEntity convertToEntity(Class<ExternalStorageEntity> externalStorageEntityClass, T dto, String... ignoreProperties){
        return super.convertToEntity(externalStorageEntityClass, dto, ignoreProperties);
    }

    public abstract void sendTestConnection(ExternalStorageDto externalStorageDto, UserDetail user);
}
