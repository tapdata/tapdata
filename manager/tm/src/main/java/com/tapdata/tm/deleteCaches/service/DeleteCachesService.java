package com.tapdata.tm.deleteCaches.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.deleteCaches.dto.DeleteCachesDto;
import com.tapdata.tm.deleteCaches.entity.DeleteCachesEntity;
import com.tapdata.tm.deleteCaches.repository.DeleteCachesRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2022/01/25
 * @Description:
 */
@Service
@Slf4j
public class DeleteCachesService extends BaseService<DeleteCachesDto, DeleteCachesEntity, ObjectId, DeleteCachesRepository> {
    public DeleteCachesService(@NonNull DeleteCachesRepository repository) {
        super(repository, DeleteCachesDto.class, DeleteCachesEntity.class);
    }

    protected void beforeSave(DeleteCachesDto deleteCaches, UserDetail user) {

    }
}