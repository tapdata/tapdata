package com.tapdata.tm.sdkModule.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.sdkModule.dto.SdkModuleDto;
import com.tapdata.tm.sdkModule.entity.SdkModuleEntity;
import com.tapdata.tm.sdkModule.repository.SdkModuleRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2025/07/02
 * @Description:
 */
@Service
@Slf4j
public class SdkModuleService extends BaseService<SdkModuleDto, SdkModuleEntity, ObjectId, SdkModuleRepository> {
    public SdkModuleService(@NonNull SdkModuleRepository repository) {
        super(repository, SdkModuleDto.class, SdkModuleEntity.class);
    }

    protected void beforeSave(SdkModuleDto sdkModule, UserDetail user) {

    }
}