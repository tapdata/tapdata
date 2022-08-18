package com.tapdata.tm.verison.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.verison.dto.VersionDto;
import com.tapdata.tm.verison.entity.VersionEntity;
import com.tapdata.tm.verison.repository.VersionRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2022/08/15
 * @Description:
 */
@Service
@Slf4j
public class VersionService extends BaseService<VersionDto, VersionEntity, ObjectId, VersionRepository> {
    public VersionService(@NonNull VersionRepository repository) {
        super(repository, VersionDto.class, VersionEntity.class);
    }

    protected void beforeSave(VersionDto version, UserDetail user) {

    }
}