package com.tapdata.tm.libSupported.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.libSupported.dto.LibSupportedsDto;
import com.tapdata.tm.libSupported.entity.LibSupportedsEntity;
import com.tapdata.tm.libSupported.repository.LibSupportedsRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2021/12/21
 * @Description:
 */
@Service
@Slf4j
public class LibSupportedsService extends BaseService<LibSupportedsDto, LibSupportedsEntity, ObjectId, LibSupportedsRepository> {
    public LibSupportedsService(@NonNull LibSupportedsRepository repository) {
        super(repository, LibSupportedsDto.class, LibSupportedsEntity.class);
    }

    protected void beforeSave(LibSupportedsDto libSupporteds, UserDetail user) {

    }
}