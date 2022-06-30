package com.tapdata.tm.dictionary.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.schema.DictionaryDto;
import com.tapdata.tm.dictionary.entity.DictionaryEntity;
import com.tapdata.tm.dictionary.repository.DictionaryRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2021/10/19
 * @Description:
 */
@Service
@Slf4j
public class DictionaryService extends BaseService<DictionaryDto, DictionaryEntity, ObjectId, DictionaryRepository> {
    public DictionaryService(@NonNull DictionaryRepository repository) {
        super(repository, DictionaryDto.class, DictionaryEntity.class);
    }

    protected void beforeSave(DictionaryDto dictionary, UserDetail user) {

    }
}