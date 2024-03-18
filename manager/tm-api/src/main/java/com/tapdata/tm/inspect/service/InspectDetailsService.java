package com.tapdata.tm.inspect.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import com.tapdata.tm.inspect.entity.InspectDetailsEntity;
import com.tapdata.tm.inspect.repository.InspectDetailsRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;

public abstract class InspectDetailsService extends BaseService<InspectDetailsDto, InspectDetailsEntity, ObjectId, InspectDetailsRepository> {
    public InspectDetailsService(@NonNull InspectDetailsRepository repository) {
        super(repository, InspectDetailsDto.class, InspectDetailsEntity.class);
    }

}