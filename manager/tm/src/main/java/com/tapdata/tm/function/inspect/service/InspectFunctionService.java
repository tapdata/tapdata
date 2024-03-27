package com.tapdata.tm.function.inspect.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.function.inspect.dto.InspectFunctionDto;
import com.tapdata.tm.function.inspect.entity.InspectFunctionEntity;
import com.tapdata.tm.function.inspect.repository.InspectFunctionRepository;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author: Gavin
 * @Date: 2023/03/27
 * @Description:
 */
@Service
@Slf4j
public class InspectFunctionService extends BaseService<InspectFunctionDto, InspectFunctionEntity, ObjectId, InspectFunctionRepository> {

	public InspectFunctionService(@NonNull InspectFunctionRepository repository) {
        super(repository, InspectFunctionDto.class, InspectFunctionEntity.class);
    }

	protected void beforeSave(InspectFunctionDto functionDto, UserDetail user) {
        //do nothing
    }
}