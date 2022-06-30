package com.tapdata.tm.dataflowsdebug.service;

import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.dataflowsdebug.dto.DataFlowsDebugDto;
import com.tapdata.tm.dataflowsdebug.entity.DataFlowsDebugEntity;
import com.tapdata.tm.dataflowsdebug.repository.DataFlowsDebugRepository;
import com.tapdata.tm.config.security.UserDetail;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Service;

/**
 * @Author:
 * @Date: 2021/09/13
 * @Description:
 */
@Service
@Slf4j
public class DataFlowsDebugService extends BaseService<DataFlowsDebugDto, DataFlowsDebugEntity, ObjectId, DataFlowsDebugRepository> {
    public DataFlowsDebugService(@NonNull DataFlowsDebugRepository repository) {
        super(repository, DataFlowsDebugDto.class, DataFlowsDebugEntity.class);
    }

    protected void beforeSave(DataFlowsDebugDto dataFlowsDebug, UserDetail user) {

    }
}