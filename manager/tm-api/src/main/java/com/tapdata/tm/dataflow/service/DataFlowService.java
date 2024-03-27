package com.tapdata.tm.dataflow.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.dag.SchemaTransformerResult;
import com.tapdata.tm.commons.websocket.AllowRemoteCall;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.dto.DataFlowResetAllReqDto;
import com.tapdata.tm.dataflow.dto.DataFlowResetAllResDto;
import com.tapdata.tm.dataflow.entity.DataFlow;
import com.tapdata.tm.dataflow.repository.DataFlowRepository;
import lombok.NonNull;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.CloseableIterator;

import java.util.List;
import java.util.Map;

public abstract class DataFlowService extends BaseService<DataFlowDto, DataFlow, ObjectId, DataFlowRepository> {
    public DataFlowService(@NonNull DataFlowRepository repository) {
        super(repository, DataFlowDto.class, DataFlow.class);
    }
    public Page<DataFlowDto> find(Filter filter, UserDetail userDetail){
        return super.find(filter, userDetail);
    }

    public abstract long updateById(String id, DataFlowDto dto, UserDetail userDetail);

    public abstract Map<String, Object> patch(Map<String, Object> dto, UserDetail userDetail);

    public DataFlowDto save(DataFlowDto dto, UserDetail userDetail){
        return super.save(dto, userDetail);
    }

    public boolean deleteById(ObjectId objectId, UserDetail userDetail){
        return super.deleteById(objectId, userDetail);
    }

    public abstract void setNextScheduleTime(DataFlowDto dto);

    public abstract UpdateResult updateOne(Query query, Map<String, Object> map);

    public abstract DataFlowDto copyDataFlow(String id, UserDetail userDetail);

    public abstract DataFlowResetAllResDto resetDataFlow(DataFlowResetAllReqDto dataFlowResetAllReqDto, UserDetail userDetail);

    public abstract DataFlowResetAllResDto removeDataFlow(String whereJson, UserDetail userDetail);

    public abstract void chart();

    public abstract List<SchemaTransformerResult> transformSchema(DataFlowDto dataFlowDto, UserDetail userDetail);

    @AllowRemoteCall
    public abstract int pingRunningDataFlow(String ids);

    public abstract CloseableIterator<DataFlow> stream(Query query);
}
