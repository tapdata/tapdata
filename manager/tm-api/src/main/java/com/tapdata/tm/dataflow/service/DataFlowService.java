package com.tapdata.tm.dataflow.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.service.IBaseService;
import com.tapdata.tm.commons.dag.SchemaTransformerResult;
import com.tapdata.tm.commons.websocket.AllowRemoteCall;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflow.dto.DataFlowDto;
import com.tapdata.tm.dataflow.dto.DataFlowResetAllReqDto;
import com.tapdata.tm.dataflow.dto.DataFlowResetAllResDto;
import com.tapdata.tm.dataflow.entity.DataFlow;
import com.tapdata.tm.dataflow.repository.DataFlowRepository;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.CloseableIterator;

import java.util.List;
import java.util.Map;

public interface DataFlowService extends IBaseService<DataFlowDto, DataFlow, ObjectId, DataFlowRepository> {
    Page<DataFlowDto> find(Filter filter, UserDetail userDetail);

    long updateById(String id, DataFlowDto dto, UserDetail userDetail);

    Map<String, Object> patch(Map<String, Object> dto, UserDetail userDetail);

    DataFlowDto save(DataFlowDto dto, UserDetail userDetail);

    boolean deleteById(ObjectId objectId, UserDetail userDetail);

    void setNextScheduleTime(DataFlowDto dto);

    UpdateResult updateOne(Query query, Map<String, Object> map);

    DataFlowDto copyDataFlow(String id, UserDetail userDetail);

    DataFlowResetAllResDto resetDataFlow(DataFlowResetAllReqDto dataFlowResetAllReqDto, UserDetail userDetail);

    DataFlowResetAllResDto removeDataFlow(String whereJson, UserDetail userDetail);

    void chart();

    List<SchemaTransformerResult> transformSchema(DataFlowDto dataFlowDto, UserDetail userDetail);

    @AllowRemoteCall
    int pingRunningDataFlow(String ids);

    CloseableIterator<DataFlow> stream(Query query);
}
