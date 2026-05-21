package com.tapdata.tm.trace.service.date;

import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.trace.dto.TraceQueryCondition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class TraceDataQueryRpcAdapter{

    private static final String TRACE_QUERY_SERVICE = "TraceDataQueryService";
    private static final String QUERY_METHOD = "query";

    @Autowired
    private TaskService taskService;

    public List<Map<String, Object>> query(TraceQueryCondition condition) {
        if (condition == null) {
            return Collections.emptyList();
        }
        try {
            List<Map<String, Object>> records = taskService.callEngineRpc(
                    null,
                    List.class,
                    TRACE_QUERY_SERVICE,
                    QUERY_METHOD,
                    condition.getConnectionId(),
                    condition.getTable(),
                    condition.getSql(),
                    condition.getFilters(),
                    condition.isSqlMode(),
                    condition.getLimit(),
                    condition.getBatchSize(),
                    condition.getQueryOperators(),
                    condition.getExecuteParams()
            );
            return records == null ? Collections.emptyList() : records;
        } catch (Throwable e) {
            log.error("Trace data query rpc failed, connectionId: {}, table: {}",
                    condition.getConnectionId(), condition.getTable(), e);
            return Collections.emptyList();
        }
    }
}
