package com.tapdata.tm.trace.controller;

import com.tapdata.tm.trace.dto.ChangeLog;
import com.tapdata.tm.trace.dto.TargetWithLineageDto;
import com.tapdata.tm.trace.param.ChangeLogParam;
import com.tapdata.tm.trace.param.WideTableTraceRequest;
import com.tapdata.tm.trace.service.TraceService;
import com.tapdata.tm.trace.service.log.ChangeLogQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.trace.param.TaskLineageParam;
import com.tapdata.tm.trace.service.bloodline.BloodlineFinder;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import jakarta.annotation.Resource;
import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/20 13:14 Create
 * @description
 */
@Tag(name = "Data Trace", description = "Data Trace相关接口")
@RestController
@RequestMapping("/api/lineage")
public class TraceController extends BaseController {
    @Resource(name = "bloodlineFinder")
    private BloodlineFinder bloodlineFinder;
    @Autowired
    private TraceService traceService;
    @Resource(name = "changeLogQuery")
    ChangeLogQuery changeLogQuery;

    @GetMapping("/wide-table/bloodline-diagram")
    public ResponseMessage<TargetWithLineageDto> findDataTraceDag(
            @RequestParam(required = true, name = "connectionId") String connectionId,
            @RequestParam(required = true, name = "table") String table,
            @RequestParam(required = false, name = "trackedFields") List<String> trackedFields) {
        final TaskLineageParam param = TaskLineageParam.instance()
                .connectionId(connectionId)
                .table(table)
                .traceFilterFieldNames(trackedFields);
        return success(bloodlineFinder.findTaskLineageSimply(param));
    }

    @PostMapping(value = "/wide-table/trace/stream",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = "application/x-ndjson;charset=UTF-8")
    public ResponseEntity<StreamingResponseBody> streamWideTableTrace(@RequestBody WideTableTraceRequest request) {
        StreamingResponseBody body = outputStream -> traceService.streamWideTableTrace(request, outputStream);
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("application/x-ndjson;charset=UTF-8"))
                .body(body);
    }

    @PostMapping("/change-log")
    public ResponseMessage<ChangeLog> findChangeLog(@RequestBody ChangeLogParam param) {
        return success(changeLogQuery.query(param, getLoginUser()));
    }
}
