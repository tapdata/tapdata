package com.tapdata.tm.apiCalls.controller;

import com.tapdata.tm.apiCalls.vo.ApiCountMetricVo;
import com.tapdata.tm.apiServer.vo.ApiCallMetricVo;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/2 16:15 Create
 * @description
 */
@Tag(name = "ApiCalls", description = "WorkerCall api")
@RestController
@Slf4j
@RequestMapping(value = {"/api/worker-call"})
public class WorkerCallController extends BaseController {

    /**
     * @deprecated
     * Query the API call details of the worker
     *
     * @param processId   api-server's id, required
     * @param from        Query start time, default is five minutes past the current time
     * @param to          Query end time, default current time
     * @param type        type of query data (0 is default type)：
     *                    0-rps of worker，
     *                    1-response time of worker，
     *                    2-error rate of worker，
     *                    com.tapdata.tm.apiCalls.service.metric.Metric.Type
     * @param granularity Query granularity: 0-minute, 1-hour, 2-day, 3-week, 4-month, default 0
     *                    com.tapdata.tm.apiCalls.service.compress.Compress.Type
     */
    @Deprecated(since = "release-4.13")
    @GetMapping
    public ResponseMessage<ApiCallMetricVo> find(
            @RequestParam(name = "processId", required = true) String processId,
            @RequestParam(name = "from", required = false) Long from,
            @RequestParam(name = "to", required = false) Long to,
            @RequestParam(name = "type", required = false, defaultValue = "0") Integer type,
            @RequestParam(name = "granularity", required = false, defaultValue = "0") Integer granularity
    ) {
        return success();
    }

    /**
     * @deprecated
     * Query the API call status of the worker corresponding to the API server
     *
     * @param processId api-server's id, required
     */
    @Deprecated(since = "release-4.13")
    @GetMapping("/api-calls/{processId}")
    public ResponseMessage<ApiCountMetricVo> findWorkerApiCalls(@PathVariable("processId") String processId) {
        return success();
    }
}