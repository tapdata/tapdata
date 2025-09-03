package com.tapdata.tm.apiCalls.controller;

import com.tapdata.tm.apiCalls.service.WorkerCallService;
import com.tapdata.tm.apiCalls.vo.ApiCallMetricVo;
import com.tapdata.tm.apiCalls.vo.ApiCountMetricVo;
import com.tapdata.tm.apiCalls.vo.WorkerCallData;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

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
    private WorkerCallService workerCallService;

    /**
     * 查询worker的api调用详情
     * @param processId   api-server的id, 必填
     * @param from        查询开始时间，默认当前时间过去五分钟
     * @param to          查询结束时间，默认当前时间
     * @param type        type of query data (0 is default type)：
     *                     0-rps of worker，
     *                     1-response time of worker，
     *                     2-error rate of worker，
     *        com.tapdata.tm.apiCalls.service.metric.Metric.Type
     *
     * @param granularity 查询粒度：0-分钟，1-小时，2-天，3-周，4-月，默认0
     *        com.tapdata.tm.apiCalls.service.compress.Compress.Type
     */
    @GetMapping
    public ResponseMessage<ApiCallMetricVo> find(
            @RequestParam(name = "processId", required = true) String processId,
            @RequestParam(name = "from", required = false) Long from,
            @RequestParam(name = "to", required = false) Long to,
            @RequestParam(name = "type", required = false, defaultValue = "0") Integer type,
            @RequestParam(name = "granularity", required = false, defaultValue = "0") Integer granularity
    ) {
        return success(workerCallService.find(processId, from, to, type, granularity));
    }

    /**
     * 分页查询api-server的api调用情况
     * @param processId   api-server的id, 选填。未填表示查询全部server的列表
     * */
    @GetMapping("/api-calls-of-server")
    public ResponseMessage<Page<ApiCountMetricVo.ProcessMetric>> findApiCalls(
            @RequestParam(name = "processId") String processId,
            @RequestParam(name = "page") Integer page,
            @RequestParam(name = "size") Integer size
    ) {
        return success(workerCallService.findApiCallsOfServer(processId, page, size));
    }

    /**
     * 查询api-server对应的worker的api调用情况
     * @param processId   api-server的id, 必填
     * */
    @GetMapping("/api-calls/{processId}")
    public ResponseMessage<ApiCountMetricVo.WorkerMetrics> findWorkerApiCalls(@PathVariable("processId") String processId) {
        return success(workerCallService.findApiCallsOrWorker(processId));
    }
}
