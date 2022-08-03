package com.tapdata.tm.observability.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.observability.dto.InspectDto;
import com.tapdata.tm.observability.dto.BatchRequestDto;
import com.tapdata.tm.observability.dto.TaskLogDto;
import com.tapdata.tm.observability.dto.TaskStatisticsDto;
import com.tapdata.tm.observability.service.ObservabilityService;
import com.tapdata.tm.observability.vo.BatchResponeVo;
import com.tapdata.tm.observability.vo.TaskStatisticsVO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Tag(name = "可观测性")
@RestController
@RequestMapping("/api/observability")
@Setter(onMethod_ = {@Autowired})
public class ObservabilityController extends BaseController {
    private ObservabilityService observabilityService;

    @Operation(summary = "可观测性并行请求接口", description = "一个接口返回任务事件统计、任务日志和校验数据")
    @PostMapping("/batch")
    public ResponseMessage<BatchResponeVo> getList(@Parameter(description = "多个service和method的参数集合", required = true)
                                          @RequestBody BatchRequestDto batchRequestDto) throws ExecutionException, InterruptedException {
        return success(observabilityService.batch(batchRequestDto));
    }

    @Operation(summary = "任务事件统计接口")
    @GetMapping("/task-statistics")
    public ResponseMessage<TaskStatisticsVO> getTaskStatistics(@Parameter(name = "dto", description = "事件统计dto", required = true)
                                                    @RequestBody TaskStatisticsDto dto) {

        return success();
    }

    @Operation(summary = "任务日志数据接口")
    @GetMapping("/task-logs")
    public ResponseMessage<?> getTaskLogs(@Parameter(name = "dto", description = "任务日志dto", required = true)
                                              @RequestBody TaskLogDto dto) {

        return success();
    }

    @Operation(summary = "任务校验数据接口")
    @GetMapping("/inspect-info")
    public ResponseMessage<?> getInspectInfo(@Parameter(name = "dto", description = "数据校验dto", required = true)
                                                 @RequestBody InspectDto dto) {

        return success();
    }
}
