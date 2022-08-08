package com.tapdata.tm.observability.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.observability.dto.InspectDto;
import com.tapdata.tm.observability.dto.ServiceMethodDto;
import com.tapdata.tm.observability.dto.TaskLogDto;
import com.tapdata.tm.observability.dto.TaskStatisticsDto;
import com.tapdata.tm.observability.vo.ParallelInfoVO;
import com.tapdata.tm.observability.vo.TaskStatisticsVO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Tag(name = "可观测性")
@RestController
@RequestMapping("/api/observability")
public class ObservabilityController extends BaseController {

    @Operation(summary = "可观测性并行请求接口", description = "一个接口返回任务事件统计、任务日志和校验数据")
    @GetMapping("/list")
    public ResponseMessage<List<ParallelInfoVO<?>>> getList(@Parameter(name = "dtos", description = "多个service和method的参数集合", required = true)
                                          @RequestBody List<ServiceMethodDto<?>> dtos) {

        return success();
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
