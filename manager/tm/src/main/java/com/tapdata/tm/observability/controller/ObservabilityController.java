package com.tapdata.tm.observability.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.observability.dto.BatchRequestDto;
import com.tapdata.tm.observability.service.ObservabilityService;
import com.tapdata.tm.observability.vo.BatchResponeVo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
    public ResponseMessage<BatchResponeVo> batch(@Parameter(description = "多个service和method的参数集合", required = true)
                                                   @RequestBody BatchRequestDto batchRequestDto) throws ExecutionException, InterruptedException {
        return success(observabilityService.batch(batchRequestDto));
    }
}
