package com.tapdata.tm.monitor.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.dto.BatchRequestDto;
import com.tapdata.tm.monitor.dto.TableSyncStaticDto;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.param.AggregateMeasurementParam;
import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import com.tapdata.tm.monitor.param.SyncStatusStatisticsParam;
import com.tapdata.tm.monitor.service.BatchService;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.monitor.vo.SyncStatusStatisticsVo;
import com.tapdata.tm.monitor.vo.TableSyncStaticVo;
import com.tapdata.tm.utils.WebUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.tapdata.common.sample.request.BulkRequest;
import io.tapdata.common.sample.request.SampleRequest;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping({"/api/measurement","/api/AgentMeasurementV2"})
@Slf4j
@Tag(name = "可观测性")
@Setter(onMethod_ = {@Autowired})
public class MeasureController extends BaseController {
    private MeasurementServiceV2 measurementServiceV2;
    private BatchService batchService;

    @PostMapping("points/v2")
    public ResponseMessage<Void> pointsV2(@RequestBody BulkRequest bulkRequest) {
//        log.info("MeasureController-- {}", JsonUtil.toJson(bulkRequest));
        List<SampleRequest> samples = bulkRequest.getSamples();
        //List statistics = bulkRequest.getStatistics();
        if (CollectionUtils.isNotEmpty(samples)) {
            measurementServiceV2.addAgentMeasurement(samples);
        }
        return success();
    }

    @PostMapping("points/aggregate")
    public ResponseMessage<Void> pointsAggregate(@RequestBody AggregateMeasurementParam aggregateMeasurementParam) {
        measurementServiceV2.aggregateMeasurement(aggregateMeasurementParam);
        return success();
    }

    @PostMapping("query/v2")
    public ResponseMessage<Object> queryV2(@RequestBody MeasurementQueryParam measurementQueryParam) {
        Object data = measurementServiceV2.getSamples(measurementQueryParam);
        return success(data);
    }

    @Operation(summary = "可观测性并行请求接口", description = "一个接口返回任务事件统计、任务日志和校验数据")
    @PostMapping("/batch")
    public ResponseMessage<Map<String, Object>> batch(@Parameter(description = "多个请求的参数集合", required = true,
                                                         content = @Content(schema = @Schema(implementation = BatchRequestDto.class)))
                                                 @RequestBody BatchRequestDto batchRequestDto,
                                                      HttpServletRequest request) throws ExecutionException, InterruptedException {
        Locale locale = WebUtils.getLocale(request);
        Map<String, Object> data = batchService.batch(batchRequestDto, locale);
        return success(data);
    }

    @Operation(summary = "全量信息接口")
    @GetMapping("/full_statistics")
    public ResponseMessage<Page<TableSyncStaticVo>> querySyncStatic (@RequestParam String taskRecordId,
                                                                     @RequestParam(required = false) String tableName,
                                                                     @RequestParam(defaultValue = "1") Integer page,
                                                                     @RequestParam(defaultValue = "20") Integer size) {
			UserDetail userDetail = getLoginUser();
			userDetail.setFreeAuth();
			return success(measurementServiceV2.querySyncStatic(new TableSyncStaticDto(taskRecordId, page, size,tableName), userDetail));
    }

    @Operation(summary = "表同步状态统计")
    @GetMapping("/table_sync_status_statistics")
    public ResponseMessage<List<SyncStatusStatisticsVo>> queryTableSyncStatusStatistics(
			@RequestParam String taskId,
			@RequestParam String taskRecordId
		) {
			return success(measurementServiceV2.queryTableSyncStatusStatistics(new SyncStatusStatisticsParam(taskId, taskRecordId)));
		}


    @Operation(summary = "任务分钟级延迟查询")
    @GetMapping()
    public ResponseMessage<MeasurementEntity> findLastMinuteByTaskId(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);

        Where where = filter.getWhere();
        String taskId = where.get("taskId").toString();
        return success(measurementServiceV2.findLastMinuteByTaskId(taskId));
    }
}
