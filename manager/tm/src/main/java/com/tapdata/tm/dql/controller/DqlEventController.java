package com.tapdata.tm.dql.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.annotation.IgnoreRequestBodyLog;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.service.DqlEventService;
import com.tapdata.tm.dql.service.DqlRecoveryBatchService;
import com.tapdata.tm.dql.vo.DqlEventDetailVo;
import com.tapdata.tm.dql.vo.DqlEventListVo;
import com.tapdata.tm.dql.vo.DqlEventQueryVo;
import com.tapdata.tm.dql.vo.DqlEventReportResultVo;
import com.tapdata.tm.dql.vo.DqlEventReportVo;
import com.tapdata.tm.dql.vo.DqlEventSummaryVo;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportResultVo;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportVo;
import com.tapdata.tm.dql.vo.DqlRecoveryPreviewVo;
import com.tapdata.tm.dql.vo.DqlRecoveryPayloadVo;
import com.tapdata.tm.dql.vo.DqlRecoveryRequestVo;
import com.tapdata.tm.dql.vo.DqlRecoveryResultReportVo;
import com.tapdata.tm.dql.vo.DqlStormGuardReportVo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

/**
 * Exposes DLQ exception event APIs for the frontend list/detail/recovery workflow and Engine callbacks.
 */
@Tag(name = "DLQ Event", description = "DLQ exception event APIs")
@RestController
public class DqlEventController extends BaseController {
    private final DqlEventService eventService;
    private final DqlRecoveryBatchService recoveryBatchService;

    public DqlEventController(DqlEventService eventService, DqlRecoveryBatchService recoveryBatchService) {
        this.eventService = eventService;
        this.recoveryBatchService = recoveryBatchService;
    }

    /**
     * Receives DLQ exception events from Engine and persists them as frontend-visible event records.
     */
    @Operation(summary = "Engine reports a DLQ exception event")
    @PostMapping({"/api/task/{taskId}/dql-events/report", "/api/Task/{taskId}/dql-events/report"})
    @IgnoreRequestBodyLog
    public ResponseMessage<DqlEventReportResultVo> report(@PathVariable("taskId") String taskId,
                                                          @RequestBody DqlEventReportVo request) {
        return success(eventService.report(taskId, request));
    }

    /**
     * Receives successful normal record writes from Engine and marks previous DLQ events for the same record with overwrite risk.
     */
    @Operation(summary = "Engine reports a successful record write after DLQ skip")
    @PostMapping({"/api/task/{taskId}/dql-events/record-success/report", "/api/Task/{taskId}/dql-events/record-success/report"})
    @IgnoreRequestBodyLog
    public ResponseMessage<DqlRecordSuccessReportResultVo> reportRecordSuccess(@PathVariable("taskId") String taskId,
                                                                               @RequestBody DqlRecordSuccessReportVo request) {
        return success(eventService.reportRecordSuccess(taskId, request));
    }

    /**
     * Returns the paged event list used by the independent Exception Events page.
     */
    @Operation(summary = "Query DLQ exception events")
    @GetMapping("/api/dql-events")
    public ResponseMessage<Page<DqlEventListVo>> page(@RequestParam(name = "taskId", required = false) String taskId,
                                                   @RequestParam(name = "eventId", required = false) String eventId,
                                                   @RequestParam(name = "taskName", required = false) String taskName,
                                                   @RequestParam(name = "sourceTable", required = false) String sourceTable,
                                                   @RequestParam(name = "targetTable", required = false) String targetTable,
                                                   @RequestParam(name = "keyword", required = false) String keyword,
                                                      @RequestParam(name = "errorCode", required = false) String errorCode,
                                                   @RequestParam(name = "dmlType", required = false) String dmlType,
                                                   @RequestParam(name = "errorType", required = false) String errorType,
                                                   @RequestParam(name = "status", required = false) String status,
                                                   @RequestParam(name = "startTime", required = false) Long startTime,
                                                   @RequestParam(name = "endTime", required = false) Long endTime,
                                                   @RequestParam(name = "skip", required = false, defaultValue = "0") long skip,
                                                   @RequestParam(name = "limit", required = false, defaultValue = "20") int limit,
                                                   @RequestParam(name = "order", required = false) String order) {
        return success(eventService.page(query(taskId, eventId, taskName, sourceTable, targetTable, keyword, dmlType, errorType, status, startTime, endTime, errorCode, skip, limit, order), getLoginUser()));
    }

    /**
     * Returns status counters for summary tabs using the same filters as the event list.
     */
    @Operation(summary = "Query DLQ exception event summary")
    @GetMapping("/api/dql-events/summary")
    public ResponseMessage<DqlEventSummaryVo> summary(@RequestParam(name = "taskId", required = false) String taskId,
                                                      @RequestParam(name = "eventId", required = false) String eventId,
                                                      @RequestParam(name = "taskName", required = false) String taskName,
                                                      @RequestParam(name = "sourceTable", required = false) String sourceTable,
                                                      @RequestParam(name = "targetTable", required = false) String targetTable,
                                                      @RequestParam(name = "keyword", required = false) String keyword,
                                                      @RequestParam(name = "errorCode", required = false) String errorCode,
                                                      @RequestParam(name = "dmlType", required = false) String dmlType,
                                                      @RequestParam(name = "errorType", required = false) String errorType,
                                                      @RequestParam(name = "status", required = false) String status,
                                                      @RequestParam(name = "startTime", required = false) Long startTime,
                                                      @RequestParam(name = "endTime", required = false) Long endTime) {
        return success(eventService.summary(query(taskId, eventId, taskName, sourceTable, targetTable, keyword, dmlType, errorType, status, startTime, endTime, errorCode, 0L, 0, null), getLoginUser()));
    }

    /**
     * Returns event detail for the frontend drawer, including payload preview and current recovery batch.
     */
    @Operation(summary = "Query DLQ exception event detail")
    @GetMapping("/api/dql-events/{eventId}")
    public ResponseMessage<DqlEventDetailVo> detail(@PathVariable("eventId") String eventId) {
        return success(eventService.detail(eventId, getLoginUser()));
    }

    /**
     * Returns the immutable payload required by Engine when reprocessing one DQL event.
     */
    @Operation(summary = "Query DQL recovery payload")
    @GetMapping("/api/dql-events/{eventId}/recovery-payload")
    @IgnoreRequestBodyLog
    public ResponseMessage<DqlRecoveryPayloadVo> recoveryPayload(@PathVariable("eventId") String eventId) {
        return success(eventService.recoveryPayload(eventId, getLoginUser()));
    }

    /**
     * Validates selected events and returns the server-defined recovery order before confirmation.
     */
    @Operation(summary = "Preview DLQ recovery order and blockers")
    @PostMapping("/api/dql-events/recovery/preview")
    public ResponseMessage<DqlRecoveryPreviewVo> previewRecovery(@RequestBody DqlRecoveryRequestVo request) {
        return success(recoveryBatchService.preview(request, getLoginUser()));
    }

    /**
     * Starts a confirmed recovery batch and locks selected events for Engine reprocessing.
     */
    @Operation(summary = "Start DLQ recovery")
    @PostMapping("/api/dql-events/recovery")
    public ResponseMessage<DqlRecoveryBatchDto> startRecovery(@RequestBody DqlRecoveryRequestVo request) {
        return success(recoveryBatchService.start(request, getLoginUser()));
    }

    /**
     * Returns recovery batch progress for the frontend progress drawer and polling loop.
     */
    @Operation(summary = "Query DLQ recovery batch")
    @GetMapping("/api/dql-events/recovery-batches/{batchId}")
    public ResponseMessage<DqlRecoveryBatchDto> batchDetail(@PathVariable("batchId") String batchId) {
        return success(recoveryBatchService.detail(batchId, getLoginUser()));
    }

    /**
     * Receives recovery execution progress and terminal results from Engine.
     */
    @Operation(summary = "Engine reports DLQ recovery result")
    @PostMapping({"/api/task/{taskId}/dql-events/recovery/report", "/api/Task/{taskId}/dql-events/recovery/report"})
    @IgnoreRequestBodyLog
    public ResponseMessage<Boolean> reportRecovery(@PathVariable("taskId") String taskId,
                                                   @RequestBody DqlRecoveryResultReportVo request) {
        recoveryBatchService.report(taskId, request);
        return success(Boolean.TRUE);
    }

    /**
     * Receives a safe Storm Guard activation signal from Engine for task-level alarm handling.
     */
    @Operation(summary = "Engine reports DQL Storm Guard activation")
    @PostMapping({"/api/task/{taskId}/dql-events/storm-guard/report", "/api/Task/{taskId}/dql-events/storm-guard/report"})
    @IgnoreRequestBodyLog
    public ResponseMessage<Boolean> reportStormGuard(@PathVariable("taskId") String taskId,
                                                     @RequestBody DqlStormGuardReportVo request) {
        eventService.reportStormGuard(taskId, request);
        return success(Boolean.TRUE);
    }

    private DqlEventQueryVo query(String taskId,
                                  String eventId,
                                  String taskName,
                                  String sourceTable,
                                  String targetTable,
                                  String keyword,
                                  String dmlType,
                                  String errorType,
                                  String status,
                                  Long startTime,
                                  Long endTime,
                                  String errorCode,
                                  long skip,
                                  int limit,
                                  String order) {
        DqlEventQueryVo query = new DqlEventQueryVo();
        query.setTaskId(taskId);
        query.setEventId(eventId);
        query.setTaskName(taskName);
        query.setSourceTable(sourceTable);
        query.setTargetTable(targetTable);
        query.setKeyword(keyword);
        query.setErrorCode(errorCode);
        query.setDmlType(dmlType);
        query.setErrorType(errorType);
        query.setStatus(status);
        query.setStartTime(startTime == null ? null : new Date(startTime));
        query.setEndTime(endTime == null ? null : new Date(endTime));
        query.setSkip(skip);
        query.setLimit(limit);
        query.setOrder(order);
        return query;
    }
}
