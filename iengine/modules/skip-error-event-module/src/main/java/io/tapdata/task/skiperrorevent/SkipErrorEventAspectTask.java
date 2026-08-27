package io.tapdata.task.skiperrorevent;

import com.alibaba.fastjson.JSON;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.ErrorCodeConfig;
import io.tapdata.ErrorCodeEntity;
import io.tapdata.aspect.SkipErrorDataAspect;
import io.tapdata.aspect.SkipErrorProcessAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.WriteRecordFuncAspect;
import io.tapdata.aspect.task.AbstractAspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.dql.classifier.DlqStormGuard;
import io.tapdata.dql.classifier.DqlBatchContext;
import io.tapdata.dql.classifier.DqlClassificationContext;
import io.tapdata.dql.classifier.DqlExceptionClassifier;
import io.tapdata.dql.classifier.DqlFailedStage;
import io.tapdata.dql.classifier.DqlNodeType;
import io.tapdata.dql.classifier.DqlStormGuardContext;
import io.tapdata.dql.classifier.DqlTaskContext;
import io.tapdata.dql.client.DqlTmClient;
import io.tapdata.dql.identity.DqlEventIdentityGenerator;
import io.tapdata.dql.model.DqlEventIdentity;
import io.tapdata.dql.model.DqlClassificationResult;
import io.tapdata.dql.model.DqlEventReport;
import io.tapdata.dql.model.DqlExceptionScope;
import io.tapdata.dql.model.DqlRecordSuccessReport;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.dql.model.DqlRouteDecision;
import io.tapdata.dql.preview.DqlPayloadPreview;
import io.tapdata.dql.preview.DqlPayloadPreviewBuilder;
import io.tapdata.dql.reporter.DqlEventReportException;
import io.tapdata.dql.reporter.DqlEventReporter;
import io.tapdata.dql.recovery.DqlRecoveryCaptureGuard;
import io.tapdata.dql.serializer.DqlPayloadSerializer;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.TapCodeException;
import io.tapdata.exception.TapPdkViolateUniqueEx;
import io.tapdata.exception.TapPdkWriteLengthEx;
import io.tapdata.exception.TapPdkWriteTypeEx;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.logging.log4j.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC}, ignoreErrors = false)
public class SkipErrorEventAspectTask extends AbstractAspectTask {
    public static final String SKIP_ERROR_EVENT_DATA = "Skip error event data:{}";
    // Set a maximum of 10 threads to report status, if delay please check the net work and DB stress
    private final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);

    private static final String METRICS_SYNC = "sync";
    private static final String METRICS_SKIP = "skip";

    private String taskId;
    private TaskDto.SkipErrorEvent skipErrorEvent;
    private final Map<String, Map<String, AtomicLong>> syncAndSkipMap = new ConcurrentHashMap<>();

    private final DqlExceptionClassifier dqlExceptionClassifier = new DqlExceptionClassifier();
    private final DlqStormGuard dlqStormGuard = new DlqStormGuard();
    private final DqlPayloadSerializer dqlPayloadSerializer = new DqlPayloadSerializer();
    private final DqlPayloadPreviewBuilder dqlPayloadPreviewBuilder = new DqlPayloadPreviewBuilder();
    private final DqlEventIdentityGenerator dqlEventIdentityGenerator = new DqlEventIdentityGenerator();
    private DqlEventReporter dqlEventReporter;

    private Function<SkipErrorDataAspect, AspectInterceptResult> skipErrorDataNoeAspect = aspect -> null;
    private long lastSkipTimes;
    private long nextPrintTimes;
    private ClientMongoOperator clientMongoOperator;
    private final AtomicReference<Future<?>> storeFuture = new AtomicReference<>();
    private SplitFileLogger logger;

    public SkipErrorEventAspectTask() {
        interceptHandlers.register(SkipErrorDataAspect.class, this::skipErrorDataNoeAspectHandle);
        interceptHandlers.register(SkipErrorProcessAspect.class, this::skipErrorProcessAspectHandle);
        observerHandlers.register(WriteRecordFuncAspect.class, this::writeRecordFuncAspectHandle);
    }

    private void save2TaskAttrs() {
        Update update = Update.update(String.format("attrs.%s", TaskDto.ATTRS_SKIP_ERROR_EVENT), syncAndSkipMap);
        clientMongoOperator.update(Query.query(Criteria.where("_id").is(taskId)), update, ConnectorConstant.TASK_COLLECTION);
    }

    protected synchronized void logSkipEvent(TapRecordEvent tapRecordEvent, Throwable ex) {
        logger.info("task-{} skip event: {}", taskId, tapRecordEvent);
        logger.info("task-{} skip exception: {}", taskId, ex.getMessage(), ex.getCause());

        long now = System.currentTimeMillis();
        if (now > nextPrintTimes) {
            String skipInfo = JSON.toJSONString(syncAndSkipMap);
            log.warn("DQL record isolated: task={}, skip counts={}", taskId, skipInfo);
            if (ex instanceof TapPdkViolateUniqueEx && ((TapPdkViolateUniqueEx) ex).getData() != null) {
                log.warn(SKIP_ERROR_EVENT_DATA, ((TapPdkViolateUniqueEx) ex).getData());
            }
            if (ex instanceof TapPdkWriteTypeEx && ((TapPdkWriteTypeEx) ex).getData() != null) {
                log.warn(SKIP_ERROR_EVENT_DATA, ((TapPdkWriteTypeEx) ex).getData());
            }
            if (ex instanceof TapPdkWriteLengthEx && ((TapPdkWriteLengthEx) ex).getData() != null) {
                log.warn(SKIP_ERROR_EVENT_DATA, ((TapPdkWriteLengthEx) ex).getData());
            }
            nextPrintTimes = now + 30 * 1000;
        }
        lastSkipTimes = now;
    }

    private Map<String, AtomicLong> getTableMetrics(String tableName) {
        Map<String, AtomicLong> tableMetrics = syncAndSkipMap.get(tableName);
        if (null == tableMetrics) {
            tableMetrics = syncAndSkipMap.computeIfAbsent(tableName, s -> new ConcurrentHashMap<>());
        }
        return tableMetrics;
    }

    private AtomicLong getTypeMetrics(Map<String, AtomicLong> tableMetrics, String type) {
        AtomicLong typeMetrics = tableMetrics.get(type);
        if (null == typeMetrics) {
            typeMetrics = tableMetrics.computeIfAbsent(type, (k) -> new AtomicLong(0));
        }
        return typeMetrics;
    }

    private AtomicLong getTypeMetrics(String tableName, String type) {
        Map<String, AtomicLong> tableMetrics = getTableMetrics(tableName);
        return getTypeMetrics(tableMetrics, type);
    }

    private boolean checkSkipByLimitMode(String tableName, long syncCounts, long skipCounts) {
        TaskDto.SkipErrorEvent.LimitMode limitMode = skipErrorEvent == null
                ? TaskDto.SkipErrorEvent.LimitMode.Disable : skipErrorEvent.getLimitModeEnum();
        if (limitMode == null) {
            limitMode = TaskDto.SkipErrorEvent.LimitMode.Disable;
        }
        switch (limitMode) {
            case Disable:
                return true;
            case SkipByLimit:
                if (skipErrorEvent.getLimit() >= skipCounts) {
                    return true;
                } else {
                    String skipInfo = JSON.toJSONString(syncAndSkipMap);
                    logTaskLevelHandling(tableName, DqlExceptionScope.RECORD.name(),
                            DqlRouteDecision.TASK_ERROR.name(),
                            "skip limit reached: count=" + skipCounts + ", status=" + skipInfo);
                }
                break;
            case SkipByRate:
                float rate = 1f * skipCounts / (syncCounts + skipCounts);
                if (skipErrorEvent.getRate() / 100.0 >= rate) {
                    return true;
                } else {
                    String skipInfo = JSON.toJSONString(syncAndSkipMap);
                    logTaskLevelHandling(tableName, DqlExceptionScope.RECORD.name(),
                            DqlRouteDecision.TASK_ERROR.name(),
                            "skip rate reached: rate=" + String.format("%.2f", rate)
                                    + ", status=" + skipInfo);
                }
                break;
            default:
                break;
        }
        return false;
    }

    private boolean checkSkipByThrowable(Throwable ex) {
        String code = errorCode(ex);
        if (code != null) {
            ErrorCodeEntity errorCode = ErrorCodeConfig.getInstance().getErrorCode(code);
            return errorCode != null && errorCode.isSkippable();
        }
        return false;
    }

    @Override
    public void onStart(TaskStartAspect startAspect) {
        try {
            this.taskId = getTask().getId().toHexString();
            this.clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
            if (this.clientMongoOperator instanceof HttpClientMongoOperator) {
                this.dqlEventReporter = new DqlEventReporter(
                        new DqlTmClient((HttpClientMongoOperator) this.clientMongoOperator));
            }
            this.logger = new SplitFileLogger(Level.INFO, taskId);

            synchronized (storeFuture) {
                stopStoreFuture();
                AtomicLong lastStoreTimes = new AtomicLong(System.currentTimeMillis());
                storeFuture.set(EXECUTOR.scheduleWithFixedDelay(() -> {
                    try {
                        long nowTime = System.currentTimeMillis();
                        if (lastStoreTimes.get() < lastSkipTimes) {
                            Thread.currentThread().setName(String.format("%s-skipErrorEvent", taskId));
                            save2TaskAttrs();
                            lastStoreTimes.set(nowTime);
                        }
                    } catch (Exception e) {
                        logger.warn("Skip error event store failed: {}", e.getMessage());
                    }
                }, 0, 5, TimeUnit.SECONDS));
            }

            Optional.ofNullable(getTask().getAttrs()).map(
                    attrs -> (Map<String, Map<String, Object>>) attrs.get(TaskDto.ATTRS_SKIP_ERROR_EVENT)
            ).map(m -> {
                Map<String, AtomicLong> subMap;
                for (Map.Entry<String, Map<String, Object>> tabEn : m.entrySet()) {
                    if (null == tabEn.getKey() || null == tabEn.getValue()) continue;
                    subMap = new ConcurrentHashMap<>();
                    for (Map.Entry<String, Object> subEn : tabEn.getValue().entrySet()) {
                        if (null == subEn.getKey() || null == subEn.getValue()) continue;
                        if (subEn.getValue() instanceof Integer) {
                            subMap.put(subEn.getKey(), new AtomicLong((int) subEn.getValue()));
                        } else if (subEn.getValue() instanceof Long) {
                            subMap.put(subEn.getKey(), new AtomicLong((int) subEn.getValue()));
                        }
                    }
                    syncAndSkipMap.put(tabEn.getKey(), subMap);
                }
                return null;
            });

            this.skipErrorEvent = getTask().getSkipErrorEvent();
            if (Optional.ofNullable(this.skipErrorEvent).map(vo -> {
                if (null == vo.getErrorMode()) vo.setErrorMode(TaskDto.SkipErrorEvent.ErrorMode.Disable);
                if (null == vo.getLimitMode()) vo.setLimitMode(TaskDto.SkipErrorEvent.LimitMode.Disable);
                if (null == vo.getLimit() || vo.getLimit() < 0) vo.setLimit(0L);
                if (null == vo.getRate() || vo.getRate() < 0) vo.setRate(0);

                switch (vo.getErrorModeEnum()) {
                    case SkipTable:
                        // has one error skip table
                        vo.setLimitMode(TaskDto.SkipErrorEvent.LimitMode.SkipByLimit);
                        vo.setLimit(0L);
                        return true;
                    case SkipData:
                        return true;
                    // case SkipTableForMigrateSnapshot:
                    // 此配置将复制任务的全量同步「跳过错误表」功能配置合并
                    // 其逻辑不在 SkipErrorEventAspectTask 中处理
                    // 请参考: io.tapdata.task.skiperrortable.ISkipErrorTable
                    default:
                        return false;
                }
            }).orElse(false)) {
                this.skipErrorDataNoeAspect = this::skipErrorDataNoeAspectImpl;
            }
        } catch (Exception e) {
            log.warn("Skip error event is not enable: {}", e.getMessage(), e);
        }
    }

    @Override
    public void onStop(TaskStopAspect stopAspect) {
        try {
            stopStoreFuture();
            shutdownExecutor();
        } finally {
            try {
                this.logger.close();
            } catch (Exception ignore) {
            }
        }
    }

    protected void shutdownExecutor() {
        if (EXECUTOR != null && !EXECUTOR.isShutdown()) {
            EXECUTOR.shutdown();
            try {
                if (!EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                    EXECUTOR.shutdownNow();
                    if (!EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                        logger.error("shutdown executor failed");
                    }
                }
            } catch (InterruptedException e) {
                EXECUTOR.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void stopStoreFuture() {
        synchronized (storeFuture) {
            Future<?> future = storeFuture.get();
            if (null == future) return;

            while (!Thread.interrupted()) {
                future.cancel(true);
                if (future.isDone() || future.isCancelled()) {
                    break;
                }
            }
            storeFuture.set(null);
        }
    }

    public AspectInterceptResult skipErrorDataNoeAspectHandle(SkipErrorDataAspect aspect) {
        if (aspect == null || !isSkipDataEnabled()) {
            return null;
        }
        return this.skipErrorDataNoeAspect.apply(aspect);
    }

    /**
     * Attaches a best-effort callback to normal target writes. The write
     * result is the source of truth for distinguishing successful records
     * from records that were rejected by a connector.
     */
    public Void writeRecordFuncAspectHandle(WriteRecordFuncAspect aspect) {
        if (aspect == null || aspect.getState() != WriteRecordFuncAspect.STATE_START
                || !isSkipDataEnabled() || dqlEventReporter == null) {
            return null;
        }
        TapTable targetTable = aspect.getTable();
        aspect.consumer((events, writeResult) -> reportSuccessfulRecords(targetTable, events, writeResult));
        return null;
    }

    private void reportSuccessfulRecords(TapTable targetTable,
                                         List<TapRecordEvent> events,
                                         WriteListResult<TapRecordEvent> writeResult) {
        if (events == null || events.isEmpty() || writeResult == null) {
            return;
        }
        Map<TapRecordEvent, Throwable> errorMap = writeResult.getErrorMap();
        long successAt = System.currentTimeMillis();
        for (TapRecordEvent event : events) {
            if (event == null || DqlRecoveryCaptureGuard.isRecoveryRecord(event)
                    || (errorMap != null && errorMap.containsKey(event))) {
                continue;
            }
            try {
                reportRecordSuccess(targetTable, event, successAt);
            } catch (RuntimeException exception) {
                // The target write has already succeeded. A failure to update
                // audit metadata must not turn it into a failed data write.
                if (log != null) {
                    log.warn("DQL later-success report failed for task {}: {}", taskId, exception.getMessage());
                }
            }
        }
    }

    private void reportRecordSuccess(TapTable targetTable, TapRecordEvent event, long successAt) {
        TaskDto currentTask = getTask();
        String taskRecordId = currentTask == null ? taskId : currentTask.getTaskRecordId();
        if (StringUtils.isBlank(taskRecordId)) {
            taskRecordId = taskId;
        }

        String sourceTable = event.getTableId();
        String targetTableId = targetTableId(targetTable, sourceTable);
        DqlRecordSuccessReport report = new DqlRecordSuccessReport();
        report.setTaskRecordId(taskRecordId);
        report.setSourceTable(sourceTable);
        report.setTargetTable(targetTableId);
        report.setTableId(targetTableId);
        report.setDmlType(dmlType(event));
        report.setEventTime(eventTime(event, successAt));
        report.setSuccessAt(successAt);

        DqlEventIdentity identity = dqlEventIdentityGenerator.generate(event, targetTable, taskRecordId, null);
        identity.applyTo(report);
        dqlEventReporter.reportRecordSuccess(taskId, report);
    }

    private String targetTableId(TapTable targetTable, String sourceTable) {
        if (targetTable != null) {
            if (StringUtils.isNotBlank(targetTable.getId())) {
                return targetTable.getId();
            }
            if (StringUtils.isNotBlank(targetTable.getName())) {
                return targetTable.getName();
            }
        }
        return sourceTable;
    }

    private Long eventTime(TapRecordEvent event, long captureTime) {
        if (event.getReferenceTime() != null) {
            return event.getReferenceTime();
        }
        if (event.getTime() != null) {
            return event.getTime();
        }
        return captureTime;
    }

    /**
     * Captures a single DML event failed by a processor before the node falls
     * back to its existing task-level error handling path.
     */
    public AspectInterceptResult skipErrorProcessAspectHandle(SkipErrorProcessAspect aspect) {
        if (aspect == null || aspect.getInputEvent() == null
                || !(aspect.getInputEvent().getTapEvent() instanceof TapRecordEvent event)
                || aspect.getError() == null) {
            return null;
        }
        if (DqlRecoveryCaptureGuard.isRecoveryRecord(event)) {
            DqlRecoveryCaptureGuard.notifyFailure(event, aspect.getError());
            return null;
        }
        if (!isSkipDataEnabled() || dqlEventReporter == null) {
            return null;
        }

        String tableId = event.getTableId();
        DqlFailedStage failedStage = aspect.getProcessStage() == null
                ? DqlFailedStage.PROCESSOR : aspect.getProcessStage();
        DqlClassificationResult classification = dqlExceptionClassifier.classify(
                aspect.getError(), new DqlClassificationContext(
                        failedStage,
                        DqlNodeType.PROCESSOR,
                        event,
                        DqlBatchContext.singleRecord(),
                        currentDqlTaskContext()));
        AtomicLong skipMetric;
        if (classification.getExceptionScope() == DqlExceptionScope.UNKNOWN) {
            skipMetric = reserveSkipCandidate(tableId, classification);
            classification = dlqStormGuard.protect(classification, DqlStormGuardContext.singleRecord(
                    taskId, aspect.getNodeId(), tableId, errorCode(aspect.getError()),
                    aspect.getError().getMessage()));
        } else {
            skipMetric = reserveSkipCandidate(tableId, classification);
        }
        boolean committed = false;
        try {
            if (classification.getRouteDecision() != DqlRouteDecision.RECORD_DLQ) {
                logTaskLevelHandling(tableId, classification);
                return null;
            }
            long syncCounts = getTypeMetrics(tableId, METRICS_SYNC).get();
            if (!checkSkipByLimitMode(tableId, syncCounts, skipMetric.get())) {
                return null;
            }

            TapTable table = resolveProcessorTable(aspect, tableId);
            reportDqlEvent(table, event, tableId, aspect.getError(), classification,
                    failedStage, aspect.getNodeId(), aspect.getNodeName(), null);
            logSkipEvent(event, aspect.getError());
            committed = true;
            return new AspectInterceptResult().intercepted(true);
        } catch (DqlEventReportException exception) {
            logTaskLevelHandling(tableId, DqlExceptionScope.RECORD.name(),
                    DqlRouteDecision.TASK_ERROR.name(), "DQL report failed");
            throw exception;
        } finally {
            if (!committed) {
                rollbackSkipCandidate(skipMetric);
            }
        }
    }

    public AspectInterceptResult skipErrorDataNoeAspectImpl(SkipErrorDataAspect aspect) {
        aspect.getPdkMethodInvoker().setEnableSkipErrorEvent(true);

        TapTable table = aspect.getTapTable();
        String tableId = table.getId();
        if (tableId == null || tableId.isBlank()) {
            tableId = table.getName();
        }
        AspectInterceptResult result = new AspectInterceptResult();
        result.setIntercepted(true);

        try {
            aspect.getWriteRecordFunction().apply(aspect.getTapRecordEvents());
            getTypeMetrics(tableId, METRICS_SYNC).addAndGet(aspect.getTapRecordEvents().size());
        } catch (Throwable e1) {
            if (aspect.getTapRecordEvents().size() == 1) {
                if (!checkSkip(table, tableId, aspect.getTapRecordEvents().get(0), e1)) {
                    throwAsRuntime(e1);
                }
            } else if (shouldSplitBatch(aspect, e1)) {
                for (TapRecordEvent tapRecordEvent : aspect.getTapRecordEvents()) {
                    try {
                        aspect.getWriteRecordFunction().apply(Collections.singletonList(tapRecordEvent));
                        getTypeMetrics(tableId, METRICS_SYNC).addAndGet(1);
                    } catch (Throwable e2) {
                        if (!checkSkip(table, tableId, tapRecordEvent, e2)) {
                            throwAsRuntime(e2);
                        }
                    }
                }
            } else {
                throwAsRuntime(e1);
            }
        }

        return result;
    }

    private boolean shouldSplitBatch(SkipErrorDataAspect aspect, Throwable error) {
        DqlClassificationResult classification = classify(error, null,
                DqlBatchContext.batchFailure(aspect.getTapRecordEvents().size(), 1));
        String tableName = tableId(aspect.getTapTable());
        if (classification.getExceptionScope() == DqlExceptionScope.SYSTEM
                || isSharedFailure(classification)) {
            logTaskLevelHandling(tableName, classification);
            return false;
        }
        boolean skippable = checkSkipByThrowable(error);
        if (!skippable) {
            logTaskLevelHandling(tableName, classification);
        }
        return skippable;
    }

    private boolean isSharedFailure(DqlClassificationResult classification) {
        String reason = classification.getClassificationReason();
        return reason != null && reason.startsWith("shared failure");
    }

    private AtomicLong reserveSkipCandidate(String tableName, DqlClassificationResult classification) {
        if (classification == null
                || (classification.getExceptionScope() != DqlExceptionScope.UNKNOWN
                && classification.getRouteDecision() != DqlRouteDecision.RECORD_DLQ)) {
            return null;
        }
        AtomicLong skipMetric = getTypeMetrics(tableName, METRICS_SKIP);
        skipMetric.incrementAndGet();
        return skipMetric;
    }

    private void rollbackSkipCandidate(AtomicLong skipMetric) {
        if (skipMetric != null) {
            skipMetric.decrementAndGet();
        }
    }

    private String tableId(TapTable table) {
        if (table == null) {
            return null;
        }
        if (StringUtils.isNotBlank(table.getId())) {
            return table.getId();
        }
        return table.getName();
    }

    private void logTaskLevelHandling(String tableName, DqlClassificationResult classification) {
        if (classification == null) {
            return;
        }
        logTaskLevelHandling(tableName,
                classification.getExceptionScope() == null ? null : classification.getExceptionScope().name(),
                classification.getRouteDecision() == null ? null : classification.getRouteDecision().name(),
                classification.getClassificationReason());
    }

    private void logTaskLevelHandling(String tableName, String scope, String route, String reason) {
        if (log != null) {
            log.warn("DQL task-level handling: task={}, table={}, scope={}, route={}, reason={}",
                    taskId, tableName, scope, route, reason);
        }
    }

    private boolean checkSkip(TapTable table,
                               String tableName,
                               TapRecordEvent tapRecordEvent,
                               Throwable ex) {
        if (DqlRecoveryCaptureGuard.isRecoveryRecord(tapRecordEvent)) {
            DqlRecoveryCaptureGuard.notifyFailure(tapRecordEvent, ex);
            return false;
        }
        DqlClassificationResult classification = classify(ex, tapRecordEvent, DqlBatchContext.singleRecord());
        AtomicLong skipMetric = reserveSkipCandidate(tableName, classification);
        if (classification.getExceptionScope() == DqlExceptionScope.UNKNOWN) {
            classification = dlqStormGuard.protect(classification, DqlStormGuardContext.singleRecord(
                    taskId, null, tableName, errorCode(ex), ex.getMessage()));
        }
        boolean committed = false;
        try {
            if (classification.getRouteDecision() != DqlRouteDecision.RECORD_DLQ) {
                logTaskLevelHandling(tableName, classification);
                return false;
            }
            long syncCounts = getTypeMetrics(tableName, METRICS_SYNC).get();
            if (!checkSkipByLimitMode(tableName, syncCounts, skipMetric.get())) {
                return false;
            }
            reportDqlEvent(table, tapRecordEvent, tableName, ex, classification);
            logSkipEvent(tapRecordEvent, ex);
            committed = true;
            return true;
        } catch (DqlEventReportException exception) {
            logTaskLevelHandling(tableName, DqlExceptionScope.RECORD.name(),
                    DqlRouteDecision.TASK_ERROR.name(), "DQL report failed");
            throw exception;
        } catch (RuntimeException exception) {
            logTaskLevelHandling(tableName, DqlExceptionScope.RECORD.name(),
                    DqlRouteDecision.TASK_ERROR.name(), "DQL capture failed");
            throw new DqlEventReportException(taskId, exception);
        } finally {
            if (!committed) {
                rollbackSkipCandidate(skipMetric);
            }
        }
    }

    private DqlClassificationResult classify(Throwable error,
                                              TapRecordEvent event,
                                              DqlBatchContext batchContext) {
        return dqlExceptionClassifier.classify(error, new DqlClassificationContext(
                DqlFailedStage.TARGET_WRITE,
                DqlNodeType.TARGET,
                event,
                batchContext,
                currentDqlTaskContext()));
    }

    private DqlTaskContext currentDqlTaskContext() {
        TaskDto currentTask = getTask();
        String taskType = currentTask == null ? TaskDto.SYNC_TYPE_SYNC : currentTask.getSyncType();
        String taskStatus = currentTask == null ? TaskDto.STATUS_RUNNING : currentTask.getStatus();
        boolean configured = currentTask == null || currentTask.getId() != null;
        return new DqlTaskContext(taskType, taskStatus, isSkipDataEnabled(),
                isSkipDataEnabled(), false, configured);
    }

    private boolean isSkipDataEnabled() {
        return skipErrorEvent != null
                && skipErrorEvent.getErrorModeEnum() == TaskDto.SkipErrorEvent.ErrorMode.SkipData;
    }

    private void reportDqlEvent(TapTable table,
                                TapRecordEvent event,
                                String tableId,
                                Throwable error,
                                DqlClassificationResult classification) {
        reportDqlEvent(table, event, tableId, error, classification,
                DqlFailedStage.TARGET_WRITE, null, null, tableId);
    }

    private void reportDqlEvent(TapTable table,
                                TapRecordEvent event,
                                String tableId,
                                Throwable error,
                                DqlClassificationResult classification,
                                DqlFailedStage failedStage,
                                String failedNodeId,
                                String failedNodeName,
                                String targetTable) {
        try {
            if (dqlEventReporter == null) {
                throw new DqlEventReportException(taskId, "DQL TM reporter is unavailable");
            }
            DqlEventReport report = buildDqlEventReport(table, event, tableId, error, classification,
                    failedStage, failedNodeId, failedNodeName, targetTable);
            dqlEventReporter.report(taskId, report);
        } catch (DqlEventReportException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new DqlEventReportException(taskId, exception);
        }
    }

    private DqlEventReport buildDqlEventReport(TapTable table,
                                               TapRecordEvent event,
                                               String tableId,
                                               Throwable error,
                                               DqlClassificationResult classification) {
        return buildDqlEventReport(table, event, tableId, error, classification,
                DqlFailedStage.TARGET_WRITE, null, null, tableId);
    }

    private DqlEventReport buildDqlEventReport(TapTable table,
                                               TapRecordEvent event,
                                               String tableId,
                                               Throwable error,
                                               DqlClassificationResult classification,
                                               DqlFailedStage failedStage,
                                               String failedNodeId,
                                               String failedNodeName,
                                               String targetTable) {
        TaskDto currentTask = getTask();
        DqlEventReport report = new DqlEventReport();
        if (currentTask != null) {
            report.setTaskRecordId(currentTask.getTaskRecordId());
            report.setTaskName(currentTask.getName());
            report.setTaskVersion(currentTask.getVersion());
            report.setAgentId(currentTask.getAgentId());
        }
        report.setTaskRecordId(report.getTaskRecordId() == null ? taskId : report.getTaskRecordId());
        report.setFailedStage(failedStage == null ? null : failedStage.name());
        report.setFailedNodeId(failedNodeId);
        report.setFailedNodeName(failedNodeName);
        report.setSourceTable(event.getTableId());
        report.setTargetTable(targetTable);
        report.setTableId(tableId);
        report.setDmlType(dmlType(event));
        report.setEventTime(event.getReferenceTime() == null ? event.getTime() : event.getReferenceTime());
        report.setErrorCode(errorCode(error));

        DqlPayloadSnapshot payload = dqlPayloadSerializer.serialize(event);
        DqlEventIdentity identity = dqlEventIdentityGenerator.generate(
                event, table, report.getTaskRecordId(), null);
        DqlPayloadPreview preview = dqlPayloadPreviewBuilder.build(event, identity.getEventKey());
        payload.setPayloadPreview(preview.getPayloadPreview());
        payload.setPayloadPreviewTruncated(preview.isTruncated());
        report.setPayload(payload);
        identity.applyTo(report);
        classification.applyTo(report);
        return report;
    }

    private TapTable resolveProcessorTable(SkipErrorProcessAspect aspect, String tableId) {
        if (aspect.getProcessorBaseContext() == null
                || aspect.getProcessorBaseContext().getTapTableMap() == null
                || tableId == null || tableId.isBlank()) {
            return null;
        }
        try {
            return aspect.getProcessorBaseContext().getTapTableMap().get(tableId);
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    private String dmlType(TapRecordEvent event) {
        if (event instanceof TapInsertRecordEvent) {
            return "I";
        }
        if (event instanceof TapUpdateRecordEvent) {
            return "U";
        }
        if (event instanceof TapDeleteRecordEvent) {
            return "D";
        }
        throw new IllegalArgumentException("Unsupported DQL event type: " + event.getClass().getName());
    }

    private String errorCode(Throwable error) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = error;
        while (current != null && visited.add(current)) {
            if (current instanceof TapCodeException && ((TapCodeException) current).getCode() != null) {
                return ((TapCodeException) current).getCode();
            }
            current = current.getCause();
        }
        return null;
    }

    private void throwAsRuntime(Throwable error) {
        if (error instanceof RuntimeException) {
            throw (RuntimeException) error;
        }
        throw new RuntimeException(error);
    }
}
