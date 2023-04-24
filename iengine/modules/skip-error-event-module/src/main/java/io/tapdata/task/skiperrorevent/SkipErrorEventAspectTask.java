package io.tapdata.task.skiperrorevent;

import com.alibaba.fastjson.JSON;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.ErrorCodeConfig;
import io.tapdata.ErrorCodeEntity;
import io.tapdata.aspect.SkipErrorDataAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AbstractAspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.exception.TapCodeException;
import org.apache.logging.log4j.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC}, ignoreErrors = false)
public class SkipErrorEventAspectTask extends AbstractAspectTask {
    // Set a maximum of 10 threads to report status, if delay please check the net work and DB stress
    private final static ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(10);

    private static final String METRICS_SYNC = "sync";
    private static final String METRICS_SKIP = "skip";

    private String taskId;
    private TaskDto.SkipErrorEvent skipErrorEvent;
    private final Map<String, Map<String, AtomicLong>> syncAndSkipMap = new ConcurrentHashMap<>();

    private Function<SkipErrorDataAspect, AspectInterceptResult> skipErrorDataNoeAspect = aspect -> null;
    private long lastSkipTimes;
    private long nextPrintTimes;
    private ClientMongoOperator clientMongoOperator;
    private final AtomicReference<Future<?>> storeFuture = new AtomicReference<>();
    private SplitFileLogger logger;

    public SkipErrorEventAspectTask() {
        interceptHandlers.register(SkipErrorDataAspect.class, this::skipErrorDataNoeAspectHandle);
    }

    private void save2TaskAttrs() {
        Update update = Update.update(String.format("attrs.%s", TaskDto.ATTRS_SKIP_ERROR_EVENT), syncAndSkipMap);
        clientMongoOperator.update(Query.query(Criteria.where("_id").is(taskId)), update, ConnectorConstant.TASK_COLLECTION);
    }

    private synchronized void logSkipEvent(TapRecordEvent tapRecordEvent, Throwable ex) {
        logger.info("task-{} skip event: {}", taskId, tapRecordEvent);
        logger.info("task-{} skip exception: {}", taskId, ex.getMessage(), ex.getCause());

        long now = System.currentTimeMillis();
        if (now > nextPrintTimes) {
            String skipInfo = JSON.toJSONString(syncAndSkipMap);
            log.warn("Skip error event counts:{}", skipInfo);
            nextPrintTimes = now + 30 * 1000;
        }
        lastSkipTimes = now;
    }

    private Map<String, AtomicLong> getTableMetrics(String tableName) {
        Map<String, AtomicLong> tableMetrics = syncAndSkipMap.get(tableName);
        if (null == tableMetrics) {
            tableMetrics = syncAndSkipMap.computeIfAbsent(tableName, s -> new HashMap<>());
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

    private boolean checkSkip(String tableName, TapRecordEvent tapRecordEvent, Throwable ex) {
        if (checkSkipByThrowable(ex)) {
            long syncCounts = getTypeMetrics(tableName, METRICS_SYNC).get();
            long skipCounts = getTypeMetrics(tableName, METRICS_SKIP).addAndGet(1);
            if (checkSkipByLimitMode(syncCounts, skipCounts)) {
                logSkipEvent(tapRecordEvent, ex);
                return true;
            }
        }
        return false;
    }

    private boolean checkSkipByLimitMode(long syncCounts, long skipCounts) {
        switch (skipErrorEvent.getLimitModeEnum()) {
            case SkipByLimit:
                if (skipErrorEvent.getLimit() >= skipCounts) {
                    return true;
                } else {
                    String skipInfo = JSON.toJSONString(syncAndSkipMap);
                    log.warn("Reach the skip limit: {}, status: {}", skipCounts, skipInfo);
                }
                break;
            case SkipByRate:
                float rate = 1f * skipCounts / (syncCounts + skipCounts);
                if (skipErrorEvent.getRate() / 100.0 >= rate) {
                    return true;
                } else {
                    String skipInfo = JSON.toJSONString(syncAndSkipMap);
                    log.warn("Reach the skip rate: {}, status: {}", String.format("%.2f", rate), skipInfo);
                }
                break;
            default:
                break;
        }
        return false;
    }

    private boolean checkSkipByThrowable(Throwable ex) {
        if (ex instanceof TapCodeException) {
            String code = ((TapCodeException) ex).getCode();
            ErrorCodeEntity errorCode = ErrorCodeConfig.getInstance().getErrorCode(code);
            return errorCode.isSkippable();
        }
        return false;
    }

    @Override
    public void onStart(TaskStartAspect startAspect) {
        try {
            this.taskId = getTask().getId().toHexString();
            this.clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
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
        } finally {
            try {
                this.logger.close();
            } catch (Exception ignore) {
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
        return this.skipErrorDataNoeAspect.apply(aspect);
    }

    public AspectInterceptResult skipErrorDataNoeAspectImpl(SkipErrorDataAspect aspect) {
        aspect.getPdkMethodInvoker().setEnableSkipErrorEvent(true);

        String tableId = aspect.getTapTable().getId();
        AspectInterceptResult result = new AspectInterceptResult();
        result.setIntercepted(true);

        try {
            aspect.getWriteRecordFunction().apply(aspect.getTapRecordEvents());
            getTypeMetrics(tableId, METRICS_SYNC).addAndGet(aspect.getTapRecordEvents().size());
        } catch (Throwable e1) {
            // Here the exception is thrown as is and handled by the connector and engine
            if (checkSkipByThrowable(e1)) {
                for (TapRecordEvent tapRecordEvent : aspect.getTapRecordEvents()) {
                    try {
                        aspect.getWriteRecordFunction().apply(Collections.singletonList(tapRecordEvent));
                        getTypeMetrics(tableId, METRICS_SYNC).addAndGet(1);
                    } catch (Throwable e2) {
                        if (!checkSkip(tableId, tapRecordEvent, e2)) {
                            throw new RuntimeException(e2);
                        }
                    }
                }
            } else if (e1 instanceof RuntimeException) {
                throw (RuntimeException) e1;
            } else {
                throw new RuntimeException(e1);
            }
        }

        return result;
    }
}
