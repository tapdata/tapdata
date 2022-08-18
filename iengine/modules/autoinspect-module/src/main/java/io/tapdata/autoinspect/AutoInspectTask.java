package io.tapdata.autoinspect;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.autoinspect.constants.AutoInspectTaskStatus;
import com.tapdata.tm.autoinspect.constants.Constants;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import io.tapdata.autoinspect.compare.IAutoCompare;
import io.tapdata.autoinspect.connector.IConnector;
import io.tapdata.autoinspect.connector.IDataCursor;
import io.tapdata.autoinspect.entity.CompareEvent;
import io.tapdata.autoinspect.entity.CompareItem;
import io.tapdata.autoinspect.entity.CompareRecord;
import io.tapdata.autoinspect.status.AutoInspectStatusCtl;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Function;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/8 18:36 Create
 */
public abstract class AutoInspectTask implements Runnable {
    private static final Logger logger = LogManager.getLogger(AutoInspectTask.class);
    protected final @NonNull AutoInspectStatusCtl statusCtl;
    protected final @NonNull Function<AutoInspectStatusCtl, Boolean> closedFn;

    public AutoInspectTask(@NonNull AutoInspectStatusCtl statusCtl, @NonNull Function<AutoInspectStatusCtl, Boolean> closedFn) {
        this.statusCtl = statusCtl;
        this.closedFn = closedFn;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(String.format("th-%s-%s", Constants.MODULE_NAME, statusCtl.getTaskId()));
        try (
                IConnector sourceConnector = openSourceConnector();
                IConnector targetConnector = openTargetConnector();
                IAutoCompare autoCompare = openAutoCompare(sourceConnector, targetConnector)
        ) {
            logger.info("start");
            statusCtl.waitSyncInitialed();

            if (statusCtl.getSyncType().hasInitial()) {
                statusCtl.inspectInitialing();
                logger.info("begin initial compare");

                long beginTimes = System.currentTimeMillis();
                initialCompare(sourceConnector, targetConnector, autoCompare);
                logger.info("completed initial compare use {} ms", System.currentTimeMillis() - beginTimes);
            }

            if (statusCtl.getSyncType().hasIncrement()) {
                statusCtl.inspectIncrementing();
                long beginTimes = System.currentTimeMillis();
                incrementalCompare(sourceConnector, targetConnector, autoCompare);
                logger.info("completed increment compare use {} ms", System.currentTimeMillis() - beginTimes);
            }

            statusCtl.inspectDone();
        } catch (Exception e) {
            logger.warn("execute failed: {}", e.getMessage(), e);
            statusCtl.inspectError(e.getMessage());
        } finally {
            close();
        }
    }

    protected void close() {
        logger.info("exit");
        closedFn.apply(statusCtl);
    }

    public boolean isRunning() {
        AutoInspectTaskStatus.Sync syncStatus = statusCtl.getSyncStatus();
        if (syncStatus.in(AutoInspectTaskStatus.Sync.Error)) {
            if (!isStopping()) {
                statusCtl.inspectStopping();
            }
            return false;
        }
        return !(statusCtl.getInspectStatus().in(AutoInspectTaskStatus.Inspect.Done) || Thread.interrupted());
    }

    public boolean isStopping() {
        return statusCtl.getInspectStatus().in(AutoInspectTaskStatus.Inspect.Stopping);
    }

    protected void initialCompare(IConnector sourceConnector, IConnector targetConnector, IAutoCompare autoCompare) throws Exception {
        TaskAutoInspectResultDto taskAutoInspectResultDto;
        for (CompareItem compareItem : statusCtl.getTables()) {
            logger.info("begin '{}' table initial compare", compareItem.getTableName());

            try (IDataCursor<CompareRecord> sourceCursor = sourceConnector.queryAll(compareItem.getTableName())) {
                try (IDataCursor<CompareRecord> targetCursor = targetConnector.queryAll(compareItem.getTableName())) {
                    CompareRecord sourceData = sourceCursor.next();
                    CompareRecord targetData = targetCursor.next();
                    do {
                        logger.info("compare {} and {}", JSON.toJSONString(sourceData), JSON.toJSONString(targetData));
                        if (null == sourceData) {
                            // more target
                            while (isRunning() && null != targetData) {
                                autoCompare.add(new TaskAutoInspectResultDto(statusCtl.getTaskId(), sourceConnector.getConnId(), targetConnector.getConnId(), compareItem.getTableName(), null, null, targetData.getData()));
                                targetData = targetCursor.next();
                            }
                            break;
                        }
                        if (null == targetData) {
                            // more source
                            while (isRunning() && null != sourceData) {
                                autoCompare.add(new TaskAutoInspectResultDto(statusCtl.getTaskId(), sourceConnector.getConnId(), targetConnector.getConnId(), compareItem.getTableName(), sourceData.getOriginalKey(), sourceData.getData(), null));
                                sourceData = sourceCursor.next();
                            }
                            break;
                        }

                        switch (sourceData.compareKeys(targetData)) {
                            case Lager:
                                autoCompare.add(new TaskAutoInspectResultDto(statusCtl.getTaskId(), sourceConnector.getConnId(), targetConnector.getConnId(), compareItem.getTableName(), null, null, targetData.getData()));
                                targetData = targetCursor.next();
                                break;
                            case Less:
                                autoCompare.add(new TaskAutoInspectResultDto(statusCtl.getTaskId(), sourceConnector.getConnId(), targetConnector.getConnId(), compareItem.getTableName(), sourceData.getOriginalKey(), sourceData.getData(), null));
                                sourceData = sourceCursor.next();
                                break;
                            default:
                                taskAutoInspectResultDto = sourceData.compareData(statusCtl.getTaskId(), sourceConnector.getConnId(), targetConnector.getConnId(), compareItem.getTableName(), targetData);
                                if (null != taskAutoInspectResultDto) {
                                    autoCompare.add(taskAutoInspectResultDto);
                                }
                                targetData = targetCursor.next();
                                sourceData = sourceCursor.next();
                                break;
                        }
                    } while (isRunning() && !isStopping());
                }
            }
        }
    }

    protected void incrementalCompare(IConnector sourceConnector, IConnector targetConnector, IAutoCompare autoCompare) throws Exception {

        new Thread(() -> {
            try {
                Thread.sleep(10000);
                statusCtl.inspectStopping();
            } catch (InterruptedException ignore) {
            }
        }).start();

        sourceConnector.increment(compareEvents -> {
            if (null != compareEvents && !compareEvents.isEmpty()) {
                TaskAutoInspectResultDto taskAutoInspectResultDto;
                CompareRecord sourceRecord, queryRecord;
                String tableName = compareEvents.get(0).getTableName();

                for (CompareEvent e : compareEvents) {

                    queryRecord = targetConnector.queryByKey(tableName, e.getOriginalKeymap());
                    if (null == queryRecord) {
                        autoCompare.add(new TaskAutoInspectResultDto(statusCtl.getTaskId(), sourceConnector.getConnId(), targetConnector.getConnId(), tableName, e.getOriginalKeymap(), e.getData(), null));
                    } else {
                        sourceRecord = new CompareRecord();
                        sourceRecord.getKeyNames().addAll(e.getOriginalKeymap().keySet());
                        sourceRecord.getData().putAll(e.getData());

                        taskAutoInspectResultDto = sourceRecord.compareData(statusCtl.getTaskId(), sourceConnector.getConnId(), targetConnector.getConnId(), tableName, queryRecord);
                        if (null != taskAutoInspectResultDto) {
                            autoCompare.add(taskAutoInspectResultDto);
                        }
                    }
                }
            }
            return isRunning() && !isStopping();
        });
    }

    protected abstract IConnector openSourceConnector();

    protected abstract IConnector openTargetConnector();

    protected abstract IAutoCompare openAutoCompare(IConnector sourceConnector, IConnector targetConnector);
}
