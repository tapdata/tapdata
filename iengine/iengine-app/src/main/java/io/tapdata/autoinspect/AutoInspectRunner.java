package io.tapdata.autoinspect;

import com.tapdata.tm.autoinspect.compare.IAutoCompare;
import com.tapdata.tm.autoinspect.connector.IConnector;
import com.tapdata.tm.autoinspect.connector.IDataCursor;
import com.tapdata.tm.autoinspect.constants.CompareStatus;
import com.tapdata.tm.autoinspect.constants.CompareStep;
import com.tapdata.tm.autoinspect.constants.Constants;
import com.tapdata.tm.autoinspect.constants.TaskType;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.entity.AutoInspectProgress;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import com.tapdata.tm.autoinspect.entity.CompareTableItem;
import io.tapdata.observable.logging.ObsLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/18 13:12 Create
 */
public abstract class AutoInspectRunner<S extends IConnector, T extends IConnector, A extends IAutoCompare> implements Runnable {
    protected final ObsLogger userLogger;
    protected final String taskId;
    protected final TaskType taskType;
    protected AutoInspectProgress progress;

    public AutoInspectRunner(ObsLogger userLogger, String taskId, TaskType taskType, AutoInspectProgress progress) {
        this.userLogger = userLogger;
        this.taskId = taskId;
        this.taskType = taskType;
        this.progress = progress;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(String.format("th-%s-%s", Constants.MODULE_NAME, taskId));
        try (
                S sourceConnector = openSourceConnector();
                T targetConnector = openTargetConnector();
                A autoCompare = openAutoCompare(sourceConnector, targetConnector)
        ) {
            if (null == progress) {
                progress = new AutoInspectProgress();
                init(progress, sourceConnector, targetConnector);
                updateProgress(progress);
            }

            if (CompareStep.Initial == progress.getStep() && taskType.hasInitial()) {
                userLogger.info("Start initial compare");
                long beginTimes = System.currentTimeMillis();
                initialCompare(sourceConnector, targetConnector, autoCompare);
                userLogger.info("Completed initial compare use {} ms", System.currentTimeMillis() - beginTimes);
            }

            progress.setStep(CompareStep.Increment);
            updateProgress(progress);

            if (taskType.hasIncrement()) {
                userLogger.info("Start Incremental compare");
                long beginTimes = System.currentTimeMillis();
                incrementalCompare(sourceConnector, targetConnector, autoCompare);
                userLogger.info("Completed increment compare use {} ms", System.currentTimeMillis() - beginTimes);
            }
        } catch (Exception e) {
            handleError(String.format("Execute failed: %s", e.getMessage()), e);
        } finally {
            exit();
        }
    }

    protected abstract S openSourceConnector() throws Exception;

    protected abstract T openTargetConnector() throws Exception;

    protected abstract A openAutoCompare(S sourceConnector, T targetConnector) throws Exception;

    protected abstract void init(AutoInspectProgress progress, S sourceConnector, T targetConnector) throws Exception;

    protected void initialCompare(S sourceConnector, T targetConnector, A autoCompare) throws Exception {
        for (CompareTableItem tableItem : progress.getTableItems()) {
            initialCompare(sourceConnector, targetConnector, autoCompare, tableItem);
            tableItem.setInitialed(true);
            updateProgress(progress);
        }
    }

    protected void initialCompare(S sourceConnector, T targetConnector, A autoCompare, CompareTableItem compareItem) throws Exception {
        userLogger.info("Start '{}' table initial compare", compareItem.getTableName());
        try (IDataCursor<CompareRecord> sourceCursor = sourceConnector.queryAll(compareItem.getTableName(), compareItem.getOffset())) {
            try (IDataCursor<CompareRecord> targetCursor = targetConnector.queryAll(compareItem.getTableName(), compareItem.getOffset())) {
                CompareRecord sourceData = sourceCursor.next();
                CompareRecord targetData = targetCursor.next();
                do {
                    if (null == sourceData) {
                        // more target
                        int counts = 0;
                        while (isRunning() && null != targetData) {
                            counts++;
                            targetData = targetCursor.next();
                        }
                        if (counts > 0) {
                            userLogger.warn("Target has more data: {}", counts);
                        }
                        break;
                    }
                    if (null == targetData) {
                        // more source
                        while (isRunning() && null != sourceData) {
                            autoCompare.add(new TaskAutoInspectResultDto(taskId, sourceConnector.getConnId(), compareItem.getTableName(), sourceData.getOriginalKey(), sourceData.getData(), targetConnector.getConnId(), compareItem.getTableName(), null));
                            sourceData = sourceCursor.next();
                        }
                        break;
                    }

                    CompareStatus compareStatus = compare(sourceData, targetData);
                    switch (compareStatus) {
                        case MoveSource:
                            autoCompare.add(new TaskAutoInspectResultDto(taskId, sourceConnector.getConnId(), compareItem.getTableName(), sourceData.getOriginalKey(), sourceData.getData(), targetConnector.getConnId(), compareItem.getTableName(), null));
                            sourceData = sourceCursor.next();
                            break;
                        case MoveTarget:
                            autoCompare.add(new TaskAutoInspectResultDto(taskId, sourceConnector.getConnId(), compareItem.getTableName(), sourceData.getOriginalKey(), null, targetConnector.getConnId(), compareItem.getTableName(), targetData.getData()));
                            targetData = targetCursor.next();
                            break;
                        case Diff:
                            autoCompare.add(new TaskAutoInspectResultDto(taskId, sourceConnector.getConnId(), compareItem.getTableName(), sourceData.getOriginalKey(), sourceData.getData(), targetConnector.getConnId(), compareItem.getTableName(), targetData.getData()));
                            sourceData = sourceCursor.next();
                            targetData = targetCursor.next();
                            break;
                        case Ok:
                            sourceData = sourceCursor.next();
                            targetData = targetCursor.next();
                            break;
                        default:
                            throw new RuntimeException("No support compare result type: " + compareStatus);
                    }
                } while (isRunning() && !isStopping());
            }
        }
        userLogger.info("'{}' table initial compare completed", compareItem.getTableName());
    }

    protected abstract void incrementalCompare(S sourceConnector, T targetConnector, A autoCompare) throws Exception;

    protected CompareStatus compare(CompareRecord sourceData, CompareRecord targetData) {
        CompareStatus compareStatus = sourceData.compareKeys(targetData);
        if (CompareStatus.Diff == compareStatus) {
            Object odata;
            List<String> diffKeys = new ArrayList<>();
            Map<String, Object> omap = targetData.getData();
            for (Map.Entry<String, Object> en : sourceData.getData().entrySet()) {
                //filter keys
                if (sourceData.getKeyNames().contains(en.getKey())) continue;

                //check null value
                odata = omap.get(en.getKey());
                if (null == en.getValue()) {
                    if (null != odata) {
                        diffKeys.add(en.getKey());
                    }
                    continue;
                }

                //check value
                if (!en.getValue().equals(odata)) {
                    diffKeys.add(en.getKey());
                }
            }

            //has not difference
            if (diffKeys.isEmpty()) {
                compareStatus = CompareStatus.Ok;
            }
        }

        return compareStatus;
    }

    protected abstract void updateProgress(AutoInspectProgress progress);

    protected abstract boolean isRunning();

    protected abstract boolean isStopping();

    protected void handleError(String msg, Throwable e) {
        userLogger.warn(msg, e);
    }

    protected void exit() {
        userLogger.info("Exit");
    }
}
