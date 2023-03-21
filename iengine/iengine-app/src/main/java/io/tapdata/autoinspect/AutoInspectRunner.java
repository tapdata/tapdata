package io.tapdata.autoinspect;

import com.tapdata.tm.autoinspect.compare.IAutoCompare;
import com.tapdata.tm.autoinspect.connector.IConnector;
import com.tapdata.tm.autoinspect.connector.IDataCursor;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.constants.CompareStatus;
import com.tapdata.tm.autoinspect.constants.CompareStep;
import com.tapdata.tm.autoinspect.constants.TaskType;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.entity.AutoInspectProgress;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import com.tapdata.tm.autoinspect.entity.CompareTableItem;
import io.tapdata.observable.logging.ObsLogger;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/18 13:12 Create
 */
public abstract class AutoInspectRunner<S extends IConnector, T extends IConnector, A extends IAutoCompare> implements Runnable {
    private static final Logger logger = LogManager.getLogger(AutoInspectRunner.class);
    protected final ObsLogger userLogger;
    protected final String taskId;
    protected final TaskType taskType;
    protected AutoInspectProgress progress;
    protected final AtomicBoolean forceStopping = new AtomicBoolean(false);

    public AutoInspectRunner(@NonNull ObsLogger userLogger, @NonNull String taskId, @NonNull TaskType taskType, AutoInspectProgress progress) {
        this.userLogger = userLogger;
        this.taskId = taskId;
        this.taskType = taskType;
        this.progress = progress;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(String.format("th-%s-%s", AutoInspectConstants.MODULE_NAME, taskId));
        try (
                S sourceConnector = openSourceConnector();
                T targetConnector = openTargetConnector()
        ) {
            if (null == progress) {
                progress = new AutoInspectProgress();
                init(progress, sourceConnector, targetConnector);
                updateProgress(progress);
            }

            try (A autoCompare = openAutoCompare(sourceConnector, targetConnector)) {
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

                while (isRunning() && !autoCompare.stop(forceStopping.get())) {
                    Thread.sleep(2000);
                }
            }
        } catch (Throwable e) {
            errorHandle(e, String.format("Execute failed: %s", e.getMessage()));
        } finally {
            exit();
        }
    }

    protected abstract S openSourceConnector() throws Exception;

    protected abstract T openTargetConnector() throws Exception;

    protected abstract A openAutoCompare(@NonNull S sourceConnector, @NonNull T targetConnector) throws Exception;

    protected abstract void init(@NonNull AutoInspectProgress progress, @NonNull S sourceConnector, @NonNull T targetConnector) throws Exception;

    protected void initialCompare(@NonNull S sourceConnector, @NonNull T targetConnector, @NonNull A autoCompare) throws Exception {
        for (CompareTableItem tableItem : progress.getTableItems().values()) {
            initialCompare(sourceConnector, targetConnector, autoCompare, tableItem);
            tableItem.setInitialed(true);
            updateProgress(progress);
        }
    }

    protected void initialCompare(@NonNull S sourceConnector, @NonNull T targetConnector, @NonNull A autoCompare, @NonNull CompareTableItem compareItem) throws Exception {
        logger.info("Start '{}' table initial compare", compareItem.getTableName());
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
                            autoCompare.autoCompare(TaskAutoInspectResultDto.parseNoneTarget(taskId, sourceData, targetConnector.getConnId(), compareItem.getTableName()));
                            sourceData = sourceCursor.next();
                        }
                        break;
                    }

                    CompareStatus compareStatus = sourceData.compare(targetData);
                    switch (compareStatus) {
                        case MoveSource:
                            autoCompare.autoCompare(TaskAutoInspectResultDto.parseNoneTarget(taskId, sourceData, targetConnector.getConnId(), compareItem.getTableName()));
                            sourceData = sourceCursor.next();
                            break;
                        case MoveTarget:
                            // more target data
                            targetData = targetCursor.next();
                            break;
                        case Diff:
                            autoCompare.autoCompare(TaskAutoInspectResultDto.parse(taskId, sourceData, targetData));
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
                } while (isRunning());
            }
        }
        logger.info("Completed '{}' table initial compare", compareItem.getTableName());
    }

    protected abstract void incrementalCompare(@NonNull S sourceConnector, @NonNull T targetConnector, @NonNull A autoCompare) throws Exception;

    protected abstract void updateProgress(@NonNull AutoInspectProgress progress);

    protected abstract boolean isRunning();

    protected abstract boolean isStopping();

    protected void errorHandle(Throwable e, String msg) {
        userLogger.warn(msg, e);
    }

    protected void exit() {
        userLogger.info("Exit AutoInspect.");
    }
}
