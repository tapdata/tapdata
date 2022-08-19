package io.tapdata.autoinspect;

import com.tapdata.tm.autoinspect.connector.IConnector;
import com.tapdata.tm.autoinspect.connector.IDataCursor;
import com.tapdata.tm.autoinspect.constants.CompareStatus;
import com.tapdata.tm.autoinspect.constants.Constants;
import com.tapdata.tm.autoinspect.constants.TaskType;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.entity.CompareRecord;
import com.tapdata.tm.autoinspect.entity.CompareTableItem;
import io.tapdata.autoinspect.compare.IAutoCompare;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/18 13:12 Create
 */
public abstract class AutoInspectRunner<S extends IConnector, T extends IConnector, A extends IAutoCompare> implements Runnable {
    private static final Logger logger = LogManager.getLogger(AutoInspectRunner.class);
    protected final String taskId;
    protected final TaskType taskType;

    public AutoInspectRunner(String taskId, TaskType taskType) {
        this.taskId = taskId;
        this.taskType = taskType;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(String.format("th-%s-%s", Constants.MODULE_NAME, taskId));
        try (
                S sourceConnector = openSourceConnector();
                T targetConnector = openTargetConnector();
                A autoCompare = openAutoCompare(sourceConnector, targetConnector)
        ) {
            if (taskType.hasInitial()) {
                long beginTimes = System.currentTimeMillis();
                initialCompare(sourceConnector, targetConnector, autoCompare);
                logger.info("completed initial compare use {} ms", System.currentTimeMillis() - beginTimes);
            }

            if (taskType.hasIncrement()) {
                long beginTimes = System.currentTimeMillis();
                incrementalCompare(sourceConnector, targetConnector, autoCompare);
                logger.info("completed increment compare use {} ms", System.currentTimeMillis() - beginTimes);
            }
        } catch (Exception e) {
            handleError(String.format("execute failed: %s", e.getMessage()), e);
        } finally {
            exit();
        }
    }

    protected abstract S openSourceConnector() throws Exception;

    protected abstract T openTargetConnector() throws Exception;

    protected abstract A openAutoCompare(S sourceConnector, T targetConnector) throws Exception;

    protected abstract void initialCompare(S sourceConnector, T targetConnector, A autoCompare) throws Exception;

    protected void initialCompare(S sourceConnector, T targetConnector, A autoCompare, CompareTableItem compareItem) throws Exception {
        logger.info("begin '{}' table initial compare", compareItem.getTableName());
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
                        logger.info("has more target: {}", counts);
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
                            throw new RuntimeException("no support compare result type: " + compareStatus);
                    }
                } while (isRunning() && !isStopping());
            }
        }
        logger.info("'{}' table initial compare completed", compareItem.getTableName());
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

    protected abstract boolean isRunning();

    protected abstract boolean isStopping();

    protected void handleError(String msg, Throwable e) {
        logger.warn(msg, e);
    }

    protected void exit() {
        logger.info("exit");
    }
}
