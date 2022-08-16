package io.tapdata.autoinspect.status;

import com.tapdata.tm.autoinspect.constants.AutoInspectTaskStatus;
import com.tapdata.tm.autoinspect.constants.AutoInspectTaskType;
import com.tapdata.tm.autoinspect.exception.AutoInspectException;
import com.tapdata.tm.autoinspect.exception.AutoInspectTaskStatusException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.autoinspect.entity.CompareItem;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/8 14:21 Create
 */
@Setter(AccessLevel.PRIVATE)
@Getter
public class AutoInspectStatusCtl implements Serializable, ISyncStatusCtl {
    private static final Logger logger = LogManager.getLogger(AutoInspectStatusCtl.class);

    private final @NonNull String taskId;
    private final @NonNull List<CompareItem> tables;
    private final @NonNull AutoInspectTaskType syncType;
    private @NonNull AutoInspectTaskStatus.Sync syncStatus;
    private long syncUpdated;
    private String syncError;

    private @NonNull AutoInspectTaskStatus.Inspect inspectStatus;
    private long inspectUpdated;
    private String inspectError;

    public AutoInspectStatusCtl(@NonNull String taskId, @NonNull AutoInspectTaskType syncType) {
        this.taskId = taskId;
        this.tables = new ArrayList<>();
        this.syncType = syncType;
        this.syncStatus = AutoInspectTaskStatus.Sync.Scheduling;
        this.syncUpdated = System.currentTimeMillis();
        this.inspectStatus = AutoInspectTaskStatus.Inspect.Scheduling;
        this.inspectUpdated = System.currentTimeMillis();
    }

    private void setSyncStatus(@NonNull AutoInspectTaskStatus.Sync status) {
        if (AutoInspectTaskStatus.Sync.Error != status && AutoInspectTaskStatus.Inspect.Error.in(getInspectStatus())) {
            AutoInspectTaskStatusException.notChangeWithInspectError(status, getInspectError());
        }
        logger.info("Change sync status {} to {}", getSyncStatus(), status);
        this.syncStatus = status;
    }

    private void setInspectStatus(@NonNull AutoInspectTaskStatus.Inspect status) {
        logger.info("Change inspect status {} to {}", getInspectStatus(), status);
        this.inspectStatus = status;
    }

    @Override
    public synchronized void syncInitialized() {
        setSyncStatus(AutoInspectTaskStatus.Sync.Initialized);
    }

    @Override
    public synchronized void syncError(String error) {
        setSyncStatus(AutoInspectTaskStatus.Sync.Error);
        setSyncError(error);
    }

    @Override
    public synchronized void syncInitialing() {
        setSyncStatus(AutoInspectTaskStatus.Sync.Initialing);
    }

    @Override
    public synchronized void syncIncremental() {
        setSyncStatus(AutoInspectTaskStatus.Sync.Incremental);
    }

    @Override
    public synchronized void syncDone() {
        setSyncStatus(AutoInspectTaskStatus.Sync.Done);
    }

    public synchronized void inspectIncrementing() {
        setInspectStatus(AutoInspectTaskStatus.Inspect.Incrementing);
    }

    public synchronized void inspectStopping() {
        switch (getInspectStatus()) {
            case Stopping:
            case Error:
            case Done:
                return;
            default:
                break;
        }
        setInspectStatus(AutoInspectTaskStatus.Inspect.Stopping);
    }

    public synchronized void inspectError(String error) {
        setInspectStatus(AutoInspectTaskStatus.Inspect.Error);
        setInspectError(error);
    }

    public synchronized void inspectDone() {
        setInspectStatus(AutoInspectTaskStatus.Inspect.Done);
    }

    public synchronized void inspectInitialing() {
        setInspectStatus(AutoInspectTaskStatus.Inspect.Initialing);
    }

    public void waitSyncInitialed() throws InterruptedException {
        synchronized (this) {
            setInspectStatus(AutoInspectTaskStatus.Inspect.WaitSyncInitialed);
        }

        AutoInspectTaskStatus.Sync status;
        while (!Thread.interrupted()) {
            status = getSyncStatus();
            if (AutoInspectTaskStatus.Sync.Error == status) {
                throw AutoInspectTaskStatusException.notSyncError(syncError);
            } else if (status.ordinal() > AutoInspectTaskStatus.Sync.Initialized.ordinal()) {
                break;
            }
            logger.info("wait sync initialed: {}", status);
            Thread.sleep(1000L);
        }
    }

    public boolean isAllDone() {
        AutoInspectTaskStatusException.notInspectError(inspectStatus, inspectError);
        if (AutoInspectTaskStatus.Sync.Error == getSyncStatus()) {
            throw AutoInspectTaskStatusException.notSyncError(syncError);
        }

        return syncStatus == AutoInspectTaskStatus.Sync.Done
                && inspectStatus == AutoInspectTaskStatus.Inspect.Done;
    }

    @Override
    public void waitExit(long timeouts) throws InterruptedException, TimeoutException {
        inspectStopping();

        long begin = System.currentTimeMillis();
        while (!isAllDone()) {
            if (System.currentTimeMillis() - begin > timeouts) {
                if (!getInspectStatus().in(AutoInspectTaskStatus.Inspect.Error, AutoInspectTaskStatus.Inspect.Done)) {
                    throw new TimeoutException("wait exit timeouts: " + timeouts + ", sync:" + getSyncStatus() + ", inspect" + getInspectStatus());
                }
                break;
            }

            logger.info("wait all exit [Sync:{}, Inspect:{}]", getSyncStatus(), getInspectStatus());
            Thread.sleep(1000L);
        }
    }

    public static AutoInspectStatusCtl createByTask(TaskDto task) {
        String taskId = task.getId().toHexString();
        AutoInspectTaskType autoInspectTaskType = AutoInspectTaskType.parseByTaskSyncType(task.getType());

        AutoInspectStatusCtl statusCtl = new AutoInspectStatusCtl(taskId, autoInspectTaskType);
        if (statusCtl.getTables().isEmpty()) {
            DatabaseNode node = Optional.ofNullable(task.getDag()).map(DAG::getSourceNode).map(databaseNodes -> {
                if (databaseNodes.isEmpty()) {
                    return null;
                }
                return databaseNodes.get(0);
            }).orElse(null);
            if (null == node) throw AutoInspectException.notSourceNode(taskId);

            Optional.ofNullable(node.getTableNames()).ifPresent(names -> {
                for (String n : names) {
                    statusCtl.getTables().add(new CompareItem(n));
                }
            });
            if (statusCtl.getTables().isEmpty()) {
                throw AutoInspectException.notEmptyTables(taskId);
            }
        }
        return statusCtl;
    }
}
