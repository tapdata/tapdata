package io.tapdata.autoinspect.tester.samples;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.autoinspect.AutoInspectManager;
import com.tapdata.tm.autoinspect.constants.Constants;
import io.tapdata.autoinspect.status.ISyncStatusCtl;
import io.tapdata.autoinspect.tester.AutoInspectTester;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/8 15:35 Create
 */
public class SyncSampleTask implements Runnable {
    private static final Logger logger = LogManager.getLogger(SyncSampleTask.class);

    private final TaskDto task;
    private final String taskId;

    public SyncSampleTask(TaskDto task) {
        this.task = task;
        this.taskId = task.getId().toHexString();
    }

    @Override
    public void run() {
        ISyncStatusCtl statusCtl = AutoInspectManager.start(task);
        if (null == statusCtl) return;
        try {
            Thread.currentThread().setName(String.format("th-%s-%s-sync", Constants.MODULE_NAME, taskId));

            statusCtl.syncInitialing();
            startInitial();
            statusCtl.syncInitialized();

            startIncremental();
            statusCtl.syncDone();
        } catch (Exception e) {
            logger.warn("execute sync task failed: {}", e.getMessage(), e);
            statusCtl.syncError(e.getMessage());
        } finally {
            logger.info("exit");
        }
    }

    protected void startInitial() throws Exception {
        logger.info("begin initial");

        long processTimes = AutoInspectTester.randomTimes();
        Thread.sleep(processTimes);
        logger.info("initial use {} ms", processTimes);
    }

    protected void startIncremental() throws Exception {
        logger.info("begin incremental");

        long processTimes = AutoInspectTester.randomTimes();
        Thread.sleep(processTimes);
        logger.info("incremental use {} ms", processTimes);
    }

}
