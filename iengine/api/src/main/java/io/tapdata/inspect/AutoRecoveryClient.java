package io.tapdata.inspect;

import com.tapdata.entity.TapdataRecoveryEvent;
import io.tapdata.utils.EngineHelper;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * 自动修复数据客户端
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/13 19:40 Create
 */
public abstract class AutoRecoveryClient implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(AutoRecoveryClient.class);
    @Getter
    private final String taskId;
    @Getter
    private final String inspectTaskId;
    private final Consumer<TapdataRecoveryEvent> enqueueConsumer;
    private final Consumer<TapdataRecoveryEvent> completedConsumer;

    private String lastErrorManualId; // 控制同个操作编号仅处理一次错误

    protected AutoRecoveryClient(String taskId, String inspectTaskId, Consumer<TapdataRecoveryEvent> enqueueConsumer, Consumer<TapdataRecoveryEvent> completedConsumer) {
        this.taskId = taskId;
        this.inspectTaskId = inspectTaskId;
        this.enqueueConsumer = enqueueConsumer;
        this.completedConsumer = completedConsumer;
        logger.info("Init auto-recovery client: '{}-{}'", getTaskId(), getInspectTaskId());
    }

    public void enqueue(TapdataRecoveryEvent event) {
        enqueueConsumer.accept(event);
    }

    public void completed(TapdataRecoveryEvent event) {
        exportRecoverSql(event);
        completedConsumer.accept(event);
    }

    protected void exportRecoverSql(TapdataRecoveryEvent event) {
        String recoverSqlFile = event.getRecoverySqlFile();
        if (null == recoverSqlFile) return;

        String manualId = event.getManualId();
        if (null == manualId) return;

        try {
            String recoveryType = event.getRecoveryType();
            switch (recoveryType) {
                case TapdataRecoveryEvent.RECOVERY_TYPE_BEGIN:
                    deleteRecoverSqlHistories(recoverSqlFile, manualId);
                    appendRecoverSqlBegin(recoverSqlFile, manualId);
                    break;
                case TapdataRecoveryEvent.RECOVERY_TYPE_DATA:
                    EngineHelper.vfs().append(recoverSqlFile, Collections.singleton(event.getRecoverySql() + ";"));
                    break;
                case TapdataRecoveryEvent.RECOVERY_TYPE_END:
                    appendRecoverSqlEnd(recoverSqlFile);
                    break;
            }
        } catch (Exception e) {
            if (!manualId.equals(lastErrorManualId)) {
                logger.error("manualId: {}, recover sql append failed: {}", manualId, e.getMessage(), e);
                lastErrorManualId = manualId;
            }
        }
    }

    protected void appendRecoverSqlBegin(String filepath, String manualId) throws IOException {
        EngineHelper.vfs().append(filepath, List.of(
            "-- createTime: " + Instant.now().toString(),
            "--     taskId: " + taskId,
            "--   manualId: " + manualId,
            ""
        ));
    }

    protected void deleteRecoverSqlHistories(String filepath, String manualId) throws IOException {
        int deleteTotals = EngineHelper.vfs().deleteFrom3DaysAgo(filepath, true);
        if (deleteTotals > 0) {
            logger.info("manualId: {}, recover sql delete {} files", manualId, deleteTotals);
        }
    }

    protected void appendRecoverSqlEnd(String filepath) throws IOException {
        EngineHelper.vfs().append(filepath, List.of(
            "-- completedTime: " + Instant.now().toString()
        ));
    }
}
