package io.tapdata.inspect;

import com.tapdata.entity.TapdataRecoveryEvent;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * 自动修复数据客户端
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/13 19:40 Create
 */
public abstract class AutoRecoveryClient implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(AutoRecoveryClient.class);
    private static final String EXPORT_SQL = "exportSql";
    @Getter
    private final String taskId;
    @Getter
    private final String inspectTaskId;
    private final Consumer<TapdataRecoveryEvent> enqueueConsumer;
    private final Consumer<TapdataRecoveryEvent> completedConsumer;

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
        completedConsumer.accept(event);
    }
    public void exportRecoverySql(String fileName,TapdataRecoveryEvent event) {
        String filePath = EXPORT_SQL + File.separator + fileName;
        File file = new File(filePath);
        try {
            File parentDir = file.getParentFile();
            if (parentDir != null && !parentDir.exists() && !parentDir.mkdirs()) {
                logger.error("Create directory error: {}", parentDir.getAbsolutePath());
                return;
            }
            if (!file.exists() && !file.createNewFile()) {
                logger.error("Create file error: {}", fileName);
                return;
            }
            if(StringUtils.isNotBlank(event.getRecoverySql())) {
                FileUtils.writeStringToFile(file, event.getRecoverySql() + System.lineSeparator(), StandardCharsets.UTF_8, true);
            }
        } catch (Exception e) {
            logger.error("Export recovery sql error: {}", e.getMessage());
        }

    }
}
