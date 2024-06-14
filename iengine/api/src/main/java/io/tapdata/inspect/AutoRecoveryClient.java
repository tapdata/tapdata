package io.tapdata.inspect;

import com.tapdata.entity.TapdataRecoveryEvent;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    protected AutoRecoveryClient(String taskId, String inspectTaskId, Consumer<TapdataRecoveryEvent> enqueueConsumer, Consumer<TapdataRecoveryEvent> completedConsumer) {
        this.taskId = taskId;
        this.inspectTaskId = inspectTaskId;
        this.enqueueConsumer = enqueueConsumer;
        this.completedConsumer = completedConsumer;
        logger.info("Init auto-recovery client: '{}-{}'", taskId, inspectTaskId);
    }

    public void enqueue(TapdataRecoveryEvent event) {
        enqueueConsumer.accept(event);
    }

    public void completed(TapdataRecoveryEvent event) {
        completedConsumer.accept(event);
    }
}
