package io.tapdata.inspect;

import com.tapdata.entity.TapdataRecoveryEvent;
import io.tapdata.exception.AutoRecoveryException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 数据修复接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/5 18:50 Create
 */
public class AutoRecovery implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(AutoRecovery.class);
    private static final Map<String, AutoRecovery> instances = new HashMap<>();

    private final String taskId;
    private Consumer<TapdataRecoveryEvent> enqueueConsumer;
    private Consumer<TapdataRecoveryEvent> completedConsumer;

    protected AutoRecovery(String taskId) {
        this.taskId = taskId;
    }

    public void setEnqueueConsumer(Consumer<TapdataRecoveryEvent> enqueueConsumer) {
        this.enqueueConsumer = enqueueConsumer;
        logger.info("Set auto recovery enqueue consumer '{}': {}", taskId, enqueueConsumer);
    }

    public void setCompletedConsumer(Consumer<TapdataRecoveryEvent> completedConsumer) {
        this.completedConsumer = completedConsumer;
        logger.info("Set auto recovery completed consumer '{}': {}", taskId, completedConsumer);
    }

    public void enqueue(TapdataRecoveryEvent event) {
        if (null != enqueueConsumer) {
            enqueueConsumer.accept(event);
        }
    }

    public void completed(TapdataRecoveryEvent event) {
        if (null != completedConsumer) {
            completedConsumer.accept(event);
        }
    }

    @Override
    public void close() throws Exception {
        logger.info("Releasing auto recovery instance {}", taskId);
        instances.remove(taskId);
    }

    public static AutoRecovery init(String taskId) {
        return instances.compute(taskId, (id, ins) -> {
            if (null != ins) {
                throw AutoRecoveryException.instanceExists(id);
            }
            logger.info("Init auto recovery instance {}", taskId);
            return new AutoRecovery(taskId);
        });
    }

    public static AutoRecovery get(String taskId) {
        return instances.computeIfAbsent(taskId, id -> {
            throw AutoRecoveryException.notFoundInstance(id);
        });
    }

    public static void computeIfPresent(String taskId, Consumer<AutoRecovery> consumer) {
        AutoRecovery ins = instances.get(taskId);
        if (null != ins) {
            consumer.accept(ins);
        }
    }
}
