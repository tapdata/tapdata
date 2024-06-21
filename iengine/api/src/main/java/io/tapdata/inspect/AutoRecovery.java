package io.tapdata.inspect;

import com.tapdata.entity.TapdataRecoveryEvent;
import io.tapdata.inspect.exception.DuplicateAutoRecoveryException;
import io.tapdata.inspect.exception.DuplicateClientAutoRecoveryException;
import io.tapdata.inspect.exception.NotfoundAutoRecoveryException;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * 数据修复操作入口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/5 18:50 Create
 */
public class AutoRecovery implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(AutoRecovery.class);
    private static final Map<String, AutoRecovery> instances = new HashMap<>();

    @Getter
    private final String taskId;
    private final Map<String, AutoRecoveryClient> clients = new ConcurrentHashMap<>();
    private Consumer<TapdataRecoveryEvent> enqueueConsumer;

    protected AutoRecovery(String taskId) {
        this.taskId = taskId;
    }

    @Override
    @SuppressWarnings("resource")
    public void close() throws Exception {
        logger.info("Releasing auto-recovery instance '{}'", getTaskId());
        instances.remove(getTaskId());
    }

    public static AutoRecovery init(String taskId) {
        return instances.compute(taskId, (id, ins) -> {
            DuplicateAutoRecoveryException.assertNull(ins);
            logger.info("Init auto-recovery instance '{}'", taskId);
            return new AutoRecovery(taskId);
        });
    }

    @SuppressWarnings("resource")
    public static AutoRecoveryClient initClient(String taskId, String inspectTaskId, Consumer<TapdataRecoveryEvent> completedConsumer) {
        AutoRecovery autoRecovery = instances.computeIfAbsent(taskId, NotfoundAutoRecoveryException::failed);
        return autoRecovery.clients.compute(inspectTaskId, (id, client) -> {
            DuplicateClientAutoRecoveryException.assertNull(client);
            return new AutoRecoveryClient(taskId, id, autoRecovery.enqueueConsumer, completedConsumer) {
                @Override
                public void close() throws Exception {
                    autoRecovery.clients.remove(id);
                }
            };
        });
    }

    @SuppressWarnings("resource")
    public static void setEnqueueConsumer(String taskId, Consumer<TapdataRecoveryEvent> enqueueConsumer) {
        instances.computeIfPresent(taskId, (id, ins) -> {
            logger.info("Set auto-recovery enqueue: '{}-{}'", taskId, id);
            ins.enqueueConsumer = enqueueConsumer;
            return ins;
        });
    }

    @SuppressWarnings("resource")
    public static void completed(String taskId, TapdataRecoveryEvent event) {
        instances.computeIfPresent(taskId, (id, ins) -> {
            ins.clients.computeIfPresent(event.getInspectTaskId(), (inspectTaskId, autoRecoveryClient) -> {
                autoRecoveryClient.completed(event);
                return autoRecoveryClient;
            });
            return ins;
        });
    }
}
