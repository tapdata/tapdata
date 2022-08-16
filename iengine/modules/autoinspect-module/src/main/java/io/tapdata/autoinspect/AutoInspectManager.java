package io.tapdata.autoinspect;

import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.exception.AutoInspectException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.autoinspect.compare.IAutoCompare;
import io.tapdata.autoinspect.connector.IConnector;
import io.tapdata.autoinspect.status.AutoInspectStatusCtl;
import io.tapdata.autoinspect.status.ISyncStatusCtl;
import io.tapdata.autoinspect.tester.AutoInspectTester;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiFunction;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/8 14:17 Create
 */
public class AutoInspectManager {
    private static final Logger logger = LogManager.getLogger(AutoInspectManager.class);
    private static final ExecutorService EXECUTORS = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
            60L, TimeUnit.SECONDS, new SynchronousQueue<>());

    private static final Map<String, AutoInspectStatusCtl> INSTANCES = new ConcurrentHashMap<>();

    public static ISyncStatusCtl get(TaskDto task) {
        if (null != task && null != task.getId()) {
            return INSTANCES.get(task.getId().toHexString());
        }
        return null;
    }

    public static <T> T call(TaskDto task, BiFunction<TaskDto, ISyncStatusCtl, T> callFn) {
        if (null != task && null != task.getId()) {
            AutoInspectStatusCtl ctl = INSTANCES.get(task.getId().toHexString());
            if (null != ctl) {
                return callFn.apply(task, ctl);
            }
        }
        return null;
    }

    public static ISyncStatusCtl start(ClientMongoOperator clientMongoOperator, TaskDto task) {
        if (!task.isAutoInspect()) return null;

        String taskId = task.getId().toHexString();
        return INSTANCES.compute(taskId, (s, ctl) -> {
            if (null != ctl) {
                ctl.syncError("last not call exit");
                throw AutoInspectException.existRunner(taskId, ctl.getSyncStatus(), ctl.getInspectStatus());
            }
            try {
                logger.info("init and start inspect task");
                ctl = AutoInspectStatusCtl.createByTask(task);
                EXECUTORS.submit(new AutoInspectPdkTask(ctl, task, clientMongoOperator, context -> {
                    INSTANCES.remove(context.getTaskId());
                    return true;
                }));
                return ctl;
            } catch (Exception e) {
                throw AutoInspectException.startError(taskId, e);
            }
        });
    }

    public static ISyncStatusCtl start(TaskDto task) {
        if (!task.isAutoInspect()) return null;

        String taskId = task.getId().toHexString();
        return INSTANCES.compute(taskId, (s, ctl) -> {
            if (null != ctl) {
                throw AutoInspectException.existRunner(taskId, ctl.getSyncStatus(), ctl.getInspectStatus());
            }
            try {
                logger.info("init and start inspect task");
                ctl = AutoInspectStatusCtl.createByTask(task);
                AutoInspectTask taskRunner = new AutoInspectTask(ctl, context -> {
                    INSTANCES.remove(context.getTaskId());
                    return true;
                }) {
                    @Override
                    protected IConnector openSourceConnector() {
                        return AutoInspectTester.sourceConnector();
                    }

                    @Override
                    protected IConnector openTargetConnector() {
                        return AutoInspectTester.targetConnector();
                    }

                    @Override
                    protected IAutoCompare openAutoCompare(IConnector sourceConnector, IConnector targetConnector) {
                        return AutoInspectTester.autoCompare();
                    }
                };
                EXECUTORS.submit(taskRunner);
                return ctl;
            } catch (Exception e) {
                throw AutoInspectException.startError(taskId, e);
            }
        });
    }

}
