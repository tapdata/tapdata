package com.tapdata.taskinspect;

import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.taskinspect.TaskInspectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务内校验-入口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/17 19:05 Create
 */
public class TaskInspectHelper {
    private static final Logger log = LogManager.getLogger(TaskInspectHelper.class);

    private static final Map<String, ITaskInspect> INSTANCES = new ConcurrentHashMap<>();
    private static String CLASS_NAME = "com.tapdata.taskinspect.TaskInspect";

    public static ITaskInspect create(TaskDto task, ClientMongoOperator clientMongoOperator) {
        String taskId = Optional.ofNullable(task.getId()).map(ObjectId::toHexString).orElse(null);
        if (null == taskId) {
            return null;
        } else if (isIgnoreTaskSyncType(task.getSyncType())) {
            return null;
        } else if (Optional.ofNullable(task.getDag())
            .map(DAG::getSourceNodes)
            .map(List::size)
            .orElse(0) > 1) {
            return null; // 多源节点的任务不支持
        }

        synchronized (INSTANCES) {
            // 如果已经存在一个实例，代表流程不正常，需要定位并修复
            closeWithExists(taskId);

            if (!(clientMongoOperator instanceof HttpClientMongoOperator)) {
                return null;
            }

            if (null == CLASS_NAME) {
                return null;
            }

            ITaskInspect ins = null;
            try {
                Class<?> clz = Class.forName(CLASS_NAME);
                if (ITaskInspect.class.isAssignableFrom(clz)) {
                    Constructor<ITaskInspect> constructor = ((Class<ITaskInspect>) clz).getConstructor(TaskDto.class, HttpClientMongoOperator.class);
                    ins = constructor.newInstance(task, clientMongoOperator);
                    INSTANCES.put(taskId, ins);
                } else {
                    log.warn("{} not instance class by '{}'", TaskInspectUtils.MODULE_NAME, CLASS_NAME);
                    CLASS_NAME = null;
                }
            } catch (ClassNotFoundException e) {
                log.warn("{} not found class by '{}'", TaskInspectUtils.MODULE_NAME, CLASS_NAME, e);
                CLASS_NAME = null;
            } catch (NoSuchMethodException e) {
                log.warn("{} not found constructor by '{}'", TaskInspectUtils.MODULE_NAME, CLASS_NAME, e);
                CLASS_NAME = null;
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                log.warn("{} not create instance by '{}'", TaskInspectUtils.MODULE_NAME, CLASS_NAME, e);
//                CLASS_NAME = null;
            }

            return ins;
        }
    }

    /**
     * 根据任务ID获取任务检查信息
     *
     * @param taskId 任务ID，用于唯一标识一个任务
     * @return 返回对应任务ID的任务检查信息实例，如果不存在则返回null
     */
    public static ITaskInspect get(String taskId) {
        if (null == taskId) {
            return null;
        }
        return INSTANCES.get(taskId);
    }

    /**
     * 移除任务校验实例
     *
     * @param taskId 任务编号
     */
    static ITaskInspect remove(String taskId) {
        if (null == taskId) {
            return null;
        }
        return INSTANCES.remove(taskId);
    }

    protected static boolean isIgnoreTaskSyncType(String taskSyncType) {
        if (null != taskSyncType) {
            return switch (taskSyncType) {
                case TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC -> false;
                default -> true;
            };
        }
        return true;
    }

    protected static void closeWithExists(String taskId) {
        ITaskInspect ins = remove(taskId);
        if (null == ins) {
            return;
        }

        log.warn("{} '{}' release because exist one instance", TaskInspectUtils.MODULE_NAME, taskId);
        try {
            ins.close();
        } catch (Exception e) {
            // 捕获异常，避免影响同步任务流程
            log.error("{} '{}' release failed: {}", TaskInspectUtils.MODULE_NAME, taskId, e.getMessage(), e);
        }
    }
}
