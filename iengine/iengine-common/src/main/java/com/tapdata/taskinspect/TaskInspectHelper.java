package com.tapdata.taskinspect;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务内校验-入口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/17 19:05 Create
 */
@Slf4j
public class TaskInspectHelper {
    private static final Map<String, TaskInspect> INSTANCES = new ConcurrentHashMap<>();
    private static IOperator operator;

    /**
     * 根据任务检查上下文创建任务检查对象
     * 如果已经存在一个实例，会先释放现有的实例，然后创建并返回新的实例
     *
     * @param context 任务检查的上下文信息，包含任务ID等数据
     * @return 返回新创建的任务检查对象
     */
    public static TaskInspect create(TaskInspectContext context) {
        synchronized (INSTANCES) {
            String taskId = context.getTaskId();
            closeWithExists(taskId);

            TaskInspect ins = new TaskInspect(context, getOperator());
            INSTANCES.put(taskId, ins);

            return ins;
        }
    }

    /**
     * 根据任务ID获取任务检查信息
     *
     * @param taskId 任务ID，用于唯一标识一个任务
     * @return 返回对应任务ID的任务检查信息实例，如果不存在则返回null
     */
    public static TaskInspect get(String taskId) {
        return INSTANCES.get(taskId);
    }

    /**
     * 移除任务校验实例
     *
     * @param taskId 任务编号
     */
    static TaskInspect remove(String taskId) {
        return INSTANCES.remove(taskId);
    }

    protected static void closeWithExists(String taskId) {
        // 如果已经存在一个实例，代表流程不正常，需要定位并修复
        TaskInspect ins = remove(taskId);
        if (null != ins) {
            log.warn("'{}' release because exist one instance", taskId);
            try {
                ins.close();
            } catch (Exception e) {
                // 捕获异常，避免影响同步任务流程
                log.error("'{}' release failed: {}", taskId, e.getMessage(), e);
            }
        }
    }

    protected static synchronized IOperator getOperator() {
        if (null == operator) {
            try {
                String className = "com.tapdata.taskinspect.TaskInspectOperator";
                operator = (IOperator) insForName(className);
                log.info("Used task-inspect operator: {}", className);
            } catch (Exception ignore) {
                operator = new IOperator() {
                };
                log.info("Used task-inspect operator: {}", IOperator.class.getName());
            }
        }
        return operator;
    }

    protected static Object insForName(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class<?> clz = Class.forName(className);
        return clz.newInstance();
    }
}
