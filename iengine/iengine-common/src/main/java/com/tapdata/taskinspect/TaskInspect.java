package com.tapdata.taskinspect;

import com.tapdata.constant.Log4jUtil;
import com.tapdata.tm.taskinspect.TaskInspectConfig;
import com.tapdata.tm.taskinspect.TaskInspectMode;
import com.tapdata.tm.taskinspect.TaskInspectUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

/**
 * 任务内校验入口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/17 11:44 Create
 */
@Slf4j
public class TaskInspect implements AutoCloseable {
    private static final long MAX_TIMEOUT = TimeUnit.SECONDS.toMillis(60);
    private final TaskInspectContext context;
    private final IOperator operator;
    @Getter
    private IMode modeJob;

    protected TaskInspect(TaskInspectContext context, IOperator operator) {
        this.context = context;
        this.operator = operator;
        this.modeJob = create(TaskInspectMode.CLOSE, context, operator);
        init();
    }

    private void init() {
        try {
            TaskInspectConfig config = operator.getConfig(context.getTaskId());
            if (null != config) {
                refresh(config);
            }
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("init failed: {}", e.getMessage(), e);
        }
    }

    /**
     * 刷新任务检查配置
     * <p>
     * 该方法用于更新任务检查配置，确保当前的检查模式与配置中指定的模式一致
     * 如果模式不一致，将停止当前模式的检查，并根据新配置的模式重新初始化检查模式
     *
     * @param config 新的任务检查配置
     * @throws InterruptedException 如果在等待模式停止时被中断
     */
    public synchronized void refresh(TaskInspectConfig config) throws InterruptedException {
        if (context.isStopping()) return;
        // 初始化配置，保证取值不异常
        config.init(-1);

        // 获取新的检查模式
        TaskInspectMode mode = config.getMode();

        // 检查当前模式与新配置的模式是否不同
        if (modeJob.getMode() != mode) {
            // 如果模式不同，优雅地停止当前模式的检查，并等待其停止完成
            TaskInspectUtils.stop(() -> modeJob.stop(), MAX_TIMEOUT);
            // 根据新的检查模式创建并初始化新的检查模式实例
            modeJob = create(mode, context, operator);
        }

        // 使用新的配置刷新当前检查模式
        modeJob.refresh(config);
    }

    public boolean stop(boolean force) {
        context.setStop(force);
        return getModeJob().stop();
    }

    public void stop(long timeout) throws InterruptedException {
        TaskInspectUtils.stop(() -> stop(context.isForceStop()), timeout);
    }

    /**
     * 关闭资源释放实例
     * <p>
     * 此方法确保在释放与任务关联的资源时，还从全局实例管理中移除对应的实例
     * 它首先尝试通过调用stop方法来停止任务，然后无论停止操作是否成功，都会从INSTANCES中移除任务的实例
     *
     * @throws Exception 如果关闭过程中有异常发生
     */
    @Override
    public void close() throws Exception {
        log.info("release task-inspect instance...");
        try {
            // 尝试停止任务，使用最大超时时间
            stop(MAX_TIMEOUT);
        } finally {
            // 确保在任何情况下都移除任务实例
            TaskInspectHelper.remove(context.getTaskId());
        }
    }

    /**
     * 根据检查模式创建具体的作业执行策略实例
     * 此方法首先尝试根据提供的模式类名实例化对象如果失败，将尝试使用默认模式进行实例化
     *
     * @param mode    检查模式，包含实现类名
     * @param context 任务检查上下文，传递给构造函数
     * @return IModeJob 实例，用于执行特定模式的检查任务
     */
    private static IMode create(TaskInspectMode mode, TaskInspectContext context, IOperator operator) {
        // 获取模式的具体实现类名
        String className = mode.getImplClassName();
        if (!TaskInspectMode.CLOSE.getImplClassName().equals(className)) {
            try {
                // 加载实现类
                Class<?> clz = Class.forName(className);
                // 获取构造函数，并实例化对象
                Constructor<?> constructor = clz.getConstructor(TaskInspectContext.class, IOperator.class);
                return (IMode) constructor.newInstance(context, operator);
            } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException |
                     InvocationTargetException e) {
                // 如果实例化失败，记录日志，并尝试使用默认模式实例化
                log.warn("'{}' create mode {} failed, use default mode: {}\n {}", context.getTaskId(), className, e.getMessage(), Log4jUtil.getStackString(e));
            }
        }
        return new IMode() {
        };
    }
}
