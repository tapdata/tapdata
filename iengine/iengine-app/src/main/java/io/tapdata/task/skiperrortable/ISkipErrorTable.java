package io.tapdata.task.skiperrortable;

import com.tapdata.entity.SyncStage;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableReportVo;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableStatusVo;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import org.bson.types.ObjectId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 跳过错误表-接口
 * <p>
 * 该接口定义了处理数据迁移过程中跳过错误表的相关功能。提供了创建、获取实例以及判断和跳过错误表的方法。
 * 主要用于在数据同步任务中处理特定表的错误情况，避免因单个表的问题导致整个任务失败。
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/19 14:42 Create
 */
public interface ISkipErrorTable extends AutoCloseable {
    /**
     * 空对象，未开启时使用此对象对接
     */
    ISkipErrorTable EMPTY = new ISkipErrorTable() {
    };

    /**
     * 存放所有任务实例
     */
    Map<String, ISkipErrorTable> INSTANCES = new HashMap<>();

    /**
     * 根据任务信息创建跳过错误表实例
     *
     * @param taskDto 任务信息
     * @return 跳过错误表实例，如果任务不符合条件则返回空实例
     */
    static ISkipErrorTable create(TaskDto taskDto, ClientMongoOperator clientMongoOperator) {
        if (null != taskDto) {
            boolean enable = Optional.of(taskDto)
                .map(dto -> Boolean.TRUE.equals(dto.getEnableSkipErrorTable()) ? dto : null)   // 禁用，没开启功能
                .map(dto -> TaskDto.SYNC_TYPE_MIGRATE.equals(dto.getSyncType()) ? dto : null)  // 禁用，非迁移任务
                .map(dto -> dto.isTestTask() ? null : dto)                                     // 禁用，测试任务
                .map(dto -> dto.isPreviewTask() ? null : dto)                                  // 禁用，预览任务
                .map(dto -> Optional.ofNullable(dto.getSkipErrorEvent())
                    .map(TaskDto.SkipErrorEvent::getErrorModeEnum)
                    .map(mode -> TaskDto.SkipErrorEvent.ErrorMode.Disable == mode ? dto : null)
                    .orElse(dto))                                                                      // 禁用，开启了跳过错误数据功能
                .isPresent();                                                                          // 启用

            if (enable && clientMongoOperator instanceof HttpClientMongoOperator mongoOperator) {
                String taskId = Optional.of(taskDto).map(TaskDto::getId).map(ObjectId::toHexString).orElse(null);
                synchronized (INSTANCES) {
                    ObsLogger obsLogger = ObsLoggerFactory.getInstance().getObsLogger(taskDto);
                    ISkipErrorTable instance = INSTANCES.get(taskId);
                    if (null != instance) {
                        obsLogger.warn("Instance already exists, taskId '{}'", taskId);
                        return instance;
                    }

                    SkipErrorTableStorage storage = new SkipErrorTableStorage(mongoOperator);
                    instance = new SkipErrorTable(taskId, obsLogger, storage);
                    INSTANCES.put(taskId, instance);
                    return instance;
                }
            }
        }

        return ISkipErrorTable.EMPTY;
    }

    /**
     * 根据任务ID获取跳过错误表实例
     *
     * @param taskId 任务ID
     * @return 对应的跳过错误表实例，如果不存在则返回空实例
     */
    static ISkipErrorTable get(String taskId) {
        if (null == taskId) {
            return EMPTY;
        }
        synchronized (INSTANCES) {
            return INSTANCES.getOrDefault(taskId, EMPTY);
        }
    }

    default int getSkipCounts() {
        return 0;
    }

    default String getTaskId() {
        return null;
    }

    default void setSyncStage(SyncStage syncStage) {
    }

    /**
     * 判断指定源表是否已被跳过
     *
     * @param sourceTableName 源表名称
     * @return 如果该表已被跳过返回true，否则返回false
     */
    default boolean isSkipped(String sourceTableName) {
        return false;
    }

    /**
     * 表完成同步时，判断是否被跳过
     *
     * @param sourceTableName 表名
     * @return 是否被跳过
     */
    default boolean isSkippedOnCompleted(String sourceTableName) {
        return false;
    }

    /**
     * 全量同步完成时，如果有跳过错误表，则抛出异常
     */
    default void checkOnSnapshotCompleted() {
    }

    /**
     * 初始化错误表状态
     *
     * @param consumer FE 中对跳过表进行特殊配置
     */
    default void initTables(Consumer<SkipErrorTableStatusVo> consumer) {
    }

    /**
     * 跳过指定源表
     *
     * @param sourceTableName 源表名称
     * @param ex              导致跳过的异常
     * @return 如果成功跳过返回true，否则返回false
     */
    default boolean skipTable(String sourceTableName, Throwable ex, Supplier<SkipErrorTableReportVo> supplier) {
        return false;
    }

    /**
     * 从异常链中获取SkipErrorTableException异常
     *
     * @param e 原始异常
     * @return SkipErrorTableException实例，如果不存在则返回null
     */
    default SkipErrorTableException getSkipErrorTableException(Throwable e) {
        Throwable cause = e;
        while (null != cause) {
            if (cause instanceof SkipErrorTableException ex) {
                return ex;
            }
            cause = cause.getCause();
        }
        return null;
    }

    @Override
    default void close() throws Exception {
        String taskId = getTaskId();
        if (null == taskId) {
            return;
        }
        synchronized (INSTANCES) {
            INSTANCES.remove(taskId);
        }
    }
}
