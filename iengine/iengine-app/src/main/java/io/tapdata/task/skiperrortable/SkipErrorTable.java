package io.tapdata.task.skiperrortable;

import com.tapdata.entity.SyncStage;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableReportVo;
import com.tapdata.tm.skiperrortable.vo.SkipErrorTableStatusVo;
import io.tapdata.ErrorCodeConfig;
import io.tapdata.ErrorCodeEntity;
import io.tapdata.error.TaskTargetProcessorExCode_15;
import io.tapdata.exception.TapCodeException;
import io.tapdata.observable.logging.ObsLogger;

import java.sql.BatchUpdateException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 跳过错误表实现，只针对复制任务
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/19 11:37 Create
 */
public class SkipErrorTable implements ISkipErrorTable {
    private final String taskId;
    private final ObsLogger obsLogger;
    private final Set<String> skippedSet;
    private final Set<String> recoveringSet;
    private final SkipErrorTableStorage storage;
    private SyncStage syncStage;

    SkipErrorTable(String taskId, ObsLogger obsLogger, SkipErrorTableStorage storage) {
        this.taskId = taskId;
        this.obsLogger = obsLogger;
        this.storage = storage;
        this.skippedSet = new HashSet<>();
        this.recoveringSet = new HashSet<>();
    }

    @Override
    public int getSkipCounts() {
        return skippedSet.size();
    }

    @Override
    public String getTaskId() {
        return this.taskId;
    }

    @Override
    public void setSyncStage(SyncStage syncStage) {
        this.syncStage = syncStage;
    }

    @Override
    public boolean isSkipped(String sourceTableName) {
        return skippedSet.contains(sourceTableName);
    }

    @Override
    public boolean isSkippedOnCompleted(String sourceTableName) {
        if (skippedSet.contains(sourceTableName)) {
            return true;
        }
        if (recoveringSet.contains(sourceTableName)) {
            storage.reportTableRecovered(taskId, sourceTableName);
        }
        return false;
    }

    @Override
    public void checkOnSnapshotCompleted() {
        int skippedSize = skippedSet.size();
        if (skippedSize > 0) {
            try {
                // 5 秒后再报错，等指标上报完成
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException ignore) {
            }
            throw new TapCodeException(TaskTargetProcessorExCode_15.HAS_SKIP_ERROR_TABLE, String.format("%s error tables have been skipped", skippedSize))
                .dynamicDescriptionParameters(skippedSize);
        }
    }

    @Override
    public synchronized boolean skipTable(String sourceTableName, Throwable ex, Supplier<SkipErrorTableReportVo> supplier) {
        // 暂不支持增量阶段的表错误跳过
        if (SyncStage.CDC == syncStage) {
            return false;
        }
        // 已跳过的表，不需要再判断
        if (skippedSet.contains(sourceTableName)) {
            return true;
        }

        // 不可跳过场景：网络异常、连接异常
        // 可跳过场景：特殊格式数据、类型不匹配、字段名不匹配、非空字段限制、字段值超出范围
        boolean doSkip = false;
        if (ex instanceof TapCodeException tapCodeException) {
            // 可跳过 + 不可恢复
            String code = tapCodeException.getCode();
            ErrorCodeEntity errorCode = ErrorCodeConfig.getInstance().getErrorCode(code);
            if (errorCode.isSkippable() && !errorCode.isRecoverable()) {
                doSkip = true;
            }
        } else if (ex instanceof RuntimeException) {
            // 适配未纳入 TapException 的错误
            Throwable cause = ex.getCause();
            if (cause instanceof BatchUpdateException) {
                doSkip = true;
            }
        }

        if (doSkip) {
            skippedSet.add(sourceTableName);
            obsLogger.error("Table '{}' skipped by error: {}", sourceTableName, ex.getMessage(), ex);
            storage.reportTableSkipped(taskId, supplier.get());
            return true;
        }
        return false;
    }

    @Override
    public void initTables(Consumer<SkipErrorTableStatusVo> consumer) {
        List<SkipErrorTableStatusVo> statusList = storage.getAllTableStatus(taskId);
        if (null != statusList) {
            for (SkipErrorTableStatusVo vo : statusList) {
                Optional.ofNullable(vo.getStatus()).ifPresent(status -> {
                    switch (status) {
                        case SKIPPED -> skippedSet.add(vo.getSourceTable());
                        case RECOVERING -> recoveringSet.add(vo.getSourceTable());
                    }
                });
                consumer.accept(vo);
            }
        }
    }
}
