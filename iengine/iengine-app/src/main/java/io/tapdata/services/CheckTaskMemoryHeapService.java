package io.tapdata.services;

import com.tapdata.tm.commons.task.dto.CheckTaskMemoryParam;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.service.PdkCountEntity;
import io.tapdata.service.PdkCountReadType;
import io.tapdata.service.skeleton.annotation.RemoteService;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.tapdata.tm.commons.task.dto.CheckTaskMemoryResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

@RemoteService
public class CheckTaskMemoryHeapService {
    /**
     * JVM 堆内存安全水位 (K值)
     * 建议保持在 0.9 (90%)，不要太高，给 GC 留空间。
     */
    private static final double HEAP_UTILIZATION_LIMIT = 0.9;
    private static final long JVM_BLOAT_FACTOR = 5;
    private static final double QUEUE_OVERHEAD_FACTOR = 0.8;

    public CheckTaskMemoryResult checkTaskMemoryHeap(List<CheckTaskMemoryParam> checkTaskMemoryParams) {
        if (CollectionUtils.isEmpty(checkTaskMemoryParams)) {
            return null;
        }
        System.gc();
        MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long heapCapacity = resolveHeapCapacity(heapMemoryUsage);
        long heapUsed = Math.max(0L, heapMemoryUsage.getUsed());
        long realFree = Math.max(0L, heapCapacity - heapUsed);

        double heapUtilizationLimit = normalizeHeapUtilizationLimit(
                stringToDouble(CommonUtils.getProperty("HEAP_UTILIZATION_LIMIT"), HEAP_UTILIZATION_LIMIT)
        );
        // 安全阈值 = 安全水位允许上限 - 当前已用堆内存
        double safeThreshold = Math.max(0D, heapCapacity * heapUtilizationLimit - heapUsed);
        long jvmBloatFactor = Math.max(1L, CommonUtils.getPropertyLong("JVM_BLOAT_FACTOR", JVM_BLOAT_FACTOR));
        double queueOverheadFactor = normalizePositiveFactor(
                stringToDouble(CommonUtils.getProperty("QUEUE_OVERHEAD_FACTOR"), QUEUE_OVERHEAD_FACTOR),
                QUEUE_OVERHEAD_FACTOR
        );
        long configuredQueueSize = Math.max(
                1L,
                CommonUtils.getPropertyInt(
                        HazelcastTaskService.JET_EDGE_QUEUE_SIZE_PROP_KEY,
                        HazelcastTaskService.DEFAULT_JET_EDGE_QUEUE_SIZE
                )
        );
        PdkCountService pdkCountService = new PdkCountService();

        double totalEstimatedWithOverhead = 0D;
        RiskDetail maxRiskTable = null;
        for (CheckTaskMemoryParam checkTaskMemoryParam : checkTaskMemoryParams) {
            if (checkTaskMemoryParam == null || MapUtils.isEmpty(checkTaskMemoryParam.getTableMap())) {
                continue;
            }
            long inFlightUpper = calculateInFlightUpper(checkTaskMemoryParam, configuredQueueSize);
            if (inFlightUpper <= 0) {
                continue;
            }

            RiskDetail sourceRisk = estimateSourceRisk(
                    checkTaskMemoryParam,
                    inFlightUpper,
                    jvmBloatFactor,
                    queueOverheadFactor,
                    pdkCountService
            );
            if (sourceRisk == null) {
                continue;
            }
            totalEstimatedWithOverhead += sourceRisk.getEstimatedWithOverhead();
            if (maxRiskTable == null || sourceRisk.getEstimatedWithOverhead() > maxRiskTable.getEstimatedWithOverhead()) {
                maxRiskTable = sourceRisk;
            }
        }
        if (maxRiskTable == null || totalEstimatedWithOverhead <= 0D) {
            return null;
        }
        return CheckTaskMemoryResult.create(
                totalEstimatedWithOverhead,
                safeThreshold,
                realFree,
                maxRiskTable.getTableName(),
                maxRiskTable.getAvgSize(),
                maxRiskTable.getInFlightEffective(),
                maxRiskTable.getBatchSize()
        );
    }

    private RiskDetail estimateSourceRisk(CheckTaskMemoryParam checkTaskMemoryParam,
                                          long inFlightUpper,
                                          long jvmBloatFactor,
                                          double queueOverheadFactor,
                                          PdkCountService pdkCountService) {
        RiskDetail sourceRisk = null;
        for (Map.Entry<String, Long> entry : checkTaskMemoryParam.getTableMap().entrySet()) {
            if (StringUtils.isBlank(entry.getKey())) {
                continue;
            }
            PdkCountEntity pdkCountEntity = pdkCountService.count(
                    checkTaskMemoryParam.getConnectionId(),
                    entry.getKey(),
                    PdkCountReadType.info.name()
            );
            if (!Objects.equals(pdkCountEntity.getCode(), 200)) {
                throw new RuntimeException(StringUtils.defaultIfBlank(pdkCountEntity.getErrorMsg(), "Check table memory failed"));
            }
            long rowCount = Math.max(0L, pdkCountEntity.getRows());
            if (rowCount <= 0) {
                continue;
            }

            long fallbackAvgSize = Math.max(0L, entry.getValue() == null ? 0L : entry.getValue());
            long avgObjSize = pdkCountEntity.getAvgObjSize() != null && pdkCountEntity.getAvgObjSize() > 0
                    ? pdkCountEntity.getAvgObjSize()
                    : fallbackAvgSize;
            if (avgObjSize <= 0) {
                continue;
            }

            long inFlightEffective = Math.min(inFlightUpper, rowCount);
            double estimatedPayload = avgObjSize * 1D * jvmBloatFactor * inFlightEffective * queueOverheadFactor;
            if (sourceRisk == null || estimatedPayload > sourceRisk.getEstimatedWithOverhead()) {
                sourceRisk = new RiskDetail(
                        entry.getKey(),
                        avgObjSize,
                        inFlightEffective,
                        Math.max(0L, checkTaskMemoryParam.getBatchSize()),
                        estimatedPayload
                );
            }
        }
        return sourceRisk;
    }

    protected long calculateInFlightUpper(CheckTaskMemoryParam checkTaskMemoryParam, long configuredQueueSize) {
        long batchSize = Math.max(0L, checkTaskMemoryParam.getBatchSize());
        long writeBatchSize = Math.max(0L, checkTaskMemoryParam.getWriteBatchSize());
        long edgeCount = Math.max(0, checkTaskMemoryParam.getNodeSize());
        long queueSize = Math.max(configuredQueueSize, batchSize);
        long edgeItems = safeMultiply(edgeCount, queueSize);
        long readAndBuffer = safeAdd(safeMultiply(batchSize, 2), writeBatchSize);
        return safeAdd(edgeItems, readAndBuffer);
    }

    protected long resolveHeapCapacity(MemoryUsage heapMemoryUsage) {
        long max = heapMemoryUsage.getMax();
        if (max > 0) {
            return max;
        }
        return Math.max(0L, heapMemoryUsage.getCommitted());
    }

    protected double normalizeHeapUtilizationLimit(double limit) {
        if (Double.isNaN(limit) || Double.isInfinite(limit) || limit <= 0D) {
            return HEAP_UTILIZATION_LIMIT;
        }
        return Math.min(limit, 1D);
    }

    protected double normalizePositiveFactor(double factor, double defaultValue) {
        if (Double.isNaN(factor) || Double.isInfinite(factor) || factor <= 0D) {
            return defaultValue;
        }
        return factor;
    }

    protected long safeAdd(long left, long right) {
        if (left <= 0) {
            return Math.max(0L, right);
        }
        if (right <= 0) {
            return left;
        }
        if (Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    protected long safeMultiply(long left, long right) {
        if (left <= 0 || right <= 0) {
            return 0L;
        }
        if (Long.MAX_VALUE / left < right) {
            return Long.MAX_VALUE;
        }
        return left * right;
    }

    public static class RiskDetail {
        private final String tableName;
        private final long avgSize;
        private final long inFlightEffective;
        private final long batchSize;
        private final double estimatedWithOverhead;

        private RiskDetail(String tableName, long avgSize, long inFlightEffective, long batchSize, double estimatedWithOverhead) {
            this.tableName = tableName;
            this.avgSize = avgSize;
            this.inFlightEffective = inFlightEffective;
            this.batchSize = batchSize;
            this.estimatedWithOverhead = estimatedWithOverhead;
        }

        private String getTableName() {
            return tableName;
        }

        private long getAvgSize() {
            return avgSize;
        }

        private long getInFlightEffective() {
            return inFlightEffective;
        }

        private long getBatchSize() {
            return batchSize;
        }

        private double getEstimatedWithOverhead() {
            return estimatedWithOverhead;
        }
    }

    protected double stringToDouble(String value, double defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }


}
