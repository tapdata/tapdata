package io.tapdata.observable.metric.throttle;

import com.tapdata.tm.commons.throttle.DmlType;
import com.tapdata.tm.commons.throttle.SampleQuality;
import com.tapdata.tm.commons.throttle.ThrottleConstants;
import com.tapdata.tm.commons.throttle.ThrottleDirection;
import com.tapdata.tm.commons.throttle.ThrottlePhase;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Unified batch metering service covering both FULL (first-event per endpoint+table) and CDC (table x DML cache).
 */
public final class ContinuousBatchMeter {

    private final String taskId;
    private final long runEpoch;
    private final ThrottleDirection direction;
    private final EventCostMeter eventCostMeter;
    private final CdcTableDmlSampleCache cdcSampleCache;
    private final ConcurrentHashMap<String, AtomicLong> tableSchemaGenerations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> endpointGlobalGenerations = new ConcurrentHashMap<>();

    public ContinuousBatchMeter(
            String taskId,
            long runEpoch,
            ThrottleDirection direction,
            EventCostMeter eventCostMeter,
            CdcTableDmlSampleCache cdcSampleCache) {
        this.taskId = taskId != null ? taskId : "";
        this.runEpoch = runEpoch;
        if (direction == null || eventCostMeter == null || cdcSampleCache == null) {
            throw new IllegalArgumentException("ContinuousBatchMeter requires non-null direction, meter, and cache");
        }
        this.direction = direction;
        this.eventCostMeter = eventCostMeter;
        this.cdcSampleCache = cdcSampleCache;
    }

    public CdcTableDmlSampleCache getCdcSampleCache() {
        return cdcSampleCache;
    }

    public BatchMeteringResult meterFullBatch(
            String endpointId,
            List<? extends TapEvent> events,
            Function<TapEvent, String> tableNormalizer) {
        String resolvedEndpoint = endpointId != null ? endpointId : "";
        if (events == null || events.isEmpty()) {
            return emptyBatchResult(ThrottlePhase.FULL, 0L);
        }

        long inserts = 0L;
        long updates = 0L;
        long deletes = 0L;
        long controls = 0L;

        // Zero-map fast path when all DML events belong to a single table
        String singleTable = null;
        TapRecordEvent singleFirstEvent = null;
        long singleCount = 0L;
        Map<String, FullTableBucket> multiTableBuckets = null;

        for (TapEvent event : events) {
            if (!(event instanceof TapRecordEvent recordEvent)) {
                controls++;
                continue;
            }
            DmlType dmlType = resolveDmlType(recordEvent);
            if (dmlType == null) {
                controls++;
                continue;
            }
            switch (dmlType) {
                case INSERT -> inserts++;
                case UPDATE -> updates++;
                case DELETE -> deletes++;
            }
            String normalizedTable = normalizeTable(recordEvent, recordEvent.getTableId(), tableNormalizer);
            if (multiTableBuckets != null) {
                FullTableBucket bucket = multiTableBuckets.computeIfAbsent(normalizedTable, k -> new FullTableBucket(recordEvent));
                bucket.count++;
            } else if (singleTable == null) {
                singleTable = normalizedTable;
                singleFirstEvent = recordEvent;
                singleCount = 1L;
            } else if (singleTable.equals(normalizedTable)) {
                singleCount++;
            } else {
                multiTableBuckets = new LinkedHashMap<>();
                multiTableBuckets.put(singleTable, new FullTableBucket(singleFirstEvent, singleCount));
                multiTableBuckets.put(normalizedTable, new FullTableBucket(recordEvent, 1L));
            }
        }

        long dmlCount = inserts + updates + deletes;
        if (dmlCount == 0L) {
            return emptyBatchResult(ThrottlePhase.FULL, controls);
        }

        long totalBytes = 0L;
        long sampleCount = 0L;
        boolean budgetHit = false;
        boolean missing = false;
        StringBuilder diag = new StringBuilder();

        if (multiTableBuckets == null) {
            try {
                long firstBytes = eventCostMeter.measure(EventV1View.fromTapRecordEvent(singleFirstEvent, singleTable));
                sampleCount = 1L;
                totalBytes = saturatingMultiply(firstBytes, singleCount);
            } catch (MeterBudgetExceededException ex) {
                budgetHit = true;
                missing = true;
                diag.append(ex.getMessage());
            } catch (RuntimeException ex) {
                missing = true;
                diag.append(ex.getMessage());
            }
        } else {
            for (Map.Entry<String, FullTableBucket> entry : multiTableBuckets.entrySet()) {
                try {
                    long firstBytes = eventCostMeter.measure(
                            EventV1View.fromTapRecordEvent(entry.getValue().firstEvent, entry.getKey()));
                    sampleCount++;
                    long tableBytes = saturatingMultiply(firstBytes, entry.getValue().count);
                    totalBytes = saturatingAdd(totalBytes, tableBytes);
                } catch (MeterBudgetExceededException ex) {
                    budgetHit = true;
                    missing = true;
                    diag.append(resolvedEndpoint).append("|").append(entry.getKey()).append(": ").append(ex.getMessage()).append("; ");
                    break;
                } catch (RuntimeException ex) {
                    missing = true;
                    diag.append(resolvedEndpoint).append("|").append(entry.getKey()).append(": ").append(ex.getMessage()).append("; ");
                    break;
                }
            }
        }

        SampleQuality quality = missing
                ? (budgetHit ? SampleQuality.BUDGET_LIMITED : SampleQuality.MISSING)
                : SampleQuality.VALID;

        return BatchMeteringResult.builder()
                .taskId(taskId)
                .runEpoch(runEpoch)
                .phase(ThrottlePhase.FULL)
                .direction(direction)
                .dmlCount(dmlCount)
                .insertCount(inserts)
                .updateCount(updates)
                .deleteCount(deletes)
                .controlCount(controls)
                .estimatedBytes(missing ? null : totalBytes)
                .sampleQuality(quality)
                .sampleCount(sampleCount)
                .budgetHit(budgetHit)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .sampleConfigVersion(cdcSampleCache.getSampleConfigVersion())
                .cdcSampleRefreshIntervalSeconds(cdcSampleCache.getRefreshIntervalSeconds())
                .missingGroups(List.of())
                .sampleDiagnostics(diag.length() == 0 ? null : diag.toString())
                .build();
    }

    public BatchMeteringResult meterCdcBatch(
            String endpointId,
            List<? extends TapEvent> events,
            Function<TapEvent, String> tableNormalizer,
            boolean rateWaiting) {
        String resolvedEndpoint = endpointId != null ? endpointId : "";
        if (events == null || events.isEmpty()) {
            return cdcSampleCache.estimateBatch(List.of(), 0L, 0L, 0L, 0L, rateWaiting);
        }

        long inserts = 0L;
        long updates = 0L;
        long deletes = 0L;
        long controls = 0L;
        Map<CdcSampleGroupKey, CdcTableDmlSampleCache.GroupBatchEvents> grouped = new LinkedHashMap<>();

        for (TapEvent event : events) {
            if (event instanceof TapDDLEvent ddlEvent) {
                controls++;
                advanceSchemaGeneration(resolvedEndpoint, ddlEvent, tableNormalizer);
                continue;
            }
            if (!(event instanceof TapRecordEvent recordEvent)) {
                controls++;
                continue;
            }
            DmlType dmlType = resolveDmlType(recordEvent);
            if (dmlType == null) {
                controls++;
                continue;
            }
            switch (dmlType) {
                case INSERT -> inserts++;
                case UPDATE -> updates++;
                case DELETE -> deletes++;
            }
            String normalizedTable = normalizeTable(recordEvent, recordEvent.getTableId(), tableNormalizer);
            long gen = currentGeneration(resolvedEndpoint, normalizedTable);
            CdcSampleGroupKey key = new CdcSampleGroupKey(resolvedEndpoint, normalizedTable, dmlType, gen);
            grouped.computeIfAbsent(key, CdcTableDmlSampleCache.GroupBatchEvents::new).add(recordEvent);
        }

        return cdcSampleCache.estimateBatch(
                new ArrayList<>(grouped.values()),
                inserts,
                updates,
                deletes,
                controls,
                rateWaiting);
    }

    private void advanceSchemaGeneration(
            String endpointId,
            TapDDLEvent ddlEvent,
            Function<TapEvent, String> tableNormalizer) {
        String table = normalizeTable(ddlEvent, ddlEvent.getTableId(), tableNormalizer);
        if (table.isEmpty()) {
            endpointGlobalGenerations.computeIfAbsent(endpointId, k -> new AtomicLong()).incrementAndGet();
        } else {
            tableSchemaGenerations.computeIfAbsent(endpointId + "\u0000" + table, k -> new AtomicLong()).incrementAndGet();
        }
    }

    private long currentGeneration(String endpointId, String normalizedTable) {
        AtomicLong globalGen = endpointGlobalGenerations.get(endpointId);
        AtomicLong tableGen = tableSchemaGenerations.get(endpointId + "\u0000" + normalizedTable);
        return (globalGen == null ? 0L : globalGen.get()) + (tableGen == null ? 0L : tableGen.get());
    }

    private BatchMeteringResult emptyBatchResult(ThrottlePhase phase, long controlCount) {
        return BatchMeteringResult.builder()
                .taskId(taskId)
                .runEpoch(runEpoch)
                .phase(phase)
                .direction(direction)
                .dmlCount(0L)
                .insertCount(0L)
                .updateCount(0L)
                .deleteCount(0L)
                .controlCount(controlCount)
                .estimatedBytes(0L)
                .sampleQuality(SampleQuality.VALID)
                .sampleCount(0L)
                .budgetHit(false)
                .meterVersion(ThrottleConstants.METER_VERSION)
                .sampleConfigVersion(cdcSampleCache.getSampleConfigVersion())
                .cdcSampleRefreshIntervalSeconds(cdcSampleCache.getRefreshIntervalSeconds())
                .missingGroups(List.of())
                .build();
    }

    private static DmlType resolveDmlType(TapRecordEvent event) {
        if (event instanceof TapInsertRecordEvent) {
            return DmlType.INSERT;
        }
        if (event instanceof TapUpdateRecordEvent) {
            return DmlType.UPDATE;
        }
        if (event instanceof TapDeleteRecordEvent) {
            return DmlType.DELETE;
        }
        return null;
    }

    private static String normalizeTable(TapEvent event, String rawTableId, Function<TapEvent, String> normalizer) {
        if (normalizer != null) {
            String custom = normalizer.apply(event);
            if (custom != null) {
                return custom;
            }
        }
        return rawTableId != null ? rawTableId : "";
    }

    private static long saturatingMultiply(long a, long b) {
        if (a <= 0L || b <= 0L) {
            return 0L;
        }
        if (a > Long.MAX_VALUE / b) {
            return Long.MAX_VALUE;
        }
        return a * b;
    }

    private static long saturatingAdd(long a, long b) {
        if (a > Long.MAX_VALUE - b) {
            return Long.MAX_VALUE;
        }
        return a + b;
    }

    private static final class FullTableBucket {
        final TapRecordEvent firstEvent;
        long count;

        FullTableBucket(TapRecordEvent firstEvent) {
            this(firstEvent, 0L);
        }

        FullTableBucket(TapRecordEvent firstEvent, long count) {
            this.firstEvent = firstEvent;
            this.count = count;
        }
    }
}
