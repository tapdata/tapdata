package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.entity.CountResult;
import io.tapdata.observable.metric.TaskSampleRetriever;
import lombok.NonNull;
import lombok.Setter;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * @author Dexter
 */
public class TableSampleHandler extends AbstractHandler {
    static final String SAMPLE_TYPE_TABLE = MetricCons.SampleType.TABLE.code();

    private final String table;
    private final Long snapshotRowTotal;
    private BigDecimal snapshotSyncRate;
    private final Map<String, Number> retrievedTableValues;
    private CounterSampler snapshotInsertRowCounter;
    @Setter
    private TaskSampleHandler taskSampleHandler = null;
    private Boolean isSnapshotDone;
    @Setter
    private boolean errorSkipped;

    public TableSampleHandler(TaskDto task, String table, @NonNull CountResult countResult,
                              @NonNull Map<String, Number> retrievedTableValues, BigDecimal snapshotSyncRate) {
        super(task);
        this.table = table;
        this.snapshotRowTotal = countResult.getCount();
        this.retrievedTableValues = retrievedTableValues;
        this.snapshotSyncRate = countResult.getCount() == 0 || countResult.getDone() ? BigDecimal.ONE : snapshotSyncRate;
        this.isSnapshotDone  = countResult.getCount() == 0 || countResult.getDone();
    }

    @Override
    String type() {
        return SAMPLE_TYPE_TABLE;
    }

    @Override
    public Map<String, String> tags() {
        Map<String, String> tags = super.tags();
        tags.put("table", table);

        return tags;
    }

    @Override
    List<String> samples() {
        return Arrays.asList(
            MetricCons.SS.VS.F_SNAPSHOT_ROW_TOTAL,
            MetricCons.SS.VS.F_SNAPSHOT_INSERT_ROW_TOTAL
        );
    }

    public Map<String, Number> retrieve() {
       return retrievedTableValues;
    }


    public void doInit(Map<String, Number> values) {
        collector.addSampler(MetricCons.SS.VS.F_SNAPSHOT_ROW_TOTAL, () -> snapshotRowTotal);
        collector.addSampler(MetricCons.SS.VS.F_SNAPSHOT_ERROR_SKIPPED, () -> errorSkipped ? 1 : 0);
        snapshotInsertRowCounter = getCounterSampler(values, MetricCons.SS.VS.F_SNAPSHOT_INSERT_ROW_TOTAL);

        collector.addSampler(MetricCons.SS.VS.F_SNAPSHOT_SYNC_RATE, () -> {
            if ((snapshotSyncRate.compareTo(BigDecimal.ONE) != 0 || errorSkipped) &&
                    Objects.nonNull(snapshotRowTotal) && Objects.nonNull(snapshotInsertRowCounter.value())) {
                BigDecimal decimal = BigDecimal.valueOf(snapshotInsertRowCounter.value().longValue())
                        .divide(new BigDecimal(snapshotRowTotal), 2, RoundingMode.HALF_UP);
                if (errorSkipped) {
                    snapshotSyncRate = decimal;
                } else if (isSnapshotDone) {
                    snapshotSyncRate = BigDecimal.ONE;
                } else if(decimal.compareTo(BigDecimal.ONE) >= 0) {
                    snapshotSyncRate = new BigDecimal("0.99");
                } else {
                    snapshotSyncRate = decimal;
                }
            } else if (Objects.nonNull(taskSampleHandler) && Objects.nonNull(taskSampleHandler.getSnapshotDone())) {
                snapshotSyncRate = BigDecimal.ONE;
            }

            return snapshotSyncRate;
        });
    }

    public void incrTableSnapshotInsertTotal() {
        snapshotInsertRowCounter.inc();
    }

    public void incrTableSnapshotInsertTotal(long value) {
        snapshotInsertRowCounter.inc(value);
    }

    public void setSnapshotDone() {
        isSnapshotDone = true;
    }

    public static Map<String, Map<String, Number>> retrieveAllTables(TaskDto task) {
        Map<String, String> tags = new HashMap<>();
        tags.put(MetricCons.Tags.F_TYPE, SAMPLE_TYPE_TABLE);
        tags.put(MetricCons.Tags.F_TASK_ID, task.getId().toHexString());
        tags.put(MetricCons.Tags.F_TASK_RECORD_ID, task.getTaskRecordId());

        List<String> fields = new ArrayList<>();
        fields.add(MetricCons.SS.VS.F_SNAPSHOT_INSERT_ROW_TOTAL);

        TaskSampleRetriever.SampleResponse response = TaskSampleRetriever.getInstance().retrieveRaw(
            task.getStartTime().getTime(),
            tags,
            fields
        );

        Map<String, Map<String, Number>> samples = new HashMap<>();
        if (response != null && response.getSamples() != null) {
            for (Map<String, Object> item : response.getSamples().getRetriever()) {
                Map<String, String> tagsMap = (Map<String, String>) item.get(MetricCons.F_TAGS);
                String table = tagsMap.get(MetricCons.Tags.F_TABLE);
                samples.putIfAbsent(table, new HashMap<>());
                for (Map.Entry<String, Object> entry : item.entrySet()) {
                    String key = entry.getKey();
                    if (!key.equals(MetricCons.F_TAGS)) {
                        samples.get(table).put(key, (Number) entry.getValue());
                    }
                }
            }
        }

        return samples;
    }
}
