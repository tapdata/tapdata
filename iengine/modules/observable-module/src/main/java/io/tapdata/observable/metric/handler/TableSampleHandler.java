package io.tapdata.observable.metric.handler;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.observable.metric.TaskSampleRetriever;
import lombok.NonNull;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

/**
 * @author Dexter
 */
public class TableSampleHandler extends AbstractHandler {
    static final String SAMPLE_TYPE_TABLE                  = "table";

    static final String SNAPSHOT_ROW_TOTAL                 = "snapshotRowTotal";
    static final String SNAPSHOT_INSERT_ROW_TOTAL          = "snapshotInsertRowTotal";
    static final String SNAPSHOT_SYNCRATE                  = "snapshotSyncRate";

    private final String table;
    private final Long snapshotRowTotal;
    private BigDecimal snapshotSyncRate;
    private final Map<String, Number> retrievedTableValues;
    private CounterSampler snapshotInsertRowCounter;
    private TaskSampleHandler taskSampleHandler = null;

    public TableSampleHandler(TaskDto task, String table, @NonNull Long snapshotRowTotal,
                              @NonNull Map<String, Number> retrievedTableValues, BigDecimal snapshotSyncRate) {
        super(task);
        this.table = table;
        this.snapshotRowTotal = snapshotRowTotal;
        this.retrievedTableValues = retrievedTableValues;
        this.snapshotSyncRate = snapshotRowTotal == 0 ? BigDecimal.ONE : snapshotSyncRate;
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
                SNAPSHOT_ROW_TOTAL,
                SNAPSHOT_INSERT_ROW_TOTAL
        );
    }

    public Map<String, Number> retrieve() {
       return retrievedTableValues;
    }


    public void doInit(Map<String, Number> values) {
        collector.addSampler(SNAPSHOT_ROW_TOTAL, () -> snapshotRowTotal);
        snapshotInsertRowCounter = getCounterSampler(values, SNAPSHOT_INSERT_ROW_TOTAL);

        collector.addSampler(SNAPSHOT_SYNCRATE, () -> {
            if (snapshotSyncRate.compareTo(BigDecimal.ONE) != 0 &&
                    Objects.nonNull(snapshotRowTotal) && Objects.nonNull(snapshotInsertRowCounter.value())) {
                BigDecimal decimal = BigDecimal.valueOf(snapshotInsertRowCounter.value().longValue())
                        .divide(new BigDecimal(snapshotRowTotal), 2, RoundingMode.HALF_UP);
                if (decimal.compareTo(BigDecimal.ONE) >= 0) {
                    snapshotSyncRate = BigDecimal.ONE;
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

    public static Map<String, Map<String, Number>> retrieveAllTables(TaskDto task) {
        Map<String, String> tags = new HashMap<String, String>(){{
            put("type", SAMPLE_TYPE_TABLE);
            put("taskId", task.getId().toHexString());
            put("taskRecordId", task.getTaskRecordId());
        }};
        List<String> fields = new ArrayList<String>(){{
            add(SNAPSHOT_INSERT_ROW_TOTAL);
        }};
        TaskSampleRetriever.SampleResponse response = TaskSampleRetriever.getInstance().retrieveRaw(task.getStartTime().getTime(), tags, fields);

        Map<String, Map<String, Number>> samples = new HashMap<>();
        if (response != null && response.getSamples() != null) {
            for (Map<String, Object> item : response.getSamples().getRetriever()) {
                String table = ((Map<String, String>) item.get("tags")).get("table");
                samples.putIfAbsent(table, new HashMap<>());
                for (Map.Entry<String, Object> entry : item.entrySet()) {
                    String key = entry.getKey();
                    if (!key.equals("tags")) {
                        samples.get(table).put(key, (Number) entry.getValue());
                    }
                }
            }
        }

        return samples;
    }

    public void setTaskSampleHandler(TaskSampleHandler taskSampleHandler) {
        this.taskSampleHandler = taskSampleHandler;
    }
}
