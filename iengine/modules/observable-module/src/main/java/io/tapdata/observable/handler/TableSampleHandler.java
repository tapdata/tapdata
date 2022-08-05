package io.tapdata.observable.handler;

import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.common.sample.CollectorFactory;
import io.tapdata.common.sample.SampleCollector;
import io.tapdata.common.sample.sampler.CounterSampler;
import io.tapdata.observable.TaskSampleRetriever;

import java.util.*;

/**
 * @author Dexter
 */
public class TableSampleHandler extends AbstractHandler {
    private static final String TAG_KEY_TABLE = "table";
    public TableSampleHandler(SubTaskDto task) {
        super(task);
    }

    public Map<String, String> tableTags(String table) {
        Map<String, String> tags = baseTableTags();
        tags.put(TAG_KEY_TABLE, table);

        return tags;
    }

    public Map<String, String> baseTableTags() {
        return baseTags(SAMPLE_TYPE_TABLE);
    }

    private final Map<String, Map<String, SampleCollector>> collectors = new HashMap<>();

    private final Map<String, Map<String, CounterSampler>> snapshotTotals = new HashMap<>();

    private Map<String, Map<String, Number>> retrieve(Map<String, String> tags, List<String> fields) {
        TaskSampleRetriever.SampleResponse response = TaskSampleRetriever.getInstance().retrieveRaw(tags, fields);

        Map<String, Map<String, Number>> samples = new HashMap<>();
        if (response != null && response.getSamples() != null) {
            for (Map<String, Object> item : response.getSamples().getRetriever()) {
                String table = ((Map<String, String>) item.get("tags")).get("TAG_KEY_TABLE");
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

    private Map<String, Map<String, Number>> tableValues;
    public void retrieve() {
        tableValues = retrieve(baseTableTags(), Arrays.asList(
                "snapshotTotal", "snapshotInsertTotal"
        ));
    }


    public void init(Node<?> node, String table, long cnt) {
        String nodeId = node.getId();

        Map<String, Number> values = tableValues.getOrDefault(table, new HashMap<>());
        Map<String, String> tags = tableTags(table);
        SampleCollector collector = CollectorFactory.getInstance("v2").getSampleCollectorByTags("tableSamplers", tags);
        collectors.putIfAbsent(nodeId, new HashMap<>());
        collectors.get(nodeId).put(table, collector);

        snapshotTotals.putIfAbsent(nodeId, new HashMap<>());
        snapshotTotals.get(nodeId).put(table, collector.getCounterSampler("snapshotTotal", cnt));

        snapshotInsertTotals.putIfAbsent(nodeId, new HashMap<>());
        snapshotInsertTotals.get(nodeId).put(table, collector.getCounterSampler("snapshotInsertTotal",
                values.getOrDefault("snapshotInsertTotal", 0).longValue()));

        // cache the initial sample value
        CollectorFactory.getInstance("v2").recordCurrentValueByTag(tags);
    }

    public void close(Node<?> node) {
        String nodeId = node.getId();
        Map<String, SampleCollector> tableCollectors = collectors.get(nodeId);
        if (null == tableCollectors || tableCollectors.isEmpty()) {
            return;
        }
        for(SampleCollector collector : tableCollectors.values()) {
            Map<String, String> tags = collector.tags();
            // cache the last sample value
            CollectorFactory.getInstance("v2").recordCurrentValueByTag(tags);
            CollectorFactory.getInstance("v2").removeSampleCollectorByTags(tags);
        }
    }

    private final Map<String, Map<String, CounterSampler>> snapshotInsertTotals = new HashMap<>();

    public void incrTableSnapshotInsertTotal(String nodeId, String table) {
        incrTableSnapshotInsertTotal(nodeId, table, 1L);
    }

    public void incrTableSnapshotInsertTotal(String nodeId, String table, long value) {
        if (snapshotInsertTotals.containsKey(nodeId)) {
            Optional.ofNullable(snapshotInsertTotals.get(nodeId).get(table)).ifPresent(counter -> counter.inc(value));
        }
    }
}
