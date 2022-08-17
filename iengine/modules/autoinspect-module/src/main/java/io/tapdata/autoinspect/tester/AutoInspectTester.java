package io.tapdata.autoinspect.tester;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.autoinspect.compare.IAutoCompare;
import io.tapdata.autoinspect.connector.IConnector;
import io.tapdata.autoinspect.entity.CompareEvent;
import io.tapdata.autoinspect.entity.CompareRecord;
import io.tapdata.autoinspect.tester.samples.SampleAutoCompare;
import io.tapdata.autoinspect.tester.samples.SampleConnector;
import io.tapdata.autoinspect.tester.samples.SyncSampleTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/8 14:44 Create
 */
public class AutoInspectTester {
    private static final Logger logger = LogManager.getLogger(AutoInspectTester.class);

    private static SampleType sampleType = SampleType.MoreTarget;

    private enum SampleType {
        MoreSource, MoreTarget;
    }

    public static void main(String[] args) throws Exception {
        TaskDto task = new TaskDto();
        task.setId(ObjectId.get());
        task.setIsAutoInspect(true);
        task.setSyncType(ParentTaskDto.TYPE_INITIAL_SYNC_CDC);

        new Thread(new SyncSampleTask(task)).start();
    }

    public static long randomTimes() {
        return randomTimes(500, 1000);
    }

    public static long randomTimes(long basic, long interval) {
        return basic + (long) (Math.random() * interval);
    }

    public static CompareRecord createCompareRecord(String id, Object value) {
        CompareRecord record = new CompareRecord();
        record.getKeyNames().add("id");
        record.getOriginalKey().put("id", id);
        record.getData().put("id", id);
        record.getData().put("value", value);
        return record;
    }

    public static CompareEvent createCompareEvent(String tableName, String id, Object value) {
        LinkedHashMap<String, Object> keymap = new LinkedHashMap<>();
        keymap.put("id", id);
        Map<String, Object> data = new HashMap<>();
        data.put("id", id);
        data.put("value", value);
        return new CompareEvent(tableName, keymap, data, System.currentTimeMillis());
    }

    public static IConnector sourceConnector() {
        List<CompareRecord> compareRecords = new ArrayList<>();
        compareRecords.add(AutoInspectTester.createCompareRecord("1", "ok"));
        compareRecords.add(AutoInspectTester.createCompareRecord("2", "diff2"));
        compareRecords.add(AutoInspectTester.createCompareRecord("3", "miss3"));
        compareRecords.add(AutoInspectTester.createCompareRecord("4", "ok"));
        compareRecords.add(AutoInspectTester.createCompareRecord("5", "fix"));
        compareRecords.add(AutoInspectTester.createCompareRecord("6", "ok"));
        compareRecords.add(AutoInspectTester.createCompareRecord("8", "ok"));
        switch (sampleType) {
            case MoreSource:
                compareRecords.add(AutoInspectTester.createCompareRecord("9", "more"));
                compareRecords.add(AutoInspectTester.createCompareRecord("10", "more"));
            default:
                break;
        }
        return new SampleConnector(compareRecords);
    }

    public static IConnector targetConnector() {
        List<CompareRecord> compareRecords = new ArrayList<>();
        compareRecords.add(AutoInspectTester.createCompareRecord("1", "ok"));
        compareRecords.add(AutoInspectTester.createCompareRecord("2", "ok"));
        compareRecords.add(AutoInspectTester.createCompareRecord("4", "ok"));
        compareRecords.add(AutoInspectTester.createCompareRecord("5", "ok"));
        compareRecords.add(AutoInspectTester.createCompareRecord("6", "diff6"));
        compareRecords.add(AutoInspectTester.createCompareRecord("7", "miss7"));
        compareRecords.add(AutoInspectTester.createCompareRecord("8", "ok"));
        switch (sampleType) {
            case MoreTarget:
                compareRecords.add(AutoInspectTester.createCompareRecord("9", "more"));
                compareRecords.add(AutoInspectTester.createCompareRecord("10", "more"));
            default:
                break;
        }
        return new SampleConnector(compareRecords);
    }

    public static IAutoCompare autoCompare() {
        return new SampleAutoCompare();
    }

    public static String toJson(Object o) {
        SerializeConfig serializeConfig = new SerializeConfig();
        serializeConfig.put(ObjectId.class, (jsonSerializer, o12, o1, type, i) -> {
            if (o12 instanceof ObjectId) {
                jsonSerializer.write(((ObjectId) o12).toHexString());
            } else {
                jsonSerializer.write(o12);
            }
        });
        return JSON.toJSONString(o, serializeConfig);
    }
}
