package io.tapdata.dummy.utils;

import io.tapdata.dummy.constants.RecordOperators;
import io.tapdata.dummy.constants.SyncStage;
import io.tapdata.dummy.po.DummyOffset;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * TapEvent builder
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/8 15:21 Create
 */
public class TapEventBuilder {

    private static final char[] RANDOM_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    private static final int RANDOM_CHARS_LENGTH = RANDOM_CHARS.length;

    private final AtomicLong eventIndex = new AtomicLong(0);
    private AtomicLong serial;
    private Integer serialStep;

    private DummyOffset offset;

    private final Consumer<Map<String, Object>> heartbeatSetter;

    public TapEventBuilder() {
        this((data) -> {});
    }

    public TapEventBuilder(Consumer<Map<String, Object>> heartbeatSetter) {
        this.heartbeatSetter = heartbeatSetter;
    }

    public void reset(Object offsetState, SyncStage syncStage) {
        if (offsetState instanceof DummyOffset) {
            this.offset = (DummyOffset) offsetState;
        } else {
            this.offset = new DummyOffset();
            offset.setBeginTimes(System.currentTimeMillis());
        }
        offset.setSyncStage(syncStage);
    }

    private void updateOffset(String tableName, RecordOperators op, TapRecordEvent recordEvent) {
        recordEvent.setReferenceTime(System.currentTimeMillis());
        offset.setLastTN(eventIndex.addAndGet(1));
        offset.setLastTimes(recordEvent.getTime());
        offset.addCounts(op, tableName, 1);
    }

    public TapInsertRecordEvent generateInsertRecordEvent(TapTable table) {
        Map<String, Object> after = new HashMap<>();
        for (TapField tapField : table.childItems()) {
            after.put(tapField.getName(), generateEventValue(tapField, RecordOperators.Insert));
        }
        heartbeatSetter.accept(after);

        TapInsertRecordEvent tapEvent = TapSimplify.insertRecordEvent(after, table.getName());
        updateOffset(table.getName(), RecordOperators.Insert, tapEvent);
        return tapEvent;
    }

    public TapInsertRecordEvent copy(TapTable table, TapInsertRecordEvent event) {
        TapInsertRecordEvent tapEvent = TapSimplify.insertRecordEvent(event.getAfter(), table.getName());
        updateOffset(table.getName(), RecordOperators.Insert, tapEvent);
        return tapEvent;
    }

    public TapUpdateRecordEvent generateUpdateRecordEvent(TapTable table, Map<String, Object> before) {
        before = (null == before) ? generateInsertRecordEvent(table).getAfter() : new HashMap<>(before);
        Map<String, Object> after = new HashMap<>(before);
        table.childItems().forEach(tapField -> {
            if (Boolean.FALSE.equals(tapField.getPrimaryKey())) {
                after.put(tapField.getName(), generateEventValue(tapField, RecordOperators.Update));
            }
        });
        heartbeatSetter.accept(after);

        String tableName = table.getName();
        TapUpdateRecordEvent updateRecordEvent = TapSimplify.updateDMLEvent(before, after, tableName);
        updateOffset(tableName, RecordOperators.Update, updateRecordEvent);
        return updateRecordEvent;
    }

    public TapDeleteRecordEvent generateDeleteRecordEvent(TapTable table, Map<String, Object> before) {
        String tableName = table.getName();
        if (null == before) {
            before = new HashMap<>();
            for (TapField tapField : table.childItems()) {
                before.put(tapField.getName(), generateEventValue(tapField, RecordOperators.Update));
            }
        } else {
            before = new HashMap<>(before);
        }
        heartbeatSetter.accept(before);

        TapDeleteRecordEvent deleteRecordEvent = TapSimplify.deleteDMLEvent(before, tableName);
        updateOffset(tableName, RecordOperators.Delete, deleteRecordEvent);
        return deleteRecordEvent;
    }

    public DummyOffset getOffset() {
        return offset;
    }

    /**
     * generate event value by TapField
     *
     * @param field TapField
     * @param op    operate type
     * @return TapEvent
     */
    private Object generateEventValue(TapField field, RecordOperators op) {
        if (null != field.getDefaultValue()) {
            return field.getDefaultValue();
        }

        if ("uuid".equalsIgnoreCase(field.getDataType())) {
            return UUID.randomUUID().toString();
        } else if ("now".equalsIgnoreCase(field.getDataType())) {
            return Timestamp.from(Instant.now());
        } else {
            String ftype = field.getDataType();
            if (ftype.startsWith("rnumber")) {
                if (ftype.endsWith(")")) {
                    try {
                        return Math.random() * Long.parseLong(ftype.substring(8, ftype.length() - 1));
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                }
                return Math.random() * 4;
            } else if (ftype.startsWith("rstring")) {
                if (ftype.endsWith(")")) {
                    try {
                        return randomString(Integer.parseInt(ftype.substring(8, ftype.length() - 1)));
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                }
                return randomString(8);
            } else if (ftype.startsWith("serial")) {
                if (RecordOperators.Insert == op) {
                    if (null == serial) {
                        try {
                            String[] splitStr = ftype.substring(7, ftype.length() - 1).split(",");
                            serial = new AtomicLong(Integer.parseInt(splitStr[0]));
                            serialStep = Math.max(Integer.parseInt(splitStr[1]), 1);
                        } catch (Throwable e) {
                            serial = new AtomicLong(1);
                            serialStep = 1;
                        }
                    }
                    return serial.getAndAdd(serialStep);
                }
                return (int) (Math.random() * serial.get()) - serial.get() % serialStep;
            }
        }
        return field.getDefaultValue();
    }

    /**
     * Get random string by length
     *
     * @param length String length
     * @return string
     */
    private String randomString(int length) {
        StringBuilder buf = new StringBuilder();
        for (int i = length; i > 0; i--) {
            buf.append(RANDOM_CHARS[(int) (Math.random() * RANDOM_CHARS_LENGTH)]);
        }
        return buf.toString();
    }
}
