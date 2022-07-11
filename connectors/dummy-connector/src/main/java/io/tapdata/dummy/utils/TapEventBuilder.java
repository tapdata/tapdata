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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TapEvent builder
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/8 15:21 Create
 */
public class TapEventBuilder {

    private static final char[] CHAR_64 = new char[]{
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'
            , 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
            , 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'
            , '_', '-'
    };

    private final AtomicLong eventIndex = new AtomicLong(0);
    private AtomicLong serial;
    private Integer serialStep;

    private final DummyOffset offset;

    public TapEventBuilder(Object offsetState) {
        if (offsetState instanceof DummyOffset) {
            this.offset = (DummyOffset) offsetState;
        } else {
            this.offset = new DummyOffset();
            offset.setBeginTimes(System.currentTimeMillis());
        }
    }

    public TapEventBuilder(Object offsetState, SyncStage syncStage) {
        this(offsetState);
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

        TapInsertRecordEvent tapEvent = TapSimplify.insertRecordEvent(after, table.getName());
        updateOffset(table.getName(), RecordOperators.Insert, tapEvent);
        return tapEvent;
    }

    public TapUpdateRecordEvent generateUpdateRecordEvent(TapTable table, TapInsertRecordEvent insertRecordEvent) {
        insertRecordEvent = (null == insertRecordEvent) ? generateInsertRecordEvent(table) : insertRecordEvent;
        Map<String, Object> before = insertRecordEvent.getAfter();
        Map<String, Object> after = new HashMap<>(before);
        table.childItems().forEach(tapField -> {
            if (Boolean.FALSE.equals(tapField.getPrimaryKey())) {
                after.put(tapField.getName(), generateEventValue(tapField, RecordOperators.Update));
            }
        });

        String tableName = table.getName();
        TapUpdateRecordEvent updateRecordEvent = TapSimplify.updateDMLEvent(before, after, tableName);
        updateOffset(tableName, RecordOperators.Update, updateRecordEvent);
        return updateRecordEvent;
    }

    public TapDeleteRecordEvent generateDeleteRecordEvent(TapTable table, TapInsertRecordEvent insertRecordEvent) {
        String tableName = table.getName();
        Map<String, Object> after;
        if (null == insertRecordEvent) {
            after = new HashMap<>();
            for (TapField tapField : table.childItems()) {
                after.put(tapField.getName(), generateEventValue(tapField, RecordOperators.Update));
            }
        } else {
            after = insertRecordEvent.getAfter();
        }
        TapDeleteRecordEvent deleteRecordEvent = TapSimplify.deleteDMLEvent(after, tableName);
        updateOffset(tableName, RecordOperators.Delete, deleteRecordEvent);
        return deleteRecordEvent;
    }

    public TapDeleteRecordEvent generateDeleteRecordEvent(TapTable table, TapUpdateRecordEvent updateRecordEvent) {
        String tableName = table.getName();
        Map<String, Object> after;
        if (null == updateRecordEvent) {
            after = new HashMap<>();
            for (TapField tapField : table.childItems()) {
                after.put(tapField.getName(), generateEventValue(tapField, RecordOperators.Update));
            }
        } else {
            after = updateRecordEvent.getAfter();
        }
        TapDeleteRecordEvent deleteRecordEvent = TapSimplify.deleteDMLEvent(after, tableName);
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
        Object def = field.getDefaultValue();
        if (def instanceof String) {
            Matcher m = Pattern.compile("^([^(]+)\\(([^)]*)\\)$").matcher((String) def);
            if (m.find()) {
                String fn = m.group(1);
                String params = m.group(2);

                switch (fn.toLowerCase()) {
                    case "serial":
                        if (RecordOperators.Insert == op) {
                            if (null == serial) {
                                try {
                                    String[] splitStr = params.split(",");
                                    serialStep = Integer.parseInt(splitStr[1]);
                                    serial = new AtomicLong(Integer.parseInt(splitStr[0]));
                                } catch (Throwable e) {
                                    serialStep = 1;
                                    serial = new AtomicLong(0);
                                }
                            }
                            return serial.addAndGet(serialStep);
                        } else {
                            return (int) (Math.random() * serial.get()) - serial.get() % serialStep;
                        }
                    case "now":
                        return System.currentTimeMillis();
                    case "randomlong":
                        return Math.random() * Long.parseLong(params);
                    case "randomstring":
                        return randomString(Integer.parseInt(params));
                    case "cuid":
                    case "uuid":
                        return UUID.randomUUID().toString();
                    default:
                        break;
                }

            }
            return def;
        }
        return def;
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
            buf.append(CHAR_64[(int) (Math.random() * 64)]);
        }
        return buf.toString();
    }
}
