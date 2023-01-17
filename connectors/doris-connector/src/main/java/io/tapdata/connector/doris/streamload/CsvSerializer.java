package io.tapdata.connector.doris.streamload;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.commons.collections4.MapUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.StringJoiner;

/**
 * @Author dayun
 * @Date 7/14/22
 */
public class CsvSerializer implements MessageSerializer {
    @Override
    public byte[] serialize(TapTable table, TapRecordEvent recordEvent) throws Throwable {
        // in some case, the before might be null, here we use after if the before is null
        // and doris will use pk to delete the data
//        if (before == null) {
//            before = after;
//        }

        String value = "";
        if (recordEvent instanceof TapInsertRecordEvent) {
            final TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
            final Map<String, Object> after = insertRecordEvent.getAfter();
            value += buildCSVString(table, after, false);
        } else if (recordEvent instanceof TapUpdateRecordEvent) {
            final TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
            final Map<String, Object> before = updateRecordEvent.getBefore();
            final Map<String, Object> after = updateRecordEvent.getAfter();
            value += buildCSVString(table, before, true);
            value += Constants.LINE_DELIMITER_DEFAULT;
            value += buildCSVString(table, after, false);
        } else {
            final TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
            final Map<String, Object> before = deleteRecordEvent.getBefore();
            value += buildCSVString(table, before, true);
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private String buildCSVString(TapTable table, Map<String, Object> values, boolean delete) throws IOException {
        if(MapUtils.isNotEmpty(values)) {
            Object value="";
            StringJoiner joiner = new StringJoiner(Constants.FIELD_DELIMITER_DEFAULT);
            final Map<String, TapField> tapFieldMap = table.getNameFieldMap();
            for (final Map.Entry<String, TapField> entry : tapFieldMap.entrySet()) {
                value = values.getOrDefault(entry.getKey(), Constants.NULL_VALUE);
                // value get from the value map may be null
                if (value == null) {
                    value = Constants.NULL_VALUE;
                }
                joiner.add(value.toString());
            }
            joiner.add(delete ? "1" : "0");
            return joiner.toString();
        }
        return "";
    }

    @Override
    public byte[] lineEnd() {
        return Constants.LINE_DELIMITER_DEFAULT.getBytes(StandardCharsets.UTF_8);
    }
}
