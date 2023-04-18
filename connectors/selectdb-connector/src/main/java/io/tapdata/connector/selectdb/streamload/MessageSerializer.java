package io.tapdata.connector.selectdb.streamload;


import com.alibaba.fastjson.JSON;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import org.apache.commons.collections4.MapUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

/**
 * Author:Skeet
 * Date: 2022/12/14
 **/
public class MessageSerializer {
    public static List<Map<String, Object>> serializeMap(TapTable table, TapRecordEvent recordEvent) {
        List<Map<String, Object>> list = new ArrayList<>();
        if (recordEvent instanceof TapInsertRecordEvent) {
            final TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
            final Map<String, Object> after = insertRecordEvent.getAfter();
            list.add(buildJsonMap(table, after, false, recordEvent));
        } else if (recordEvent instanceof TapUpdateRecordEvent) {
            final TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
            Map<String, Object> before = updateRecordEvent.getBefore();
            final Map<String, Object> after = updateRecordEvent.getAfter();
            before = before == null ? after : before;
            list.add(buildJsonMap(table, before, true, recordEvent));
            list.add(buildJsonMap(table, after, false, recordEvent));
        } else {
            final TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
            final Map<String, Object> before = deleteRecordEvent.getBefore();
            list.add(buildJsonMap(table, before, true, recordEvent));
        }
        return list;
    }

    public static HashMap buildJsonMap(TapTable table, Map<String, Object> values, boolean delete, TapRecordEvent recordEvent) {
        HashMap<String, Object> jsonName = new HashMap<>();
        if (MapUtils.isNotEmpty(values)) {
            final Map<String, TapField> tapFieldMap = table.getNameFieldMap();
            for (final Map.Entry<String, TapField> entry : tapFieldMap.entrySet()) {
                if (values.containsKey(entry.getKey())) {
                    Object o = values.get(entry.getKey());
                    if (o instanceof Timestamp) {
                        jsonName.put(entry.getKey(), o.toString());
                    } else if (o instanceof Date) {
                        jsonName.put(entry.getKey(), o.toString());
                    } else {
                        jsonName.put(entry.getKey(), values.get(entry.getKey()));
                    }
                }
            }
            jsonName.put("__DORIS_DELETE_SIGN__", delete ? "1" : "0");
        } else {
            throw new CoreException("Build json map failed, values is empty, record {} class {}", InstanceFactory.instance(JsonParser.class).toJson(recordEvent), recordEvent.getClass().getSimpleName());
        }
        return jsonName;
    }
}
