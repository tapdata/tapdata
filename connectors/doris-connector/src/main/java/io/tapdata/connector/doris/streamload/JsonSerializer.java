package io.tapdata.connector.doris.streamload;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-12-26 12:38
 **/
public class JsonSerializer implements MessageSerializer {

	public static final String LINE_END = ",";
	private ObjectMapper objectMapper;

	public JsonSerializer() {
		objectMapper = new ObjectMapper();
	}

	@Override
	public byte[] serialize(TapTable table, TapRecordEvent recordEvent) throws Throwable {
		String jsonString;
		if (recordEvent instanceof TapInsertRecordEvent) {
			final TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
			final Map<String, Object> after = insertRecordEvent.getAfter();
			jsonString = toJsonString(table, after, false);
		} else if (recordEvent instanceof TapUpdateRecordEvent) {
			final TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
			Map<String, Object> after = updateRecordEvent.getAfter();
			Map<String, Object> before = updateRecordEvent.getBefore();
			before = before == null ? after : before;
			jsonString = toJsonString(table, before, true);
			jsonString += LINE_END;
			jsonString += toJsonString(table, after, false);
		} else {
			final TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
			final Map<String, Object> before = deleteRecordEvent.getBefore();
			jsonString = toJsonString(table, before, true);
		}
		return jsonString.getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public byte[] lineEnd() {
		return LINE_END.getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public byte[] batchStart() {
		return "[".getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public byte[] batchEnd() {
		return "]".getBytes(StandardCharsets.UTF_8);
	}

	private String toJsonString(TapTable tapTable, Map<String, Object> record, boolean delete) throws JsonProcessingException {
		if (null == tapTable) throw new IllegalArgumentException("TapTable cannot be null");
		if (null == record) throw new IllegalArgumentException("Record cannot be null");
		LinkedHashMap<String, Object> linkedRecord = new LinkedHashMap<>();
		for (String field : tapTable.getNameFieldMap().keySet()) {
			Object value = record.get(field);
			if (null == value) {
				linkedRecord.put(field, null);
			} else {
				linkedRecord.put(field, value.toString());
			}
		}
		linkedRecord.put(Constants.DORIS_DELETE_SIGN, delete ? 1 : 0);
		return objectMapper.writeValueAsString(linkedRecord);
	}
}
