package com.tapdata.entity.inspect;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.error.TaskInspectExCode_27;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class CustomQueryInspectContractTest {
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Test
	void customQueryConfigurationSurvivesJsonRoundTrip() throws Exception {
		InspectTask task = new InspectTask();
		InspectDataSource source = new InspectDataSource();
		source.setConnectionId("source-connection");
		source.setCustomQuery("select account_id, total from account_totals");
		task.setSource(source);
		InspectDataSource target = new InspectDataSource();
		target.setConnectionId("target-connection");
		target.setCustomQuery("select account_id, total from target_totals");
		task.setTarget(target);
		task.setFieldMapping(Map.of("account_id", "id", "total", "amount"));
		task.setSortFields(List.of("account_id"));

		String json = objectMapper.writeValueAsString(task);
		InspectTask restored = objectMapper.readValue(json, InspectTask.class);

		assertEquals("select account_id, total from account_totals", restored.getSource().getCustomQuery());
		assertEquals("select account_id, total from target_totals", restored.getTarget().getCustomQuery());
		assertEquals("source-connection", restored.getSource().getConnectionId());
		assertEquals("target-connection", restored.getTarget().getConnectionId());
		assertEquals(Map.of("account_id", "id", "total", "amount"), restored.getFieldMapping());
		assertEquals(List.of("account_id"), restored.getSortFields());
		assertEquals(InspectMethod.CUSTOM_QUERY, InspectMethod.get("custom_query"));
		assertEquals(InspectMethod.FIELD, InspectMethod.get("aggregate"));
	}

	@Test
	void legacyInspectTaskWithoutCustomQueryFieldsRemainsReadable() throws Exception {
		InspectTask restored = objectMapper.readValue("{\"source\":{},\"target\":{}}", InspectTask.class);

		assertNull(restored.getFieldMapping());
		assertNull(restored.getSortFields());
		assertNull(restored.getSource().getCustomQuery());
		assertNull(restored.getTarget().getCustomQuery());
	}

	@Test
	void customQueryErrorCodesAreUniqueWithinInspectErrorCodeGroup() throws Exception {
		Set<String> codes = new HashSet<>();
		for (Field field : TaskInspectExCode_27.class.getDeclaredFields()) {
			if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
				assertFalse("".equals(field.get(null)), field.getName() + " must have a code");
				assertFalse(!codes.add((String) field.get(null)), "Duplicate inspect error code: " + field.get(null));
			}
		}

		assertEquals("27011", TaskInspectExCode_27.INSPECT_CUSTOM_QUERY_CONFIG_MISSING);
		assertEquals("27012", TaskInspectExCode_27.INSPECT_RESULT_EXCEEDS_LIMIT);
	}
}
