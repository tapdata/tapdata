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

class CustomQueryInspectContractTest {
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Test
	void customQueryConfigurationSurvivesJsonRoundTrip() throws Exception {
		InspectTask task = new InspectTask();
		InspectDataSource source = new InspectDataSource();
		source.setCustomQuery("select account_id, total from account_totals");
		task.setSource(source);
		task.setFieldMapping(Map.of("account_id", "id", "total", "amount"));
		task.setSortFields(List.of("account_id"));

		String json = objectMapper.writeValueAsString(task);
		InspectTask restored = objectMapper.readValue(json, InspectTask.class);

		assertEquals("select account_id, total from account_totals", restored.getSource().getCustomQuery());
		assertEquals(Map.of("account_id", "id", "total", "amount"), restored.getFieldMapping());
		assertEquals(List.of("account_id"), restored.getSortFields());
		assertEquals(InspectMethod.CUSTOM_QUERY, InspectMethod.get("custom_query"));
		assertEquals(InspectMethod.FIELD, InspectMethod.get("aggregate"));
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
