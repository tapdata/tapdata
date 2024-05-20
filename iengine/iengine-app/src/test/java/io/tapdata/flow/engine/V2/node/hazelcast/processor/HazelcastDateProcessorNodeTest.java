package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.error.TaskDateProcessorExCode_17;
import io.tapdata.exception.TapCodeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;

/**
 * @author samuel
 * @Description
 * @create 2024-05-18 17:45
 **/
@DisplayName("Class HazelcastDateProcessorNode Test")
class HazelcastDateProcessorNodeTest {

	private HazelcastDateProcessorNode hazelcastDateProcessorNode;

	@BeforeEach
	void setUp() {
		hazelcastDateProcessorNode = mock(HazelcastDateProcessorNode.class);
	}

	@Nested
	@DisplayName("Method addTime test")
	class addTimeTest {

		private String tableName;

		@BeforeEach
		void setUp() {
			doCallRealMethod().when(hazelcastDateProcessorNode).addTime(any(), any(), any(), any(), any(), any());
			tableName = "test";
		}

		@Test
		@DisplayName("test add 8 hours")
		void testAdd8Hours() {
			ReflectionTestUtils.setField(hazelcastDateProcessorNode, "add", true);
			ReflectionTestUtils.setField(hazelcastDateProcessorNode, "hours", 8);

			List<String> addTimeFields = new ArrayList<>();
			addTimeFields.add("_date");
			Map<String, Object> after = new HashMap<>();
			LocalDateTime localDateTime = LocalDateTime.of(2024, 5, 18, 23, 0, 0);
			DateTime dateTime = new DateTime(localDateTime);
			after.put("_date", dateTime);
			hazelcastDateProcessorNode.addTime(addTimeFields, after, tableName, "_date", "_date", dateTime);

			Object result = after.get("_date");
			assertInstanceOf(DateTime.class, result);
			DateTime resultDateTime = (DateTime) result;
			LocalDateTime resultLocalDateTime = LocalDateTime.ofInstant(resultDateTime.toInstant(), ZoneId.of("GMT"));
			assertEquals(19, resultLocalDateTime.getDayOfMonth());
			assertEquals(7, resultLocalDateTime.getHour());
		}

		@Test
		@DisplayName("test minus 8 hours")
		void testMinus8Hours() {
			ReflectionTestUtils.setField(hazelcastDateProcessorNode, "add", false);
			ReflectionTestUtils.setField(hazelcastDateProcessorNode, "hours", 8);

			List<String> addTimeFields = new ArrayList<>();
			addTimeFields.add("_date");
			Map<String, Object> after = new HashMap<>();
			LocalDateTime localDateTime = LocalDateTime.of(2024, 5, 18, 2, 0, 0);
			DateTime dateTime = new DateTime(localDateTime);
			after.put("_date", dateTime);
			hazelcastDateProcessorNode.addTime(addTimeFields, after, tableName, "_date", "_date", dateTime);

			Object result = after.get("_date");
			assertInstanceOf(DateTime.class, result);
			DateTime resultDateTime = (DateTime) result;
			LocalDateTime resultLocalDateTime = LocalDateTime.ofInstant(resultDateTime.toInstant(), ZoneId.of("GMT"));
			assertEquals(17, resultLocalDateTime.getDayOfMonth());
			assertEquals(18, resultLocalDateTime.getHour());
		}

		@Test
		@DisplayName("test input a illegal DateTime")
		void testIllegalDate() {
			List<String> addTimeFields = new ArrayList<>();
			addTimeFields.add("_date");
			Map<String, Object> after = new HashMap<>();
			DateTime dateTime = new DateTime("0000-0-0-0-0-0", DateTime.DATETIME_TYPE);
			after.put("_date", dateTime);
			hazelcastDateProcessorNode.addTime(addTimeFields, after, tableName, "_date", "_date", dateTime);

			Object result = after.get("_date");
			assertSame(dateTime, result);
		}

		@Test
		@DisplayName("test input parameter is not of the DateTime type, for example, an Instant type is used as input")
		void testInputInstantType() {
			List<String> addTimeFields = new ArrayList<>();
			addTimeFields.add("_date");
			Map<String, Object> after = new HashMap<>();
			LocalDateTime localDateTime = LocalDateTime.of(2024, 5, 18, 2, 0, 0);
			DateTime dateTime = new DateTime(localDateTime);
			Instant instant = dateTime.toInstant();
			after.put("_date", instant);
			TapCodeException tapCodeException = assertThrows(TapCodeException.class,
					() -> hazelcastDateProcessorNode.addTime(addTimeFields, after, tableName, "_date", "_date", instant)
			);
			assertEquals(TaskDateProcessorExCode_17.SELECTED_TYPE_IS_NON_TIME, tapCodeException.getCode());
			System.out.println(tapCodeException.getMessage());
		}

		@Test
		@DisplayName("test input null")
		void testInputNull() {
			List<String> addTimeFields = new ArrayList<>();
			addTimeFields.add("_date");
			Map<String, Object> after = new HashMap<>();
			after.put("_date", null);
			hazelcastDateProcessorNode.addTime(addTimeFields, after, tableName, "_date", "_date", null);
			assertNull(after.get("_date"));
		}

		@Test
		@DisplayName("test parameter k is not included addTimeFields list")
		void testNotContainsAddTimeFields() {
			List<String> addTimeFields = new ArrayList<>();
			Map<String, Object> after = new HashMap<>();
			LocalDateTime localDateTime = LocalDateTime.of(2024, 5, 18, 2, 0, 0);
			DateTime dateTime = new DateTime(localDateTime);
			after.put("_date", dateTime);
			hazelcastDateProcessorNode.addTime(addTimeFields, after, tableName, "_date", "_date", dateTime);
			assertSame(dateTime, after.get("_date"));
		}
	}
}