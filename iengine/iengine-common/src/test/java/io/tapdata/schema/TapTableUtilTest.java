package io.tapdata.schema;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2024-12-18 17:48
 **/
@DisplayName("Class io.tapdata.schema.TapTableUtil Test")
class TapTableUtilTest {
	@Nested
	@DisplayName("Method sortFieldMap test")
	class sortFieldMapTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();
			fieldMap.put("f5", new TapField().name("f5").pos(null));
			fieldMap.put("f4", new TapField().name("f4").pos(null));
			fieldMap.put("f2", new TapField().name("f2").pos(2));
			fieldMap.put("f1", new TapField().name("f1").pos(1));
			fieldMap.put("f3", new TapField().name("f3").pos(3));
			fieldMap.put("f6", new TapField().name("f6").pos(null));
			TapTable tapTable = new TapTable("test");
			tapTable.setNameFieldMap(fieldMap);
			TapTableUtil.sortFieldMap(tapTable);
			LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
			assertNotSame(fieldMap, nameFieldMap);
			Iterator<TapField> iterator = nameFieldMap.values().iterator();
			assertEquals("f5", iterator.next().getName());
			assertEquals("f4", iterator.next().getName());
			assertEquals("f6", iterator.next().getName());
			assertEquals("f1", iterator.next().getName());
			assertEquals("f2", iterator.next().getName());
			assertEquals("f3", iterator.next().getName());
		}

		@Test
		@DisplayName("test empty name field map")
		void test2() {
			TapTable tapTable = new TapTable("test");
			assertDoesNotThrow(() -> TapTableUtil.sortFieldMap(tapTable));
			assertNull(tapTable.getNameFieldMap());
			tapTable.setNameFieldMap(new LinkedHashMap<>());
			assertDoesNotThrow(() -> TapTableUtil.sortFieldMap(tapTable));
		}
	}
}