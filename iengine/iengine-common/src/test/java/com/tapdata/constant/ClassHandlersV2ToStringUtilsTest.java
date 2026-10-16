package com.tapdata.constant;

import io.tapdata.entity.schema.value.TapDoubleValue;
import io.tapdata.entity.schema.value.TapFloatValue;
import io.tapdata.entity.schema.value.TapMoneyValue;
import io.tapdata.entity.schema.value.TapNumberValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@DisplayName("Class ClassHandlersV2ToStringUtils Test")
class ClassHandlersV2ToStringUtilsTest {

	private static <V extends TapValue<?, ?>> V withOrigin(V tapValue, Object origin) {
		tapValue.setOriginValue(origin);
		return tapValue;
	}

	@Nested
	@DisplayName("Method recursiveHandleMap numeric TapValue test")
	class RecursiveHandleMapNumberTest {

		@Test
		@DisplayName("unwrap TapNumberValue to its origin value, keep long precision")
		void unwrapTapNumberValueToOrigin() {
			Map<String, Object> map = new HashMap<>();
			map.put("diff_ms", withOrigin(new TapNumberValue(3600000d), 3600000L));
			map.put("big", withOrigin(new TapNumberValue(9007199254740993d), 9007199254740993L));
			map.put("age", withOrigin(new TapNumberValue(18d), 18));

			ClassHandlersV2ToStringUtils.recursiveHandleMap(map);

			assertEquals(3600000L, map.get("diff_ms"));
			assertEquals(9007199254740993L, map.get("big"));
			assertEquals(18, map.get("age"));
		}

		@Test
		@DisplayName("unwrap TapDoubleValue / TapFloatValue / TapMoneyValue")
		void unwrapOtherNumericTapValues() {
			Map<String, Object> map = new HashMap<>();
			map.put("d", withOrigin(new TapDoubleValue(1.5d), 1.5d));
			map.put("f", withOrigin(new TapFloatValue(2.5d), 2.5f));
			map.put("m", withOrigin(new TapMoneyValue(9.99d), new BigDecimal("9.99")));

			ClassHandlersV2ToStringUtils.recursiveHandleMap(map);

			assertEquals(1.5d, map.get("d"));
			assertEquals(2.5f, map.get("f"));
			assertEquals(new BigDecimal("9.99"), map.get("m"));
		}

		@Test
		@DisplayName("fall back to value when origin value is absent or not a number")
		void fallbackToValue() {
			Map<String, Object> map = new HashMap<>();
			map.put("noOrigin", new TapNumberValue(7d));
			map.put("strOrigin", withOrigin(new TapNumberValue(8d), "8"));

			ClassHandlersV2ToStringUtils.recursiveHandleMap(map);

			assertEquals(7d, map.get("noOrigin"));
			assertEquals(8d, map.get("strOrigin"));
		}

		@Test
		@DisplayName("unwrap numeric TapValue nested in map and list")
		void unwrapNested() {
			Map<String, Object> inner = new HashMap<>();
			inner.put("id", withOrigin(new TapNumberValue(1d), 1L));
			List<Object> list = new ArrayList<>();
			list.add(withOrigin(new TapNumberValue(2d), 2L));
			list.add(new TapStringValue("s"));
			Map<String, Object> map = new HashMap<>();
			map.put("inner", inner);
			map.put("list", list);

			ClassHandlersV2ToStringUtils.recursiveHandleMap(map);

			assertEquals(1L, inner.get("id"));
			assertEquals(2L, list.get(0));
			assertEquals("s", list.get(1));
			assertFalse(map.values().stream().anyMatch(TapValue.class::isInstance));
		}
	}
}
