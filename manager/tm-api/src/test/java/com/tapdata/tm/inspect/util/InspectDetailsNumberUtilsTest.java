package com.tapdata.tm.inspect.util;

import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class InspectDetailsNumberUtilsTest {

	@Test
	void testLong() {
		assertEquals(9007199254740991L, InspectDetailsNumberUtils.toJsSafeValue(9007199254740991L));
		assertEquals(-9007199254740991L, InspectDetailsNumberUtils.toJsSafeValue(-9007199254740991L));
		assertEquals("9007199254740992", InspectDetailsNumberUtils.toJsSafeValue(9007199254740992L));
		assertEquals("9223372036854775806", InspectDetailsNumberUtils.toJsSafeValue(9223372036854775806L));
		assertEquals("-9223372036854775808", InspectDetailsNumberUtils.toJsSafeValue(Long.MIN_VALUE));
		assertEquals(5, InspectDetailsNumberUtils.toJsSafeValue(5));
	}

	@Test
	void testBigInteger() {
		assertEquals(BigInteger.valueOf(123), InspectDetailsNumberUtils.toJsSafeValue(BigInteger.valueOf(123)));
		assertEquals("18446744073709551615", InspectDetailsNumberUtils.toJsSafeValue(new BigInteger("18446744073709551615")));
	}

	@Test
	void testBigDecimal() {
		assertEquals(new BigDecimal("123456789.123456"), InspectDetailsNumberUtils.toJsSafeValue(new BigDecimal("123456789.123456")));
		assertEquals(new BigDecimal("1.10000000000000000000"), InspectDetailsNumberUtils.toJsSafeValue(new BigDecimal("1.10000000000000000000")));
		assertEquals("1234509876501234509", InspectDetailsNumberUtils.toJsSafeValue(new BigDecimal("1234509876501234509")));
		assertEquals("123456789.12345678", InspectDetailsNumberUtils.toJsSafeValue(new BigDecimal("123456789.12345678")));
		assertEquals("12345678901234567890", InspectDetailsNumberUtils.toJsSafeValue(new BigDecimal("1.234567890123456789E+19")));
	}

	@Test
	void testDecimal128() {
		assertEquals(new BigDecimal("123.45"), InspectDetailsNumberUtils.toJsSafeValue(Decimal128.parse("123.45")));
		assertEquals("1234509876501234509", InspectDetailsNumberUtils.toJsSafeValue(Decimal128.parse("1234509876501234509")));
		assertEquals("NaN", InspectDetailsNumberUtils.toJsSafeValue(Decimal128.NaN));
		assertEquals("-0", InspectDetailsNumberUtils.toJsSafeValue(Decimal128.NEGATIVE_ZERO));
	}

	@Test
	void testOtherTypesUnchanged() {
		Double d = 1.2345098765012344E18;
		assertSame(d, InspectDetailsNumberUtils.toJsSafeValue(d));
		assertEquals("1234509876501234509", InspectDetailsNumberUtils.toJsSafeValue("1234509876501234509"));
		assertNull(InspectDetailsNumberUtils.toJsSafeValue(null));
	}

	@Test
	void testNested() {
		Map<String, Object> nested = new HashMap<>();
		nested.put("big", 9223372036854775806L);
		nested.put("small", 1L);
		Object result = InspectDetailsNumberUtils.toJsSafeValue(Arrays.asList(nested, 9223372036854775806L));
		List<Object> expected = new ArrayList<>();
		Map<String, Object> expectedNested = new HashMap<>();
		expectedNested.put("big", "9223372036854775806");
		expectedNested.put("small", 1L);
		expected.add(expectedNested);
		expected.add("9223372036854775806");
		assertEquals(expected, result);
	}

	@Test
	void testInspectDetailsDto() {
		Map<String, Object> source = new LinkedHashMap<>();
		source.put("id", 6);
		source.put("data", 1234509876501234509L);
		InspectDetailsDto dto = new InspectDetailsDto();
		dto.setSource(source);
		dto.setTarget(null);

		InspectDetailsNumberUtils.toJsSafe(Arrays.asList(dto, null));
		assertEquals(6, dto.getSource().get("id"));
		assertEquals("1234509876501234509", dto.getSource().get("data"));
		assertNull(dto.getTarget());
		// 不修改原始 Map
		assertEquals(1234509876501234509L, source.get("data"));

		assertNull(InspectDetailsNumberUtils.toJsSafe((InspectDetailsDto) null));
		assertNull(InspectDetailsNumberUtils.toJsSafe((List<InspectDetailsDto>) null));
	}
}
