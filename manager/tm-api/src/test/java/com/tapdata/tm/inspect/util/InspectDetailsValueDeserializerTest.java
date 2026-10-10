package com.tapdata.tm.inspect.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.inspect.dto.InspectDetailsDto;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class InspectDetailsValueDeserializerTest {
	private final ObjectMapper mapper = new ObjectMapper();

	@Test
	void testNumbers() throws Exception {
		InspectDetailsDto dto = mapper.readValue("{\"source\":{"
				+ "\"int\":5,\"long\":1234509876501234509,\"bigInt\":18446744073709551615,"
				+ "\"dbl\":123.456,\"dec17\":123456789.12345678,\"dec38\":0.123456789012345678,"
				+ "\"decInt\":1234509876501234509.0,\"exp\":1.5E300,\"huge\":1E400}}", InspectDetailsDto.class);
		Map<String, Object> source = dto.getSource();
		assertEquals(5, source.get("int"));
		assertEquals(1234509876501234509L, source.get("long"));
		assertEquals(new BigInteger("18446744073709551615"), source.get("bigInt"));
		// double 可无损表示，保持 Double
		assertEquals(123.456d, source.get("dbl"));
		assertEquals(123456789.12345678d, source.get("dec17"));
		assertEquals(1.5E300d, source.get("exp"));
		// double 有损，保留 BigDecimal
		assertEquals(new BigDecimal("0.123456789012345678"), source.get("dec38"));
		assertEquals(new BigDecimal("1234509876501234509.0"), source.get("decInt"));
		assertEquals(new BigDecimal("1E400"), source.get("huge"));
	}

	@Test
	void testOtherValues() throws Exception {
		InspectDetailsDto dto = mapper.readValue("{\"source\":{"
				+ "\"str\":\"s\",\"t\":true,\"f\":false,\"n\":null,"
				+ "\"nested\":{\"a\":0.123456789012345678,\"b\":[1,0.5,{\"c\":\"x\"}]},\"arr\":[]},"
				+ "\"target\":null,\"message\":\"Different fields:a\"}", InspectDetailsDto.class);
		Map<String, Object> source = dto.getSource();
		assertEquals("s", source.get("str"));
		assertEquals(Boolean.TRUE, source.get("t"));
		assertEquals(Boolean.FALSE, source.get("f"));
		assertNull(source.get("n"));
		assertEquals(true, source.containsKey("n"));
		Map<?, ?> nested = (Map<?, ?>) source.get("nested");
		assertEquals(new BigDecimal("0.123456789012345678"), nested.get("a"));
		List<?> b = (List<?>) nested.get("b");
		assertEquals(Arrays.asList(1, 0.5d), b.subList(0, 2));
		assertEquals("x", ((Map<?, ?>) b.get(2)).get("c"));
		assertEquals(Arrays.asList(), source.get("arr"));
		assertNull(dto.getTarget());
		assertEquals("Different fields:a", dto.getMessage());
	}
}
