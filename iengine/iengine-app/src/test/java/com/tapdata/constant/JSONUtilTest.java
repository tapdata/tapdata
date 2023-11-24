package com.tapdata.constant;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.time.Instant;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2023-11-21 21:11
 **/
class JSONUtilTest {

	@Test
	void testJson2POJO() {
		URL url = JSONUtilTest.class.getClassLoader().getResource("constant/jsonutil/test.json");
		JsonUtilTestPojo jsonUtilTestPojo = assertDoesNotThrow(() -> JSONUtil.json2POJO(url, JsonUtilTestPojo.class));
		assertEquals(1, jsonUtilTestPojo.intValue);
		assertEquals(1L, jsonUtilTestPojo.longValue);
		assertEquals("test", jsonUtilTestPojo.strValue);
		assertEquals(1.11D, jsonUtilTestPojo.doubleValue);
		assertNull(jsonUtilTestPojo.nullValue);
		assertEquals(Instant.parse("2023-01-01T15:59:59Z"), jsonUtilTestPojo.dateValue.toInstant());
	}

	static class JsonUtilTestPojo {
		private Integer intValue;
		private String strValue;
		private Long longValue;
		private Date dateValue;
		private Double doubleValue;
		private Object nullValue;

		public JsonUtilTestPojo() {
		}

		public void setIntValue(Integer intValue) {
			this.intValue = intValue;
		}

		public void setStrValue(String strValue) {
			this.strValue = strValue;
		}

		public void setLongValue(Long longValue) {
			this.longValue = longValue;
		}

		public void setDateValue(Date dateValue) {
			this.dateValue = dateValue;
		}

		public void setDoubleValue(Double doubleValue) {
			this.doubleValue = doubleValue;
		}

		public void setNullValue(Object nullValue) {
			this.nullValue = nullValue;
		}
	}
}
