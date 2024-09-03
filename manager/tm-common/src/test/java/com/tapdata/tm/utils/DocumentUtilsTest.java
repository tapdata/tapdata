package com.tapdata.tm.utils;

import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author samuel
 * @Description
 * @create 2024-09-03 18:55
 **/
@DisplayName("Class DocumentUtils Test")
class DocumentUtilsTest {
	@Nested
	@DisplayName("Method getLong test")
	class getLongTest {
		@Test
		@DisplayName("test when value is long type")
		void test1() {
			Document document = new Document("test", 1L);
			assertEquals(1L, DocumentUtils.getLong(document, "test"));
		}

		@Test
		@DisplayName("test when value is integer type")
		void test2() {
			Document document = new Document("test", 1);
			assertEquals(1L, DocumentUtils.getLong(document, "test"));
		}

		@Test
		@DisplayName("test when value is null")
		void test3() {
			Document document = new Document("test", null);
			assertEquals(0L, DocumentUtils.getLong(document, "test"));
		}

		@Test
		@DisplayName("test when document is null")
		void test4() {
			assertEquals(0L, DocumentUtils.getLong(null, "test"));
		}

		@Test
		@DisplayName("test when key is null")
		void test5() {
			Document document = new Document("test", 1L);
			assertEquals(0L, DocumentUtils.getLong(document, null));
		}

		@Test
		@DisplayName("test when value is string type")
		void test6() {
			Document document = new Document("test", "1");
			assertEquals(0L, DocumentUtils.getLong(document, "test"));
		}
	}
}