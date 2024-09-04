package com.tapdata.tm.utils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.mapping.Document;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author samuel
 * @Description
 * @create 2024-09-04 16:40
 **/
@DisplayName("Class EntityUtils Test")
class EntityUtilsTest {
	@Nested
	@DisplayName("Method documentAnnotationValue test")
	@Document("documentAnnotationValueTest")
	class documentAnnotationValueTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			assertEquals("documentAnnotationValueTest", EntityUtils.documentAnnotationValue(documentAnnotationValueTest.class));
		}

		@Test
		@DisplayName("test input class is null")
		void test2() {
			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> EntityUtils.documentAnnotationValue(null));
			assertEquals("Input class is null", exception.getMessage());
		}

		@Test
		@DisplayName("test class not have a org.springframework.data.mongodb.core.mapping.Document annotation")
		void test3() {
			IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> EntityUtils.documentAnnotationValue(EntityUtilsTest.class));
			assertEquals("Class %s not have a org.springframework.data.mongodb.core.mapping.Document annotation", exception.getMessage());
		}
	}
}