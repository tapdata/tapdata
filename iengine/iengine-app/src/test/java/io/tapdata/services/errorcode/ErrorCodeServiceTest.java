package io.tapdata.services.errorcode;

import io.tapdata.ErrorCodeConfig;
import io.tapdata.ErrorCodeEntity;
import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-10-30 15:25
 **/
@DisplayName("Class ErrorCodeService Test")
class ErrorCodeServiceTest {
	private ErrorCodeService errorCodeService;

	@BeforeEach
	void setUp() {
		errorCodeService = mock(ErrorCodeService.class);
	}

	@Nested
	@DisplayName("Method getErrorCodeWithDynamic test")
	class getErrorCodeWithDynamicTest {
		@BeforeEach
		void setUp() {
			when(errorCodeService.getDynamicDescribe(any(), any())).thenCallRealMethod();
			when(errorCodeService.getErrorCodeWithDynamic(anyString(), anyString(), any(String[].class))).thenCallRealMethod();
		}

		@Test
		@DisplayName("test main process, language: en")
		void test1() {
			try (
					MockedStatic<ErrorCodeConfig> errorCodeConfigMockedStatic = mockStatic(ErrorCodeConfig.class)
			) {
				ErrorCodeConfig errorCodeConfig = mock(ErrorCodeConfig.class);
				errorCodeConfigMockedStatic.when(ErrorCodeConfig::getInstance).thenReturn(errorCodeConfig);
				ErrorCodeEntity errorCodeEntity = ErrorCodeEntity.create()
						.code(ErrorCodeTestClass.TEST_ERROR)
						.describe("describe")
						.solution("solution")
						.dynamicDescription("dynamic description: {}")
						.seeAlso(new String[]{"see also"})
						.sourceExClass(ErrorCodeTestClass.class);
				when(errorCodeConfig.getErrorCode(ErrorCodeTestClass.TEST_ERROR)).thenReturn(errorCodeEntity);
				Map<String, Object> result = errorCodeService.getErrorCodeWithDynamic(ErrorCodeTestClass.TEST_ERROR, "en", new String[]{"xxx"});
				assertNotNull(result);
				assertFalse(result.isEmpty());
				assertInstanceOf(String.class, result.get("errorCode"));
				assertInstanceOf(String.class, result.get("fullErrorCode"));
				assertInstanceOf(String.class, result.get("module"));
				assertInstanceOf(Integer.class, result.get("moduleCode"));
				assertInstanceOf(String.class, result.get("describe"));
				assertInstanceOf(String[].class, result.get("seeAlso"));
				assertInstanceOf(String.class, result.get("dynamicDescribe"));
				assertEquals(errorCodeEntity.getDescribe(), result.get("describe"));
				assertEquals("dynamic description: xxx", result.get("dynamicDescribe"));
			}
		}

		@Test
		@DisplayName("test main process, language: cn")
		void test2() {
			try (
					MockedStatic<ErrorCodeConfig> errorCodeConfigMockedStatic = mockStatic(ErrorCodeConfig.class)
			) {
				ErrorCodeConfig errorCodeConfig = mock(ErrorCodeConfig.class);
				errorCodeConfigMockedStatic.when(ErrorCodeConfig::getInstance).thenReturn(errorCodeConfig);
				ErrorCodeEntity errorCodeEntity = ErrorCodeEntity.create()
						.code(ErrorCodeTestClass.TEST_ERROR)
						.describeCN("描述")
						.solutionCN("解决方案")
						.dynamicDescriptionCN("动态描述: {}")
						.seeAlso(new String[]{"see also"})
						.sourceExClass(ErrorCodeTestClass.class);
				when(errorCodeConfig.getErrorCode(ErrorCodeTestClass.TEST_ERROR)).thenReturn(errorCodeEntity);
				Map<String, Object> result = errorCodeService.getErrorCodeWithDynamic(ErrorCodeTestClass.TEST_ERROR, "cn", new String[]{"xxx"});
				assertNotNull(result);
				assertFalse(result.isEmpty());
				assertInstanceOf(String.class, result.get("errorCode"));
				assertInstanceOf(String.class, result.get("fullErrorCode"));
				assertInstanceOf(String.class, result.get("module"));
				assertInstanceOf(Integer.class, result.get("moduleCode"));
				assertInstanceOf(String.class, result.get("describe"));
				assertInstanceOf(String[].class, result.get("seeAlso"));
				assertInstanceOf(String.class, result.get("dynamicDescribe"));
				assertEquals(errorCodeEntity.getDescribeCN(), result.get("describe"));
				assertEquals("动态描述: xxx", result.get("dynamicDescribe"));
			}
		}

		@Test
		@DisplayName("test error code not found")
		void test3() {
			try (
					MockedStatic<ErrorCodeConfig> errorCodeConfigMockedStatic = mockStatic(ErrorCodeConfig.class)
			) {
				ErrorCodeConfig errorCodeConfig = mock(ErrorCodeConfig.class);
				errorCodeConfigMockedStatic.when(ErrorCodeConfig::getInstance).thenReturn(errorCodeConfig);
				when(errorCodeConfig.getErrorCode(ErrorCodeTestClass.TEST_ERROR)).thenReturn(null);
				Map<String, Object> result = errorCodeService.getErrorCodeWithDynamic(ErrorCodeTestClass.TEST_ERROR, "cn", new String[]{"xxx"});
				assertNotNull(result);
				assertTrue(result.isEmpty());
			}
		}
	}

	@Nested
	@DisplayName("Method getDynamicDescribe test")
	class getDynamicDescribeTest {
		@BeforeEach
		void setUp() {
			when(errorCodeService.getDynamicDescribe(any(), any())).thenCallRealMethod();
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			String[] dynamicDescriptionParameters = new String[]{"xxx", "yyy"};
			String dynamicDescribe = "Test dynamicDescribe: {}, {}";
			String result = errorCodeService.getDynamicDescribe(dynamicDescriptionParameters, dynamicDescribe);
			assertEquals("Test dynamicDescribe: xxx, yyy", result);
		}

		@Test
		@DisplayName("test dynamicDescriptionParameters is null")
		void test2() {
			String dynamicDescribe = "Test dynamicDescribe: {}, {}";
			String result = errorCodeService.getDynamicDescribe(null, dynamicDescribe);
			assertEquals("", result);
		}

		@Test
		@DisplayName("test dynamicDescriptionParameters is empty array")
		void test3() {
			String[] dynamicDescriptionParameters = new String[]{};
			String dynamicDescribe = "Test dynamicDescribe: {}, {}";
			String result = errorCodeService.getDynamicDescribe(dynamicDescriptionParameters, dynamicDescribe);
			assertEquals("", result);
		}

		@Test
		@DisplayName("test dynamicDescribe is null")
		void test4() {
			String[] dynamicDescriptionParameters = new String[]{"xxx", "yyy"};
			String result = errorCodeService.getDynamicDescribe(dynamicDescriptionParameters, null);
			assertEquals("", result);
		}

		@Test
		@DisplayName("test dynamicDescribe is empty string")
		void test5() {
			String[] dynamicDescriptionParameters = new String[]{"xxx", "yyy"};
			String result = errorCodeService.getDynamicDescribe(dynamicDescriptionParameters, "");
			assertEquals("", result);
		}
	}

	@TapExClass(code = 99999, module = "test", describe = "test", prefix = "test")
	interface ErrorCodeTestClass {
		String TEST_ERROR = "99999001";
	}
}