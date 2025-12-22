package com.tapdata.tm.config.security;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * JsonToFormRequestWrapper 测试类
 * 
 * @author test
 */
@ExtendWith(MockitoExtension.class)
class JsonToFormRequestWrapperTest {

    @Mock
    private HttpServletRequest originalRequest;

    private JsonToFormRequestWrapper wrapper;
    private Map<String, String> jsonBody;

    @BeforeEach
    void setUp() {
        jsonBody = new HashMap<>();
    }

    @Nested
    @DisplayName("构造函数测试")
    class ConstructorTest {

        @Test
        @DisplayName("应该成功创建包装器实例")
        void shouldCreateWrapperSuccessfully() {
            // 准备
            jsonBody.put("username", "test");
            jsonBody.put("password", "123456");

            // 执行
            wrapper = new JsonToFormRequestWrapper(originalRequest, jsonBody);

            // 验证
            assertNotNull(wrapper);
        }

        @Test
        @DisplayName("应该接受空的 JSON body")
        void shouldAcceptEmptyJsonBody() {
            // 执行
            wrapper = new JsonToFormRequestWrapper(originalRequest, jsonBody);

            // 验证
            assertNotNull(wrapper);
        }

        @Test
        @DisplayName("应该正确转换 JSON body 为参数")
        void shouldConvertJsonBodyToParameters() {
            // 准备
            jsonBody.put("grant_type", "password");
            jsonBody.put("username", "admin");
            jsonBody.put("password", "secret");

            // 执行
            wrapper = new JsonToFormRequestWrapper(originalRequest, jsonBody);

            // 验证
            assertEquals("password", wrapper.getParameter("grant_type"));
            assertEquals("admin", wrapper.getParameter("username"));
            assertEquals("secret", wrapper.getParameter("password"));
        }
    }

    @Nested
    @DisplayName("getParameter 方法测试")
    class GetParameterTest {

        @BeforeEach
        void setUp() {
            jsonBody.put("client_id", "test-client");
            jsonBody.put("client_secret", "test-secret");
            wrapper = new JsonToFormRequestWrapper(originalRequest, jsonBody);
        }

        @Test
        @DisplayName("应该返回 JSON body 中的参数值")
        void shouldReturnParameterFromJsonBody() {
            // 执行 & 验证
            assertEquals("test-client", wrapper.getParameter("client_id"));
            assertEquals("test-secret", wrapper.getParameter("client_secret"));
        }

        @Test
        @DisplayName("当参数不存在于 JSON body 时，应该从原始请求获取")
        void shouldFallbackToOriginalRequestWhenParameterNotInJsonBody() {
            // 准备
            when(originalRequest.getParameter("other_param")).thenReturn("other_value");

            // 执行
            String result = wrapper.getParameter("other_param");

            // 验证
            assertEquals("other_value", result);
            verify(originalRequest).getParameter("other_param");
        }

        @Test
        @DisplayName("当参数不存在时，应该返回 null")
        void shouldReturnNullWhenParameterNotExists() {
            // 准备
            when(originalRequest.getParameter("non_existent")).thenReturn(null);

            // 执行
            String result = wrapper.getParameter("non_existent");

            // 验证
            assertNull(result);
        }

    }

    @Nested
    @DisplayName("getParameterMap 方法测试")
    class GetParameterMapTest {

        @BeforeEach
        void setUp() {
            jsonBody.put("username", "testuser");
            jsonBody.put("password", "testpass");
            wrapper = new JsonToFormRequestWrapper(originalRequest, jsonBody);
        }

        @Test
        @DisplayName("应该返回包含 JSON body 参数的 Map")
        void shouldReturnMapWithJsonBodyParameters() {
            // 执行
            Map<String, String[]> paramMap = wrapper.getParameterMap();

            // 验证
            assertNotNull(paramMap);
            assertTrue(paramMap.containsKey("username"));
            assertTrue(paramMap.containsKey("password"));
            assertArrayEquals(new String[]{"testuser"}, paramMap.get("username"));
            assertArrayEquals(new String[]{"testpass"}, paramMap.get("password"));
        }

        @Test
        @DisplayName("应该合并原始请求的参数")
        void shouldMergeOriginalRequestParameters() {
            // 准备
            Map<String, String[]> originalParams = new HashMap<>();
            originalParams.put("original_param", new String[]{"original_value"});
            when(originalRequest.getParameterMap()).thenReturn(originalParams);

            // 执行
            Map<String, String[]> paramMap = wrapper.getParameterMap();

            // 验证
            assertTrue(paramMap.containsKey("original_param"));
            assertArrayEquals(new String[]{"original_value"}, paramMap.get("original_param"));
            assertTrue(paramMap.containsKey("username"));
            assertTrue(paramMap.containsKey("password"));
        }

        @Test
        @DisplayName("JSON body 参数应该覆盖原始请求的同名参数")
        void shouldOverrideOriginalParametersWithJsonBody() {
            // 准备
            Map<String, String[]> originalParams = new HashMap<>();
            originalParams.put("username", new String[]{"original_user"});
            when(originalRequest.getParameterMap()).thenReturn(originalParams);

            // 执行
            Map<String, String[]> paramMap = wrapper.getParameterMap();

            // 验证
            assertArrayEquals(new String[]{"testuser"}, paramMap.get("username"), 
                "JSON body 中的参数应该覆盖原始请求的参数");
        }

        @Test
        @DisplayName("当原始请求没有参数时，应该只返回 JSON body 参数")
        void shouldReturnOnlyJsonBodyParametersWhenOriginalHasNone() {
            // 准备
            when(originalRequest.getParameterMap()).thenReturn(new HashMap<>());

            // 执行
            Map<String, String[]> paramMap = wrapper.getParameterMap();

            // 验证
            assertEquals(2, paramMap.size());
            assertTrue(paramMap.containsKey("username"));
            assertTrue(paramMap.containsKey("password"));
        }
    }

    @Nested
    @DisplayName("getParameterNames 方法测试")
    class GetParameterNamesTest {

        @BeforeEach
        void setUp() {
            jsonBody.put("param1", "value1");
            jsonBody.put("param2", "value2");
            wrapper = new JsonToFormRequestWrapper(originalRequest, jsonBody);
        }

        @Test
        @DisplayName("应该返回所有参数名称的枚举")
        void shouldReturnEnumerationOfAllParameterNames() {
            // 准备
            when(originalRequest.getParameterMap()).thenReturn(new HashMap<>());

            // 执行
            Enumeration<String> names = wrapper.getParameterNames();

            // 验证
            assertNotNull(names);
            Set<String> nameSet = new HashSet<>();
            while (names.hasMoreElements()) {
                nameSet.add(names.nextElement());
            }
            assertTrue(nameSet.contains("param1"));
            assertTrue(nameSet.contains("param2"));
        }

        @Test
        @DisplayName("应该包含原始请求和 JSON body 的所有参数名称")
        void shouldIncludeBothOriginalAndJsonBodyParameterNames() {
            // 准备
            Map<String, String[]> originalParams = new HashMap<>();
            originalParams.put("original_param", new String[]{"value"});
            when(originalRequest.getParameterMap()).thenReturn(originalParams);

            // 执行
            Enumeration<String> names = wrapper.getParameterNames();

            // 验证
            Set<String> nameSet = new HashSet<>();
            while (names.hasMoreElements()) {
                nameSet.add(names.nextElement());
            }
            assertTrue(nameSet.contains("param1"));
            assertTrue(nameSet.contains("param2"));
            assertTrue(nameSet.contains("original_param"));
        }

        @Test
        @DisplayName("当没有参数时，应该返回空枚举")
        void shouldReturnEmptyEnumerationWhenNoParameters() {
            // 准备
            wrapper = new JsonToFormRequestWrapper(originalRequest, new HashMap<>());
            when(originalRequest.getParameterMap()).thenReturn(new HashMap<>());

            // 执行
            Enumeration<String> names = wrapper.getParameterNames();

            // 验证
            assertNotNull(names);
            assertFalse(names.hasMoreElements());
        }
    }

    @Nested
    @DisplayName("getParameterValues 方法测试")
    class GetParameterValuesTest {

        @BeforeEach
        void setUp() {
            jsonBody.put("username", "testuser");
            wrapper = new JsonToFormRequestWrapper(originalRequest, jsonBody);
        }

        @Test
        @DisplayName("应该返回参数值数组")
        void shouldReturnParameterValuesArray() {
            // 执行
            String[] values = wrapper.getParameterValues("username");

            // 验证
            assertNotNull(values);
            assertEquals(1, values.length);
            assertEquals("testuser", values[0]);
        }

        @Test
        @DisplayName("应该从原始请求获取不在 JSON body 中的参数值")
        void shouldGetValuesFromOriginalRequestWhenNotInJsonBody() {
            // 准备
            Map<String, String[]> originalParams = new HashMap<>();
            originalParams.put("other_param", new String[]{"value1", "value2"});
            when(originalRequest.getParameterMap()).thenReturn(originalParams);

            // 执行
            String[] values = wrapper.getParameterValues("other_param");

            // 验证
            assertNotNull(values);
            assertEquals(2, values.length);
            assertArrayEquals(new String[]{"value1", "value2"}, values);
        }

        @Test
        @DisplayName("当参数不存在时，应该返回 null")
        void shouldReturnNullWhenParameterNotExists() {
            // 准备
            when(originalRequest.getParameterMap()).thenReturn(new HashMap<>());

            // 执行
            String[] values = wrapper.getParameterValues("non_existent");

            // 验证
            assertNull(values);
        }
    }

    @Nested
    @DisplayName("特殊字符和边界条件测试")
    class SpecialCasesTest {

        @Test
        @DisplayName("应该正确处理包含特殊字符的参数值")
        void shouldHandleSpecialCharactersInValues() {
            // 准备
            jsonBody.put("email", "test@example.com");
            jsonBody.put("password", "p@ss!w0rd#123");
            wrapper = new JsonToFormRequestWrapper(originalRequest, jsonBody);

            // 执行 & 验证
            assertEquals("test@example.com", wrapper.getParameter("email"));
            assertEquals("p@ss!w0rd#123", wrapper.getParameter("password"));
        }

        @Test
        @DisplayName("应该正确处理包含中文字符的参数值")
        void shouldHandleChineseCharactersInValues() {
            // 准备
            jsonBody.put("name", "测试用户");
            jsonBody.put("description", "这是一个测试描述");
            wrapper = new JsonToFormRequestWrapper(originalRequest, jsonBody);

            // 执行 & 验证
            assertEquals("测试用户", wrapper.getParameter("name"));
            assertEquals("这是一个测试描述", wrapper.getParameter("description"));
        }

        @Test
        @DisplayName("应该正确处理空字符串值")
        void shouldHandleEmptyStringValues() {
            // 准备
            jsonBody.put("empty_param", "");
            wrapper = new JsonToFormRequestWrapper(originalRequest, jsonBody);

            // 执行 & 验证
            assertEquals("", wrapper.getParameter("empty_param"));
        }

        @Test
        @DisplayName("应该正确处理包含空格的参数值")
        void shouldHandleValuesWithSpaces() {
            // 准备
            jsonBody.put("description", "This is a test description");
            wrapper = new JsonToFormRequestWrapper(originalRequest, jsonBody);

            // 执行 & 验证
            assertEquals("This is a test description", wrapper.getParameter("description"));
        }

        @Test
        @DisplayName("应该正确处理 URL 编码字符")
        void shouldHandleUrlEncodedCharacters() {
            // 准备
            jsonBody.put("redirect_uri", "http://localhost:3000/callback?code=123");
            wrapper = new JsonToFormRequestWrapper(originalRequest, jsonBody);

            // 执行 & 验证
            assertEquals("http://localhost:3000/callback?code=123", 
                wrapper.getParameter("redirect_uri"));
        }
    }
}

