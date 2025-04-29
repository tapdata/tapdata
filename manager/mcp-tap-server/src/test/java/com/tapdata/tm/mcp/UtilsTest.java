package com.tapdata.tm.mcp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import io.modelcontextprotocol.server.McpAsyncServerExchange;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpServerSession;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.web.servlet.function.ServerRequest;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 15:06
 */
public class UtilsTest {

    @Test
    public void testToJson() {
        Map<String, Object> data = new HashMap<>();
        data.put("test", "test");
        data.put("id", 123);
        assertEquals("{\n" +
                "  \"test\" : \"test\",\n" +
                "  \"id\" : 123\n" +
                "}", Utils.toJson(data));

        assertEquals("[\n" +
                "  \"a\",\n" +
                "  \"b\"\n" +
                "]", Utils.toJson(Arrays.asList("a", "b")));

        record TestRecord(String name, int age) {
            public String getName() {
                throw new RuntimeException("Test");
            }
        }
        assertThrows(RuntimeException.class, () -> {
            Map<String, TestRecord> dataMap = new HashMap<>();
            dataMap.put("test", new TestRecord("test", 123));
            String json = Utils.toJson(dataMap);
            System.out.println(json);
        });
    }

    @Test
    void testGetStringValue() {
        Map<String, Object> params = new HashMap<>();
        params.put("key1", "value1");
        params.put("key2", null);
        params.put("key3", 123);

        // 测试正常字符串值
        assertEquals("value1", Utils.getStringValue(params, "key1"));
        
        // 测试空值
        assertNull(Utils.getStringValue(params, "key2"));
        
        // 测试不存在的键
        assertNull(Utils.getStringValue(params, "nonexistent"));
        
        // 测试带默认值的方法
        assertEquals("default", Utils.getStringValue(params, "nonexistent", "default"));
        assertEquals("value1", Utils.getStringValue(params, "key1", "default"));
        
        // 测试数字转字符串
        assertEquals("123", Utils.getStringValue(params, "key3"));
    }

    @Test
    void testGetIntegerValue() {
        Map<String, Object> params = new HashMap<>();
        params.put("int1", 123);
        params.put("int2", "456");
        params.put("invalid", "abc");
        params.put("null", null);

        // 测试整数值
        assertEquals(123, Utils.getIntegerValue(params, "int1"));
        
        // 测试字符串数字
        assertEquals(456, Utils.getIntegerValue(params, "int2"));
        
        // 测试空值
        assertNull(Utils.getIntegerValue(params, "null"));
        
        // 测试不存在的键
        assertNull(Utils.getIntegerValue(params, "nonexistent"));
        
        // 测试无效数字格式
        assertThrows(NumberFormatException.class, () -> Utils.getIntegerValue(params, "invalid"));
    }

    @Test
    void testGetLongValue() {
        Map<String, Object> params = new HashMap<>();
        params.put("long1", 123L);
        params.put("long2", "456");
        params.put("invalid", "abc");
        params.put("null", null);

        // 测试长整型值
        assertEquals(123L, Utils.getLongValue(params, "long1"));
        
        // 测试字符串数字
        assertEquals(456L, Utils.getLongValue(params, "long2"));
        
        // 测试空值
        assertNull(Utils.getLongValue(params, "null"));
        
        // 测试不存在的键
        assertNull(Utils.getLongValue(params, "nonexistent"));
        
        // 测试无效数字格式
        assertThrows(NumberFormatException.class, () -> Utils.getLongValue(params, "invalid"));
    }

    @Test
    void testJsonOperations() {
        // 测试对象转JSON
        Map<String, Object> testData = new HashMap<>();
        testData.put("name", "test");
        testData.put("value", 123);
        
        String json = Utils.toJson(testData);
        assertNotNull(json);
        assertTrue(json.contains("\"name\""));
        assertTrue(json.contains("\"test\""));
        
        // 测试JSON转对象
        Map<String, Object> parsedData = Utils.parseJson(json, new TypeReference<Map<String, Object>>() {});
        assertEquals("test", parsedData.get("name"));
        assertEquals(123, parsedData.get("value"));
    }

    @Test
    void testReadConnection() {
        // 创建测试数据源实体
        DataSourceEntity ds = new DataSourceEntity();
        ds.setId(new ObjectId());
        ds.setName("TestDB");
        ds.setDatabase_type("MongoDB");
        ds.setConnection_type("Source");
        ds.setTableCount(10L);
        ds.setLoadSchemaTime(new Date());
        
        List<Map<String, String>> tags = new ArrayList<>();
        Map<String, String> tag = new HashMap<>();
        tag.put("value", "test-tag");
        tags.add(tag);
        ds.setListtags(tags);

        // 测试转换结果
        Map<String, Object> result = Utils.readConnection(ds);
        
        assertEquals(ds.getId().toHexString(), result.get("id"));
        assertEquals(ds.getName(), result.get("name"));
        assertEquals(ds.getDatabase_type(), result.get("databaseType"));
        assertEquals(ds.getConnection_type(), result.get("connectionType"));
        assertEquals(ds.getTableCount(), result.get("tableCount"));
        assertEquals(ds.getLoadSchemaTime(), result.get("loadSchemaTime"));
        
        List<?> resultTags = (List<?>) result.get("tags");
        assertNotNull(resultTags);
        assertEquals(1, resultTags.size());
        assertEquals("test-tag", resultTags.get(0));
    }

    @Test
    void testReadJsonSchema() throws IOException {
        Path testResourcePath = Paths.get("target", "classes");
        if (!Files.exists(testResourcePath)) {
            Files.createDirectories(testResourcePath);
        }

        // 创建测试JSON文件
        File testJsonFile = testResourcePath.resolve("test-schema.json").toFile();
        String jsonContent = """
                {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string"
                        },
                        "age": {
                            "type": "integer"
                        }
                    },
                    "required": ["name"]
                }
                """;
        Files.writeString(testJsonFile.toPath(), jsonContent);

        // 测试读取存在的schema文件
        String schema = Utils.readJsonSchema("test-schema.json");
        assertNotNull(schema);
        assertTrue(schema.contains("\"type\": \"object\""));
        assertTrue(schema.contains("\"properties\""));

        // 清理测试文件
        if (testJsonFile != null && testJsonFile.exists()) {
            Files.delete(testJsonFile.toPath());
        }
        
        // 测试读取不存在的schema文件
        String defaultSchema = Utils.readJsonSchema("nonexistent.json");
        assertNull(defaultSchema);
    }

    @Test
    void testGetSession() {
        // 创建模拟对象
        McpSyncServerExchange syncExchange = mock(McpSyncServerExchange.class);
        McpAsyncServerExchange asyncExchange = mock(McpAsyncServerExchange.class);
        McpServerSession session = mock(McpServerSession.class);

        // 设置模拟行为
        try {
            // 使用反射设置私有字段
            java.lang.reflect.Field exchangeField = syncExchange.getClass().getDeclaredField("exchange");
            exchangeField.setAccessible(true);
            exchangeField.set(syncExchange, asyncExchange);

            java.lang.reflect.Field sessionField = asyncExchange.getClass().getDeclaredField("session");
            sessionField.setAccessible(true);
            sessionField.set(asyncExchange, session);

            // 执行测试
            McpServerSession result = Utils.getSession(syncExchange);
            
            // 验证结果
            assertNotNull(result);
            assertEquals(session, result);
        } catch (Exception e) {
            fail("测试失败: " + e.getMessage());
        }
    }

    @Test
    void testParseJson() {
        // 测试解析简单对象
        String jsonObject = "{\"name\":\"test\",\"value\":123}";
        Map<String, Object> result = Utils.parseJson(jsonObject, new TypeReference<Map<String, Object>>() {});
        assertEquals("test", result.get("name"));
        assertEquals(123, result.get("value"));

        // 测试解析数组
        String jsonArray = "[{\"id\":1,\"name\":\"item1\"},{\"id\":2,\"name\":\"item2\"}]";
        List<Map<String, Object>> arrayResult = Utils.parseJson(jsonArray, new TypeReference<List<Map<String, Object>>>() {});
        assertEquals(2, arrayResult.size());
        assertEquals(1, arrayResult.get(0).get("id"));
        assertEquals("item2", arrayResult.get(1).get("name"));

        // 测试解析为具体类
        String jsonPerson = "{\"name\":\"John\",\"age\":30}";
        TestPerson person = Utils.parseJson(jsonPerson, TestPerson.class);
        assertEquals("John", person.getName());
        assertEquals(30, person.getAge());

        // 测试无效JSON
        String invalidJson = "{invalid json}";
        assertThrows(RuntimeException.class, () -> Utils.parseJson(invalidJson, new TypeReference<Map<String, Object>>() {}));

        record TestRecord(String name, int age) {
            public String getName() {
                throw new RuntimeException("Test");
            }
        }
        assertThrows(RuntimeException.class, () -> {
            TestRecord testRecord = Utils.parseJson("""
                    """, TestRecord.class);
            System.out.println(testRecord);
        });
    }

    @Test
    void testSendPostRequest() throws IOException, InterruptedException {
        // 创建 MockWebServer
        MockWebServer mockWebServer = new MockWebServer();
        
        try {
            // 启动服务器
            mockWebServer.start();
            String baseUrl = mockWebServer.url("/").toString();

            // 准备测试数据
            Map<String, Object> testData = new HashMap<>();
            testData.put("name", "test");
            testData.put("value", 123);

            // 测试场景1：成功响应
            mockWebServer.enqueue(new MockResponse()
                    .setResponseCode(200)
                    .setHeader("Content-Type", "application/json")
                    .setBody("{\"status\":\"success\"}"));

            String response = Utils.sendPostRequest(baseUrl + "success", testData);
            assertNotNull(response);
            assertEquals("{\"status\":\"success\"}", response);

            // 验证请求
            RecordedRequest request1 = mockWebServer.takeRequest(5, TimeUnit.SECONDS);
            assertNotNull(request1);
            assertEquals("POST", request1.getMethod());
            assertEquals("application/json; utf-8", request1.getHeader("Content-Type"));
            String requestBody = request1.getBody().readString(StandardCharsets.UTF_8);
            assertTrue(requestBody.contains("\"name\":\"test\""));
            assertTrue(requestBody.contains("\"value\":123"));

            // 测试场景2：服务器错误
            mockWebServer.enqueue(new MockResponse()
                    .setResponseCode(500)
                    .setBody("Internal Server Error"));

            IOException exception = assertThrows(IOException.class, () ->
                    Utils.sendPostRequest(baseUrl + "error", testData));
            assertTrue(exception.getMessage().contains("HTTP错误: 500"));

            // 测试场景3：无效URL
            assertThrows(IOException.class, () ->
                    Utils.sendPostRequest("invalid-url", testData));
        } finally {
            // 关闭服务器
            mockWebServer.shutdown();
        }
    }

    @Test
    void testGetAccessCode() {
        // 测试从 Authorization 头获取
        ServerRequest request = mock(ServerRequest.class);
        ServerRequest.Headers headers = mock(ServerRequest.Headers.class);
        when(request.headers()).thenReturn(headers);
        when(headers.header("Authorization")).thenReturn(Arrays.asList("Bearer test-token"));
        assertEquals("test-token", Utils.getAccessCode(request));

        // 测试从简单 Authorization 头获取
        when(headers.header("Authorization")).thenReturn(Arrays.asList("simple-token"));
        assertEquals("simple-token", Utils.getAccessCode(request));

        // 测试从查询参数获取
        when(headers.header("Authorization")).thenReturn(Collections.emptyList());
        when(request.param("accessCode")).thenReturn(Optional.of("param-token"));
        assertEquals("param-token", Utils.getAccessCode(request));

        // 测试无 token 场景
        when(request.param("accessCode")).thenReturn(Optional.empty());
        assertNull(Utils.getAccessCode(request));
    }

    // 用于测试的内部类
    private static class TestPerson {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}
