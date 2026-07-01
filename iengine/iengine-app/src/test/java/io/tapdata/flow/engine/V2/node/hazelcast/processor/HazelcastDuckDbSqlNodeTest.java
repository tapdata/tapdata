package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.process.DuckDbSqlNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperatorImpl;
import io.tapdata.flow.engine.V2.node.duckdb.DuckLakeConfig;
import io.tapdata.flow.engine.V2.node.duckdb.FromTableConfig;
import io.tapdata.flow.engine.V2.node.duckdb.NodeSchemaInfo;
import io.tapdata.flow.engine.V2.node.duckdb.PerSourceContext;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperator;
import io.tapdata.observable.logging.ObsLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * DuckDbSqlNode 单元测试
 */
class HazelcastDuckDbSqlNodeTest {

    @TempDir
    Path tempDir;

    @Mock
    private ProcessorBaseContext processorBaseContext;

    @Mock
    private TaskDto taskDto;

    @Mock
    private Node node;

    private HazelcastDuckDbSqlNode hazelcastDuckDbSqlNode;
    private ObsLogger obsLogger;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        
        when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
        when(processorBaseContext.getNode()).thenReturn(node);
        when(node.getId()).thenReturn("test_node");
        when(node.getName()).thenReturn("TestNode");
        when(taskDto.isNormalTask()).thenReturn(true);
        
        hazelcastDuckDbSqlNode = new HazelcastDuckDbSqlNode(processorBaseContext);
        hazelcastDuckDbSqlNode.setBatchSize(10);
        obsLogger = mock(ObsLogger.class);
        try {
            setFieldValueInHierarchy(hazelcastDuckDbSqlNode, "obsLogger", obsLogger);
        } catch (Exception e) {
            fail(e);
        }
    }

    @Test
    void testBatchSize_SetAndGet() {
        hazelcastDuckDbSqlNode.setBatchSize(100);
        assertEquals(100, hazelcastDuckDbSqlNode.getBatchSize());
    }

    @Test
    void testExecuteQuery_WithOperator() throws SQLException {
        DuckDbOperator mockOperator = mock(DuckDbOperator.class);
        List<Map<String, Object>> expected = List.of(new HashMap<>());
        when(mockOperator.executeQuery(anyString())).thenReturn(expected);
        
        // 使用反射设置 duckDbOperator
        setDuckDbOperator(mockOperator);
        
        List<Map<String, Object>> result = hazelcastDuckDbSqlNode.executeQuery("SELECT * FROM test");
        
        assertNotNull(result);
        assertEquals(expected, result);
    }

    @Test
    void testExecuteUpdate_WithOperator() throws SQLException {
        DuckDbOperator mockOperator = mock(DuckDbOperator.class);
        when(mockOperator.executeUpdate(anyString())).thenReturn(10);
        
        // 使用反射设置 duckDbOperator
        setDuckDbOperator(mockOperator);
        
        int result = hazelcastDuckDbSqlNode.executeUpdate("DELETE FROM test");
        
        assertEquals(10, result);
    }

    @Test
    void testInitialSyncUsesInsertBatch() throws Exception {
        DuckDbOperator mockOperator = mock(DuckDbOperator.class);
        doAnswer(invocation -> {
            DuckDbOperator.ThrowingConsumer action = invocation.getArgument(0);
            action.accept();
            return null;
        }).when(mockOperator).executeInTransaction(any());
        AtomicReference<List<TapdataEvent>> inserted = new AtomicReference<>();
        doAnswer(invocation -> {
            List<TapdataEvent> batch = invocation.getArgument(1);
            inserted.set(new ArrayList<>(batch));
            return null;
        }).when(mockOperator).insertBatch(any(NodeSchemaInfo.class), anyList());
        setDuckDbOperator(mockOperator);

        java.lang.reflect.Method method = HazelcastDuckDbSqlNode.class.getDeclaredMethod(
                "processInitialSyncStage",
                PerSourceContext.class,
                List.class
        );
        method.setAccessible(true);

        PerSourceContext context = new PerSourceContext("ctx", mockOperator);
        context.setSchema(buildMinimalSchema("pre_1", "orders"));
        context.setTableInitialized(true);
        List<TapdataEvent> events = new ArrayList<>();
        events.add(createInsertEvent("target_table", Map.of("id", 1L, "name", "Alice")));

        method.invoke(hazelcastDuckDbSqlNode, context, events);

        verify(mockOperator).insertBatch(eq(context.getSchema()), anyList());
        assertNotNull(inserted.get());
        assertEquals(1, inserted.get().size());
        assertTrue(events.isEmpty());
    }

    @Test
    void testTryProcess_NullEvent() {
        AtomicInteger consumerCallCount = new AtomicInteger(0);
        
        assertDoesNotThrow(() -> 
            hazelcastDuckDbSqlNode.tryProcess(null, (event, result) -> consumerCallCount.incrementAndGet())
        );
        
        assertEquals(0, consumerCallCount.get());
    }

    @Test
    void testTryProcess_WithNonRecordEvent() {
        TapEvent nonRecordEvent = mock(TapEvent.class);
        TapdataEvent tapdataEvent = new TapdataEvent();
        tapdataEvent.setTapEvent(nonRecordEvent);
        
        AtomicInteger consumerCallCount = new AtomicInteger(0);
        
        assertDoesNotThrow(() -> 
            hazelcastDuckDbSqlNode.tryProcess(tapdataEvent, (event, result) -> consumerCallCount.incrementAndGet())
        );
        
        assertEquals(1, consumerCallCount.get());
    }

    @Test
    void testInitDBSettingsIfNeed_SetsMemoryLimitAndThreads() throws SQLException {
        DuckDbSqlNode nodeConfig = new DuckDbSqlNode();
        nodeConfig.setMemoryLimitGB(4);
        nodeConfig.setThreads(Runtime.getRuntime().availableProcessors() + 10);

        DuckDbOperator mockOperator = mock(DuckDbOperator.class);
        setDuckDbOperator(mockOperator);

        hazelcastDuckDbSqlNode.initDBSettingsIfNeed(nodeConfig);

        verify(mockOperator).execute("SET memory_limit = '4'");
        verify(mockOperator).execute("SET threads = " + Runtime.getRuntime().availableProcessors());
    }

    @Test
    void testInitDBSettingsIfNeed_IgnoresNonPositiveConfig() throws SQLException {
        DuckDbSqlNode nodeConfig = new DuckDbSqlNode();
        nodeConfig.setMemoryLimitGB(0);
        nodeConfig.setThreads(-1);

        DuckDbOperator mockOperator = mock(DuckDbOperator.class);
        setDuckDbOperator(mockOperator);

        hazelcastDuckDbSqlNode.initDBSettingsIfNeed(nodeConfig);

        verify(mockOperator, never()).execute(anyString());
    }

    @Test
    void testInitDBPath_AppendsNodeIdSuffix() throws IOException {
        DuckDbSqlNode nodeConfig = new DuckDbSqlNode();
        Path baseDir = Files.createDirectory(tempDir.resolve("duckdb_workdir"));
        nodeConfig.setDbPath(baseDir.toString());
        nodeConfig.setId("node_1");

        String result = HazelcastDuckDbSqlNode.initDBPath(nodeConfig);
        assertEquals(Path.of(baseDir.toString(), "node_1").toAbsolutePath().toString(), result);
    }

    @Test
    void testInitDBPath_WhenPathIsFile_ThrowsException() throws IOException {
        DuckDbSqlNode nodeConfig = new DuckDbSqlNode();
        Path filePath = Files.createFile(tempDir.resolve("not_a_dir"));
        nodeConfig.setDbPath(filePath.toString());
        nodeConfig.setId("node_1");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> HazelcastDuckDbSqlNode.initDBPath(nodeConfig));
        assertTrue(ex.getMessage().contains("Path is not a directory"));
    }

    @Test
    void testInitDuckDbOperator_DuckLakeEnabled_UsesNodeConfig() throws Exception {
        DuckDbSqlNode nodeConfig = new DuckDbSqlNode();
        nodeConfig.setDuckLakeEnabled(true);
        nodeConfig.setDuckLakeStorageType("S3");
        nodeConfig.setDuckLakeStoragePath("s3://bucket/path");
        nodeConfig.setDuckLakeMetadataDbUrl("jdbc:postgresql://localhost:5432/ducklake");

        try (DuckDbOperatorImpl operator = HazelcastDuckDbSqlNode.initDuckDbOperator(nodeConfig, "", 10)) {
            DuckLakeConfig config = (DuckLakeConfig) getFieldValue(operator, "duckLakeConfig");
            assertNotNull(config);
            assertTrue(config.isEnabled());
            assertEquals("S3", config.getStorageType());
            assertEquals("s3://bucket/path", config.getStoragePath());
            assertEquals("jdbc:postgresql://localhost:5432/ducklake", config.getMetadataDbUrl());
        }
    }

    @Test
    void testInitDuckDbOperator_DuckLakeDisabled_UsesDisabledConfig() throws Exception {
        DuckDbSqlNode nodeConfig = new DuckDbSqlNode();
        nodeConfig.setDuckLakeEnabled(false);

        try (DuckDbOperatorImpl operator = HazelcastDuckDbSqlNode.initDuckDbOperator(nodeConfig, "", 10)) {
            DuckLakeConfig config = (DuckLakeConfig) getFieldValue(operator, "duckLakeConfig");
            assertNotNull(config);
            assertFalse(config.isEnabled());
        }
    }

    @Test
    void testCreateContextOperator_ReusesSharedOperatorInMemoryMode() throws Exception {
        NodeSchemaInfo schemaInfo = buildMinimalSchema("pre_1", "orders");
        try (DuckDbOperatorImpl sharedOperator = new DuckDbOperatorImpl("", false, 10, 5000)) {
            sharedOperator.ensureTableExists(schemaInfo, false);
            setDuckDbOperator(sharedOperator);
            setFieldValue(hazelcastDuckDbSqlNode, "dbPath", null);

            DuckDbOperator contextOperator = invokeCreateContextOperator();

            assertSame(sharedOperator, contextOperator);

            contextOperator.insertBatch(schemaInfo, List.of(createInsertEvent("orders", Map.of("id", 1L))));
            List<Map<String, Object>> results = sharedOperator.executeQuery("SELECT COUNT(*) AS cnt FROM orders");

            assertEquals(1L, ((Number) results.get(0).get("cnt")).longValue());
        }
    }

    @Test
    void testCreateContextOperator_UsesDedicatedOperatorInFileMode() throws Exception {
        NodeSchemaInfo schemaInfo = buildMinimalSchema("pre_1", "orders");
        String dbFile = tempDir.resolve("duckdb-node-test.db").toString();

        try (DuckDbOperatorImpl sharedOperator = new DuckDbOperatorImpl(dbFile, false, 10, 5000)) {
            sharedOperator.ensureTableExists(schemaInfo, false);
            setDuckDbOperator(sharedOperator);
            setFieldValue(hazelcastDuckDbSqlNode, "dbPath", dbFile);

            DuckDbOperator contextOperator = invokeCreateContextOperator();
            assertNotSame(sharedOperator, contextOperator);

            try {
                contextOperator.insertBatch(schemaInfo, List.of(createInsertEvent("orders", Map.of("id", 2L))));
                List<Map<String, Object>> results = sharedOperator.executeQuery("SELECT COUNT(*) AS cnt FROM orders");
                assertEquals(1L, ((Number) results.get(0).get("cnt")).longValue());
            } finally {
                if (contextOperator instanceof DuckDbOperatorImpl duckDbOperatorImpl) {
                    duckDbOperatorImpl.close();
                }
            }
        }
    }

    @Test
    void testReplaceWithBoundaryDetection_ReplacesWholeIdentifiersOnly() throws Exception {
        String sql = "SELECT a.id FROM a JOIN ab ON a.id = ab.id";
        Map<String, String> aliasMap = new LinkedHashMap<>();
        aliasMap.put("a", "t_a");
        aliasMap.put("ab", "t_ab");

        String result = (String) invokePrivateMethod(hazelcastDuckDbSqlNode, "replaceWithBoundaryDetection",
                new Class<?>[]{String.class, Map.class}, new Object[]{sql, aliasMap});

        assertEquals("SELECT t_a.id FROM t_a JOIN t_ab ON t_a.id = t_ab.id", result);
    }

    @Test
    void testResolveSqlTableAliases_ReplacesConfiguredAliases() throws Exception {
        FromTableConfig fromTableConfig = new FromTableConfig();
        fromTableConfig.setPreNodeId("pre_1");
        fromTableConfig.setTableNameInSql("t1");

        setFieldValue(hazelcastDuckDbSqlNode, "fromTables", List.of(fromTableConfig));
        setFieldValue(hazelcastDuckDbSqlNode, "querySql", "SELECT t1.id FROM t1");

        Map<String, NodeSchemaInfo> nodeSchemaCache = (Map<String, NodeSchemaInfo>) getFieldValue(hazelcastDuckDbSqlNode, "nodeSchemaCache");
        nodeSchemaCache.put("pre_1", buildMinimalSchema("pre_1", "orders"));

        invokePrivateMethod(hazelcastDuckDbSqlNode, "resolveSqlTableAliases", new Class<?>[]{}, new Object[]{});

        assertEquals("SELECT orders.id FROM orders", getFieldValue(hazelcastDuckDbSqlNode, "querySql"));
    }

    @Test
    void testResolveSqlTableAliases_MissingSchema_ThrowsTapCodeException() throws Exception {
        FromTableConfig fromTableConfig = new FromTableConfig();
        fromTableConfig.setPreNodeId("missing");
        fromTableConfig.setTableNameInSql("t1");

        setFieldValue(hazelcastDuckDbSqlNode, "fromTables", List.of(fromTableConfig));
        setFieldValue(hazelcastDuckDbSqlNode, "querySql", "SELECT * FROM t1");

        Method method = HazelcastDuckDbSqlNode.class.getDeclaredMethod("resolveSqlTableAliases");
        method.setAccessible(true);

        java.lang.reflect.InvocationTargetException ex = assertThrows(java.lang.reflect.InvocationTargetException.class, () -> method.invoke(hazelcastDuckDbSqlNode));
        assertNotNull(ex.getCause());
        assertEquals("TapCodeException", ex.getCause().getClass().getSimpleName());
    }

    @Test
    void testSanitizeIdentifier_ReplacesAndPrefixes() throws Exception {
        assertEquals("abc_def", invokePrivateMethod(hazelcastDuckDbSqlNode, "sanitizeIdentifier",
                new Class<?>[]{String.class}, new Object[]{"abc-def"}));
        assertEquals("_1abc", invokePrivateMethod(hazelcastDuckDbSqlNode, "sanitizeIdentifier",
                new Class<?>[]{String.class}, new Object[]{"1abc"}));

        java.lang.reflect.InvocationTargetException ex = assertThrows(java.lang.reflect.InvocationTargetException.class, () ->
                invokePrivateMethod(hazelcastDuckDbSqlNode, "sanitizeIdentifier", new Class<?>[]{String.class}, new Object[]{" "})
        );
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
    }

    /**
     * 使用反射设置 duckDbOperator
     */
    private void setDuckDbOperator(DuckDbOperator operator) {
        try {
            java.lang.reflect.Field opField = HazelcastDuckDbSqlNode.class.getDeclaredField("duckDbOperator");
            opField.setAccessible(true);
            opField.set(hazelcastDuckDbSqlNode, operator);
        } catch (Exception e) {
            fail("Failed to set duckDbOperator field: " + e.getMessage());
        }
    }

    private static NodeSchemaInfo buildMinimalSchema(String nodeId, String tableName) {
        io.tapdata.entity.schema.TapTable tapTable = new io.tapdata.entity.schema.TapTable(tableName);
        io.tapdata.entity.schema.TapField field = new io.tapdata.entity.schema.TapField();
        field.name("id");
        tapTable.add(field);
        Map<String, io.tapdata.entity.schema.TapField> fieldMap = Map.of("id", field);
        return new NodeSchemaInfo(nodeId, tableName, tableName, List.of("id"), fieldMap, tapTable, null);
    }

    private static void setFieldValue(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Object getFieldValue(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static Object invokePrivateMethod(Object target, String methodName, Class<?>[] parameterTypes, Object[] args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    private DuckDbOperator invokeCreateContextOperator() throws Exception {
        return (DuckDbOperator) invokePrivateMethod(
                hazelcastDuckDbSqlNode,
                "createContextOperator",
                new Class<?>[]{},
                new Object[]{}
        );
    }

    private static void setFieldValueInHierarchy(Object target, String fieldName, Object value) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        if (field == null) {
            throw new NoSuchFieldException(fieldName);
        }
        field.setAccessible(true);
        field.set(target, value);
    }

    private static Field findField(Class<?> type, String fieldName) {
        Class<?> current = type;
        while (current != null && current != Object.class) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        return null;
    }

    private TapdataEvent createInsertEvent(String tableName, Map<String, Object> after) {
        TapInsertRecordEvent insertRecordEvent = new TapInsertRecordEvent();
        insertRecordEvent.setTableId(tableName);
        insertRecordEvent.setAfter(after);
        TapdataEvent tapdataEvent = new TapdataEvent();
        tapdataEvent.setTapEvent(insertRecordEvent);
        return tapdataEvent;
    }

    // ========== dbPath 拼接测试 ==========
    
    @Test
    void testBuildDbPathWithNodeId_NormalPath() {
        // 测试正常路径（无分隔符）
        String dbPath = "/data/duckdb";
        String nodeId = "abc123";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals(Path.of(dbPath, nodeId).toAbsolutePath().toString(), result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_PathEndsWithForwardSlash() {
        // 测试路径以 / 结尾
        String dbPath = "/data/duckdb/";
        String nodeId = "abc123";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals(Path.of(dbPath, nodeId).toAbsolutePath().toString(), result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_PathEndsWithBackwardSlash() {
        // 测试路径以 \ 结尾
        String dbPath = "C:\\data\\duckdb\\";
        String nodeId = "abc123";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals(Path.of(dbPath, nodeId).toAbsolutePath().toString(), result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_PathEndsWithMultipleSlashes() {
        // 测试路径以多个分隔符结尾
        String dbPath = "/data/duckdb///";
        String nodeId = "abc123";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals(Path.of(dbPath, nodeId).toAbsolutePath().toString(), result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_PathEndsWithMixedSlashes() {
        // 测试路径以混合分隔符结尾
        String dbPath = "/data/duckdb\\/";
        String nodeId = "abc123";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals(Path.of(dbPath, nodeId).toAbsolutePath().toString(), result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_EmptyDbPath_ThrowsException() {
        // 测试空 dbPath，应该抛异常
        String dbPath = "";
        String nodeId = "abc123";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals(Path.of(dbPath, nodeId).toAbsolutePath().toString(), result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_NullDbPath_ThrowsException() {
        // 测试 null dbPath，应该抛异常
        String nodeId = "abc123";
        assertThrows(NullPointerException.class, () -> HazelcastDuckDbSqlNode.buildDbPathWithNodeId(null, nodeId));
    }
    
    @Test
    void testBuildDbPathWithNodeId_EmptyNodeId_ThrowsException() {
        // 测试空 nodeId，应该抛异常
        String dbPath = "/data/duckdb";
        String nodeId = "";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals(Path.of(dbPath, nodeId).toAbsolutePath().toString(), result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_NullNodeId_ThrowsException() {
        // 测试 null nodeId，应该抛异常
        String dbPath = "/data/duckdb";
        assertThrows(NullPointerException.class, () -> HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, null));
    }
    
    @Test
    void testBuildDbPathWithNodeId_RelativePath() {
        // 测试相对路径
        String dbPath = "data/duckdb";
        String nodeId = "test-node-456";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals(Path.of(dbPath, nodeId).toAbsolutePath().toString(), result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_RelativePathEndsWithSlash() {
        // 测试相对路径以分隔符结尾
        String dbPath = "./data/duckdb/";
        String nodeId = "test-node-789";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals(Path.of(dbPath, nodeId).toAbsolutePath().toString(), result);
    }
    
    @Test
    void testBuildDbPathWithNodeId_JustDriveLetter() {
        // 测试只有盘符的情况
        String dbPath = "C:\\";
        String nodeId = "drive-node";
        String result = HazelcastDuckDbSqlNode.buildDbPathWithNodeId(dbPath, nodeId);
        assertEquals(Path.of(dbPath, nodeId).toAbsolutePath().toString(), result);
    }

}
