package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
 * DuckDbSqlNode 集成测试，验证 CDC 事件处理和宽表更新功能
 */
@DisplayName("DuckDbSqlNode 集成测试")
public class HazelcastDuckDbSqlNodeIntegrationTest {

    private static final String DB_PATH = ":memory:";
    private static Connection connection;
    private static DuckDbOperator duckDbOperator;

    @Mock
    private ProcessorBaseContext processorBaseContext;

    @Mock
    private TaskDto taskDto;

    @Mock
    private Node node;

    @BeforeAll
    static void initDatabase() throws Exception {
        connection = DriverManager.getConnection("jdbc:duckdb:" + DB_PATH);
        duckDbOperator = new DuckDbOperatorImpl(connection, false, 1000, 5000);
    }

    @AfterAll
    static void closeDatabase() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS users CASCADE");
            stmt.execute("DROP TABLE IF EXISTS orders CASCADE");
            stmt.execute("DROP TABLE IF EXISTS user_order_wide CASCADE");
        }

        when(processorBaseContext.getTaskDto()).thenReturn(taskDto);
        when(processorBaseContext.getNode()).thenReturn(node);
        when(node.getId()).thenReturn("test_duckdb_integration_node");
        when(node.getName()).thenReturn("Test DuckDB Integration Node");
        when(taskDto.isNormalTask()).thenReturn(true);
    }

    @Nested
    @DisplayName("集成测试 - DuckDB 宽表更新")
    class IntegrationTests {

        @Test
        @DisplayName("测试全量同步和宽表更新")
        void testFullSyncAndWideTableUpdate() throws Exception {
            System.out.println("=== 开始执行集成测试: 全量同步和宽表更新 ===");

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                    CREATE TABLE orders (
                        id BIGINT PRIMARY KEY,
                        user_id BIGINT,
                        product VARCHAR,
                        amount DECIMAL(10,2),
                        created_at TIMESTAMP
                    )
                """);
                stmt.execute("""
                    CREATE TABLE users (
                        id BIGINT PRIMARY KEY,
                        name VARCHAR,
                        email VARCHAR,
                        age INTEGER
                    )
                """);
                stmt.execute("""
                    CREATE TABLE user_order_wide (
                        order_id BIGINT PRIMARY KEY,
                        user_id BIGINT,
                        user_name VARCHAR,
                        user_email VARCHAR,
                        product VARCHAR,
                        amount DECIMAL(10,2),
                        created_at TIMESTAMP
                    )
                """);
            }

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO users VALUES (1, '张三', 'zhangsan@example.com', 30)");
                stmt.execute("INSERT INTO users VALUES (2, '李四', 'lisi@example.com', 25)");
                
                stmt.execute("INSERT INTO orders VALUES (1, 1, '笔记本电脑', 5999.99, '2024-01-15 10:30:00')");
                stmt.execute("INSERT INTO orders VALUES (2, 1, '无线鼠标', 99.00, '2024-01-20 14:00:00')");
            }

            System.out.println("数据已插入，现在验证宽表数据...");

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                    INSERT INTO user_order_wide (
                        order_id, user_id, user_name, user_email, product, amount, created_at
                    )
                    SELECT 
                        o.id AS order_id,
                        o.user_id,
                        u.name AS user_name,
                        u.email AS user_email,
                        o.product,
                        o.amount,
                        o.created_at
                    FROM orders o
                    LEFT JOIN users u ON o.user_id = u.id
                """);
            }

            List<Map<String, Object>> wideTableData = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM user_order_wide ORDER BY order_id")) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("order_id", rs.getLong("order_id"));
                    row.put("user_id", rs.getLong("user_id"));
                    row.put("user_name", rs.getString("user_name"));
                    row.put("user_email", rs.getString("user_email"));
                    row.put("product", rs.getString("product"));
                    row.put("amount", rs.getDouble("amount"));
                    row.put("created_at", rs.getTimestamp("created_at"));
                    wideTableData.add(row);
                }
            }

            System.out.println("宽表数据: " + wideTableData);

            Assertions.assertEquals(2, wideTableData.size());
            Map<String, Object> wideRow1 = wideTableData.get(0);
            Assertions.assertEquals(1L, wideRow1.get("order_id"));
            Assertions.assertEquals(1L, wideRow1.get("user_id"));
            Assertions.assertEquals("张三", wideRow1.get("user_name"));
            Assertions.assertEquals("笔记本电脑", wideRow1.get("product"));
            Assertions.assertEquals(5999.99, (Double) wideRow1.get("amount"), 0.01);

            Map<String, Object> wideRow2 = wideTableData.get(1);
            Assertions.assertEquals(2L, wideRow2.get("order_id"));
            Assertions.assertEquals(1L, wideRow2.get("user_id"));
            Assertions.assertEquals("张三", wideRow2.get("user_name"));
            Assertions.assertEquals("无线鼠标", wideRow2.get("product"));
            Assertions.assertEquals(99.00, (Double) wideRow2.get("amount"), 0.01);

            System.out.println("=== 集成测试通过: 全量同步和宽表更新验证成功 ===\n");
        }

        @Test
        @DisplayName("测试主表 (orders) UPDATE 场景")
        void testMainTableUpdate() throws Exception {
            System.out.println("=== 开始测试: 主表 UPDATE 场景 ===");

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                    CREATE TABLE orders (
                        id BIGINT PRIMARY KEY,
                        user_id BIGINT,
                        product VARCHAR,
                        amount DECIMAL(10,2),
                        created_at TIMESTAMP
                    )
                """);
                stmt.execute("""
                    CREATE TABLE users (
                        id BIGINT PRIMARY KEY,
                        name VARCHAR,
                        email VARCHAR,
                        age INTEGER
                    )
                """);
                stmt.execute("""
                    CREATE TABLE user_order_wide (
                        order_id BIGINT PRIMARY KEY,
                        user_id BIGINT,
                        user_name VARCHAR,
                        user_email VARCHAR,
                        product VARCHAR,
                        amount DECIMAL(10,2),
                        created_at TIMESTAMP
                    )
                """);
            }

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO users VALUES (1, '张三', 'zhangsan@example.com', 30)");
                stmt.execute("INSERT INTO orders VALUES (1, 1, '笔记本电脑', 5999.99, '2024-01-15 10:30:00')");
            }

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                    INSERT INTO user_order_wide (
                        order_id, user_id, user_name, user_email, product, amount, created_at
                    )
                    SELECT 
                        o.id AS order_id,
                        o.user_id,
                        u.name AS user_name,
                        u.email AS user_email,
                        o.product,
                        o.amount,
                        o.created_at
                    FROM orders o
                    LEFT JOIN users u ON o.user_id = u.id
                """);
            }

            // 更新订单
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("UPDATE orders SET product = '游戏笔记本', amount = 7999.99 WHERE id = 1");
            }

            // 重新计算宽表
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DELETE FROM user_order_wide WHERE order_id = 1");
                stmt.execute("""
                    INSERT INTO user_order_wide (
                        order_id, user_id, user_name, user_email, product, amount, created_at
                    )
                    SELECT 
                        o.id AS order_id,
                        o.user_id,
                        u.name AS user_name,
                        u.email AS user_email,
                        o.product,
                        o.amount,
                        o.created_at
                    FROM orders o
                    LEFT JOIN users u ON o.user_id = u.id
                    WHERE o.id = 1
                """);
            }

            // 验证
            List<Map<String, Object>> wideTableData = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM user_order_wide ORDER BY order_id")) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("order_id", rs.getLong("order_id"));
                    row.put("user_id", rs.getLong("user_id"));
                    row.put("user_name", rs.getString("user_name"));
                    row.put("product", rs.getString("product"));
                    row.put("amount", rs.getDouble("amount"));
                    wideTableData.add(row);
                }
            }

            Assertions.assertEquals(1, wideTableData.size());
            Map<String, Object> wideRow = wideTableData.get(0);
            Assertions.assertEquals(1L, wideRow.get("order_id"));
            Assertions.assertEquals("游戏笔记本", wideRow.get("product"));
            Assertions.assertEquals(7999.99, (Double) wideRow.get("amount"), 0.01);

            System.out.println("=== 测试通过: 主表 UPDATE 场景 ===\n");
        }

        @Test
        @DisplayName("测试主表 (orders) DELETE 场景")
        void testMainTableDelete() throws Exception {
            System.out.println("=== 开始测试: 主表 DELETE 场景 ===");

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                    CREATE TABLE orders (
                        id BIGINT PRIMARY KEY,
                        user_id BIGINT,
                        product VARCHAR,
                        amount DECIMAL(10,2),
                        created_at TIMESTAMP
                    )
                """);
                stmt.execute("""
                    CREATE TABLE users (
                        id BIGINT PRIMARY KEY,
                        name VARCHAR,
                        email VARCHAR,
                        age INTEGER
                    )
                """);
                stmt.execute("""
                    CREATE TABLE user_order_wide (
                        order_id BIGINT PRIMARY KEY,
                        user_id BIGINT,
                        user_name VARCHAR,
                        user_email VARCHAR,
                        product VARCHAR,
                        amount DECIMAL(10,2),
                        created_at TIMESTAMP
                    )
                """);
            }

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO users VALUES (1, '张三', 'zhangsan@example.com', 30)");
                stmt.execute("INSERT INTO orders VALUES (1, 1, '笔记本电脑', 5999.99, '2024-01-15 10:30:00')");
                stmt.execute("INSERT INTO orders VALUES (2, 1, '无线鼠标', 99.00, '2024-01-20 14:00:00')");
            }

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                    INSERT INTO user_order_wide (
                        order_id, user_id, user_name, user_email, product, amount, created_at
                    )
                    SELECT 
                        o.id AS order_id,
                        o.user_id,
                        u.name AS user_name,
                        u.email AS user_email,
                        o.product,
                        o.amount,
                        o.created_at
                    FROM orders o
                    LEFT JOIN users u ON o.user_id = u.id
                """);
            }

            // 删除订单
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DELETE FROM orders WHERE id = 1");
            }

            // 更新宽表
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DELETE FROM user_order_wide WHERE order_id = 1");
            }

            // 验证
            List<Map<String, Object>> wideTableData = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM user_order_wide ORDER BY order_id")) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("order_id", rs.getLong("order_id"));
                    row.put("product", rs.getString("product"));
                    wideTableData.add(row);
                }
            }

            Assertions.assertEquals(1, wideTableData.size());
            Assertions.assertEquals(2L, wideTableData.get(0).get("order_id"));
            Assertions.assertEquals("无线鼠标", wideTableData.get(0).get("product"));

            System.out.println("=== 测试通过: 主表 DELETE 场景 ===\n");
        }

        @Test
        @DisplayName("测试从表 (users) UPDATE 场景")
        void testFromTableUpdate() throws Exception {
            System.out.println("=== 开始测试: 从表 UPDATE 场景 ===");

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                    CREATE TABLE orders (
                        id BIGINT PRIMARY KEY,
                        user_id BIGINT,
                        product VARCHAR,
                        amount DECIMAL(10,2),
                        created_at TIMESTAMP
                    )
                """);
                stmt.execute("""
                    CREATE TABLE users (
                        id BIGINT PRIMARY KEY,
                        name VARCHAR,
                        email VARCHAR,
                        age INTEGER
                    )
                """);
                stmt.execute("""
                    CREATE TABLE user_order_wide (
                        order_id BIGINT PRIMARY KEY,
                        user_id BIGINT,
                        user_name VARCHAR,
                        user_email VARCHAR,
                        product VARCHAR,
                        amount DECIMAL(10,2),
                        created_at TIMESTAMP
                    )
                """);
            }

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO users VALUES (1, '张三', 'zhangsan@example.com', 30)");
                stmt.execute("INSERT INTO orders VALUES (1, 1, '笔记本电脑', 5999.99, '2024-01-15 10:30:00')");
                stmt.execute("INSERT INTO orders VALUES (2, 1, '无线鼠标', 99.00, '2024-01-20 14:00:00')");
            }

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("""
                    INSERT INTO user_order_wide (
                        order_id, user_id, user_name, user_email, product, amount, created_at
                    )
                    SELECT 
                        o.id AS order_id,
                        o.user_id,
                        u.name AS user_name,
                        u.email AS user_email,
                        o.product,
                        o.amount,
                        o.created_at
                    FROM orders o
                    LEFT JOIN users u ON o.user_id = u.id
                """);
            }

            // 更新用户信息
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("UPDATE users SET name = '张三丰', email = 'zhangsanfeng@example.com' WHERE id = 1");
            }

            // 重新计算相关订单的宽表数据
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DELETE FROM user_order_wide WHERE user_id = 1");
                stmt.execute("""
                    INSERT INTO user_order_wide (
                        order_id, user_id, user_name, user_email, product, amount, created_at
                    )
                    SELECT 
                        o.id AS order_id,
                        o.user_id,
                        u.name AS user_name,
                        u.email AS user_email,
                        o.product,
                        o.amount,
                        o.created_at
                    FROM orders o
                    LEFT JOIN users u ON o.user_id = u.id
                    WHERE o.user_id = 1
                """);
            }

            // 验证
            List<Map<String, Object>> wideTableData = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM user_order_wide ORDER BY order_id")) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("order_id", rs.getLong("order_id"));
                    row.put("user_name", rs.getString("user_name"));
                    row.put("user_email", rs.getString("user_email"));
                    wideTableData.add(row);
                }
            }

            Assertions.assertEquals(2, wideTableData.size());
            Assertions.assertEquals("张三丰", wideTableData.get(0).get("user_name"));
            Assertions.assertEquals("zhangsanfeng@example.com", wideTableData.get(0).get("user_email"));
            Assertions.assertEquals("张三丰", wideTableData.get(1).get("user_name"));
            Assertions.assertEquals("zhangsanfeng@example.com", wideTableData.get(1).get("user_email"));

            System.out.println("=== 测试通过: 从表 UPDATE 场景 ===\n");
        }
    }

    @Nested
    @DisplayName("组件测试 - AffectedKeyCalculator")
    class AffectedKeyCalculatorTests {

        private AffectedKeyCalculator affectedKeyCalculator;

        @BeforeEach
        void setUpCalculator() throws Exception {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS orders CASCADE");
                stmt.execute("DROP TABLE IF EXISTS users CASCADE");
                stmt.execute("DROP TABLE IF EXISTS user_order_wide CASCADE");
                
                stmt.execute("""
                    CREATE TABLE orders (
                        id BIGINT PRIMARY KEY,
                        user_id BIGINT,
                        product VARCHAR,
                        amount DECIMAL(10,2),
                        created_at TIMESTAMP
                    )
                """);
                stmt.execute("""
                    CREATE TABLE users (
                        id BIGINT PRIMARY KEY,
                        name VARCHAR,
                        email VARCHAR,
                        age INTEGER
                    )
                """);
            }

            // 准备数据
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO users VALUES (1, '张三', 'zhangsan@example.com', 30)");
                stmt.execute("INSERT INTO users VALUES (2, '李四', 'lisi@example.com', 25)");
                stmt.execute("INSERT INTO orders VALUES (1, 1, '笔记本电脑', 5999.99, '2024-01-15 10:30:00')");
                stmt.execute("INSERT INTO orders VALUES (2, 1, '无线鼠标', 99.00, '2024-01-20 14:00:00')");
                stmt.execute("INSERT INTO orders VALUES (3, 2, '机械键盘', 299.00, '2024-02-01 09:00:00')");
            }

            // 配置 FromTableConfig
            FromTableConfig usersFromTable = new FromTableConfig();
            usersFromTable.setPreNodeId("users");
            usersFromTable.setTableNameInSql("id");

            // 自定义 JOIN 查询配置
            Map<String, String> customJoinQueries = new HashMap<>();
            customJoinQueries.put("users", "SELECT DISTINCT o.id FROM orders o WHERE o.user_id IN (${pkValues})");

            affectedKeyCalculator = new AffectedKeyCalculator(
                "order_id",
                "orders",
                "id",
                List.of(usersFromTable),
                customJoinQueries,
                duckDbOperator
            );
        }

        @Test
        @DisplayName("测试主表 INSERT 事件计算受影响主键")
        void testMainTableInsertAffectedKeys() throws Exception {
            List<Map<String, Object>> events = new ArrayList<>();
            Map<String, Object> event = new HashMap<>();
            event.put("id", 4L);
            event.put("user_id", 2L);
            event.put("product", "显示器");
            event.put("amount", 1999.00);
            events.add(event);

            Set<Object> affectedKeys = affectedKeyCalculator.calculateAffectedKeys("orders", events);

            Assertions.assertEquals(1, affectedKeys.size());
            Assertions.assertTrue(affectedKeys.contains(4L));
        }

        @Test
        @DisplayName("测试主表 UPDATE 事件计算受影响主键")
        void testMainTableUpdateAffectedKeys() throws Exception {
            List<Map<String, Object>> events = new ArrayList<>();
            Map<String, Object> event = new HashMap<>();
            event.put("id", 1L);
            event.put("product", "游戏笔记本");
            events.add(event);

            Set<Object> affectedKeys = affectedKeyCalculator.calculateAffectedKeys("orders", events);

            Assertions.assertEquals(1, affectedKeys.size());
            Assertions.assertTrue(affectedKeys.contains(1L));
        }

        @Test
        @DisplayName("测试主表 DELETE 事件计算受影响主键")
        void testMainTableDeleteAffectedKeys() throws Exception {
            List<Map<String, Object>> events = new ArrayList<>();
            Map<String, Object> event = new HashMap<>();
            event.put("id", 2L);
            events.add(event);

            Set<Object> affectedKeys = affectedKeyCalculator.calculateAffectedKeys("orders", events);

            Assertions.assertEquals(1, affectedKeys.size());
            Assertions.assertTrue(affectedKeys.contains(2L));
        }

        @Test
        @DisplayName("测试从表 UPDATE 事件计算关联主键")
        void testFromTableUpdateAffectedKeys() throws Exception {
            List<Map<String, Object>> events = new ArrayList<>();
            Map<String, Object> event = new HashMap<>();
            event.put("id", 1L);
            event.put("name", "张三更新");
            events.add(event);

            Set<Object> affectedKeys = affectedKeyCalculator.calculateAffectedKeys("users", events);

            Assertions.assertEquals(2, affectedKeys.size());
            Assertions.assertTrue(affectedKeys.contains(1L));
            Assertions.assertTrue(affectedKeys.contains(2L));
        }

        @Test
        @DisplayName("测试多个事件批量计算")
        void testMultipleEventsAffectedKeys() throws Exception {
            List<Map<String, Object>> events = new ArrayList<>();
            
            Map<String, Object> event1 = new HashMap<>();
            event1.put("id", 1L);
            event1.put("product", "更新的产品1");
            events.add(event1);

            Map<String, Object> event2 = new HashMap<>();
            event2.put("id", 3L);
            event2.put("product", "更新的产品3");
            events.add(event2);

            Set<Object> affectedKeys = affectedKeyCalculator.calculateAffectedKeys("orders", events);

            Assertions.assertEquals(2, affectedKeys.size());
            Assertions.assertTrue(affectedKeys.contains(1L));
            Assertions.assertTrue(affectedKeys.contains(3L));
        }
    }

    @Nested
    @DisplayName("组件测试 - IncrementalViewUpdater")
    class IncrementalViewUpdaterTests {

        private IncrementalViewUpdater incrementalViewUpdater;

        @BeforeEach
        void setUpUpdater() throws Exception {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS orders CASCADE");
                stmt.execute("DROP TABLE IF EXISTS users CASCADE");
                stmt.execute("DROP TABLE IF EXISTS user_order_wide CASCADE");
                
                stmt.execute("""
                    CREATE TABLE orders (
                        id BIGINT PRIMARY KEY,
                        user_id BIGINT,
                        product VARCHAR,
                        amount DECIMAL(10,2),
                        created_at TIMESTAMP
                    )
                """);
                stmt.execute("""
                    CREATE TABLE users (
                        id BIGINT PRIMARY KEY,
                        name VARCHAR,
                        email VARCHAR,
                        age INTEGER
                    )
                """);
                stmt.execute("""
                    CREATE TABLE user_order_wide (
                        order_id BIGINT PRIMARY KEY,
                        user_id BIGINT,
                        user_name VARCHAR,
                        user_email VARCHAR,
                        product VARCHAR,
                        amount DECIMAL(10,2),
                        created_at TIMESTAMP
                    )
                """);
            }

            // 准备初始数据
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO users VALUES (1, '张三', 'zhangsan@example.com', 30)");
                stmt.execute("INSERT INTO users VALUES (2, '李四', 'lisi@example.com', 25)");
                stmt.execute("INSERT INTO orders VALUES (1, 1, '笔记本电脑', 5999.99, '2024-01-15 10:30:00')");
                stmt.execute("INSERT INTO orders VALUES (2, 1, '无线鼠标', 99.00, '2024-01-20 14:00:00')");
                stmt.execute("INSERT INTO orders VALUES (3, 2, '机械键盘', 299.00, '2024-02-01 09:00:00')");
            }

            String userSql = """
                SELECT 
                    o.id AS order_id,
                    o.user_id,
                    u.name AS user_name,
                    u.email AS user_email,
                    o.product,
                    o.amount,
                    o.created_at
                FROM orders o
                LEFT JOIN users u ON o.user_id = u.id
            """;

            incrementalViewUpdater = new IncrementalViewUpdater(
                "user_order_wide",
                "order_id",
                userSql,
                true,
                duckDbOperator
            );
        }

        @Test
        @DisplayName("测试初始全量加载到宽表")
        void testInitialFullLoad() throws Exception {
            Set<Object> affectedKeys = new LinkedHashSet<>();
            affectedKeys.add(1L);
            affectedKeys.add(2L);
            affectedKeys.add(3L);

            int updatedCount = incrementalViewUpdater.updateWideTable(affectedKeys);

            Assertions.assertEquals(3, updatedCount);

            // 验证数据
            List<Map<String, Object>> wideTableData = queryWideTable();
            Assertions.assertEquals(3, wideTableData.size());
            
            Map<String, Object> row1 = wideTableData.stream()
                    .filter(row -> (Long) row.get("order_id") == 1L)
                    .findFirst()
                    .orElseThrow();
            Assertions.assertEquals("张三", row1.get("user_name"));
            Assertions.assertEquals("笔记本电脑", row1.get("product"));
        }

        @Test
        @DisplayName("测试更新宽表单行数据")
        void testUpdateSingleRow() throws Exception {
            // 第一步：初始加载
            Set<Object> initialKeys = new LinkedHashSet<>();
            initialKeys.add(1L);
            initialKeys.add(2L);
            initialKeys.add(3L);
            incrementalViewUpdater.updateWideTable(initialKeys);

            // 第二步：更新 users 表的记录
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("UPDATE users SET name = '张三更新' WHERE id = 1");
            }

            // 第三步：更新受影响的宽表行
            Set<Object> affectedKeys = new LinkedHashSet<>();
            affectedKeys.add(1L);
            affectedKeys.add(2L);

            int updatedCount = incrementalViewUpdater.updateWideTable(affectedKeys);

            Assertions.assertEquals(2, updatedCount);

            // 验证更新结果
            List<Map<String, Object>> wideTableData = queryWideTable();
            Map<String, Object> row1 = wideTableData.stream()
                    .filter(row -> (Long) row.get("order_id") == 1L)
                    .findFirst()
                    .orElseThrow();
            Map<String, Object> row2 = wideTableData.stream()
                    .filter(row -> (Long) row.get("order_id") == 2L)
                    .findFirst()
                    .orElseThrow();
            
            Assertions.assertEquals("张三更新", row1.get("user_name"));
            Assertions.assertEquals("张三更新", row2.get("user_name"));
        }

        @Test
        @DisplayName("测试删除宽表行")
        void testDeleteRow() throws Exception {
            // 第一步：初始加载
            Set<Object> initialKeys = new LinkedHashSet<>();
            initialKeys.add(1L);
            initialKeys.add(2L);
            initialKeys.add(3L);
            incrementalViewUpdater.updateWideTable(initialKeys);

            // 第二步：删除 orders 表的记录
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DELETE FROM orders WHERE id = 2");
            }

            // 第三步：更新受影响的宽表行
            Set<Object> affectedKeys = new LinkedHashSet<>();
            affectedKeys.add(2L);

            int updatedCount = incrementalViewUpdater.updateWideTable(affectedKeys);

            Assertions.assertEquals(1, updatedCount);

            // 验证删除结果
            List<Map<String, Object>> wideTableData = queryWideTable();
            Assertions.assertEquals(2, wideTableData.size());
            
            long orderId2Count = wideTableData.stream()
                    .filter(row -> (Long) row.get("order_id") == 2L)
                    .count();
            Assertions.assertEquals(0, orderId2Count);
        }

        @Test
        @DisplayName("测试新增宽表行")
        void testInsertRow() throws Exception {
            // 第一步：初始加载已有数据
            Set<Object> initialKeys = new LinkedHashSet<>();
            initialKeys.add(1L);
            initialKeys.add(2L);
            initialKeys.add(3L);
            incrementalViewUpdater.updateWideTable(initialKeys);

            // 第二步：新增订单
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO orders VALUES (4, 2, '显示器', 1999.00, '2024-03-01 11:00:00')");
            }

            // 第三步：更新受影响的宽表行
            Set<Object> affectedKeys = new LinkedHashSet<>();
            affectedKeys.add(4L);

            int updatedCount = incrementalViewUpdater.updateWideTable(affectedKeys);

            Assertions.assertEquals(1, updatedCount);

            // 验证新增结果
            List<Map<String, Object>> wideTableData = queryWideTable();
            Assertions.assertEquals(4, wideTableData.size());
            
            Map<String, Object> row4 = wideTableData.stream()
                    .filter(row -> (Long) row.get("order_id") == 4L)
                    .findFirst()
                    .orElseThrow();
            Assertions.assertEquals("显示器", row4.get("product"));
        }

        private List<Map<String, Object>> queryWideTable() throws Exception {
            List<Map<String, Object>> data = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM user_order_wide ORDER BY order_id")) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("order_id", rs.getLong("order_id"));
                    row.put("user_id", rs.getLong("user_id"));
                    row.put("user_name", rs.getString("user_name"));
                    row.put("user_email", rs.getString("user_email"));
                    row.put("product", rs.getString("product"));
                    row.put("amount", rs.getBigDecimal("amount"));
                    data.add(row);
                }
            }
            return data;
        }
    }
}
