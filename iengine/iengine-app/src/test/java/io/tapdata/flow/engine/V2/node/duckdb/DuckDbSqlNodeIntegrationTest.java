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

import static org.mockito.Mockito.when;

/**
 * DuckDbSqlNode 集成测试，验证 CDC 事件处理和宽表更新功能
 */
@DisplayName("DuckDbSqlNode 集成测试")
public class DuckDbSqlNodeIntegrationTest {

    private static final String DB_PATH = ":memory:";
    private static Connection connection;

    @Mock
    private ProcessorBaseContext processorBaseContext;

    @Mock
    private TaskDto taskDto;

    @Mock
    private Node node;

    @BeforeAll
    static void initDatabase() throws Exception {
        connection = DriverManager.getConnection("jdbc:duckdb:" + DB_PATH);
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
}
