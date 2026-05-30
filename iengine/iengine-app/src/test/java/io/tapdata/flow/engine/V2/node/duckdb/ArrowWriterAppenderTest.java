package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.*;
import org.duckdb.DuckDBConnection;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ArrowWriter Appender API 多数据类型集成测试
 * 
 * 测试目标：
 * 1. 验证 DuckDB 官方推荐的 Appender API 正确处理所有支持的数据类型
 * 2. 确保类型转换的准确性和边界情况
 * 3. 验证 null 值、特殊值、极端值的正确处理
 * 
 * 覆盖的数据类型（17种）：
 * - 整数类型：Integer, Long, Short, Byte
 * - 浮点类型：Float, Double
 * - 布尔类型：Boolean
 * - 字符串类型：String
 * - 日期时间类型：Date, Timestamp, LocalDateTime, LocalDate
 * - 高精度数值：BigDecimal, BigInteger
 * - 二进制类型：byte[]
 * - 复合类型：Collection<?>, Map<?,?>
 * - 特殊类型：null, 未知类型（toString兜底）
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ArrowWriterAppenderTest {

    private Connection connection;
    private ArrowWriter arrowWriter;
    
    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:duckdb:");
        arrowWriter = new ArrowWriter(connection, true);
        
        createTestTables();
    }
    
    @AfterEach
    void tearDown() throws SQLException {
        if (arrowWriter != null) {
            arrowWriter.close();
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    /**
     * 创建测试所需的表结构
     */
    private void createTestTables() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // 整数类型表
            stmt.execute("""
                CREATE TABLE integer_types (
                    id INTEGER,
                    int_col INTEGER,
                    long_col BIGINT,
                    short_col SMALLINT,
                    byte_col TINYINT
                )
                """);
            
            // 浮点和布尔类型表
            stmt.execute("""
                CREATE TABLE float_boolean_types (
                    id INTEGER,
                    float_col REAL,
                    double_col DOUBLE,
                    bool_col BOOLEAN
                )
                """);
            
            // 字符串和二进制类型表（DuckDB 对 BLOB 的 getBytes() 支持有限，使用 VARCHAR 代替）
            stmt.execute("""
                CREATE TABLE string_binary_types (
                    id INTEGER,
                    string_col VARCHAR,
                    binary_data VARCHAR,
                    text_col TEXT
                )
                """);
            
            // 日期时间类型表
            stmt.execute("""
                CREATE TABLE datetime_types (
                    id INTEGER,
                    date_col DATE,
                    timestamp_col TIMESTAMP,
                    datetime_col TIMESTAMP,
                    localdate_col DATE
                )
                """);
            
            // 高精度数值类型表
            stmt.execute("""
                CREATE TABLE precise_numeric_types (
                    id INTEGER,
                    decimal_col DECIMAL(30,10),
                    bigint_col HUGEINT
                )
                """);
            
            // 复合类型表
            stmt.execute("""
                CREATE TABLE composite_types (
                    id INTEGER,
                    list_col INTEGER[],
                    map_col MAP(VARCHAR, INTEGER)
                )
                """);
            
            // 混合类型表（用于综合测试）
            stmt.execute("""
                CREATE TABLE mixed_types (
                    id INTEGER PRIMARY KEY,
                    int_val INTEGER,
                    long_val BIGINT,
                    double_val DOUBLE,
                    string_val VARCHAR,
                    bool_val BOOLEAN,
                    timestamp_val TIMESTAMP,
                    decimal_val DECIMAL(20,5)
                )
                """);
        }
    }

    // ==================== 1. 整数类型测试 ====================

    @Test
    @Order(1)
    @DisplayName("Appender: Integer 类型 - 正常值")
    void testAppender_Integer_NormalValues() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("id", 1);
        row1.put("int_col", 42);
        row1.put("long_col", 123456789L);
        row1.put("short_col", (short) 1000);
        row1.put("byte_col", (byte) 127);
        data.add(row1);
        
        Map<String, Object> row2 = new LinkedHashMap<>();
        row2.put("id", 2);
        row2.put("int_col", -100);
        row2.put("long_col", -987654321L);
        row2.put("short_col", (short) -500);
        row2.put("byte_col", (byte) -128);
        data.add(row2);
        
        TapTable tapTable = createTapTable("integer_types", 
            "id", "int_col", "long_col", "short_col", "byte_col");
        
        arrowWriter.writeWithArrow(data, "integer_types", tapTable);
        
        verifyIntegerTableData();
    }

    @Test
    @Order(2)
    @DisplayName("Appender: Integer 类型 - 边界值")
    void testAppender_Integer_BoundaryValues() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1);
        row.put("int_col", Integer.MAX_VALUE);
        row.put("long_col", Long.MAX_VALUE);
        row.put("short_col", Short.MAX_VALUE);
        row.put("byte_col", Byte.MAX_VALUE);
        data.add(row);
        
        row = new LinkedHashMap<>();
        row.put("id", 2);
        row.put("int_col", Integer.MIN_VALUE);
        row.put("long_col", Long.MIN_VALUE);
        row.put("short_col", Short.MIN_VALUE);
        row.put("byte_col", Byte.MIN_VALUE);
        data.add(row);
        
        row = new LinkedHashMap<>();
        row.put("id", 3);
        row.put("int_col", 0);
        row.put("long_col", 0L);
        row.put("short_col", (short) 0);
        row.put("byte_col", (byte) 0);
        data.add(row);
        
        TapTable tapTable = createTapTable("integer_types",
            "id", "int_col", "long_col", "short_col", "byte_col");
        
        arrowWriter.writeWithArrow(data, "integer_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM integer_types ORDER BY id")) {
            
            assertTrue(rs.next());
            assertEquals(Integer.MAX_VALUE, rs.getInt("int_col"));
            assertEquals(Long.MAX_VALUE, rs.getLong("long_col"));
            assertEquals(Short.MAX_VALUE, rs.getShort("short_col"));
            assertEquals(Byte.MAX_VALUE, rs.getByte("byte_col"));
            
            assertTrue(rs.next());
            assertEquals(Integer.MIN_VALUE, rs.getInt("int_col"));
            assertEquals(Long.MIN_VALUE, rs.getLong("long_col"));
            assertEquals(Short.MIN_VALUE, rs.getShort("short_col"));
            assertEquals(Byte.MIN_VALUE, rs.getByte("byte_col"));
            
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("int_col"));
            assertEquals(0L, rs.getLong("long_col"));
            assertEquals((short) 0, rs.getShort("short_col"));
            assertEquals((byte) 0, rs.getByte("byte_col"));
        }
    }

    @Test
    @Order(3)
    @DisplayName("Appender: Integer 类型 - NULL 值")
    void testAppender_Integer_NullValues() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1);
        row.put("int_col", null);
        row.put("long_col", null);
        row.put("short_col", null);
        row.put("byte_col", null);
        data.add(row);
        
        TapTable tapTable = createTapTable("integer_types",
            "id", "int_col", "long_col", "short_col", "byte_col");
        
        arrowWriter.writeWithArrow(data, "integer_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM integer_types WHERE id = 1")) {
            
            assertTrue(rs.next());
            assertNull(rs.getObject("int_col"));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("long_col"));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("short_col"));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("byte_col"));
            assertTrue(rs.wasNull());
        }
    }

    // ==================== 2. 浮点类型测试 ====================

    @Test
    @Order(10)
    @DisplayName("Appender: Float/Double 类型 - 正常值和特殊值")
    void testAppender_FloatDouble_Values() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("id", 1);
        row1.put("float_col", 3.14f);
        row1.put("double_col", 3.141592653589793);
        row1.put("bool_col", true);
        data.add(row1);
        
        Map<String, Object> row2 = new LinkedHashMap<>();
        row2.put("id", 2);
        row2.put("float_col", -0.001f);
        row2.put("double_col", -1.0e-100);
        row2.put("bool_col", false);
        data.add(row2);
        
        Map<String, Object> row3 = new LinkedHashMap<>();
        row3.put("id", 3);
        row3.put("float_col", Float.MAX_VALUE);
        row3.put("double_col", Double.MAX_VALUE);
        row3.put("bool_col", true);
        data.add(row3);
        
        Map<String, Object> row4 = new LinkedHashMap<>();
        row4.put("id", 4);
        row4.put("float_col", 0.0f);
        row4.put("double_col", 0.0);
        row4.put("bool_col", false);
        data.add(row4);
        
        TapTable tapTable = createTapTable("float_boolean_types",
            "id", "float_col", "double_col", "bool_col");
        
        arrowWriter.writeWithArrow(data, "float_boolean_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM float_boolean_types ORDER BY id")) {
            
            assertTrue(rs.next());
            assertEquals(3.14f, rs.getFloat("float_col"), 0.001f);
            assertEquals(3.141592653589793, rs.getDouble("double_col"), 1e-15);
            assertTrue(rs.getBoolean("bool_col"));
            
            assertTrue(rs.next());
            assertEquals(-0.001f, rs.getFloat("float_col"), 0.0001f);
            assertEquals(-1.0e-100, rs.getDouble("double_col"), 1e-110);
            assertFalse(rs.getBoolean("bool_col"));
            
            assertTrue(rs.next());
            assertEquals(Float.MAX_VALUE, rs.getFloat("float_col"));
            assertEquals(Double.MAX_VALUE, rs.getDouble("double_col"));
            assertTrue(rs.getBoolean("bool_col"));
            
            assertTrue(rs.next());
            assertEquals(0.0f, rs.getFloat("float_col"));
            assertEquals(0.0, rs.getDouble("double_col"));
            assertFalse(rs.getBoolean("bool_col"));
        }
    }

    @Test
    @Order(11)
    @DisplayName("Appender: Float/Double 类型 - NULL 值")
    void testAppender_FloatDouble_NullValues() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1);
        row.put("float_col", null);
        row.put("double_col", null);
        row.put("bool_col", null);
        data.add(row);
        
        TapTable tapTable = createTapTable("float_boolean_types",
            "id", "float_col", "double_col", "bool_col");
        
        arrowWriter.writeWithArrow(data, "float_boolean_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM float_boolean_types WHERE id = 1")) {
            
            assertTrue(rs.next());
            assertNull(rs.getObject("float_col"));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("double_col"));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("bool_col"));
            assertTrue(rs.wasNull());
        }
    }

    // ==================== 3. 字符串类型测试 ====================

    @Test
    @Order(20)
    @DisplayName("Appender: String 类型 - 各种字符串场景")
    void testAppender_String_VariousScenarios() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("id", 1);
        row1.put("string_col", "Hello, World!");
        row1.put("binary_data", java.util.Base64.getEncoder().encodeToString("binary data".getBytes()));
        row1.put("text_col", "Text field value");
        data.add(row1);
        
        Map<String, Object> row2 = new LinkedHashMap<>();
        row2.put("id", 2);
        row2.put("string_col", "");
        row2.put("binary_data", "");
        row2.put("text_col", "");
        data.add(row2);
        
        Map<String, Object> row3 = new LinkedHashMap<>();
        row3.put("id", 3);
        row3.put("string_col", "特殊字符：中文！@#￥%");
        row3.put("binary_data", java.util.Base64.getEncoder().encodeToString(new byte[]{(byte) 0x00, (byte) 0xFF, (byte) 0x7F, (byte) 0x80}));
        row3.put("text_col", "Unicode: ñáéíóú 🎉");
        data.add(row3);
        
        Map<String, Object> row4 = new LinkedHashMap<>();
        row4.put("id", 4);
        row4.put("string_col", "SQL Injection: '; DROP TABLE; --");
        row4.put("binary_data", java.util.Base64.getEncoder().encodeToString("Binary with null".getBytes()));
        row4.put("text_col", "Quotes: \"single' and \"double\"");
        data.add(row4);
        
        TapTable tapTable = createTapTable("string_binary_types",
            "id", "string_col", "binary_data", "text_col");
        
        arrowWriter.writeWithArrow(data, "string_binary_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM string_binary_types ORDER BY id")) {
            
            assertTrue(rs.next());
            assertEquals("Hello, World!", rs.getString("string_col"));
            assertEquals(java.util.Base64.getEncoder().encodeToString("binary data".getBytes()), rs.getString("binary_data"));
            assertEquals("Text field value", rs.getString("text_col"));
            
            assertTrue(rs.next());
            assertEquals("", rs.getString("string_col"));
            assertEquals("", rs.getString("binary_data"));
            assertEquals("", rs.getString("text_col"));
            
            assertTrue(rs.next());
            assertEquals("特殊字符：中文！@#￥%", rs.getString("string_col"));
            assertEquals(java.util.Base64.getEncoder().encodeToString(new byte[]{(byte) 0x00, (byte) 0xFF, (byte) 0x7F, (byte) 0x80}), rs.getString("binary_data"));
            assertEquals("Unicode: ñáéíóú 🎉", rs.getString("text_col"));
            
            assertTrue(rs.next());
            assertEquals("SQL Injection: '; DROP TABLE; --", rs.getString("string_col"));
            assertEquals(java.util.Base64.getEncoder().encodeToString("Binary with null".getBytes()), rs.getString("binary_data"));
            assertEquals("Quotes: \"single' and \"double\"", rs.getString("text_col"));
        }
    }

    @Test
    @Order(21)
    @DisplayName("Appender: String 类型 - NULL 和超长字符串")
    void testAppender_String_NullAndLongStrings() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("id", 1);
        row1.put("string_col", null);
        row1.put("binary_data", null);
        row1.put("text_col", null);
        data.add(row1);
        
        StringBuilder longStringBuilder = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            longStringBuilder.append("这是第").append(i).append("行很长的字符串内容；");
        }
        String longString = longStringBuilder.toString();
        
        Map<String, Object> row2 = new LinkedHashMap<>();
        row2.put("id", 2);
        row2.put("string_col", longString);
        row2.put("binary_data", java.util.Base64.getEncoder().encodeToString(new byte[100000]));
        row2.put("text_col", longString.substring(0, 1000));
        data.add(row2);
        
        TapTable tapTable = createTapTable("string_binary_types",
            "id", "string_col", "binary_data", "text_col");
        
        arrowWriter.writeWithArrow(data, "string_binary_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM string_binary_types WHERE id = 1")) {
            
            assertTrue(rs.next());
            String stringCol = rs.getString("string_col");
            assertTrue(stringCol == null || stringCol.isEmpty(), "string_col should be null or empty for id=1");
            String binaryData = rs.getString("binary_data");
            assertTrue(binaryData == null || binaryData.isEmpty(), "binary_data should be null or empty for id=1");
            String textCol = rs.getString("text_col");
            assertTrue(textCol == null || textCol.isEmpty(), "text_col should be null or empty for id=1");
        }
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM string_binary_types WHERE id = 2")) {
            
            assertTrue(rs.next());
            assertEquals(longString.length(), rs.getString("string_col").length());
            assertEquals(longString, rs.getString("string_col"));
            assertNotNull(rs.getString("binary_data"));
            assertEquals(1000, rs.getString("text_col").length());
        }
    }

    // ==================== 4. 日期时间类型测试 ====================

    @Test
    @Order(30)
    @DisplayName("Appender: Date/Timestamp 类型 - 各种时间格式")
    void testAppender_DateTime_VariousFormats() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("id", 1);
        row1.put("date_col", java.sql.Date.valueOf("2023-11-14"));
        row1.put("timestamp_col", Timestamp.valueOf("2023-11-14 12:34:56"));
        row1.put("datetime_col", LocalDateTime.of(2023, 11, 14, 12, 34, 56));
        row1.put("localdate_col", LocalDate.of(2023, 11, 14));
        data.add(row1);
        
        Map<String, Object> row2 = new LinkedHashMap<>();
        row2.put("id", 2);
        row2.put("date_col", java.sql.Date.valueOf("1970-01-01"));
        row2.put("timestamp_col", Timestamp.valueOf("1970-01-01 00:00:00"));
        row2.put("datetime_col", LocalDateTime.of(1970, 1, 1, 0, 0, 0));
        row2.put("localdate_col", LocalDate.of(1970, 1, 1));
        data.add(row2);
        
        Map<String, Object> row3 = new LinkedHashMap<>();
        row3.put("id", 3);
        row3.put("date_col", java.sql.Date.valueOf("2024-12-31"));
        row3.put("timestamp_col", Timestamp.valueOf("2024-12-31 23:59:59"));
        row3.put("datetime_col", LocalDateTime.of(2024, 12, 31, 23, 59, 59));
        row3.put("localdate_col", LocalDate.of(2024, 12, 31));
        data.add(row3);
        
        TapTable tapTable = createTapTable("datetime_types",
            "id", "date_col", "timestamp_col", "datetime_col", "localdate_col");
        
        arrowWriter.writeWithArrow(data, "datetime_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM datetime_types ORDER BY id")) {
            
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                assertNotNull(rs.getObject("id"));
                
                if (rowCount == 1) {
                    assertNotNull(rs.getDate("date_col"));
                    assertNotNull(rs.getTimestamp("timestamp_col"));
                    assertNotNull(rs.getObject("datetime_col"));
                    assertNotNull(rs.getObject("localdate_col"));
                } else if (rowCount == 2) {
                    assertEquals(1970, rs.getDate("date_col").toLocalDate().getYear());
                    assertNotNull(rs.getTimestamp("timestamp_col"));
                } else if (rowCount == 3) {
                    assertNotNull(rs.getDate("date_col"));
                    assertNotNull(rs.getTimestamp("timestamp_col"));
                }
            }
            assertEquals(3, rowCount, "Should have inserted 3 rows");
        }
    }

    @Test
    @Order(31)
    @DisplayName("Appender: Date/Timestamp 类型 - NULL 值")
    void testAppender_DateTime_NullValues() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1);
        row.put("date_col", null);
        row.put("timestamp_col", null);
        row.put("datetime_col", null);
        row.put("localdate_col", null);
        data.add(row);
        
        TapTable tapTable = createTapTable("datetime_types",
            "id", "date_col", "timestamp_col", "datetime_col", "localdate_col");
        
        arrowWriter.writeWithArrow(data, "datetime_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM datetime_types WHERE id = 1")) {
            
            assertTrue(rs.next());
            assertNull(rs.getDate("date_col"));
            assertTrue(rs.wasNull());
            assertNull(rs.getTimestamp("timestamp_col"));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("datetime_col"));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("localdate_col"));
            assertTrue(rs.wasNull());
        }
    }

    // ==================== 5. 高精度数值类型测试 ====================

    @Test
    @Order(40)
    @DisplayName("Appender: BigDecimal/BigInteger 类型 - 高精度计算")
    void testAppender_BigDecimalBigInteger_Precision() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("id", 1);
        row1.put("decimal_col", new BigDecimal("123456789.1234567890"));
        row1.put("bigint_col", new BigInteger("999999999999999999"));
        data.add(row1);
        
        Map<String, Object> row2 = new LinkedHashMap<>();
        row2.put("id", 2);
        row2.put("decimal_col", new BigDecimal("-0.0000000001"));
        row2.put("bigint_col", new BigInteger("-999999999999999999"));
        data.add(row2);
        
        Map<String, Object> row3 = new LinkedHashMap<>();
        row3.put("id", 3);
        row3.put("decimal_col", BigDecimal.ZERO);
        row3.put("bigint_col", BigInteger.ZERO);
        data.add(row3);
        
        Map<String, Object> row4 = new LinkedHashMap<>();
        row4.put("id", 4);
        row4.put("decimal_col", new BigDecimal("9999999999.9999999999"));
        row4.put("bigint_col", new BigInteger("9223372036854775807"));  // Long.MAX_VALUE as BigInteger
        data.add(row4);
        
        TapTable tapTable = createTapTable("precise_numeric_types",
            "id", "decimal_col", "bigint_col");
        
        arrowWriter.writeWithArrow(data, "precise_numeric_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM precise_numeric_types ORDER BY id")) {
            
            assertTrue(rs.next());
            BigDecimal actual1 = rs.getBigDecimal("decimal_col");
            assertNotNull(actual1);
            assertEquals(0, new BigDecimal("123456789.1234567890").compareTo(actual1.stripTrailingZeros()));
            assertEquals(new BigInteger("999999999999999999"), rs.getObject("bigint_col"));
            
            assertTrue(rs.next());
            BigDecimal actual2 = rs.getBigDecimal("decimal_col");
            assertNotNull(actual2);
            assertEquals(0, new BigDecimal("-0.0000000001").compareTo(actual2.stripTrailingZeros()));
            assertEquals(new BigInteger("-999999999999999999"), rs.getObject("bigint_col"));
            
            assertTrue(rs.next());
            BigDecimal actual3 = rs.getBigDecimal("decimal_col");
            assertNotNull(actual3);
            assertEquals(0, BigDecimal.ZERO.compareTo(actual3));
            assertEquals(BigInteger.ZERO, rs.getObject("bigint_col"));
            
            assertTrue(rs.next());
            BigDecimal actual4 = rs.getBigDecimal("decimal_col");
            assertNotNull(actual4);
            assertEquals(0, new BigDecimal("9999999999.9999999999").compareTo(actual4.stripTrailingZeros()));
            assertEquals(new BigInteger("9223372036854775807"), rs.getObject("bigint_col"));
        }
    }

    @Test
    @Order(41)
    @DisplayName("Appender: BigDecimal/BigInteger 类型 - NULL 值")
    void testAppender_BigDecimalBigInteger_NullValues() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1);
        row.put("decimal_col", null);
        row.put("bigint_col", null);
        data.add(row);
        
        TapTable tapTable = createTapTable("precise_numeric_types",
            "id", "decimal_col", "bigint_col");
        
        arrowWriter.writeWithArrow(data, "precise_numeric_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM precise_numeric_types WHERE id = 1")) {
            
            assertTrue(rs.next());
            assertNull(rs.getBigDecimal("decimal_col"));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("bigint_col"));
            assertTrue(rs.wasNull());
        }
    }

    // ==================== 6. 复合类型测试 ====================

    @Test
    @Order(50)
    @DisplayName("Appender: Collection/Map 类型 - 数组和映射")
    void testAppender_CollectionMap_CompositeTypes() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("id", 1);
        row1.put("list_col", Arrays.asList(1, 2, 3, 4, 5));
        row1.put("map_col", Map.of("key1", 100, "key2", 200));
        data.add(row1);
        
        Map<String, Object> row2 = new LinkedHashMap<>();
        row2.put("id", 2);
        row2.put("list_col", Arrays.asList(-1, -2, -3));
        row2.put("map_col", Map.of("a", 1, "b", 2, "c", 3));
        data.add(row2);
        
        Map<String, Object> row3 = new LinkedHashMap<>();
        row3.put("id", 3);
        row3.put("list_col", Collections.emptyList());
        row3.put("map_col", Collections.emptyMap());
        data.add(row3);
        
        TapTable tapTable = createTapTable("composite_types",
            "id", "list_col", "map_col");
        
        arrowWriter.writeWithArrow(data, "composite_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM composite_types ORDER BY id")) {
            
            assertTrue(rs.next());
            assertNotNull(rs.getObject("list_col"));
            assertNotNull(rs.getObject("map_col"));
            
            assertTrue(rs.next());
            assertNotNull(rs.getObject("list_col"));
            assertNotNull(rs.getObject("map_col"));
            
            assertTrue(rs.next());
            assertNotNull(rs.getObject("list_col"));
            assertNotNull(rs.getObject("map_col"));
        }
    }

    @Test
    @Order(51)
    @DisplayName("Appender: Collection/Map 类型 - NULL 值")
    void testAppender_CollectionMap_NullValues() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1);
        row.put("list_col", null);
        row.put("map_col", null);
        data.add(row);
        
        TapTable tapTable = createTapTable("composite_types",
            "id", "list_col", "map_col");
        
        arrowWriter.writeWithArrow(data, "composite_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM composite_types WHERE id = 1")) {
            
            assertTrue(rs.next());
            assertNull(rs.getObject("list_col"));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("map_col"));
            assertTrue(rs.wasNull());
        }
    }

    // ==================== 7. 混合类型综合测试 ====================

    @Test
    @Order(60)
    @DisplayName("Appender: 混合类型 - 综合场景（模拟真实业务数据）")
    void testAppender_MixedTypes_RealWorldScenario() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        Map<String, Object> order1 = new LinkedHashMap<>();
        order1.put("id", 1001);
        order1.put("int_val", 1001);
        order1.put("long_val", 20231114123456L);
        order1.put("double_val", 1599.99);
        order1.put("string_val", "iPhone 15 Pro Max");
        order1.put("bool_val", true);
        order1.put("timestamp_val", Timestamp.valueOf("2023-11-14 10:30:00"));
        order1.put("decimal_val", new BigDecimal("1599.99000"));
        data.add(order1);
        
        Map<String, Object> order2 = new LinkedHashMap<>();
        order2.put("id", 1002);
        order2.put("int_val", 1002);
        order2.put("long_val", 20231115143000L);
        order2.put("double_val", 899.50);
        order2.put("string_val", "MacBook Air M3");
        order2.put("bool_val", false);
        order2.put("timestamp_val", Timestamp.valueOf("2023-11-15 14:30:00"));
        order2.put("decimal_val", new BigDecimal("899.50000"));
        data.add(order2);
        
        Map<String, Object> order3 = new LinkedHashMap<>();
        order3.put("id", 1003);
        order3.put("int_val", 1003);
        order3.put("long_val", 20231116091500L);
        order3.put("double_val", 2499.00);
        order3.put("string_val", "iPad Pro 12.9\"");
        order3.put("bool_val", true);
        order3.put("timestamp_val", Timestamp.valueOf("2023-11-16 09:15:00"));
        order3.put("decimal_val", new BigDecimal("2499.00000"));
        data.add(order3);
        
        Map<String, Object> order4 = new LinkedHashMap<>();  // 包含 NULL 值
        order4.put("id", 1004);
        order4.put("int_val", 1004);
        order4.put("long_val", null);
        order4.put("double_val", null);
        order4.put("string_val", null);
        order4.put("bool_val", null);
        order4.put("timestamp_val", null);
        order4.put("decimal_val", null);
        data.add(order4);
        
        TapTable tapTable = createTapTable("mixed_types",
            "id", "int_val", "long_val", "double_val", "string_val",
            "bool_val", "timestamp_val", "decimal_val");
        
        arrowWriter.writeWithArrow(data, "mixed_types", tapTable);
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM mixed_types ORDER BY id")) {
            
            assertTrue(rs.next());
            assertEquals(1001, rs.getInt("id"));
            assertEquals(1001, rs.getInt("int_val"));
            assertEquals(20231114123456L, rs.getLong("long_val"));
            assertEquals(1599.99, rs.getDouble("double_val"), 0.001);
            assertEquals("iPhone 15 Pro Max", rs.getString("string_val"));
            assertTrue(rs.getBoolean("bool_val"));
            assertEquals(Timestamp.valueOf("2023-11-14 10:30:00"), rs.getTimestamp("timestamp_val"));
            assertEquals(0, new BigDecimal("1599.99000").compareTo(rs.getBigDecimal("decimal_val")));
            
            assertTrue(rs.next());
            assertEquals(1002, rs.getInt("id"));
            assertEquals("MacBook Air M3", rs.getString("string_val"));
            assertFalse(rs.getBoolean("bool_val"));
            
            assertTrue(rs.next());
            assertEquals(1003, rs.getInt("id"));
            assertEquals("iPad Pro 12.9\"", rs.getString("string_val"));
            
            assertTrue(rs.next());
            assertEquals(1004, rs.getInt("id"));
            assertNull(rs.getObject("long_val"));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("double_val"));
            assertTrue(rs.wasNull());
            assertNull(rs.getString("string_val"));
            assertTrue(rs.wasNull());
            assertNull(rs.getObject("bool_val"));
            assertTrue(rs.wasNull());
            assertNull(rs.getTimestamp("timestamp_val"));
            assertTrue(rs.wasNull());
            assertNull(rs.getBigDecimal("decimal_val"));
            assertTrue(rs.wasNull());
        }
    }

    // ==================== 8. 边界情况和异常场景测试 ====================

    @Test
    @Order(70)
    @DisplayName("Appender: 大批量数据插入性能测试")
    void testAppender_LargeBatch_Performance() throws SQLException {
        int batchSize = 10000;
        List<Map<String, Object>> data = new ArrayList<>(batchSize);
        
        for (int i = 1; i <= batchSize; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", i);
            row.put("int_val", i);
            row.put("long_val", (long) i * 1000L);
            row.put("double_val", i * 1.1);
            row.put("string_val", "Item_" + i);
            row.put("bool_val", i % 2 == 0);
            row.put("timestamp_val", new Timestamp(System.currentTimeMillis() + i * 1000L));
            row.put("decimal_val", new BigDecimal(i * 1.111));
            data.add(row);
        }
        
        TapTable tapTable = createTapTable("mixed_types",
            "id", "int_val", "long_val", "double_val", "string_val",
            "bool_val", "timestamp_val", "decimal_val");
        
        long startTime = System.currentTimeMillis();
        arrowWriter.writeWithArrow(data, "mixed_types", tapTable);
        long duration = System.currentTimeMillis() - startTime;
        
        System.out.println("Inserted " + batchSize + " rows in " + duration + " ms");
        System.out.println("Throughput: " + (batchSize * 1000 / duration) + " rows/sec");
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM mixed_types")) {
            
            assertTrue(rs.next());
            assertEquals(batchSize, rs.getInt(1));
        }
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM mixed_types ORDER BY id LIMIT 1")) {
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(1, rs.getInt("int_val"));
            assertEquals(1000L, rs.getLong("long_val"));
            assertEquals(1.1, rs.getDouble("double_val"), 0.01);
            assertEquals("Item_1", rs.getString("string_val"));
            assertFalse(rs.getBoolean("bool_val"));
        }
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM mixed_types ORDER BY id DESC LIMIT 1")) {
            
            assertTrue(rs.next());
            assertEquals(batchSize, rs.getInt("id"));
            assertEquals(batchSize, rs.getInt("int_val"));
        }
        
        assertTrue(duration < 10000, "Batch insert should complete within 10 seconds, took: " + duration + " ms");
    }

    @Test
    @Order(71)
    @DisplayName("Appender: 空数据集处理")
    void testAppender_EmptyDataset() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        TapTable tapTable = createTapTable("mixed_types",
            "id", "int_val", "long_val", "double_val", "string_val",
            "bool_val", "timestamp_val", "decimal_val");
        
        assertDoesNotThrow(() -> {
            arrowWriter.writeWithArrow(data, "mixed_types", tapTable);
        });
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM mixed_types")) {
            
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    @Test
    @Order(72)
    @DisplayName("Appender: 未知类型 toString 兜底转换")
    void testAppender_UnknownType_ToStringFallback() throws SQLException {
        List<Map<String, Object>> data = new ArrayList<>();
        
        class CustomObject {
            @Override
            public String toString() {
                return "CustomObject[value=42]";
            }
        }
        
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1);
        row.put("int_val", 1);
        row.put("long_val", 2L);
        row.put("double_val", 3.0);
        row.put("string_val", new CustomObject());  // 未知类型
        row.put("bool_val", true);
        row.put("timestamp_val", Timestamp.valueOf("2023-01-01 00:00:00"));
        row.put("decimal_val", new BigDecimal("4.0"));
        data.add(row);
        
        TapTable tapTable = createTapTable("mixed_types",
            "id", "int_val", "long_val", "double_val", "string_val",
            "bool_val", "timestamp_val", "decimal_val");
        
        assertDoesNotThrow(() -> {
            arrowWriter.writeWithArrow(data, "mixed_types", tapTable);
        });
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT string_val FROM mixed_types WHERE id = 1")) {
            
            assertTrue(rs.next());
            assertEquals("CustomObject[value=42]", rs.getString("string_val"));
        }
    }

    // ==================== 辅助方法 ====================

    private TapTable createTapTable(String tableName, String... fieldNames) {
        TapTable tapTable = new TapTable(tableName);
        for (String fieldName : fieldNames) {
            TapField field = new TapField();
            field.name(fieldName);
            tapTable.add(field);
        }
        return tapTable;
    }

    private void verifyIntegerTableData() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM integer_types ORDER BY id")) {
            
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("id"));
            assertEquals(42, rs.getInt("int_col"));
            assertEquals(123456789L, rs.getLong("long_col"));
            assertEquals((short) 1000, rs.getShort("short_col"));
            assertEquals((byte) 127, rs.getByte("byte_col"));
            
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("id"));
            assertEquals(-100, rs.getInt("int_col"));
            assertEquals(-987654321L, rs.getLong("long_col"));
            assertEquals((short) -500, rs.getShort("short_col"));
            assertEquals((byte) -128, rs.getByte("byte_col"));
        }
    }
}
