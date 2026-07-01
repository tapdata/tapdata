package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperator;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperatorImpl;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class HazelcastDuckDbSqlNodeIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(HazelcastDuckDbSqlNodeIntegrationTest.class);

    private static final String JDBC_URL = "jdbc:duckdb::memory:";

    private static DuckDbOperator duckDbOperator;

    private static final AtomicLong hospitalIdSeq = new AtomicLong(1);
    private static final AtomicLong patientIdSeq = new AtomicLong(1);
    private static final AtomicLong recordIdSeq = new AtomicLong(1);

    @BeforeAll
    static void beforeAll() throws SQLException {
        logger.info("Setting up DuckDB connection...");
        java.sql.Connection connection = DriverManager.getConnection(JDBC_URL);
        duckDbOperator = new DuckDbOperatorImpl(connection);
        logger.info("DuckDB connection established successfully.");
    }

    @AfterAll
    static void afterAll() throws SQLException {
        logger.info("Closing DuckDB connection...");
        if (duckDbOperator != null) {
            duckDbOperator.close();
        }
        logger.info("DuckDB connection closed.");
    }

    @BeforeEach
    void beforeEach() throws SQLException {
        logger.info("Cleaning up test data...");
        safeDropTable("patient_wide_table");
        safeDropTable("medical_records");
        safeDropTable("patients");
        safeDropTable("hospitals");
        logger.info("Test data cleaned up.");
    }

    private void safeDropTable(String tableName) {
        try {
            duckDbOperator.execute("DROP TABLE IF EXISTS " + tableName);
        } catch (Exception e) {
            logger.warn("Failed to drop table {}: {}", tableName, e.getMessage());
        }
    }

    // ==================== 辅助方法 ====================

    private TapTable createHospitalTable() {
        TapTable table = new TapTable();
        table.setName("hospitals");
        TapField field;

        field = new TapField();
        field.setName("hospital_id");
        field.setDataType("BIGINT");
        field.setPrimaryKey(true);
        table.add(field);

        field = new TapField();
        field.setName("hospital_name");
        field.setDataType("VARCHAR(255)");
        table.add(field);

        field = new TapField();
        field.setName("address");
        field.setDataType("VARCHAR(500)");
        table.add(field);

        field = new TapField();
        field.setName("contact_phone");
        field.setDataType("VARCHAR(50)");
        table.add(field);

        field = new TapField();
        field.setName("created_at");
        field.setDataType("TIMESTAMP");
        table.add(field);

        field = new TapField();
        field.setName("updated_at");
        field.setDataType("TIMESTAMP");
        table.add(field);

        return table;
    }

    private TapTable createPatientTable() {
        TapTable table = new TapTable();
        table.setName("patients");
        TapField field;

        field = new TapField();
        field.setName("patient_id");
        field.setDataType("BIGINT");
        field.setPrimaryKey(true);
        table.add(field);

        field = new TapField();
        field.setName("hospital_id");
        field.setDataType("BIGINT");
        table.add(field);

        field = new TapField();
        field.setName("name");
        field.setDataType("VARCHAR(100)");
        table.add(field);

        field = new TapField();
        field.setName("gender");
        field.setDataType("VARCHAR(10)");
        table.add(field);

        field = new TapField();
        field.setName("birth_date");
        field.setDataType("DATE");
        table.add(field);

        field = new TapField();
        field.setName("id_card");
        field.setDataType("VARCHAR(50)");
        table.add(field);

        field = new TapField();
        field.setName("phone");
        field.setDataType("VARCHAR(50)");
        table.add(field);

        field = new TapField();
        field.setName("address");
        field.setDataType("VARCHAR(500)");
        table.add(field);

        field = new TapField();
        field.setName("created_at");
        field.setDataType("TIMESTAMP");
        table.add(field);

        field = new TapField();
        field.setName("updated_at");
        field.setDataType("TIMESTAMP");
        table.add(field);

        return table;
    }

    private TapTable createMedicalRecordTable() {
        TapTable table = new TapTable();
        table.setName("medical_records");
        TapField field;

        field = new TapField();
        field.setName("record_id");
        field.setDataType("BIGINT");
        field.setPrimaryKey(true);
        table.add(field);

        field = new TapField();
        field.setName("patient_id");
        field.setDataType("BIGINT");
        table.add(field);

        field = new TapField();
        field.setName("hospital_id");
        field.setDataType("BIGINT");
        table.add(field);

        field = new TapField();
        field.setName("visit_date");
        field.setDataType("DATE");
        table.add(field);

        field = new TapField();
        field.setName("department");
        field.setDataType("VARCHAR(100)");
        table.add(field);

        field = new TapField();
        field.setName("doctor_name");
        field.setDataType("VARCHAR(100)");
        table.add(field);

        field = new TapField();
        field.setName("diagnosis");
        field.setDataType("TEXT");
        table.add(field);

        field = new TapField();
        field.setName("prescription");
        field.setDataType("TEXT");
        table.add(field);

        field = new TapField();
        field.setName("created_at");
        field.setDataType("TIMESTAMP");
        table.add(field);

        field = new TapField();
        field.setName("updated_at");
        field.setDataType("TIMESTAMP");
        table.add(field);

        return table;
    }

    private Map<String, Object> generateHospitalData(long hospitalId) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("hospital_id", hospitalId);
        data.put("hospital_name", "第" + hospitalId + "人民医院");
        data.put("address", "北京市朝阳区第" + hospitalId + "号大街");
        data.put("contact_phone", "010-" + String.format("%08d", (10000000 + hospitalId)));
        data.put("created_at", new Date());
        data.put("updated_at", new Date());
        return data;
    }

    private Map<String, Object> generatePatientData(long patientId, long hospitalId) {
        String[] genders = {"男", "女"};
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("patient_id", patientId);
        data.put("hospital_id", hospitalId);
        data.put("name", "病人" + patientId);
        data.put("gender", genders[(int)(patientId % 2)]);
        data.put("birth_date", "19" + String.format("%02d", (1950 + patientId % 60)) + "-" + String.format("%02d", (1 + patientId % 12)) + "-" + String.format("%02d", (1 + patientId % 28)));
        data.put("id_card", "110" + String.format("%015d", patientId));
        data.put("phone", "138" + String.format("%08d", (10000000 + patientId)));
        data.put("address", "北京市海淀区第" + (patientId % 100) + "号");
        data.put("created_at", new Date());
        data.put("updated_at", new Date());
        return data;
    }

    private Map<String, Object> generateMedicalRecordData(long recordId, long patientId, long hospitalId) {
        String[] departments = {"内科", "外科", "儿科", "妇产科", "眼科", "耳鼻喉科"};
        String[] doctorNames = {"张医生", "李医生", "王医生", "赵医生", "刘医生", "陈医生"};
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("record_id", recordId);
        data.put("patient_id", patientId);
        data.put("hospital_id", hospitalId);
        data.put("visit_date", "2024-" + String.format("%02d", (1 + recordId % 12)) + "-" + String.format("%02d", (1 + recordId % 28)));
        data.put("department", departments[(int)(recordId % departments.length)]);
        data.put("doctor_name", doctorNames[(int)(recordId % doctorNames.length)]);
        data.put("diagnosis", "诊断结果" + recordId + "：常规检查，无异常");
        data.put("prescription", "处方" + recordId + "：维生素C，每日3次");
        data.put("created_at", new Date());
        data.put("updated_at", new Date());
        return data;
    }

    private TapdataEvent createInsertEvent(String tableName, Map<String, Object> data) {
        TapInsertRecordEvent tapEvent = TapInsertRecordEvent.create()
                .after(data)
                .table(tableName);
        
        TapdataEvent event = new TapdataEvent();
        event.setTapEvent(tapEvent);
        return event;
    }

    private void assertTableRowCount(String tableName, long expectedCount) throws SQLException {
        List<Map<String, Object>> result = duckDbOperator.executeQuery("SELECT COUNT(*) AS cnt FROM " + tableName);
        assertNotNull(result);
        assertEquals(1, result.size());
        Object cnt = result.get(0).get("cnt");
        if (cnt instanceof Long) {
            assertEquals(expectedCount, (Long) cnt);
        } else if (cnt instanceof Integer) {
            assertEquals(expectedCount, ((Integer) cnt).longValue());
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class FullSyncTest {

        @Test
        @Order(1)
        void testCreateTables() throws SQLException {
            logger.info("Testing table creation...");

            // 创建医院表
            TapTable hospitalTable = createHospitalTable();
            duckDbOperator.createTable(hospitalTable);
            assertTrue(duckDbOperator.tableExists("hospitals"));

            // 创建病人表
            TapTable patientTable = createPatientTable();
            duckDbOperator.createTable(patientTable);
            assertTrue(duckDbOperator.tableExists("patients"));

            // 创建病历表
            TapTable recordTable = createMedicalRecordTable();
            duckDbOperator.createTable(recordTable);
            assertTrue(duckDbOperator.tableExists("medical_records"));

            logger.info("Table creation test passed.");
        }

        @Test
        @Order(2)
        void testBulkInsertData() throws SQLException {
            logger.info("Testing bulk data insertion...");

            // 创建表
            TapTable hospitalTable = createHospitalTable();
            duckDbOperator.createTable(hospitalTable);

            TapTable patientTable = createPatientTable();
            duckDbOperator.createTable(patientTable);

            TapTable recordTable = createMedicalRecordTable();
            duckDbOperator.createTable(recordTable);

            // 插入大规模测试数据 - 注意：为了测试运行速度，我们先使用较小的规模
            int numHospitals = 2; // 原计划是 20，先使用小的测试
            int patientsPerHospital = 5; // 原计划是 100，先使用小的测试
            int recordsPerPatient = 2; // 原计划是 10，先使用小的测试

            List<Long> hospitalIds = new ArrayList<>();

            // 插入医院数据
            for (int i = 0; i < numHospitals; i++) {
                long hospitalId = hospitalIdSeq.getAndIncrement();
                hospitalIds.add(hospitalId);
                Map<String, Object> data = generateHospitalData(hospitalId);
                duckDbOperator.insert("hospitals", data);
            }
            assertTableRowCount("hospitals", numHospitals);
            logger.info("Inserted {} hospitals", numHospitals);

            // 插入病人数据
            int totalPatients = 0;
            for (long hospitalId : hospitalIds) {
                for (int j = 0; j < patientsPerHospital; j++) {
                    long patientId = patientIdSeq.getAndIncrement();
                    Map<String, Object> data = generatePatientData(patientId, hospitalId);
                    duckDbOperator.insert("patients", data);
                    totalPatients++;
                }
            }
            assertTableRowCount("patients", totalPatients);
            logger.info("Inserted {} patients", totalPatients);

            // 插入病历数据
            int totalRecords = 0;
            for (long hospitalId : hospitalIds) {
                for (int j = 0; j < patientsPerHospital; j++) {
                    long patientId = (hospitalId - (hospitalIdSeq.get() - hospitalIds.size())) * patientsPerHospital + j + 1; // 修正id计算
                    for (int k = 0; k < recordsPerPatient; k++) {
                        long recordId = recordIdSeq.getAndIncrement();
                        Map<String, Object> data = generateMedicalRecordData(recordId, patientId, hospitalId);
                        duckDbOperator.insert("medical_records", data);
                        totalRecords++;
                    }
                }
            }
            assertTableRowCount("medical_records", totalRecords);
            logger.info("Inserted {} medical records", totalRecords);

            logger.info("Bulk data insertion test passed.");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class WideTableQueryTest {

        @BeforeEach
        void setupTestData() throws SQLException {
            logger.info("Setting up test data for queries...");

            // 创建表
            TapTable hospitalTable = createHospitalTable();
            duckDbOperator.createTable(hospitalTable);

            TapTable patientTable = createPatientTable();
            duckDbOperator.createTable(patientTable);

            TapTable recordTable = createMedicalRecordTable();
            duckDbOperator.createTable(recordTable);

            // 插入小规模测试数据用于查询验证
            long hospitalId = hospitalIdSeq.getAndIncrement();
            duckDbOperator.insert("hospitals", generateHospitalData(hospitalId));

            for (int i = 0; i < 3; i++) {
                long patientId = patientIdSeq.getAndIncrement();
                duckDbOperator.insert("patients", generatePatientData(patientId, hospitalId));
                
                for (int j = 0; j < 2; j++) {
                    long recordId = recordIdSeq.getAndIncrement();
                    duckDbOperator.insert("medical_records", generateMedicalRecordData(recordId, patientId, hospitalId));
                }
            }

            logger.info("Test data setup complete.");
        }

        @Test
        @Order(1)
        void testBasicJoinQuery() throws SQLException {
            logger.info("Testing basic join query...");

            String sql = "SELECT h.hospital_id, h.hospital_name, p.patient_id, p.name " +
                        "FROM hospitals h " +
                        "JOIN patients p ON h.hospital_id = p.hospital_id";

            List<Map<String, Object>> result = duckDbOperator.executeQuery(sql);
            
            assertNotNull(result);
            assertEquals(3, result.size());

            for (Map<String, Object> row : result) {
                assertNotNull(row.get("hospital_id"));
                assertNotNull(row.get("hospital_name"));
                assertNotNull(row.get("patient_id"));
                assertNotNull(row.get("name"));
            }

            logger.info("Basic join query test passed.");
        }

        @Test
        @Order(2)
        void testAggregationQuery() throws SQLException {
            logger.info("Testing aggregation query...");

            String sql = "SELECT h.hospital_id, COUNT(p.patient_id) AS patient_count " +
                        "FROM hospitals h " +
                        "JOIN patients p ON h.hospital_id = p.hospital_id " +
                        "GROUP BY h.hospital_id";

            List<Map<String, Object>> result = duckDbOperator.executeQuery(sql);
            
            assertNotNull(result);
            assertEquals(1, result.size());

            Map<String, Object> row = result.get(0);
            assertNotNull(row.get("hospital_id"));
            
            Object patientCount = row.get("patient_count");
            if (patientCount instanceof Long) {
                assertEquals(3, (Long) patientCount);
            } else if (patientCount instanceof Integer) {
                assertEquals(3, ((Integer) patientCount).longValue());
            }

            logger.info("Aggregation query test passed.");
        }

        @Test
        @Order(3)
        void testComplexQuery() throws SQLException {
            logger.info("Testing complex query...");

            String sql = "SELECT " +
                        "h.hospital_id, " +
                        "h.hospital_name, " +
                        "p.patient_id, " +
                        "p.name AS patient_name, " +
                        "p.gender, " +
                        "COUNT(mr.record_id) AS total_records " +
                        "FROM hospitals h " +
                        "JOIN patients p ON h.hospital_id = p.hospital_id " +
                        "LEFT JOIN medical_records mr ON p.patient_id = mr.patient_id " +
                        "GROUP BY h.hospital_id, h.hospital_name, p.patient_id, p.name, p.gender";

            List<Map<String, Object>> result = duckDbOperator.executeQuery(sql);
            
            assertNotNull(result);
            assertEquals(3, result.size());

            for (Map<String, Object> row : result) {
                assertNotNull(row.get("total_records"));
            }

            logger.info("Complex query test passed.");
        }
    }

    @Nested
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class EdgeCaseTest {

        @Test
        @Order(1)
        void testEmptyTables() throws SQLException {
            logger.info("Testing empty tables...");

            // 创建但不插入数据
            TapTable hospitalTable = createHospitalTable();
            duckDbOperator.createTable(hospitalTable);
            
            TapTable patientTable = createPatientTable();
            duckDbOperator.createTable(patientTable);

            assertTableRowCount("hospitals", 0);
            assertTableRowCount("patients", 0);

            // 空表查询
            String sql = "SELECT * FROM hospitals h LEFT JOIN patients p ON h.hospital_id = p.hospital_id";
            List<Map<String, Object>> result = duckDbOperator.executeQuery(sql);
            assertNotNull(result);
            assertEquals(0, result.size());

            logger.info("Empty tables test passed.");
        }

        @Test
        @Order(2)
        void testNullValues() throws SQLException {
            logger.info("Testing null values...");

            TapTable hospitalTable = createHospitalTable();
            duckDbOperator.createTable(hospitalTable);

            // 插入带 null 值的数据
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("hospital_id", 999L);
            data.put("hospital_name", "测试医院");
            data.put("address", null);
            data.put("contact_phone", null);
            data.put("created_at", new Date());
            data.put("updated_at", new Date());

            duckDbOperator.insert("hospitals", data);

            // 验证
            List<Map<String, Object>> result = duckDbOperator.executeQuery("SELECT * FROM hospitals WHERE hospital_id = 999");
            assertEquals(1, result.size());
            assertNull(result.get(0).get("address"));
            assertNull(result.get(0).get("contact_phone"));

            logger.info("Null values test passed.");
        }

        @Test
        @Order(3)
        void testDuplicateHandling() throws SQLException {
            logger.info("Testing duplicate handling...");

            TapTable hospitalTable = createHospitalTable();
            duckDbOperator.createTable(hospitalTable);

            long hospitalId = 888L;
            Map<String, Object> data = generateHospitalData(hospitalId);
            duckDbOperator.insert("hospitals", data);

            // 尝试插入重复主键（期望失败或按业务逻辑处理）
            assertThrows(Exception.class, () -> {
                duckDbOperator.insert("hospitals", data);
            });

            logger.info("Duplicate handling test passed.");
        }
    }
}
