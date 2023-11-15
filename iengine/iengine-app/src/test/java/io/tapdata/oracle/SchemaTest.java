package io.tapdata.oracle;

import com.tapdata.entity.DatabaseSchemaConstraints;
import com.tapdata.entity.DatabaseSchemaTableColumns;

import java.util.List;

/**
 * Created by tapdata on 29/11/2017.
 */
public class SchemaTest {

	private List<DatabaseSchemaTableColumns> tableColumns;

	private List<DatabaseSchemaConstraints> pkConstraints;

	private List<DatabaseSchemaConstraints> fkConstraints;

//    @Before
//    public void init(){
//
//        try {
//
//            URL tableColsURL = SchemaTest.class.getClassLoader().getResource("dataset/oracle-schema-cols.json");
//            URL pkConsURL = SchemaTest.class.getClassLoader().getResource("dataset/oracle-schema-pri.json");
//            URL fkConsURL = SchemaTest.class.getClassLoader().getResource("dataset/oracle-schema-fk.json");
//            String tableColsJson = new String(Files.readAllBytes(Paths.get(tableColsURL.toURI())));
//            String pkConsJson = new String(Files.readAllBytes(Paths.get(pkConsURL.toURI())));
//            String fkConsJson = new String(Files.readAllBytes(Paths.get(fkConsURL.toURI())));
//
//            this.tableColumns = JSONUtil.json2List(tableColsJson, DatabaseSchemaTableColumns.class);
//            this.pkConstraints = JSONUtil.json2List(pkConsJson, DatabaseSchemaConstraints.class);
//            this.fkConstraints = JSONUtil.json2List(fkConsJson, DatabaseSchemaConstraints.class);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    @Test
//    public void integrationLoadSchemaTest() throws SQLException {
//        Connections connections = new Connections();
//        connections.setDatabase_name("XE");
//        connections.setDatabase_password("TAPDATA");
//        connections.setDatabase_username("TAPDATA");
//        connections.setDatabase_port(2521);
//        connections.setDatabase_host("192.168.0.161");
//
//    }
//
//    /**
//     * oracle schema unit test
//     */
//    @Test
//    public void loadDatabaseSchemaTest() throws JsonProcessingException {
//        Map<String, Map<String, DatabaseSchemaTableColumns>> tableColumnsMap = new HashMap<>();
//        Map<String, Map<String, DatabaseSchemaConstraints>> pkConsMap = new HashMap<>();
//        Map<String, Map<String, DatabaseSchemaConstraints>> fkConsMap = new HashMap<>();
//        OracleSchemaValidator schemaMaker = new OracleSchemaValidator();
//
//        for (DatabaseSchemaTableColumns tableColumn : tableColumns) {
//            schemaMaker.adaptTableColumnsMap(tableColumnsMap, tableColumn);
//        }
//
//        for (DatabaseSchemaConstraints pkConstraint : pkConstraints) {
//            schemaMaker.adaptConstraintsMap(pkConsMap, pkConstraint);
//        }
//
//        for (DatabaseSchemaConstraints fkConstraint : fkConstraints) {
//            schemaMaker.adaptConstraintsMap(fkConsMap, fkConstraint);
//        }
//
//        // check total columns nums
//        List<RelateDataBaseTable> relateDataBaseTables = schemaMaker.adaptToSchema(tableColumnsMap, pkConsMap, fkConsMap);
//        System.out.println(JSONUtil.obj2Json(relateDataBaseTables));
//
//        for (RelateDataBaseTable relateDataBaseTable : relateDataBaseTables) {
//            String tableName = relateDataBaseTable.getTable_name();
//            List<RelateDatabaseField> fields = relateDataBaseTable.getFields();
//            Map<String, DatabaseSchemaTableColumns> columnsMap = tableColumnsMap.get(tableName);
//            // not null
//            Assert.assertNotNull(columnsMap);
//            int realSize = fields.size();
//            int expectSize = columnsMap.size();
//            // equal
//            Assert.assertEquals(realSize, expectSize);
//
//            // check pk and fk constrains
//            if (pkConsMap.containsKey(tableName) || fkConsMap.containsKey(tableName)) {
//                Map<String, DatabaseSchemaConstraints> pkColMap = pkConsMap.get(tableName);
//                Map<String, DatabaseSchemaConstraints> fkColMap = fkConsMap.get(tableName);
//                for (RelateDatabaseField field : fields) {
//                    String fieldName = field.getField_name();
//                    if (field.getPrimary_key_position() > 0 ) {
//                        Assert.assertTrue(pkColMap != null && pkColMap.containsKey(fieldName));
//                    }
//
//                    if (!StringUtils.isEmpty(field.getForeign_key_column()) ) {
//                        Assert.assertTrue(fkColMap != null && fkColMap.containsKey(fieldName));
//                    }
//                }
//            }
//        }
//    }
}
