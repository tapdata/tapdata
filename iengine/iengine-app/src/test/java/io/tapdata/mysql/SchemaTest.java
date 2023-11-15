package io.tapdata.mysql;

import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseSchemaConstraints;
import com.tapdata.entity.DatabaseSchemaTableColumns;
import io.tapdata.base.BaseTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tapdata on 16/12/2017.
 */
public class SchemaTest extends BaseTest {

	private List<DatabaseSchemaTableColumns> tableColumns;

	private List<DatabaseSchemaConstraints> constraints;

	private Map<String, Connections> connectionsMap = new HashMap<>();
//
//    @Before
//    public void initSchemaData() throws URISyntaxException, IOException {
//
//        URL connURL = SchemaTest.class.getClassLoader().getResource("dataset/Connections.json");
//        String connectionsJson = new String(Files.readAllBytes(Paths.get(connURL.toURI())));
//
//        URL tableColsURL = SchemaTest.class.getClassLoader().getResource("dataset/mysql-schema-cols.json");
//        URL consURL = SchemaTest.class.getClassLoader().getResource("dataset/mysql-schema-cons.json");
//        String tableColsJson = new String(Files.readAllBytes(Paths.get(tableColsURL.toURI())));
//        String pkConsJson = new String(Files.readAllBytes(Paths.get(consURL.toURI())));
//
//        this.tableColumns = JSONUtil.json2List(tableColsJson, DatabaseSchemaTableColumns.class);
//        this.constraints = JSONUtil.json2List(pkConsJson, DatabaseSchemaConstraints.class);
//
//        List<Connections> list = JSONUtil.json2List(connectionsJson, Connections.class);
//        for (Connections connections : list) {
//            connectionsMap.put(connections.getName(), connections);
//        }
//    }
//
//    @Test
//    public void loadSchemaTest(){
//        MySQLSchemaMaker schemaMaker = new MySQLSchemaMaker();
//
//        Map<String, Map<String, DatabaseSchemaTableColumns>> tableColumnsMap = schemaMaker.adaptTableColsToTableColsMap(tableColumns);
//        Map<String, Map<String, DatabaseSchemaConstraints>> pkConsMap = schemaMaker.adaptTableConsToTableColsPKMap(constraints);
//        Map<String, Map<String, DatabaseSchemaConstraints>> fkConsMap = schemaMaker.adaptTableConsToTableColsFKMap(constraints);
//
//        // check total columns nums
//        List<RelateDataBaseTable> relateDataBaseTables = schemaMaker.adaptToSchema(tableColumnsMap, pkConsMap, fkConsMap);
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
