package io.tapdata.bigquery.service.bigQuery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Float;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_String;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteRecordTest {
    String sql =  " `vibrant-castle-366614.tableSet001.table1` ";
    String str = "";
    TapConnectorContext context ;
    WriteRecord writeRecord ;
    SqlMarker sqlMarker;

    TapTable tapTable = table("test").add(
            field("id", JAVA_String).isPrimaryKey(true).primaryKeyPos(1).tapType(tapString())
    ).add(field("type",JAVA_Float).tapType(tapNumber()));

    void paper(){
        context = new TapConnectorContext(null, DataMap.create().kv("serviceAccount",str).kv("tableSet","tableSet001"),null);
        writeRecord = WriteRecord.create(context);
        sqlMarker = SqlMarker.create(str);
    }

    @Test
    void testSql() throws IOException, InterruptedException {
        String sqlStr = "SELECT * FROM `vibrant-castle-366614`.`SchemaoOfJoinSet`.`All_Type_test`";
//        long star = System.currentTimeMillis();
//        sqlMarker.execute(sqlStr);
        long end = System.currentTimeMillis();
//        System.out.println( end - star);

        GoogleCredentials credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(str.getBytes("utf8")));
        BigQuery service = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlStr).build();
        service.query(queryConfig);
        System.out.println( System.currentTimeMillis() - end);
    }

    @Test
    void execute() {
        paper();
        Map<String, Object> map = map(entry("id","Gavin-test"));
        String[] insertSql = writeRecord.insertSql(list(map),tapTable);
        sqlMarker.execute(insertSql);
        Boolean aBoolean2 = writeRecord.hasRecord(sqlMarker,map,tapTable);
        assertTrue(aBoolean2);

//        map.put("type",22.3);
//        String[] updateSql = writeRecord.updateSql(list(map),tapTable);
//        sqlMarker.execute(updateSql);
//        List<BigQueryResult> excute2 = sqlMarker.execute(writeRecord.selectSql(map, tapTable));
//        assertTrue(String.valueOf(map.get("type")).equals(String.valueOf(excute2.get(0).result().get(0).get("type"))));


        String delSql = writeRecord.delSql(list(map),tapTable);
        sqlMarker.executeOnce(delSql);
        Boolean aBoolean = writeRecord.hasRecord(sqlMarker,map,tapTable);
        assertTrue(!aBoolean);
    }

    @Test
    public void sql(){
        paper();
        Map<String, Object> map = map(entry("id","Gavin-test"));
        String[] insertSql = writeRecord.insertSql(list(map),tapTable);

        String selectSql = writeRecord.selectSql(map,tapTable);

//        map.put("type",22.3);
//        String[] updateSql = writeRecord.updateSql(list(map),tapTable);

        String delSql = writeRecord.delSql(list(map,map),tapTable);


        assertTrue(((" INSERT INTO "+sql+"( `id` ) VALUES ( \"Gavin-test\" ) ").replaceAll(" ","")).equals(insertSql[0].replaceAll(" ","")));

        assertTrue(((" SELECT * FROM "+sql+" WHERE `id` = \"Gavin-test\" " ).replaceAll(" ","")).equals(selectSql.replaceAll(" ","")));

//        assertTrue(((" UPDATE "+sql+" SET `type` = 22.3 WHERE `id` = \"Gavin-test\" ").replaceAll(" ","")).equals(updateSql[0].replaceAll(" ","")));

        assertTrue(((" DELETE FROM "+sql+" WHERE (`id` = \"Gavin-test\")").replaceAll(" ","")).equals(delSql.replaceAll(" ","")));
    }

}
