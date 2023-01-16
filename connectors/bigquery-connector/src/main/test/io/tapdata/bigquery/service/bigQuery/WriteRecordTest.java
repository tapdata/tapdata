package io.tapdata.bigquery.service.bigQuery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.protobuf.Descriptors;
import io.tapdata.bigquery.service.stream.WriteCommittedStream;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Float;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_String;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteRecordTest {
    String sql = " `vibrant-castle-366614.tableSet001.table1` ";
    String str = "";
    TapConnectorContext context;
    WriteRecord writeRecord;
    SqlMarker sqlMarker;

    TapTable tapTable = table("test").add(
            field("id", JAVA_String).isPrimaryKey(true).primaryKeyPos(1).tapType(tapString())
    ).add(field("type", JAVA_Float).tapType(tapNumber()));

    void paper() {
        context = new TapConnectorContext(null, DataMap.create().kv("serviceAccount", str).kv("tableSet", "SchemaoOfJoinSet"), null);
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
        System.out.println(System.currentTimeMillis() - end);
    }

    @Test
    void selectSql() throws IOException, InterruptedException {
        String sqlStr = "SELECT schema_name FROM `vibrant-castle-366614`.INFORMATION_SCHEMA.SCHEMATA;";
        paper();
        long star = System.currentTimeMillis();
        BigQueryResult bigQueryResult = sqlMarker.executeOnce(sqlStr);
        long end = System.currentTimeMillis();
        System.out.println(end - star);

//        GoogleCredentials credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8)));
//        BigQuery service = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
//        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlStr).build();
//        TableResult query = service.query(queryConfig);
//        System.out.println( System.currentTimeMillis() - end);
    }

    String credentialsJson = str;

    @Test
    public void streamWrite() throws Exception {
//        StreamAPI api = new StreamAPI();
//        JSONArray jsonArr = new JSONArray();
//        ArrayList<TapRecordEvent> objects = new ArrayList<>();
//
//        api.updateRequestMetadataOperations(jsonArr,objects);
//        api.insertIntoBigQuery("test1",jsonArr);


        //CommonUtils.setProperty("GOOGLE_APPLICATION_CREDENTIALS", "D:\\GavinData\\deskTop\\QuickApi\\vibrant-castle-366614-b3c32fdb4ce8.json");

        //System.setProperties("GOOGLE_APPLICATION_CREDENTIALS", "D:\\GavinData\\deskTop\\QuickApi\\vibrant-castle-366614-b3c32fdb4ce8.json");
        //WriteToDefaultStream writer = new WriteToDefaultStream();
//        WriteToDefaultStream.writeToDefaultStream("vibrant-castle-366614","SchemaoOfJoinSet","test1");

//        WriteCommittedStream.writer("vibrant-castle-366614", "SchemaoOfJoinSet", credentialsJson, "test1")
//                .writeCommittedStream();
    }

    @Test
    void execute() {
        paper();
        Map<String, Object> map = map(entry("id", "Gavin-test"));
        String[] insertSql = writeRecord.insertSql(list(map), tapTable);
        sqlMarker.execute(insertSql);
        Boolean aBoolean2 = writeRecord.hasRecord(sqlMarker, map, tapTable);
        assertTrue(aBoolean2);

//        map.put("type",22.3);
//        String[] updateSql = writeRecord.updateSql(list(map),tapTable);
//        sqlMarker.execute(updateSql);
//        List<BigQueryResult> excute2 = sqlMarker.execute(writeRecord.selectSql(map, tapTable));
//        assertTrue(String.valueOf(map.get("type")).equals(String.valueOf(excute2.get(0).result().get(0).get("type"))));


        String delSql = writeRecord.delSql(list(map), tapTable);
        sqlMarker.executeOnce(delSql);
        Boolean aBoolean = writeRecord.hasRecord(sqlMarker, map, tapTable);
        assertTrue(!aBoolean);
    }

    @Test
    public void sql() {
        paper();
        Map<String, Object> map = map(entry("id", "Gavin-test"));
        String[] insertSql = writeRecord.insertSql(list(map), tapTable);

        String selectSql = writeRecord.selectSql(map, tapTable);

//        map.put("type",22.3);
//        String[] updateSql = writeRecord.updateSql(list(map),tapTable);

        String delSql = writeRecord.delSql(list(map, map), tapTable);


        assertTrue(((" INSERT INTO " + sql + "( `id` ) VALUES ( \"Gavin-test\" ) ").replaceAll(" ", "")).equals(insertSql[0].replaceAll(" ", "")));

        assertTrue(((" SELECT * FROM " + sql + " WHERE `id` = \"Gavin-test\" ").replaceAll(" ", "")).equals(selectSql.replaceAll(" ", "")));

//        assertTrue(((" UPDATE "+sql+" SET `type` = 22.3 WHERE `id` = \"Gavin-test\" ").replaceAll(" ","")).equals(updateSql[0].replaceAll(" ","")));

        assertTrue(((" DELETE FROM " + sql + " WHERE (`id` = \"Gavin-test\")").replaceAll(" ", "")).equals(delSql.replaceAll(" ", "")));
    }


    @Test
    public void writeSql() throws InterruptedException, IOException, Descriptors.DescriptorValidationException {
        List<Map<String, Object>> map = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> map1 = new HashMap<>();
            map1.put("merge_id", System.nanoTime());
            map1.put("record_event_time", System.nanoTime());
            map1.put("record_table_id", "table");
            map1.put("merge_type", "U");

//            Map<String,Object> record = new HashMap();
//            record.put("id",i);
//            record.put("name","gavin-tt"+i);
            map1.put("merge_data_before", new HashMap<String, Object>() {{
                put("id", "e09bacfb-35b0-4f3a-b5d6-afda8ea9ac00");
                put("title", "WeUAlJmc");
                put("created", "2023-01-06 16:13:27.000000");
            }});
            //map1.put("merge_data_before","{\"id\":\"1sjnk\",\"title\":\"uw\"}");

            //map1.put("record1",record);
            map.add(map1);
        }
        WriteCommittedStream writer = WriteCommittedStream.writer(
                "vibrant-castle-366614",
                "SchemaoOfJoinSet",
                "temp_bigData_33ddd9e8_3f0a_4200_8d68_9983334a4ef8",
                credentialsJson
        );
                //.streamOffset(WriteCommittedStream.Offset.offset());


        writer.appendJSON(map);
        writer.close();




        writer = WriteCommittedStream.writer(
                "vibrant-castle-366614",
                "SchemaoOfJoinSet",
                "temp_bigData_33ddd9e8_3f0a_4200_8d68_9983334a4ef8",
                credentialsJson
        );//.streamOffset(WriteCommittedStream.Offset.offset());
        map = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Map<String, Object> map1 = new HashMap<>();
            map1.put("merge_id", System.nanoTime());
            map1.put("record_event_time", System.nanoTime());
            map1.put("record_table_id", System.nanoTime()+"");
            map1.put("merge_type", "U");

//            Map<String,Object> record = new HashMap();
//            record.put("id",i);
//            record.put("name","gavin-tt"+i);
            map1.put("merge_data_before", new HashMap<String, Object>() {{
                put("id", "e09bacfb-35b0-4f3a-b5d6-afda8ea9ac00");
                put("title", "WeUAlJmc");
                put("created", "2023-01-06 16:13:27.000000");
            }});
            //map1.put("merge_data_before","{\"id\":\"1sjnk\",\"title\":\"uw\"}");

            //map1.put("record1",record);
            map.add(map1);
        }
        writer.appendJSON(map);
        writer.close();
//        String sqlStr = "update `vibrant-castle-366614`.`SchemaoOfJoinSet`.`many` set name = 'new name'" +
//                " where id='1111122' and age = 110 and name='1222222' and note='2333332'" ;
//        paper();
//        long star = System.currentTimeMillis();
//        BigQueryResult bigQueryResult = sqlMarker.executeOnce(sqlStr);
//        long end = System.currentTimeMillis();
//        System.out.println( end - star);
    }

    @Test
    public void f() {
        String sql = "Select CONCAT( 'drop table `vibrant-castle-366614`.`SchemaoOfJoinSet`.`', table_name, '`;' ) as sql\n" +
                "\n" +
                "FROM `vibrant-castle-366614`.`SchemaoOfJoinSet`.INFORMATION_SCHEMA.TABLES\n" +
                "Where table_name LIKE 'temp_bigData_%' or table_name LIKE 'bigData_%';";
        paper();
        BigQueryResult bigQueryResult = sqlMarker.executeOnce(sql);

        List<Map<String, Object>> result = bigQueryResult.result();
        StringJoiner joiner = new StringJoiner("\n");
        result.stream().forEach(map -> {
            joiner.add(String.valueOf(map.get("sql")));
        });
        System.out.println(joiner.toString());
        sqlMarker.executeOnce(joiner.toString());
    }

    @Test
    public void nullAble(){
        Long in = null;
        System.out.println(Optional.ofNullable(in).orElse(100L));

        System.out.println((new Date()).toString());
    }
}
