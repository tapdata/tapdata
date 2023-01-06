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
    String sql =  " `vibrant-castle-366614.tableSet001.table1` ";
    String str = "{\n" +
            "  \"type\": \"service_account\",\n" +
            "  \"project_id\": \"vibrant-castle-366614\",\n" +
            "  \"private_key_id\": \"2f6d726b6c549949d49fe007370f0f7b2fd35814\",\n" +
            "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1qBsZ/4D8+XDy\\nrrK+5Xro5/zRnYQVpPj4g8hFt0olfAeOBTK/z9Vy9NDwPfpPc1fGP8bo3O+Uyqco\\nEjv3QRGDyPyN/H64+1zvf5UfGI0KspgaqeXyG07Bm1giqIM9g7Ew+pjK/uXOsRij\\n9Fo9cRs+wSs88fa4YKS93Lh2XwuYCDjUfsOKF5ftn1RYzNhcXfCBPqkCArhFdkaH\\nfLlFEVP4kXBaij0HcRSzRFy+Jk1sSaDR6oSbjXLwYhS60vHAUsTQ481OagC/x1cZ\\nf9697q+pIGlkukRXMoY7tfW2EUd4QuPOWFMqWsOsYSQ1243Jk7nfaGXcENATxO+m\\nIZ4QlaV1AgMBAAECggEADyIZso5s3NDxo4tW4WeqrUZ48wQlTHb5isfSYlnE998f\\nnf7kMxiJq/xQf63ExuOmrKGu3MKzGXknIxkfHRey8YCdPD2MV7VmZklW+fKIpXCA\\nErs+qG8Knj8RFj9ll4qmCJzdJ7lubvalRTCxEoH0BsP+6c0v2ZJWMb72hbNLUEyS\\n9E7HEuX7ja7XevK/Nrholb9kIQYonkQdl9ONlJIDMjjs+fFrNBuyzcWCK/vRWIo+\\nZWoTBX+zO/faaOxVBCbxbhHCA5kdF8sQZBRGIIL1kvNCjGK51oXok3igOMOWNPqD\\n3qmlIFbFimKVBapDqVlWidSBJNhoXG8dh7zoSSHqzQKBgQDtVtDLRJYVKfI0hV3u\\n8mdNKrllTZXiWM86BPotq/jeYz45qyxiTDw79NGOFtc/tZyHYtBlHIBxBi8pBCCT\\nlJZgr+dJwPU0iTtV54ghBIidl64VicV1ZVi7o3mwZbvcNvAvjN7YT6/3oH7HveNw\\nxbCovByQzG3LQXV9y/BogwYrSwKBgQDD8IIoRSTOGZMX1VRVZMrEPze7IMXZK88t\\nVcVL7qQHSePbmdNuKGaNrvHZQjtINvQLviKm7sETL4dJxLqPicg6EY0N1/0WkXk5\\nhKY44Zgs9KIRmghxy5XYGLvi9/jQXnBQY4n2rIhqzmgWB3/jtAzVwKOcOv/Jl5Uj\\nudluaYY6PwKBgEgBXck9nrb/Cd+LUstKubJ8stCcMEwCm6RDnE887H0z6M4AM7AC\\n5wddqDIOlfFbPQkKHqV+dy1TLf2opeWAX/sngukqZHoy5FCUtQUnZfdB7GvWZ5TN\\nUZVj7GhrBQqzlD0o5PcFfiKHi883uggdwhH/OD9p9imDS40F0YMztSxvAoGBALuY\\nV2Vq3dQdlEErVDT17VR7GkalGAfW8+J5zg3nC1CXI/sqic1cDOP17UOPL+byBjH/\\n9nlQ+bX+uU6ddejbh8Jg72Wjt4KWATRaljK7etD/3vNvQEqDGpHtDGY/+A63fFzb\\nEEoF4g4wVGNTBtThm7BDYeHAcwl1garU0yHtEkBNAoGAQ7lGtLO/QvT4fxTRJd0q\\nQf//BOxzi6JRxPsPmhFC54VMnXdLvTckYLEmeK4GKCPX3WXpTd7rMCJA0k3GzVrU\\nmT4tJyJWATgX1+twfsLJK+hjaaAWvraoAmT7MVQbD5H7GZDm53SGaYUx1qDPYF5X\\n6336nxO6BNx15DWeun1Nqrg=\\n-----END PRIVATE KEY-----\\n\",\n" +
            "  \"client_email\": \"acountbygavin@vibrant-castle-366614.iam.gserviceaccount.com\",\n" +
            "  \"client_id\": \"111681922313258447427\",\n" +
            "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
            "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
            "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
            "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/acountbygavin%40vibrant-castle-366614.iam.gserviceaccount.com\"\n" +
            "}\n";
    String key = "-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCZva4Wo37cmlUW\\nxgtL3v0/inpBaHoYdF3JqR0iK84zKuDt5VqP8BIyGufFYYkpxdaR90PNNnZgV2Ko\\nhXU9SjQ7Z7inxZCTS8KXzVnX/3fGNsdxeBW7xrRIKvdm7Jf//4SGkX/0becyof+X\\nsMJHuEkMkcc3Y3VMewYZ42G/gV8KtC0SwR0RagAfRxP6CC/flPX3HT5ffs5Q4G5M\\n66kq0KLoLhcpfAQElQWrmnKanXPaW2wsh/V5EAwGG1PmcU5zDZ5Fg2DPsQ3PjhOX\\n0Xrzgw24ukGTKv+NS0jOdaWQuWYW/0ALubJSshBIUuetH7DFSocTPvc5J3G0GqXP\\nX3oVIrClAgMBAAECggEADXXoCh9iehohHQ9V6dyqO6f6MEPffMijdYaTAGzpbt1w\\nOCP+m9+fGDf21vdFNR0XPkxx6UO9dY3xG2Qj8avPiuv35OiNUfguH3BhT2IUsIwX\\nRj4HWRt6qV7prl9Ep6tNhSK0G0iMF4jLghJ90B24d5tD3/ubR4j17cpUwpmnIp6l\\nHjuZAe0Q1sMKnSeBZ31yxdgZaHlhXAj3WBGLk9bbdNwY+fXMLZA3Wut9IRGbXyzT\\nbcToqZlJojOeNPo4cHjBcg6PjhrGz4EY0bCb6z7idqTaSeawDbGSEueQsygtcoIL\\nlGJV0sCAigstZpSbXy89OpvDj5HztFk16Wc1Dt5+eQKBgQDHCwxP2dz1OsQ8/T6V\\nErvPxK46zVUP7F+NjsYH/yT7sJrbIwGvZoShlviy9WnocmzZIsrSe4HVlfmE9yCI\\n8OZnPxYzTlK7aAH+vhWPp7oQqasAsAaeYTI237oHw9lnSOmaFUtl8qwfnSsWiF0g\\nO9nHkdLYK0pN+bpf+7U5O0vRHwKBgQDFvAHuEyRXjHJcMtr6xFVYYCtpRGiod9oL\\ny2cM/M43bMajWcirBTIrAxrrnFWzKuPXAShfG85+BeSUGxu0s80B5a3MIEWQfu3z\\nthFUU2xKta6TevetclamHzNhXAysJdHrRmJr6fUyUTmCXVTQb0TUH9mZPUQ7cyFD\\n1h0SRQYxuwKBgDTWLPWBetMqP2+FNjiyWWLE7g8z9JGeiJr2PIFg7HtXnTPwrgDW\\nsPyILAqtdOi8f0KApuCK4qNFBZCTXXKcqDzeFVGXSATxjh4GbYjN2GmV8IvlLkya\\nto60gxiOl8aAJ2q8nmA4tBJMUWTQ3A+zc5MzlYnGrBnY4e2azrebkvu3AoGAD8w5\\niz/UQ3phGKSngil1eB4W2c4xXmRU82RI02zPPPZf2GUv9xnvLCiPWguffTUMBv18\\nsDyUftURshOIXyOOWXx0Kj7Zz/WUJUiCke4oVL+3Nuk4KI9eBN+xRzIHgSl0YAu7\\niUuj32VF5vh18kExipEQ3YFbljRYkAbnQ7JoEEkCgYAiOKYIbfRALl2GJdmve+cI\\n8eHFjT/PKUg99bTagRW5Z/sN4jU0j6TSzI/xEjPNW7WP8g9xE6SHo3WHAn9aVqOW\\nZB0qr962YshL4n0NtDCO73UX6EGdIEii+9IyEHEwbbb3DYp5SM0WaLa52ZyT5E0X\\n6dEb/BwoMUwF+ZwXSwWzQg==\\n-----END PRIVATE KEY-----\\n";
    TapConnectorContext context ;
    WriteRecord writeRecord ;
    SqlMarker sqlMarker;

    TapTable tapTable = table("test").add(
            field("id", JAVA_String).isPrimaryKey(true).primaryKeyPos(1).tapType(tapString())
    ).add(field("type",JAVA_Float).tapType(tapNumber()));

    void paper(){
        context = new TapConnectorContext(null, DataMap.create().kv("serviceAccount",str).kv("tableSet","SchemaoOfJoinSet"),null);
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
    void selectSql() throws IOException, InterruptedException {
        String sqlStr = "SELECT schema_name FROM `vibrant-castle-366614`.INFORMATION_SCHEMA.SCHEMATA;" ;
        paper();
        long star = System.currentTimeMillis();
        BigQueryResult bigQueryResult = sqlMarker.executeOnce(sqlStr);
        long end = System.currentTimeMillis();
        System.out.println( end - star);

//        GoogleCredentials credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8)));
//        BigQuery service = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
//        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlStr).build();
//        TableResult query = service.query(queryConfig);
//        System.out.println( System.currentTimeMillis() - end);
    }

    String credentialsJson = "{\n" +
            "  \"type\": \"service_account\",\n" +
            "  \"project_id\": \"vibrant-castle-366614\",\n" +
            "  \"private_key_id\": \"2f6d726b6c549949d49fe007370f0f7b2fd35814\",\n" +
            "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC1qBsZ/4D8+XDy\\nrrK+5Xro5/zRnYQVpPj4g8hFt0olfAeOBTK/z9Vy9NDwPfpPc1fGP8bo3O+Uyqco\\nEjv3QRGDyPyN/H64+1zvf5UfGI0KspgaqeXyG07Bm1giqIM9g7Ew+pjK/uXOsRij\\n9Fo9cRs+wSs88fa4YKS93Lh2XwuYCDjUfsOKF5ftn1RYzNhcXfCBPqkCArhFdkaH\\nfLlFEVP4kXBaij0HcRSzRFy+Jk1sSaDR6oSbjXLwYhS60vHAUsTQ481OagC/x1cZ\\nf9697q+pIGlkukRXMoY7tfW2EUd4QuPOWFMqWsOsYSQ1243Jk7nfaGXcENATxO+m\\nIZ4QlaV1AgMBAAECggEADyIZso5s3NDxo4tW4WeqrUZ48wQlTHb5isfSYlnE998f\\nnf7kMxiJq/xQf63ExuOmrKGu3MKzGXknIxkfHRey8YCdPD2MV7VmZklW+fKIpXCA\\nErs+qG8Knj8RFj9ll4qmCJzdJ7lubvalRTCxEoH0BsP+6c0v2ZJWMb72hbNLUEyS\\n9E7HEuX7ja7XevK/Nrholb9kIQYonkQdl9ONlJIDMjjs+fFrNBuyzcWCK/vRWIo+\\nZWoTBX+zO/faaOxVBCbxbhHCA5kdF8sQZBRGIIL1kvNCjGK51oXok3igOMOWNPqD\\n3qmlIFbFimKVBapDqVlWidSBJNhoXG8dh7zoSSHqzQKBgQDtVtDLRJYVKfI0hV3u\\n8mdNKrllTZXiWM86BPotq/jeYz45qyxiTDw79NGOFtc/tZyHYtBlHIBxBi8pBCCT\\nlJZgr+dJwPU0iTtV54ghBIidl64VicV1ZVi7o3mwZbvcNvAvjN7YT6/3oH7HveNw\\nxbCovByQzG3LQXV9y/BogwYrSwKBgQDD8IIoRSTOGZMX1VRVZMrEPze7IMXZK88t\\nVcVL7qQHSePbmdNuKGaNrvHZQjtINvQLviKm7sETL4dJxLqPicg6EY0N1/0WkXk5\\nhKY44Zgs9KIRmghxy5XYGLvi9/jQXnBQY4n2rIhqzmgWB3/jtAzVwKOcOv/Jl5Uj\\nudluaYY6PwKBgEgBXck9nrb/Cd+LUstKubJ8stCcMEwCm6RDnE887H0z6M4AM7AC\\n5wddqDIOlfFbPQkKHqV+dy1TLf2opeWAX/sngukqZHoy5FCUtQUnZfdB7GvWZ5TN\\nUZVj7GhrBQqzlD0o5PcFfiKHi883uggdwhH/OD9p9imDS40F0YMztSxvAoGBALuY\\nV2Vq3dQdlEErVDT17VR7GkalGAfW8+J5zg3nC1CXI/sqic1cDOP17UOPL+byBjH/\\n9nlQ+bX+uU6ddejbh8Jg72Wjt4KWATRaljK7etD/3vNvQEqDGpHtDGY/+A63fFzb\\nEEoF4g4wVGNTBtThm7BDYeHAcwl1garU0yHtEkBNAoGAQ7lGtLO/QvT4fxTRJd0q\\nQf//BOxzi6JRxPsPmhFC54VMnXdLvTckYLEmeK4GKCPX3WXpTd7rMCJA0k3GzVrU\\nmT4tJyJWATgX1+twfsLJK+hjaaAWvraoAmT7MVQbD5H7GZDm53SGaYUx1qDPYF5X\\n6336nxO6BNx15DWeun1Nqrg=\\n-----END PRIVATE KEY-----\\n\",\n" +
            "  \"client_email\": \"acountbygavin@vibrant-castle-366614.iam.gserviceaccount.com\",\n" +
            "  \"client_id\": \"111681922313258447427\",\n" +
            "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
            "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
            "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
            "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/acountbygavin%40vibrant-castle-366614.iam.gserviceaccount.com\"\n" +
            "}\n";
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

        WriteCommittedStream.writer("vibrant-castle-366614", "SchemaoOfJoinSet", credentialsJson, "test1")
                .writeCommittedStream();
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


    @Test
    public void writeSql() throws InterruptedException, IOException, Descriptors.DescriptorValidationException {
        List<Map<String,Object>> map = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Map<String,Object> map1 = new HashMap<>();
            map1.put("merge_id",new Long(265567961112397L));
            map1.put("record_event_time",System.nanoTime());
            map1.put("record_table_id",System.nanoTime());
            map1.put("merge_type","U");

//            Map<String,Object> record = new HashMap();
//            record.put("id",i);
//            record.put("name","gavin-tt"+i);
            map1.put("merge_data_before",new HashMap<String,Object>(){{
                put("id","e09bacfb-35b0-4f3a-b5d6-afda8ea9ac00");
                put("title","WeUAlJmc");
                put("created","2023-01-06 16:13:27.000000");
            }});
            //map1.put("merge_data_before","{\"id\":\"1sjnk\",\"title\":\"uw\"}");

            //map1.put("record1",record);
            map.add(map1);
        }
        WriteCommittedStream writer = WriteCommittedStream.writer(
                "vibrant-castle-366614",
                "SchemaoOfJoinSet",
                "temp_bigData_27aa42fc_6f26_4b26_9b3d_282834bd6014",
                credentialsJson
        );
        writer.appendJSON(map);
//        String sqlStr = "update `vibrant-castle-366614`.`SchemaoOfJoinSet`.`many` set name = 'new name'" +
//                " where id='1111122' and age = 110 and name='1222222' and note='2333332'" ;
//        paper();
//        long star = System.currentTimeMillis();
//        BigQueryResult bigQueryResult = sqlMarker.executeOnce(sqlStr);
//        long end = System.currentTimeMillis();
//        System.out.println( end - star);
    }

    @Test
    public void f(){
        String sql = "Select CONCAT( 'drop table `vibrant-castle-366614`.`SchemaoOfJoinSet`.`', table_name, '`;' ) as sql\n" +
                "\n" +
                "FROM `vibrant-castle-366614`.`SchemaoOfJoinSet`.INFORMATION_SCHEMA.TABLES\n" +
                "Where table_name LIKE 'temp_bigData_%';";
        paper();
        BigQueryResult bigQueryResult = sqlMarker.executeOnce(sql);

        List<Map<String, Object>> result = bigQueryResult.result();
        StringJoiner joiner = new StringJoiner("\n");
        result.stream().forEach(map->{
            joiner.add(String.valueOf(map.get("sql")));
        });
        System.out.println(joiner.toString());
        sqlMarker.executeOnce(joiner.toString());
    }

}
