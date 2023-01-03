package io.tapdata.bigquery.service.bigQuery;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import io.tapdata.bigquery.service.stream.StreamAPI;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Float;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_String;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WriteRecordTest {
    String sql =  " `vibrant-castle-366614.tableSet001.table1` ";
    String str = "{\n" +
            "  \"type\": \"service_account\",\n" +
            "  \"project_id\": \"vibrant-castle-366614\",\n" +
            "  \"private_key_id\": \"b3c32fdb4ce834da98f1b139736fee147c88909e\",\n" +
            "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCZva4Wo37cmlUW\\nxgtL3v0/inpBaHoYdF3JqR0iK84zKuDt5VqP8BIyGufFYYkpxdaR90PNNnZgV2Ko\\nhXU9SjQ7Z7inxZCTS8KXzVnX/3fGNsdxeBW7xrRIKvdm7Jf//4SGkX/0becyof+X\\nsMJHuEkMkcc3Y3VMewYZ42G/gV8KtC0SwR0RagAfRxP6CC/flPX3HT5ffs5Q4G5M\\n66kq0KLoLhcpfAQElQWrmnKanXPaW2wsh/V5EAwGG1PmcU5zDZ5Fg2DPsQ3PjhOX\\n0Xrzgw24ukGTKv+NS0jOdaWQuWYW/0ALubJSshBIUuetH7DFSocTPvc5J3G0GqXP\\nX3oVIrClAgMBAAECggEADXXoCh9iehohHQ9V6dyqO6f6MEPffMijdYaTAGzpbt1w\\nOCP+m9+fGDf21vdFNR0XPkxx6UO9dY3xG2Qj8avPiuv35OiNUfguH3BhT2IUsIwX\\nRj4HWRt6qV7prl9Ep6tNhSK0G0iMF4jLghJ90B24d5tD3/ubR4j17cpUwpmnIp6l\\nHjuZAe0Q1sMKnSeBZ31yxdgZaHlhXAj3WBGLk9bbdNwY+fXMLZA3Wut9IRGbXyzT\\nbcToqZlJojOeNPo4cHjBcg6PjhrGz4EY0bCb6z7idqTaSeawDbGSEueQsygtcoIL\\nlGJV0sCAigstZpSbXy89OpvDj5HztFk16Wc1Dt5+eQKBgQDHCwxP2dz1OsQ8/T6V\\nErvPxK46zVUP7F+NjsYH/yT7sJrbIwGvZoShlviy9WnocmzZIsrSe4HVlfmE9yCI\\n8OZnPxYzTlK7aAH+vhWPp7oQqasAsAaeYTI237oHw9lnSOmaFUtl8qwfnSsWiF0g\\nO9nHkdLYK0pN+bpf+7U5O0vRHwKBgQDFvAHuEyRXjHJcMtr6xFVYYCtpRGiod9oL\\ny2cM/M43bMajWcirBTIrAxrrnFWzKuPXAShfG85+BeSUGxu0s80B5a3MIEWQfu3z\\nthFUU2xKta6TevetclamHzNhXAysJdHrRmJr6fUyUTmCXVTQb0TUH9mZPUQ7cyFD\\n1h0SRQYxuwKBgDTWLPWBetMqP2+FNjiyWWLE7g8z9JGeiJr2PIFg7HtXnTPwrgDW\\nsPyILAqtdOi8f0KApuCK4qNFBZCTXXKcqDzeFVGXSATxjh4GbYjN2GmV8IvlLkya\\nto60gxiOl8aAJ2q8nmA4tBJMUWTQ3A+zc5MzlYnGrBnY4e2azrebkvu3AoGAD8w5\\niz/UQ3phGKSngil1eB4W2c4xXmRU82RI02zPPPZf2GUv9xnvLCiPWguffTUMBv18\\nsDyUftURshOIXyOOWXx0Kj7Zz/WUJUiCke4oVL+3Nuk4KI9eBN+xRzIHgSl0YAu7\\niUuj32VF5vh18kExipEQ3YFbljRYkAbnQ7JoEEkCgYAiOKYIbfRALl2GJdmve+cI\\n8eHFjT/PKUg99bTagRW5Z/sN4jU0j6TSzI/xEjPNW7WP8g9xE6SHo3WHAn9aVqOW\\nZB0qr962YshL4n0NtDCO73UX6EGdIEii+9IyEHEwbbb3DYp5SM0WaLa52ZyT5E0X\\n6dEb/BwoMUwF+ZwXSwWzQg==\\n-----END PRIVATE KEY-----\\n\",\n" +
            "  \"client_email\": \"acountbygavin@vibrant-castle-366614.iam.gserviceaccount.com\",\n" +
            "  \"client_id\": \"111681922313258447427\",\n" +
            "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
            "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
            "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
            "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/acountbygavin%40vibrant-castle-366614.iam.gserviceaccount.com\"\n" +
            "}";
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
    public void streamWrite() throws Exception {
        StreamAPI api = new StreamAPI();
        JSONArray jsonArr = new JSONArray();
        ArrayList<TapRecordEvent> objects = new ArrayList<>();

        api.updateRequestMetadataOperations(jsonArr,objects);
        api.insertIntoBigQuery("test",jsonArr);
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
