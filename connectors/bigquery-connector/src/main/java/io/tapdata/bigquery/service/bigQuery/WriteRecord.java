package io.tapdata.bigquery.service.bigQuery;

import cn.hutool.json.JSONUtil;
import io.tapdata.bigquery.util.http.Http;
import io.tapdata.bigquery.util.http.HttpEntity;
import io.tapdata.bigquery.util.http.HttpResult;
import io.tapdata.bigquery.util.http.HttpType;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.*;
import java.util.function.Consumer;

public class WriteRecord {

    TapConnectorContext connectorContext;

    public WriteRecord(TapConnectorContext connectorContext){
        this.connectorContext = connectorContext;
    }
    public static WriteRecord create(TapConnectorContext connectorContext){
        return new WriteRecord(connectorContext);
    }

    private String sql =  " `vibrant-castle-366614.tableSet001.table1` ";
    public String str = "{\n" +
            "  \"type\": \"service_account\",\n" +
            "  \"project_id\": \"vibrant-castle-366614\",\n" +
            "  \"private_key_id\": \"77b6e2d287f1c39a2654d3c4d55168c4f794451f\",\n" +
            "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDRWF/wJBNFA1QA\\nZ6cp6aT5gGTYn5V6URbn92Y1oYHzW7eW+CSNgFr/VTYrFamD2AmQyea1IRC/2f7x\\nXXaaiE+CB9bRURhE78G0Ada40lUt5OSbj/3UsI1Y/82h6DCOgoiZC3GXBPpGW4xN\\nUj3CkTLzhwUwWqn0F/OS9Trkh8PcyPrus8OidnF/5Si0cNTQaNZIvYfVFV2/DhOD\\nCIRuxrYUf58ftvna9qN6eUHgOtcwff5D4jgOC4oP4l6dKdGxzBGvCoQY2kbrwKZJ\\n+HCuBh9J3H0XCRN91/mDvKfJnUf9dqYDPgR232frwPe1MwigM8R37yeMx+R6ZI9J\\nA8WI9rGbAgMBAAECggEADy2jfxYj2Nwd6g2Z3tMUOrbRbldbuiDzp+FAeuD5Ort1\\nUIUwo/B2KI8gEfhMcG+9zyP5uJDrgE1+S40QPVy8DwchzyQHE3B4EI9q5xRGSBaR\\ncKn8UxXImcxU4h6jQ/c45N0h4OY5KILDZb5xwI/7LBGnvFLGgcPUIt0+5jTlwYsl\\n/II93af5qBLicPB34Azx8uhcAsTfXOH3SCsJWfv03MyU3j1uw7HtcvahnLDQpcrQ\\nxfrOqgxVblLfrCRMssocuqGY+5HEcpDQyoQ1M7jxvP0SfBYACNDmEJE8vBPCH8Bf\\nnaw270e4rTZR9QzLEla/q/iK4BvcurzYmRxdCOLUqQKBgQDopV7JMZ/OiudSLAtH\\nSZGGv4QXwfrpEhVLMfZ42X+xZxVppTTpf0bDTsTjoHLBAAfBGpdVOcC8b+AnSvDL\\nMSTJjABIHSS7XsBNAdDXpY6xNZBYx7qa35yws7sChy2BRZtAlR+tPwQU0vD8/ZGG\\n79BuuXd6YowH9EIpG9i8/vViLQKBgQDmXDZNgT5CQ+P3xAYvRRqxAV5rQTozEW65\\nzITPBosyifVn860zQ0encgIZEryAy3k+Ulm9MiBE9eNmOm3ZrE2WJRsHSJ8T9uOd\\nHJG2YZZUdLo4eAKygjOoj4lleLeHK86SafqtlyVMZuWJ3Qi+fWCWMdJykR1pjnja\\nDDdTwRdn5wKBgQCf0FoYo7o/zDOzwwXMZsFNa2p2V47hZMaz7RJ/WgnZ+BJBjHeY\\nnxIhQI8IP0QVSMwK3xVuOkooKEI3O8fGDXBT85SN9VcyT5iSTdkFCnnHSiBqnGmX\\n0lx1FkI1Ll8YGpTX/JjSDiPjmjRp1laN91ebeFSXAfNn02dPjg2JZytx0QKBgENu\\nWrb1TjQ3i1PLncPYhqeprunWfiLUx4S7yWSQlc6Fc8CqI9kNqLvrM5IDWgqZhTQp\\nBvvK4IdPMvGJyP4e4ddBpVfMekRt0NL8ueqZRlgSkzBUcPWwB08gNSfu3kpDGITj\\nYO3PgKuMs0RX32djbBKLIv9GW0W63sV1LfzmWOOhAoGBAK55Ga02lcZhAdMUOk7h\\nvceEXbZxrYu+0HVFjtoKB1jKJ+jfhlJGs+RJVuR7zbpbW305oYudJIlIi7BwVTAH\\nnse2qRmGjBeI6h+8Do4nWhHtv9CyYttXPT/5hS8s1WJ8CsgTLtHMv7mJTDf5WG/K\\nyqdT6w+sNbU3W9/FLsSp9E0N\\n-----END PRIVATE KEY-----\\n\",\n" +
            "  \"client_email\": \"bigquerywriter@vibrant-castle-366614.iam.gserviceaccount.com\",\n" +
            "  \"client_id\": \"103569465980283374650\",\n" +
            "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
            "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
            "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
            "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/bigquerywriter%40vibrant-castle-366614.iam.gserviceaccount.com\"\n" +
            "}\n";

    public void connectBigQuery(String project,String dataset,String tableId){
        Http http = Http.create("https://bigquery.googleapis.com/bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/insertAll?key={key}"
                , HttpType.POST
                , HttpEntity.create()
                        .build("Authorization", "Bearer 34ba4ff36e9ffb1914f7ccf36efb72cd475bf9eb")
                        .build("X-goog-api-key","34ba4ff36e9ffb1914f7ccf36efb72cd475bf9eb")
                        .build("Content-Type","application/json; charset=utf-8")
        ).body(HttpEntity.create()
                .build("rows",new ArrayList<Map<String,Object>>(){{
                    add(
                            HttpEntity.create()
                                    .build("insertId","1111111")
                                    .build("json", JSONUtil.toJsonStr(
                                            HttpEntity.create()
                                            .build("id","222")
                                            .build("name",1)
                                            .build("type",3.66f)
                                            .build("int",null)
                                            .build("num",null)
                                            .build("bigNum",null)
                                            .build("bool",null)
                                            .build("timestamp",null)
                                            .build("date",null)
                                            .build("dataetime",null)
                                            .build("map",null)
                                            .build("record",null)
                                            .build("json",null).entity()
                                            )
                                    ).entity()
                    );
                }})
        ).resetFull(HttpEntity.create()
                .build("projectId",project)
                .build("datasetId",dataset)
                .build("tableId",tableId)
                .build("key","-----BEGIN PRIVATE KEY-----MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCYbcMZXMmiwO2y6eqPzGKr+CRAAOSNHyPZ0DFdbI5E4Oscg6UMCaaJLwfmD4VHMXD+1d1Y9X6LGDNTEvB0s9sjJvI2kPRLm7alqUcMuJkROBT3W39cryAaKd+1Q3yEu2P7FoVSZkgAbi3JNCU6jGjvzGVW3ei/aGRNVoFCCw8xnf3SoASMb+ZJvgsvGer+O/Lt1CTURjOzjDVZjcdFeuiT+rpLcBZGmP4TUXNuy3ZEXKWS81gKbMBzhIhtW8sELgl9r8I+hJrnlYDkD2py819Ef4bCuYJhU4rbrPpCKH5iauyty5IK1OPxZSs7vAfthLNMCPENzvo8A7Sr8mM2434rAgMBAAECggEABCx5O6ANS14SBCSgjhhwGTpdr7z2hSC1qBipyV+YE62+8lRueAJpo3b8teF16kmhyPCNM4rhUKi0exFZMTDdjrxZxIG6lrloSmf0sJX7ZvvMoytHtP98lwrPe9Shu7av2ae3tdZkIVLjAQ/i9xPyKaLEoZjI7zjKCk4UkvzfiSG6BVq/8IzadG7BV2qRLBRYRFrUo06WEy3f98eUDelKyUtFKwgtzCXwNUFWocBlvGLvJtHLQUYiGijwl+1KY+IlTOIllA7lwuG4R4ZvcPeSajppuDN8C3JuuRztGIvntHjIklMz1J9k/dhrbUAbmPOwfzKKr2mIbjVSgO4ZUU/66QKBgQDPV6YN2paQUKwZy0XJQDVAmH1qhbe9qEaGutqsIvnD1WVE0JFIdD2bklr0m2SRpEmiIFACE7SOtzIbA1byTndLkHkf8a+TKyajRI89V6U6GIAiSysUCvXESo5GyOkp/2rNro+AUUdYOtf7YgLHipW+fVDWFRjUTjmVN2ho901ziQKBgQC8MxwEWNwd8FaFutz2yAKClfLwf1BHbaRi2K7+GvcqOXnJ4/W5lsN5cbEbvKTO+NrM/M9oElb9jWGn2gWoQKhg6MxuM18Boh2HEOIdpGXX4TKvfSmJYX80aDoiCZ4EzRGE6SelnWrKzhpO7PpBBRhDW1/jV2fHqzQWbohfehrTEwKBgCohsE9eXHvkuKPhJ0QWtPt0QP/VPhneyL312BtkXAZMJXDPRMZJQH+NRMgxj0T88i1sjXVulaDuXtMYYaGJCjqjl8lC7h9khExm0Qhw99UPR3IwfgdrlrcVQ0Xk62QqT4SN9QDpAytNgbfGGbR8V6NGiZeG3+28G31TrfauUeGpAoGBALisWmC1pYFHVk+xlqQejcAATjzKYUdGApnwUH8OjN0FO0nuBDDSDQx9kLJMAVkLfwDJTuirnmr9sgcYfJamo9M8fWXhyOd8YgcofQljSYB1/duQMRMa9czCPdEqqMHDTN6kP4BXIPTTG6O5DLSCwFVQM56NJUwb5mfgnLc7xVi7AoGAXt7zdhrwvCRzG0WAcAz1\\niQhdK5VReebgG5fzRNse2Un2r/LUvMZ4MHE+yQgvHEoBtmO9K+WYgCQbSk0EYXxRRi2CeYKTLe6iIHibn0GmlEVF3f+ELUXHT/dFy24OZ2w0T04zeKJ6cJsj/oU5K9jh5a5e7bWHsVKWMklBfwkapz4=-----END PRIVATE KEY-----")
        );
        HttpResult result = http.http();
        System.out.println(result.getResult());
    }

    public static void main(String[] args) {
        WriteRecord.create(null).main("vibrant-castle-366614","tableSet001","table1");

    }


    public void main(String project,String dataset,String tableId) {
        System.out.println(SqlMarker.create(this.str).executeOnce("SELECT * FROM `" + project + "." + dataset + "." + tableId + "`").result());
    }

    public void write(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer){
        Long insert = 0L;
        Long update = 0L;
        Long delete = 0L;
        SqlMarker sqlMarker = SqlMarker.create(this.str);

        List<Map<String,Object>> insertRecord = new ArrayList<>();
        List<Map<String,Object>> updateRecord = new ArrayList<>();
        List<Map<String,Object>> deleteRecord = new ArrayList<>();
        for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
            if (tapRecordEvent instanceof TapInsertRecordEvent){
                Map<String, Object> after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
                Boolean aBoolean = hasRecord(sqlMarker, after, tapTable);
                if (null == aBoolean) continue;
                if (!aBoolean) {
                    insertRecord.add(after);
                }else {
                    updateRecord.add(after);
                }
            }else if(tapRecordEvent instanceof TapUpdateRecordEvent){
                Map<String, Object> after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
                Boolean aBoolean = hasRecord(sqlMarker, after, tapTable);
                if (null == aBoolean) continue;
                if (!aBoolean) {
                    insertRecord.add(after);
                }else {
                    updateRecord.add(after);
                }
            }else if(tapRecordEvent instanceof TapDeleteRecordEvent){
                deleteRecord.add(((TapDeleteRecordEvent)tapRecordEvent).getBefore());
            }
        }
        String delSql = delSql(insertRecord,tapTable);
        String[] updateSql = updateSql(updateRecord,tapTable);
        String[] insertSql = insertSql(deleteRecord,tapTable);

        if (null != insertSql) {
            sqlMarker.execute(insertSql);
        }
        if (updateSql != null) {
            sqlMarker.execute(updateSql);
        }
        if (null != delSql) {
            sqlMarker.executeOnce(delSql);
        }
    }
    public Boolean hasRecord(SqlMarker sqlMarker,Map<String,Object> record,TapTable tapTable){
        String selectSql = selectSql(record, tapTable);
        if (null == selectSql) return null;
        if ("".equals(selectSql)){
            return false;
        }
        BigQueryResult tableResult = sqlMarker.executeOnce(selectSql);
        return null != tableResult && tableResult.getTotalRows()>0;
    }
//    public void insert(SqlMarker sqlMarker,Map<String,Object> record,TapTable tapTable){
//        String insertSql = insertSql(record,tapTable);
//        sqlMarker.excuteOnce(insertSql);
//    }
//    public void update(SqlMarker sqlMarker,Map<String,Object> record,TapTable tapTable){
//        String updateSql = updateSql(record,tapTable);
//        sqlMarker.excuteOnce(updateSql);
//    }


    public String delSql(List<Map<String,Object>> record,TapTable tapTable){
        StringBuilder sql = new StringBuilder(" DELETE FROM ");
        sql.append(this.sql).append(" WHERE 1=2 ");
        Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null != nameFieldMap && !nameFieldMap.isEmpty() && null != record && !record.isEmpty()){
            sql.append( " OR ( " );
            for (Map<String, Object> map : record) {
                sql.append(" 1 = 1 ");
                for (Map.Entry<String,TapField> key : nameFieldMap.entrySet()) {
                    sql.append(" AND ").append(key.getKey()).append(" = ").append(sqlValue(map.get(key.getKey()),key.getValue())).append(" ");
                }
            }
            sql.append(" ) ");
            return sql.toString().replaceAll("1=2  OR","").replaceAll("1 = 1  AND","");
        }
        return null;
    }

    public String[] updateSql(List<Map<String,Object>> record,TapTable tapTable){
        Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null == nameFieldMap || nameFieldMap.isEmpty()) return null;

        boolean hasKey = false;
        for (Map.Entry<String, TapField> stringTapFieldEntry : nameFieldMap.entrySet()) {
            if (null != stringTapFieldEntry && stringTapFieldEntry.getValue().getPrimaryKey()) {
                hasKey = true;
                break;
            }
        }
        if (!hasKey) return insertSql(record, tapTable);

        if (null == record || record.isEmpty()) return null;
        int size = record.size();
        String []sql = new String[size];
        for (int i = 0; i < size ; i++) {
            Map<String ,Object> recordItem = record.get(i);
            if (null == recordItem ){
                sql[i] = null;
                continue;
            }
            StringBuilder sqlBuilder = new StringBuilder(" UPDATE ");
            sqlBuilder.append(this.sql).append(" SET ");
            StringBuilder whereBuilder = new StringBuilder(" WHERE ");
            for (Map.Entry<String,TapField> field : nameFieldMap.entrySet()) {
                if (null == field) continue;
                String fieldName = field.getKey();
                Object value = recordItem.get(fieldName);
                if (null==value) continue;
                String sqlValue = sqlValue(value, field.getValue());
                if (!field.getValue().getPrimaryKey()){
                    sqlBuilder.append(fieldName).append(" = ").append(sqlValue).append(" , ");
                }else {
                    whereBuilder.append(fieldName).append(" = ").append(sqlValue).append(" AND ");
                }
            }
            sqlBuilder.append("@").append(whereBuilder.append("@"));
            sql[i] = sqlBuilder.toString().replaceAll(", @","").replaceAll("AND @","");
        }
        return sql;
    }

    public String[] insertSql(List<Map<String,Object>> record,TapTable tapTable){
        if (null == record || record.isEmpty()) return null;
        Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null == nameFieldMap || nameFieldMap.isEmpty()) return null;
        int size = record.size();
        String []sql = new String[size];
        for (int i = 0; i < size ; i++) {
            Map<String ,Object> recordItem = record.get(i);
            if (null == recordItem ){
                sql[i] = null;
                continue;
            }
            StringBuilder sqlBuilder = new StringBuilder(" INSERT INTO ");
            sqlBuilder.append(this.sql);
            StringBuilder keyBuilder = new StringBuilder();
            StringBuilder valuesBuilder = new StringBuilder();

            for (Map.Entry<String, TapField> field : nameFieldMap.entrySet()) {
                if (null == field) continue;
                String fieldName = field.getKey();
                if (null == fieldName || "".equals(fieldName)) continue;
                keyBuilder.append(fieldName).append(" , ");

                //@TODO 对不同值处理
                Object value = recordItem.get(fieldName);
                if (null != value) {
                    valuesBuilder.append(sqlValue(value,field.getValue())).append(" , ");
                }
            }
            keyBuilder.append("@");
            valuesBuilder.append("@");
            sqlBuilder.append(" ( ").append(keyBuilder.toString().replaceAll(", @","")).append(" ) ").append(" VALUES ( ").append(valuesBuilder.toString().replaceAll(", @","")).append(" ) ");
            sql[i] = sqlBuilder.toString();
        }
        return sql;
    }

    public String selectSql(Map<String,Object> record,TapTable tapTable){
        if (null == record || null == tapTable) return null;
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null == nameFieldMap || nameFieldMap.isEmpty()) return "";
        StringBuilder sql = new StringBuilder(" SELECT * FROM ").append(this.sql).append(" WHERE 1=1 ");
        for (Map.Entry<String,TapField> key : nameFieldMap.entrySet()) {
            if (null == key) continue;
            if (key.getValue().getPrimaryKey()) {
                sql.append(" AND ").append(key.getKey()).append(" = ").append(sqlValue(record.get(key.getKey()),key.getValue())).append(" ");
            }
        }
        return sql.toString().replaceAll("1=1  AND","");
    }

    public String sqlValue(Object value,TapField field){
        if (value instanceof String) return "\""+value+"\"";
        else return "+"+value+"+";
    }
}
