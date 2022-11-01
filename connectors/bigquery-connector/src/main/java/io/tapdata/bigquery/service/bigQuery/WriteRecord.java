package io.tapdata.bigquery.service.bigQuery;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.list;

public class WriteRecord extends BigQueryStart{
    private static final String TAG = WriteRecord.class.getSimpleName();

    public WriteRecord(TapConnectionContext connectorContext){
        super(connectorContext);
    }
    public static WriteRecord create(TapConnectionContext connectorContext){
        return new WriteRecord(connectorContext);
    }

    public String fullSqlTable(String tableId){
        return "`"+this.config.projectId() + "`.`" + this.config.tableSet() + "`.`"+tableId+"`";
    }

    public String str = "{\"type\": \"service_account\",\"project_id\": \"vibrant-castle-366614\",\"private_key_id\": \"77b6e2d287f1c39a2654d3c4d55168c4f794451f\",\"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDRWF/wJBNFA1QA\\nZ6cp6aT5gGTYn5V6URbn92Y1oYHzW7eW+CSNgFr/VTYrFamD2AmQyea1IRC/2f7x\\nXXaaiE+CB9bRURhE78G0Ada40lUt5OSbj/3UsI1Y/82h6DCOgoiZC3GXBPpGW4xN\\nUj3CkTLzhwUwWqn0F/OS9Trkh8PcyPrus8OidnF/5Si0cNTQaNZIvYfVFV2/DhOD\\nCIRuxrYUf58ftvna9qN6eUHgOtcwff5D4jgOC4oP4l6dKdGxzBGvCoQY2kbrwKZJ\\n+HCuBh9J3H0XCRN91/mDvKfJnUf9dqYDPgR232frwPe1MwigM8R37yeMx+R6ZI9J\\nA8WI9rGbAgMBAAECggEADy2jfxYj2Nwd6g2Z3tMUOrbRbldbuiDzp+FAeuD5Ort1\\nUIUwo/B2KI8gEfhMcG+9zyP5uJDrgE1+S40QPVy8DwchzyQHE3B4EI9q5xRGSBaR\\ncKn8UxXImcxU4h6jQ/c45N0h4OY5KILDZb5xwI/7LBGnvFLGgcPUIt0+5jTlwYsl\\n/II93af5qBLicPB34Azx8uhcAsTfXOH3SCsJWfv03MyU3j1uw7HtcvahnLDQpcrQ\\nxfrOqgxVblLfrCRMssocuqGY+5HEcpDQyoQ1M7jxvP0SfBYACNDmEJE8vBPCH8Bf\\nnaw270e4rTZR9QzLEla/q/iK4BvcurzYmRxdCOLUqQKBgQDopV7JMZ/OiudSLAtH\\nSZGGv4QXwfrpEhVLMfZ42X+xZxVppTTpf0bDTsTjoHLBAAfBGpdVOcC8b+AnSvDL\\nMSTJjABIHSS7XsBNAdDXpY6xNZBYx7qa35yws7sChy2BRZtAlR+tPwQU0vD8/ZGG\\n79BuuXd6YowH9EIpG9i8/vViLQKBgQDmXDZNgT5CQ+P3xAYvRRqxAV5rQTozEW65\\nzITPBosyifVn860zQ0encgIZEryAy3k+Ulm9MiBE9eNmOm3ZrE2WJRsHSJ8T9uOd\\nHJG2YZZUdLo4eAKygjOoj4lleLeHK86SafqtlyVMZuWJ3Qi+fWCWMdJykR1pjnja\\nDDdTwRdn5wKBgQCf0FoYo7o/zDOzwwXMZsFNa2p2V47hZMaz7RJ/WgnZ+BJBjHeY\\nnxIhQI8IP0QVSMwK3xVuOkooKEI3O8fGDXBT85SN9VcyT5iSTdkFCnnHSiBqnGmX\\n0lx1FkI1Ll8YGpTX/JjSDiPjmjRp1laN91ebeFSXAfNn02dPjg2JZytx0QKBgENu\\nWrb1TjQ3i1PLncPYhqeprunWfiLUx4S7yWSQlc6Fc8CqI9kNqLvrM5IDWgqZhTQp\\nBvvK4IdPMvGJyP4e4ddBpVfMekRt0NL8ueqZRlgSkzBUcPWwB08gNSfu3kpDGITj\\nYO3PgKuMs0RX32djbBKLIv9GW0W63sV1LfzmWOOhAoGBAK55Ga02lcZhAdMUOk7h\\nvceEXbZxrYu+0HVFjtoKB1jKJ+jfhlJGs+RJVuR7zbpbW305oYudJIlIi7BwVTAH\\nnse2qRmGjBeI6h+8Do4nWhHtv9CyYttXPT/5hS8s1WJ8CsgTLtHMv7mJTDf5WG/K\\nyqdT6w+sNbU3W9/FLsSp9E0N\\n-----END PRIVATE KEY-----\", \"client_email\": \"bigquerywriter@vibrant-castle-366614.iam.gserviceaccount.com\", \"client_id\": \"103569465980283374650\",\"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\"token_uri\": \"https://oauth2.googleapis.com/token\", \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/bigquerywriter%40vibrant-castle-366614.iam.gserviceaccount.com\"}";

//    public void connectBigQuery(String project,String dataset,String tableId){
//        Http http = Http.create("https://bigquery.googleapis.com/bigquery/v2/projects/{projectId}/datasets/{datasetId}/tables/{tableId}/insertAll?key={key}"
//                , HttpType.POST
//                , HttpEntity.create()
//                        .build("Authorization", "Bearer 34ba4ff36e9ffb1914f7ccf36efb72cd475bf9eb")
//                        .build("X-goog-api-key","34ba4ff36e9ffb1914f7ccf36efb72cd475bf9eb")
//                        .build("Content-Type","application/json; charset=utf-8")
//        ).body(HttpEntity.create()
//                .build("rows",new ArrayList<Map<String,Object>>(){{
//                    add(
//                            HttpEntity.create()
//                                    .build("insertId","1111111")
//                                    .build("json", JSONUtil.toJsonStr(
//                                            HttpEntity.create()
//                                            .build("id","222")
//                                            .build("name",1)
//                                            .build("type",3.66f)
//                                            .build("int",null)
//                                            .build("num",null)
//                                            .build("bigNum",null)
//                                            .build("bool",null)
//                                            .build("timestamp",null)
//                                            .build("date",null)
//                                            .build("dataetime",null)
//                                            .build("map",null)
//                                            .build("record",null)
//                                            .build("json",null).entity()
//                                            )
//                                    ).entity()
//                    );
//                }})
//        ).resetFull(HttpEntity.create()
//                .build("projectId",project)
//                .build("datasetId",dataset)
//                .build("tableId",tableId)
//                .build("key","-----BEGIN PRIVATE KEY-----MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCYbcMZXMmiwO2y6eqPzGKr+CRAAOSNHyPZ0DFdbI5E4Oscg6UMCaaJLwfmD4VHMXD+1d1Y9X6LGDNTEvB0s9sjJvI2kPRLm7alqUcMuJkROBT3W39cryAaKd+1Q3yEu2P7FoVSZkgAbi3JNCU6jGjvzGVW3ei/aGRNVoFCCw8xnf3SoASMb+ZJvgsvGer+O/Lt1CTURjOzjDVZjcdFeuiT+rpLcBZGmP4TUXNuy3ZEXKWS81gKbMBzhIhtW8sELgl9r8I+hJrnlYDkD2py819Ef4bCuYJhU4rbrPpCKH5iauyty5IK1OPxZSs7vAfthLNMCPENzvo8A7Sr8mM2434rAgMBAAECggEABCx5O6ANS14SBCSgjhhwGTpdr7z2hSC1qBipyV+YE62+8lRueAJpo3b8teF16kmhyPCNM4rhUKi0exFZMTDdjrxZxIG6lrloSmf0sJX7ZvvMoytHtP98lwrPe9Shu7av2ae3tdZkIVLjAQ/i9xPyKaLEoZjI7zjKCk4UkvzfiSG6BVq/8IzadG7BV2qRLBRYRFrUo06WEy3f98eUDelKyUtFKwgtzCXwNUFWocBlvGLvJtHLQUYiGijwl+1KY+IlTOIllA7lwuG4R4ZvcPeSajppuDN8C3JuuRztGIvntHjIklMz1J9k/dhrbUAbmPOwfzKKr2mIbjVSgO4ZUU/66QKBgQDPV6YN2paQUKwZy0XJQDVAmH1qhbe9qEaGutqsIvnD1WVE0JFIdD2bklr0m2SRpEmiIFACE7SOtzIbA1byTndLkHkf8a+TKyajRI89V6U6GIAiSysUCvXESo5GyOkp/2rNro+AUUdYOtf7YgLHipW+fVDWFRjUTjmVN2ho901ziQKBgQC8MxwEWNwd8FaFutz2yAKClfLwf1BHbaRi2K7+GvcqOXnJ4/W5lsN5cbEbvKTO+NrM/M9oElb9jWGn2gWoQKhg6MxuM18Boh2HEOIdpGXX4TKvfSmJYX80aDoiCZ4EzRGE6SelnWrKzhpO7PpBBRhDW1/jV2fHqzQWbohfehrTEwKBgCohsE9eXHvkuKPhJ0QWtPt0QP/VPhneyL312BtkXAZMJXDPRMZJQH+NRMgxj0T88i1sjXVulaDuXtMYYaGJCjqjl8lC7h9khExm0Qhw99UPR3IwfgdrlrcVQ0Xk62QqT4SN9QDpAytNgbfGGbR8V6NGiZeG3+28G31TrfauUeGpAoGBALisWmC1pYFHVk+xlqQejcAATjzKYUdGApnwUH8OjN0FO0nuBDDSDQx9kLJMAVkLfwDJTuirnmr9sgcYfJamo9M8fWXhyOd8YgcofQljSYB1/duQMRMa9czCPdEqqMHDTN6kP4BXIPTTG6O5DLSCwFVQM56NJUwb5mfgnLc7xVi7AoGAXt7zdhrwvCRzG0WAcAz1\\niQhdK5VReebgG5fzRNse2Un2r/LUvMZ4MHE+yQgvHEoBtmO9K+WYgCQbSk0EYXxRRi2CeYKTLe6iIHibn0GmlEVF3f+ELUXHT/dFy24OZ2w0T04zeKJ6cJsj/oU5K9jh5a5e7bWHsVKWMklBfwkapz4=-----END PRIVATE KEY-----")
//        );
//        HttpResult result = http.http();
//        System.out.println(result.getResult());
//    }

    private final AtomicBoolean running = new AtomicBoolean(true);
    public void onDestroy() {
        this.running.set(false);
    }

    /**
     * @deprecated
     * */
    public synchronized void write(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer){
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        TapRecordEvent errorRecord = null;
        try {
            for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
                if (!running.get()) break;
                try {
                    String sql = null;
                    if (! ( tapRecordEvent instanceof TapDeleteRecordEvent )){
                        Map<String, Object> after =
                                tapRecordEvent instanceof TapInsertRecordEvent ?
                                        ((TapInsertRecordEvent) tapRecordEvent).getAfter()
                                        : ( tapRecordEvent instanceof TapUpdateRecordEvent ? ((TapUpdateRecordEvent) tapRecordEvent).getAfter(): null);
                        if (null == after){
                            writeListResult.addError(tapRecordEvent, new Exception("Event type \"" + tapRecordEvent.getClass().getSimpleName() + "\" not support: " + tapRecordEvent));
                            continue;
                        }
                        Boolean aBoolean = hasRecord(sqlMarker, tapTable,tapRecordEvent);
                        if (null == aBoolean) continue;
                        String[] sqls = aBoolean ? updateSql(list(after),tapTable,tapRecordEvent) : insertSql(list(after),tapTable);
                        sql = null!=sqls?sqls[0]:null;
                    }else{
                        sql = delSql(tapTable,tapRecordEvent);
                    }
                    BigQueryResult bigQueryResult = sqlMarker.executeOnce(sql);
                    long totalRows = bigQueryResult.getTotalRows();
                    totalRows = totalRows>0?totalRows:0;
                    if (tapRecordEvent instanceof TapInsertRecordEvent){
                        writeListResult.incrementInserted(totalRows);
                    }else if(tapRecordEvent instanceof TapUpdateRecordEvent){
                        writeListResult.incrementModified(totalRows);
                    }else if(tapRecordEvent instanceof TapDeleteRecordEvent){
                        writeListResult.incrementRemove(totalRows);
                    }
                }catch (Throwable e) {
                    errorRecord = tapRecordEvent;
                    throw e;
                }
            }
        }catch (Throwable e) {
            writeListResult.setInsertedCount(0);
            writeListResult.setModifiedCount(0);
            writeListResult.setRemovedCount(0);
            if (null != errorRecord) writeListResult.addError(errorRecord, e);
            throw e;
        }finally {
            writeListResultConsumer.accept(writeListResult);
        }
    }

    public synchronized void writeV2(List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer){
        SqlMarker sqlMarker = SqlMarker.create(this.config.serviceAccount());
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        TapRecordEvent errorRecord = null;
        try {
            for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
                if (!running.get()) break;
                try {
                    if (tapRecordEvent instanceof TapInsertRecordEvent){
                        String sql = this.insertIfExitsUpdate(
                                ((TapInsertRecordEvent)tapRecordEvent).getAfter()
                                ,tapTable
                                ,tapRecordEvent
                        );
                        writeListResult.incrementInserted(this.executeSql(sqlMarker,sql));
                    }else if(tapRecordEvent instanceof TapUpdateRecordEvent){
                        String sql = this.insertIfExitsUpdate(
                                ((TapInsertRecordEvent)tapRecordEvent).getAfter()
                                ,tapTable
                                ,tapRecordEvent
                        );
                        writeListResult.incrementModified(this.executeSql(sqlMarker,sql));
                    }else if(tapRecordEvent instanceof TapDeleteRecordEvent){
                        String sql = this.delSql(tapTable,tapRecordEvent);
                        writeListResult.incrementRemove(this.executeSql(sqlMarker,sql));
                    }else {
                        writeListResult.addError(tapRecordEvent, new Exception("Event type \"" + tapRecordEvent.getClass().getSimpleName() + "\" not support: " + tapRecordEvent));
                    }
                }catch (Throwable e) {
                    errorRecord = tapRecordEvent;
                    throw e;
                }
            }
        }catch (Throwable e) {
            writeListResult.setInsertedCount(0);
            writeListResult.setModifiedCount(0);
            writeListResult.setRemovedCount(0);
            if (null != errorRecord) writeListResult.addError(errorRecord, e);
            throw e;
        }finally {
            writeListResultConsumer.accept(writeListResult);
        }
    }

    private long executeSql(SqlMarker sqlMarker,String sql){
        BigQueryResult bigQueryResult = sqlMarker.executeOnce(sql);
        long totalRows = bigQueryResult.getTotalRows();
        return totalRows>0?totalRows:0L;
    }

    /**
     * @deprecated
     * */
    public Boolean hasRecord(SqlMarker sqlMarker,Map<String,Object> record,TapTable tapTable){
        String selectSql = selectSql(record, tapTable);
        if (null == selectSql) return null;
        if ("".equals(selectSql)){
            return false;
        }
        BigQueryResult tableResult = sqlMarker.executeOnce(selectSql);
        return null != tableResult && tableResult.getTotalRows()>0;
    }

    public Boolean hasRecord(SqlMarker sqlMarker,TapTable tapTable,TapRecordEvent event){
        String selectSql = this.selectSql(tapTable,event);
        if (null == selectSql) return null;
        if ("".equals(selectSql)){
            return false;
        }
        BigQueryResult tableResult = sqlMarker.executeOnce(selectSql);
        return null != tableResult && tableResult.getTotalRows()>0;
    }

    /**
     * @deprecated
     * */
    public String delSql(List<Map<String,Object>> record,TapTable tapTable){
        StringBuilder sql = new StringBuilder(" DELETE FROM ");
        sql.append(this.fullSqlTable(tapTable.getId())).append(" WHERE 1=2 ");
        Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null != nameFieldMap && !nameFieldMap.isEmpty() && null != record && !record.isEmpty()){
            for (Map<String, Object> map : record) {
                sql.append( " OR ( " );
                StringJoiner whereSql = new StringJoiner(" ADN ");
                for (Map.Entry<String,TapField> key : nameFieldMap.entrySet()) {
                    if (key.getValue().getPrimaryKey()) {
                        whereSql.add(key.getKey()+"="+sqlValue(map.get(key.getKey()),key.getValue()));
                    }
                }
                sql.append(whereSql.toString()).append(" ) ");
            }
            return sql.toString();
        }
        return null;
    }

    public String delSql(TapTable tapTable,TapRecordEvent event){
        return this.delBatchSql(tapTable, list(event));
    }
    public String delBatchSql(TapTable tapTable,List<TapRecordEvent> event){
        StringBuilder sql = new StringBuilder(" DELETE FROM ");
        sql.append(this.fullSqlTable(tapTable.getId())).append(" WHERE 1=2 ");
        Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null != nameFieldMap && !nameFieldMap.isEmpty() ){
            event.forEach(eve->{
                Map<String, Object> filter = eve.getFilter(tapTable.primaryKeys(true));
                if (null == filter || filter.isEmpty()) {
                    TapLogger.debug(TAG,"A tapEvent can not filter primary keys,event = {}",eve);
                    return ;
                }
                int filterSize = filter.size();
                sql.append( " OR " );
                if (filterSize>1) sql.append("( ");
                StringJoiner whereSql = new StringJoiner(" ADN ");
                filter.forEach((primaryKey,value)-> whereSql.add(primaryKey+"="+sqlValue(value,nameFieldMap.get(primaryKey))));
                sql.append(whereSql.toString());
                if (filterSize>1) sql.append(" ) ");
            });
            return sql.toString();//.replaceAll("1=2  OR","");
        }
        return null;
    }

    /**
     * @deprecated please use the method name is insertIfExitsUpdate
     * */
    public String[] updateSql(List<Map<String,Object>> record,TapTable tapTable,TapRecordEvent event){
        Map<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null == nameFieldMap || nameFieldMap.isEmpty()) return null;
        Map<String, Object> filter = event.getFilter(tapTable.primaryKeys(true));
        if (null == filter || filter.isEmpty()) return insertSql(record, tapTable);

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
            sqlBuilder.append(this.fullSqlTable(tapTable.getId())).append(" SET ");
            StringBuilder whereBuilder = new StringBuilder(" WHERE ");
            filter.forEach((key,value)->whereBuilder.append("`").append(key).append("` = ").append(sqlValue(value,nameFieldMap.get(key))).append(" AND "));
            for (Map.Entry<String,TapField> field : nameFieldMap.entrySet()) {
                if (null == field) continue;
                String fieldName = field.getKey();
                Object value = recordItem.get(fieldName);
                if (null==value) continue;
                String sqlValue = sqlValue(value, field.getValue());
                if (!field.getValue().getPrimaryKey()){
                    sqlBuilder.append("`").append(fieldName).append("` = ").append(sqlValue).append(" , ");
                }else {
                    whereBuilder.append("`").append(fieldName).append("` = ").append(sqlValue).append(" AND ");
                }
            }
            sqlBuilder.append("@").append(whereBuilder.append("@"));
            sql[i] = sqlBuilder.toString().replaceAll(", @","").replaceAll("AND @","");
        }
        return sql;
    }

    /**
     * @deprecated please use the method name is insertIfExitsUpdate
     * */
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
            sqlBuilder.append(this.fullSqlTable(tapTable.getId()));
            StringBuilder keyBuilder = new StringBuilder();
            StringBuilder valuesBuilder = new StringBuilder();

            for (Map.Entry<String, TapField> field : nameFieldMap.entrySet()) {
                if (null == field) continue;
                String fieldName = field.getKey();
                if (null == fieldName || "".equals(fieldName)) continue;

                //@TODO 对不同值处理
                Object value = recordItem.get(fieldName);
                if (null != value ) {
                    keyBuilder.append("`").append(fieldName).append("` , ");
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

    /**
     * @deprecated please use the method name is insertIfExitsUpdate
     * */
    public String selectSql(Map<String,Object> record,TapTable tapTable){
        if (null == record || null == tapTable) return null;
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null == nameFieldMap || nameFieldMap.isEmpty()) return "";
        StringBuilder sql = new StringBuilder(" SELECT * FROM ").append(this.fullSqlTable(tapTable.getId())).append(" WHERE 1=1 ");
        for (Map.Entry<String,TapField> key : nameFieldMap.entrySet()) {
            if (null == key) continue;
            if (key.getValue().getPrimaryKey()) {
                sql.append(" AND `").append(key.getKey()).append("` = ").append(sqlValue(record.get(key.getKey()),key.getValue())).append(" ");
            }
        }
        return sql.toString().replaceAll("1=1  AND","");
    }

    /**
     * @deprecated please use the method name is insertIfExitsUpdate
     * */
    public String selectSql(TapTable tapTable,TapRecordEvent event){
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null == nameFieldMap || nameFieldMap.isEmpty()) return "";
        StringBuilder sql = new StringBuilder(" SELECT * FROM ").append(this.fullSqlTable(tapTable.getId())).append(" WHERE 1=1 ");
        Map<String, Object> filter = event.getFilter(tapTable.primaryKeys(true));
        if (null == filter || filter.isEmpty()) return null;
        filter.forEach((key,value)->sql.append(" AND `").append(key).append("` = ").append(sqlValue(value,nameFieldMap.get(key))).append(" "));
        return sql.toString().replaceAll("1=1  AND","");
    }

    /**
     * DECLARE exits INT64;
     * SET exits = (select 1 from `SchemaoOfJoinSet.JoinTestSchema` where _id = '2');
     * if exits = 1 then
     *   update `SchemaoOfJoinSet.JoinTestSchema` set name = 'update-test' where _id = '2';
     * else
     *   insert into `SchemaoOfJoinSet.JoinTestSchema` (_id,name) values ('2','test-insert');
     * end if
     * */
    public String insertIfExitsUpdate(Map<String,Object> record,TapTable tapTable,TapRecordEvent event){
        String whereSql = this.whereSql(tapTable,event);
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null == nameFieldMap || nameFieldMap.isEmpty()) return "";
        if (null == record || record.isEmpty()) return "";
        final String delimiter = ",";
        StringJoiner keyInsertSql = new StringJoiner(delimiter);
        StringJoiner valueInsertSql = new StringJoiner(delimiter);
        StringJoiner subUpdateSql = new StringJoiner(",");
        final String equals = "=";
        nameFieldMap.forEach((key,field)->{
            String value = this.sqlValue(record.get(key),field);
            keyInsertSql.add(key);
            valueInsertSql.add(value);
            subUpdateSql.add(key+equals+value);
        });
        String table = this.fullSqlTable(tapTable.getId());
        StringBuilder insertIfExitsUpdateSql = new StringBuilder("DECLARE exits INT64;");
        insertIfExitsUpdateSql.append("SET exits = (select 1 from ")
                .append(table)
                .append(whereSql).append(" ); ")
                .append(" if exits = 1 then ")
                .append("  update ")
                .append(table)
                .append(" SET ").append(subUpdateSql).append(" ")
                .append(whereSql).append(" ;")
                .append(" ELSE ")
                .append("  insert into ")
                .append(table)
                .append(" (").append(keyInsertSql.toString()).append(" ) VALUES ( ").append(valueInsertSql.toString()).append(" ); ")
                .append(" END IF ");
        return insertIfExitsUpdateSql.toString();
    }

    /**
     * JSON :  INSERT INTO mydataset.table1 VALUES(1, JSON '{"name": "Alice", "age": 30}');
     *
     * */
    public String sqlValue(Object value,TapField field){
        if (value instanceof String) return "\""+value+"\"";
        else return ""+value;
    }

    /**
     *
     * (key1,key2,key3,...) VALUES (value1,value2,value3,...)
     * */
    private String subInsertSql(Map<String,Object> record,TapTable tapTable){
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null == nameFieldMap || nameFieldMap.isEmpty()) return "";
        if (null == record || record.isEmpty()) return "";
        final String delimiter = ",";
        StringJoiner keySql = new StringJoiner(delimiter);
        StringJoiner valueSql = new StringJoiner(delimiter);
        nameFieldMap.forEach((key,value)->{
            keySql.add(key);
            valueSql.add(this.sqlValue(record.get(key),value));
        });
        if (keySql.length()>0 && valueSql.length()>0){
            return " (" + keySql.toString() + " ) VALUES ( " + valueSql.toString() + " ) ";
        }else {
            TapLogger.info(TAG,"A insert sql error ,keys or values can not be find. keys = {},values = {}",keySql.toString(),valueSql.toString());
            return "";
        }
    }

    /**
     * SET key1=value1,key2=value2,...
     * */
    private String subUpdateSql(Map<String,Object> record,TapTable tapTable){
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null==nameFieldMap || nameFieldMap.isEmpty()) return "";
        if (null == record || record.isEmpty()) return "";
        StringBuilder subSql = new StringBuilder(" SET ");
        final String equals = "=";
        final String delimiter = ",";
        nameFieldMap.forEach((key,field)->{
            subSql.append(" ").append(key).append(equals).append(this.sqlValue(record.get(key),field)).append(delimiter).append(" ");
        });
        if (subSql.length()<=0){
            TapLogger.debug(TAG,"A update sql error, not key-value for subSql,record = {}",record);
        }
        return subSql.toString();
    }
    /**
     * WHERE xxx = xxx ADN xxx = xxx ......
     * */
    private String whereSql(TapTable tapTable,TapRecordEvent event){
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null==nameFieldMap || nameFieldMap.isEmpty()) return "";
        Map<String, Object> filter = event.getFilter(tapTable.primaryKeys(true));
        if (null == filter || filter.isEmpty()) return "";
        StringJoiner whereSql = new StringJoiner(" ADN ");
        filter.forEach((primaryKey,value)-> whereSql.add(primaryKey+"="+sqlValue(value,nameFieldMap.get(primaryKey))));
        return whereSql.length()>0?" WHERE "+whereSql.toString():"";
    }
}
