package io.tapdata.bigquery.service.bigQuery;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import io.tapdata.bigquery.util.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

import static io.tapdata.base.ConnectorBase.field;

public class TableCreate extends BigQueryStart {
    private static final String TAG = TableCreate.class.getSimpleName();
    public static final String FIELD_NAME_REG = "^[a-z|A-Z|_]([a-z|A-Z|0-9|_]{0,299})$";

    public TableCreate(TapConnectionContext connectorContext) {
        super(connectorContext);
    }

    public static TableCreate create(TapConnectionContext connectorContext){
        return new TableCreate(connectorContext);
    }
    /**
     * DROP TABLE IF EXISTS ``;
     * CREATE TABLE SchemaoOfJoinSet.newtable (
     *   x INT64 OPTIONS NULLABLE (description = 'An optional INTEGER field'),
     *   a BYTES OPTIONS REPEATED (description = ''),
     *   b INTEGER ,
     *   c FLOAT ,
     *   d NUMERIC
     *   y STRUCT <
     *      *     a ARRAY <STRING> OPTIONS (description = 'A repeated STRING field'),
     *      *     b BOOL
     *      *   >
     * ) OPTIONS (
     *     expiration_timestamp = TIMESTAMP '2023-01-01 00:00:00 UTC',
     *     description = 'a table that expires in 2023',
     *     labels = [('org_unit', 'development')]
     *     );
     * */

    /**
     * 创建表
     *
     * */
    public void createSchema(TapCreateTableEvent tapCreateTableEvent){
        if (Checker.isEmpty(this.config)){
            throw new CoreException("Connection config is null or empty.");
        }
        String tableSet = this.config.tableSet();
        if (Checker.isEmpty(tableSet)){
            throw new CoreException("Table set is null or empty.");
        }
        if (Checker.isEmpty(tapCreateTableEvent)){
            throw new CoreException("Tap create table event is null or empty.");
        }
        TapTable tapTable = tapCreateTableEvent.getTable();
        if (Checker.isEmpty(tapTable)){
            throw new CoreException("Tap table is null or empty.");
        }
        String tableId = tapTable.getId();
        if (Checker.isEmpty(tableId)){
            tableId = "newTable-"+System.currentTimeMillis();
        }
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (io.tapdata.bigquery.util.tool.Checker.isEmptyCollection(nameFieldMap)){
            throw new CoreException("Tap table schema null or empty.");
        }
        String tableSetName = "`"+tableSet+"."+tableId+"`";

//        StringBuilder sql = new StringBuilder(" DROP TABLE IF EXISTS ");
//        sql.append(tableSetName)
//                .append("; CREATE TABLE IF NOT EXISTS ")
//                .append(tableSetName)
//                .append(" (");
        StringBuilder sql = new StringBuilder(" DROP TABLE IF NOT EXISTS ");
        sql.append(tableSetName);
        sql.append(" (");

        //@TODO
        nameFieldMap.forEach((key,tapField)->{
            /**
             * column_name 是列的名称。列名称要求：
             *
             * 只能包含字母（a-z、A-Z）、数字 (0-9) 或下划线 (_)
             * 必须以字母或下划线开头
             * 最多包含 300 个字符
             * */
            if(!key.matches(FIELD_NAME_REG)){
                throw new CoreException("Illegal field name,Can only contain letters (a-z, A-Z), numbers (0-9), or underscores (_), Must start with a letter or underscore ,and up to 300 characters.");
            }
            sql.append(" `")
                    .append(key)
                    .append("` ")
                    .append(tapField.getDataType().toUpperCase())
                    .append(" OPTIONS ");
            if ((null != tapField.getNullable() && !tapField.getNullable()) || (null != tapField.getPrimaryKeyPos() && tapField.getPrimaryKeyPos() > 0)) {
                sql.append(" NULL ");
            } else {
                sql.append(" NOT NULL ");
            }
            //DEFAULT
            String defaultValue = tapField.getDefaultValue() == null ? "" : tapField.getDefaultValue().toString();
            if (io.tapdata.bigquery.util.tool.Checker.isNotEmpty(defaultValue)) {
                if(defaultValue.contains("'")){
                    defaultValue = defaultValue.replaceAll( "'", "\\'");
                }
                if(tapField.getTapType() instanceof TapNumber){
                    defaultValue = defaultValue.trim();
                }
                sql.append(" DEFAULT '").append(defaultValue).append("' ");
            }

            // comment
            String comment = tapField.getComment();
            if (io.tapdata.bigquery.util.tool.Checker.isNotEmpty(comment)) {
                comment = comment.replace("'", "\\'");
                sql.append(" (description = '")
                        .append( comment)
                        .append("')");
            }
            sql.append(",");
        });
        sql.deleteCharAt(sql.lastIndexOf(","));

        String comment = tapTable.getComment();
        //@TODO
        sql.append(" ) ");

        String collateSpecification = "";//默认排序规则
        if (io.tapdata.bigquery.util.tool.Checker.isNotEmpty(collateSpecification)){
            sql.append(" DEFAULT COLLATE ").append(collateSpecification).append(" ");
        }

        sql.append(" OPTIONS ( ");
        if (io.tapdata.bigquery.util.tool.Checker.isNotEmpty(comment)) {
            sql.append("description = '").append(comment);
        }
        sql.append(" );");
        BigQueryResult bigQueryResult = sqlMarker.executeOnce(sql.toString());
        if (Checker.isEmpty(bigQueryResult)){
            throw new CoreException("Create table error.");
        }
    }

    public static void main(String[] args) {
    }
    /**
     * 表是否存在
     * */
    public boolean isExist(TapCreateTableEvent tapCreateTableEvent){

        return false;
    }

    /**
     * 获取表结构
     * */
    String SCHEMA_TABLES_SQL = "SELECT table_name AS tableName,ddl FROM `%s`.`%s`.INFORMATION_SCHEMA.TABLES;";
    public void schemaListDDL(TapCreateTableEvent tapCreateTableEvent){
        String sql = String.format(SCHEMA_TABLES_SQL, this.config.projectId(), this.config.tableSet());
        BigQueryResult bigQueryResult = this.sqlMarker.executeOnce(sql);
        if (io.tapdata.bigquery.util.tool.Checker.isEmpty(bigQueryResult)){
            throw new CoreException("Error to execute sql error: "+sql);
        }
        long total = bigQueryResult.getTotalRows();
        if (total<=0){
            throw new CoreException(String.format("Project: %s,table set: %s,not find any table.",this.config.projectId(),this.config.tableSet()));
        }
        List<Map<String, Object>> result = bigQueryResult.result();
        if (io.tapdata.bigquery.util.tool.Checker.isEmptyCollection(result)){
            throw new CoreException("Not get any Table in query back data.");
        }
        List<TapTable> tapTables = new ArrayList<>();
        result.stream().forEach(map->{
            Object tableNameObj = map.get("tableName");
            Object ddlObj = map.get("ddl");
            String tableName = null;
            String ddl = null;
            if (Checker.isNotEmpty(tableNameObj) && Checker.isNotEmpty(ddlObj)){
                TapTable tapTable = new TapTable();
                tableName = (String)tableNameObj;
                ddl = (String)ddlObj;
                tapTable.setName(tableName);
                /**
                 * CREATE TABLE `vibrant-castle-366614.SchemaoOfJoinSet.JoinTestSchema`
                 * (
                 *   _id STRING,
                 *   name STRING,
                 *   type STRING,
                 *   isTable STRING,
                 *   isDel STRING,
                 *   status STRING,
                 *   times STRING,
                 *   avactor STRING,
                 *   description STRING,
                 *   title STRING,
                 *   teamName STRING,
                 *   projectName STRING,
                 *   priority STRING,
                 *   workingHours STRING,
                 *   createTime STRING,
                 *   updateTime STRING,
                 *   createBy STRING,
                 *   updateBy STRING,
                 *   bytes BYTES,
                 *   bytess BYTES(10),
                 *   integer INT64,
                 *   float FLOAT64,
                 *   numeric NUMERIC(15, 8),
                 *   bignumeric BIGNUMERIC(55, 25),
                 *   big BIGNUMERIC(76, 38),
                 *   timestamp TIMESTAMP,
                 *   date DATE,
                 *   time TIME,
                 *   datetime DATETIME,
                 *   geography GEOGRAPHY,
                 *   straact STRUCT<s STRING>,
                 *   A NUMERIC
                 * );
                 * */
                StringBuilder tableDDl = new StringBuilder();

                int len = ddl.length();
                int fieldStartIndex = ddl.indexOf("(");
                int split = 0;
                for (int i = fieldStartIndex; i < len; i++) {
                    char ch = ddl.charAt(i);
                    if (split != 0 ){
                        tableDDl.append(ch);
                    }

                    if (ch == '('){
                        split -= 1;
                    }else if(ch == ')'){
                        split += 1;
                    }

                    if (split == 0){
                        break;
                    }
                }

            }
        });
    }

    public List<TapTable> listTablesSDK() {
        try {
            String projectId = this.config.projectId();
            String datasetName = this.config.tableSet();
            GoogleCredentials credentials = ServiceAccountCredentials.fromStream(new ByteArrayInputStream(this.config.serviceAccount().getBytes("utf8")));
            BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            Page<Table> tables = bigquery.listTables(datasetId, BigQuery.TableListOption.pageSize(100));
            List<TapTable> tableList = new ArrayList<>();
            tables.iterateAll().forEach(table -> {
                TableId tableId = table.getTableId();
                TableDefinition definition = table.getDefinition();
                Schema schema = definition.getSchema();
                FieldList fields = schema.getFields();
                ListIterator<Field> fieldListIterator = fields.listIterator();
                TapTable tapTable = new TapTable();
                tapTable.setId(tableId.getTable());
                tapTable.setComment(table.getDescription());
                tapTable.setLastUpdate(table.getLastModifiedTime());
                while (fieldListIterator.hasNext()){
                    Field field = fieldListIterator.next();
                    String modeName = field.getMode().name();
                    tapTable.add(
                            field(field.getName(),field.getType().name())
                                    .nullable("NULLABLE".equals(modeName))
                                    .comment(field.getDescription())
                                    .defaultValue(field.getDefaultValueExpression())
                    );
                }
                tableList.add(tapTable);
            });
            return tableList;
        } catch (BigQueryException| IOException e) {
            throw new CoreException("Tables were not listed. Error occurred: " + e.getMessage());
        }
    }
}
