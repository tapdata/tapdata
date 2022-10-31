package io.tapdata.bigquery.service.bigQuery;

import io.tapdata.bigquery.util.objUtil.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.LinkedHashMap;

public class TableCreate extends BigQueryStart {
    private static final String TAG = TableCreate.class.getSimpleName();

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
            if(!key.matches("^([a-z]|[A-Z]|[_][a-z|A-Z|0-9|_]){1,300}$")){
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
        BigQueryResult bigQueryResult = sqlMarker.executeOnce(sql);
        if (Checker.isEmpty(bigQueryResult)){
            throw new CoreException("Create table error.");
        }
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
    public void schema(TapCreateTableEvent tapCreateTableEvent){

    }
}
