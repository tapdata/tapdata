package io.tapdata.connector.tencent.db.table;

import io.tapdata.connector.mysql.util.MysqlUtil;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.stream.Collectors;

/**
 * @author GavinXiao
 * @description PartitionTable create by Gavin
 * @create 2023/4/14 13:40
 **/
public class PartitionTable extends CreateTable {
    private static final String TAG = PartitionTable.class.getSimpleName();
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE `%s`.`%s` (\n%s) %s `%s`";
    private String partitionKey;

    public PartitionTable partitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

    @Override
    public String[] createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent, String version) throws Throwable {
        TapTable tapTable = tapCreateTableEvent.getTable();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        // append field
        String fieldSql = nameFieldMap.values().stream()
                .map(field -> {
                    try {
                        field.setDataType(MysqlUtil.fixDataType(field.getDataType(), version));
                    } catch (Exception e) {
                        TapLogger.warn(TAG, e.getMessage());
                    }
                    return createTableAppendField(field);
                })
                .collect(Collectors.joining(",\n"));
        // primary key
        if (CollectionUtils.isNotEmpty(tapTable.primaryKeys())) {
            fieldSql += ",\n  " + createTableAppendPrimaryKey(tapTable);
        }
        String tablePropertiesSql = "";
        // table comment
        if (StringUtils.isNotBlank(tapTable.getComment())) {
            tablePropertiesSql += " COMMENT='" + tapTable.getComment() + "'";
        }
        if (StringUtils.isNotBlank(tablePropertiesSql)) {
            tablePropertiesSql += ",";
        }
        tablePropertiesSql += " shardkey =";

        String sql = String.format(CREATE_TABLE_TEMPLATE, database, tapTable.getId(), fieldSql, tablePropertiesSql, partitionKey);
        return new String[]{sql};
    }
}
