package io.tapdata.connector.tencent.db.table;

import io.tapdata.connector.mysql.MysqlMaker;
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
 * @description BroadcastTable create by Gavin
 * @create 2023/4/14 13:40
 **/
public class BroadcastTable extends CreateTable {
    private static final String TAG = BroadcastTable.class.getSimpleName();
    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE (`%s`.`%s`(\n%s) %s) shardkey = noshardkey_allset";
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

        String sql = String.format(CREATE_TABLE_TEMPLATE, database, tapTable.getId(), fieldSql, tablePropertiesSql);
        return new String[]{sql};
    }
}
