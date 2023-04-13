package io.tapdata.databend;

import com.google.common.collect.Lists;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.common.ddl.DDLSqlMaker;
import io.tapdata.databend.config.DatabendConfig;
import io.tapdata.databend.ddl.sqlmaker.DatabendDDLSqlMaker;
import io.tapdata.databend.dml.DatabendBatchWriter;
import io.tapdata.databend.dml.TapTableWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;


@TapConnectorClass("spec_databend.json")
public class DatabendConnector extends ConnectorBase {
    public static final String TAG = DatabendConnector.class.getSimpleName();
    private DatabendConfig databendConfig;
    private DatabendJdbcContext databendJdbcContext;
    private static final int BATCH_ADVANCE_READ_LIMIT = 1000;
    private String connectionTimezone;
    private BiClassHandlers<TapFieldBaseEvent, TapConnectorContext, List<String>> fieldDDLHandlers;
    private DDLSqlMaker ddlSqlMaker;
    private final DatabendBatchWriter databendBatchWriter = new DatabendBatchWriter(TAG);

    private void initConnection(TapConnectionContext connectionContext) throws Throwable {
        databendConfig = new DatabendConfig().load(connectionContext.getConnectionConfig());
        databendJdbcContext = new DatabendJdbcContext(databendConfig);
        this.connectionTimezone = connectionContext.getConnectionConfig().getString("timezone");
        if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
            this.connectionTimezone = databendJdbcContext.timezone();
        }
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        initConnection(connectionContext);
        ddlSqlMaker = new DatabendDDLSqlMaker();
        if (connectionContext instanceof TapConnectorContext) {
            TapConnectorContext tapConnectorContext = (TapConnectorContext) connectionContext;
            Optional.ofNullable(tapConnectorContext.getConnectorCapabilities()).ifPresent(connectorCapabilities -> {
                Optional.ofNullable(connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY)).ifPresent(databendBatchWriter::setInsertPolicy);
                Optional.ofNullable(connectorCapabilities.getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY)).ifPresent(databendBatchWriter::setUpdatePolicy);
            });
        }
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        List<DataMap> tableList = databendJdbcContext.queryAllTables(tables);
        List<List<DataMap>> tableLists = Lists.partition(tableList, tableSize);
        try {
            tableLists.forEach(subList -> {
                List<TapTable> tapTableList = TapSimplify.list();
                List<String> subTableNames = subList.stream().map(v -> v.getString("name")).collect(Collectors.toList());
                List<DataMap> columnList = databendJdbcContext.queryAllColumns(subTableNames);

                subList.forEach(subTable -> {
                    //1.table name/comment
                    String table = subTable.getString("name");
                    TapTable tapTable = table(table);
                    tapTable.setComment(subTable.getString("comment"));
                    columnList.stream().filter(col -> table.equals(col.getString("table")))
                            .forEach(col -> {
                                String columnName = col.getString("name");
                                String columnType = col.getString("type");
                                Boolean nullable = false;
                                if (columnType.contains("Nullable")) {
                                    columnType = columnType.replace("Nullable(", "");
                                    columnType = columnType.substring(0, columnType.length() - 1);
                                    nullable = true;
                                }
                                TapField field = TapSimplify.field(columnName, columnType);
                                field.nullable(nullable);
                                tapTable.add(field);
                            });
                    tapTableList.add(tapTable);
                });
                if (CollectionUtils.isNotEmpty(columnList)) {
                    consumer.accept(tapTableList);
                    tapTableList.clear();
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {

        codecRegistry.registerFromTapValue(TapRawValue.class, "String", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "String", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "String", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "UInt8", tapValue -> {
            if (tapValue.getValue()) return 1;
            else return 0;
        });

        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss.SS"));
        codecRegistry.registerFromTapValue(TapBinaryValue.class, "String", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return new String(Base64.encodeBase64(tapValue.getValue()));
            return null;
        });

        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            DateTime datetime = tapDateTimeValue.getValue();
            return datetime.toTimestamp();
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> {
            DateTime datetime = tapDateValue.getValue();
            return datetime.toSqlDate();
        });

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        //target
        connectorFunctions.supportCreateTable(this::createTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportWriteRecord(this::writeRecord);


        //query
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);


        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> databendJdbcContext.getConnection(), this::isAlive, c));
    }

    private void createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        sql.append(TapTableWriter.sqlQuota(".", databendConfig.getDatabase(), tapTable.getId()));
        sql.append("(").append(DatabendDDLSqlMaker.buildColumnDefinition(tapTable, true));
        sql.setLength(sql.length() - 1);
        sql.append(")");

        try {
            List<String> sqls = TapSimplify.list();
            sqls.add(sql.toString());
            TapLogger.info("table is:", "table->{}", tapTable.getId());
            databendJdbcContext.batchExecute(sqls);
        } catch (Throwable e) {
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage(), e);
        }
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>();
        TapTableWriter instance = databendBatchWriter.partition(databendJdbcContext, this::isAlive);
        for (TapRecordEvent event : tapRecordEvents) {
            if (!isAlive()) {
                throw new InterruptedException("node not alive");
            }
            instance.addBath(tapTable, event, writeListResult);
        }
        instance.summit(writeListResult);
        consumer.accept(writeListResult);
    }

    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        StringBuilder builder = new StringBuilder("SELECT ");
        Projection projection = filter.getProjection();
        if (EmptyKit.isNull(projection) || (EmptyKit.isEmpty(projection.getIncludeFields()) && EmptyKit.isEmpty(projection.getExcludeFields()))) {
            builder.append("*");
        } else {
            builder.append("\"");
            if (EmptyKit.isNotEmpty(filter.getProjection().getIncludeFields())) {
                builder.append(String.join("\",\"", filter.getProjection().getIncludeFields()));
            } else {
                builder.append(table.getNameFieldMap().keySet().stream()
                        .filter(tapField -> !filter.getProjection().getExcludeFields().contains(tapField)).collect(Collectors.joining("\",\"")));
            }
            builder.append("\"");
        }
        builder.append(" FROM ").append(TapTableWriter.sqlQuota(".", databendConfig.getDatabase(), table.getId())).append(" ").append(new CommonSqlMaker().buildSqlByAdvanceFilter(filter));
        databendJdbcContext.query(builder.toString(), resultSet -> {
            FilterResults filterResults = new FilterResults();
            while (resultSet != null && resultSet.next()) {
                filterResults.add(DbKit.getRowFromResultSet(resultSet, DbKit.getColumnsFromResultSet(resultSet)));
                if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                    consumer.accept(filterResults);
                    filterResults = new FilterResults();
                }
            }
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                filterResults.getResults().stream().forEach(l -> l.entrySet().forEach(v -> {
                    if (v.getValue() instanceof String) {
                        v.setValue(((String) v.getValue()).trim());
                    }
                }));
                consumer.accept(filterResults);
            }
        });
    }

    private void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (databendJdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
                databendJdbcContext.execute("TRUNCATE TABLE " + TapTableWriter.sqlQuota(".", databendConfig.getDatabase(), tapClearTableEvent.getTableId()));
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            if (databendJdbcContext.queryAllTables(Collections.singletonList(tapDropTableEvent.getTableId())).size() == 1) {
                databendJdbcContext.execute("DROP TABLE IF EXISTS " + TapTableWriter.sqlQuota(".", databendConfig.getDatabase(), tapDropTableEvent.getTableId()));
            }
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        databendConfig = (DatabendConfig) new DatabendConfig().load(connectionContext.getConnectionConfig());
        try (
                DatabendTest databendTest = new DatabendTest(databendConfig, consumer)
        ) {
            databendTest.testOneByOne();
            return connectionOptions;
        }
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return databendJdbcContext.queryAllTables(null).size();
    }


    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        EmptyKit.closeQuietly(databendJdbcContext);
        EmptyKit.closeQuietly(databendBatchWriter);
    }

}
