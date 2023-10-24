package io.tapdata.pdk.apis.functions;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.connection.GetTableNamesFunction;
import io.tapdata.pdk.apis.functions.connector.TapFunction;
import io.tapdata.pdk.apis.functions.connector.common.ReleaseExternalFunction;
import io.tapdata.pdk.apis.functions.connector.source.*;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class  ConnectorFunctions extends ConnectionFunctions<ConnectorFunctions> {
    protected CountRawCommandFunction countRawCommandFunction;
    protected RunRawCommandFunction runRawCommandFunction;
    protected ExecuteCommandFunction executeCommandFunction;
    protected ReleaseExternalFunction releaseExternalFunction;
    protected BatchReadFunction batchReadFunction;
    protected StreamReadFunction streamReadFunction;
    protected BatchCountFunction batchCountFunction;
    protected TimestampToStreamOffsetFunction timestampToStreamOffsetFunction;
    protected WriteRecordFunction writeRecordFunction;
    protected QueryByFilterFunction queryByFilterFunction;
    protected QueryByAdvanceFilterFunction queryByAdvanceFilterFunction;
    //create_table_event
    protected CreateTableFunction createTableFunction;
    protected CreateTableV2Function createTableV2Function;
    //clear_table_event
    protected ClearTableFunction clearTableFunction;
    //drop_table_event
    protected DropTableFunction dropTableFunction;
    protected ControlFunction controlFunction;
    protected CreateIndexFunction createIndexFunction;
    protected DeleteIndexFunction deleteIndexFunction;
    protected QueryIndexesFunction queryIndexesFunction;
    //alter_database_timezone_event
    protected AlterDatabaseTimeZoneFunction alterDatabaseTimeZoneFunction;
    //alter_field_attributes_event
    protected AlterFieldAttributesFunction alterFieldAttributesFunction;
    //alter_field_name_event
    protected AlterFieldNameFunction alterFieldNameFunction;
    //alter_table_charset_event
    protected AlterTableCharsetFunction alterTableCharsetFunction;
    //drop_field_event
    protected DropFieldFunction dropFieldFunction;
    //new_field_event
    protected NewFieldFunction newFieldFunction;
    protected RawDataCallbackFilterFunction rawDataCallbackFilterFunction;
    protected RawDataCallbackFilterFunctionV2 rawDataCallbackFilterFunctionV2;

    protected CountByPartitionFilterFunction countByPartitionFilterFunction;
    protected GetReadPartitionsFunction getReadPartitionsFunction;
    protected QueryFieldMinMaxValueFunction queryFieldMinMaxValueFunction;
    protected StreamReadMultiConnectionFunction streamReadMultiConnectionFunction;
    protected TransactionBeginFunction transactionBeginFunction;
    protected TransactionCommitFunction transactionCommitFunction;
    protected TransactionRollbackFunction transactionRollbackFunction;

    public ConnectorFunctions supportTransactionBeginFunction(TransactionBeginFunction function) {
        transactionBeginFunction = function;
        return this;
    }
    public ConnectorFunctions supportTransactionCommitFunction(TransactionCommitFunction function) {
        transactionCommitFunction = function;
        return this;
    }
    public ConnectorFunctions supportTransactionRollbackFunction(TransactionRollbackFunction function) {
        transactionRollbackFunction = function;
        return this;
    }
    public ConnectorFunctions supportStreamReadMultiConnectionFunction(StreamReadMultiConnectionFunction function) {
        streamReadMultiConnectionFunction = function;
        return this;
    }

    public ConnectorFunctions supportCountRawCommandFunction(CountRawCommandFunction function) {
        countRawCommandFunction = function;
        return this;
    }

    public ConnectorFunctions supportRunRawCommandFunction(RunRawCommandFunction function) {
        runRawCommandFunction = function;
        return this;
    }
    public ConnectorFunctions supportCountByPartitionFilterFunction(CountByPartitionFilterFunction function) {
        countByPartitionFilterFunction = function;
        return this;
    }
    public ConnectorFunctions supportGetReadPartitionsFunction(GetReadPartitionsFunction function) {
        getReadPartitionsFunction = function;
        return this;
    }
    public ConnectorFunctions supportQueryFieldMinMaxValueFunction(QueryFieldMinMaxValueFunction function) {
        this.queryFieldMinMaxValueFunction = function;
        return this;
    }
    public ConnectorFunctions supportRawDataCallbackFilterFunction(RawDataCallbackFilterFunction function) {
        rawDataCallbackFilterFunction = function;
        return this;
    }

    public ConnectorFunctions supportRawDataCallbackFilterFunctionV2(RawDataCallbackFilterFunctionV2 function) {
        rawDataCallbackFilterFunctionV2 = function;
        return this;
    }
    public ConnectorFunctions supportAlterDatabaseTimeZoneFunction(AlterDatabaseTimeZoneFunction function) {
        alterDatabaseTimeZoneFunction = function;
        return this;
    }
    public ConnectorFunctions supportAlterFieldAttributesFunction(AlterFieldAttributesFunction function) {
        alterFieldAttributesFunction = function;
        return this;
    }
    public ConnectorFunctions supportAlterFieldNameFunction(AlterFieldNameFunction function) {
        alterFieldNameFunction = function;
        return this;
    }
    public ConnectorFunctions supportAlterTableCharsetFunction(AlterTableCharsetFunction function) {
        alterTableCharsetFunction = function;
        return this;
    }
    public ConnectorFunctions supportDropFieldFunction(DropFieldFunction function) {
        dropFieldFunction = function;
        return this;
    }
    public ConnectorFunctions supportNewFieldFunction(NewFieldFunction function) {
        newFieldFunction = function;
        return this;
    }

    public ConnectorFunctions supportExecuteCommandFunction(ExecuteCommandFunction function) {
        executeCommandFunction = function;
        return this;
    }

    public List<Capability> getCapabilities() {
        Field[] connectorFields = ConnectorFunctions.class.getDeclaredFields();
        Field[] connectionFields = ConnectionFunctions.class.getDeclaredFields();

        List<Field> allFields = new ArrayList<>();
        allFields.addAll(Arrays.asList(connectorFields));
        allFields.addAll(Arrays.asList(connectionFields));
        List<Capability> fieldArray = new ArrayList<>();
        for(Field field : allFields) {
            try {
                Object value = field.get(this);
                if(value instanceof TapFunction) {
                    String fieldName = field.getName();
//                    if(fieldName.endsWith("Function")) {
//                        fieldName = fieldName.substring(0, fieldName.length() - "Function".length());
//                    }
                    StringBuilder fieldNameBuilder = new StringBuilder();
                    for(char c : fieldName.toCharArray()) {
                        if(c >= 'A' && c <= 'Z') {
                            fieldNameBuilder.append("_");
                            c += 32;
                        }
                        fieldNameBuilder.append(c);
                    }
                    fieldArray.add(Capability.create(fieldNameBuilder.toString()).type(Capability.TYPE_FUNCTION));
                }
            } catch (Throwable ignored) {}
        }
        return fieldArray;
    }

    public static void main(String[] args) {
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        connectorFunctions.supportGetTableNamesFunction(new GetTableNamesFunction() {
            @Override
            public void tableNames(TapConnectionContext nodeContext, int batchSize, Consumer<List<String>> consumer) throws Throwable {

            }
        });
        connectorFunctions.supportReleaseExternalFunction(new ReleaseExternalFunction() {
            @Override
            public void release(TapConnectorContext connectorContext) throws Throwable {

            }
        });
        connectorFunctions.supportBatchCount(new BatchCountFunction() {
            @Override
            public long count(TapConnectorContext nodeContext, TapTable table) throws Throwable {
                return 0;
            }
        });
        connectorFunctions.supportGetReadPartitionsFunction(new GetReadPartitionsFunction() {
            @Override
            public void getReadPartitions(TapConnectorContext connectorContext, TapTable table, GetReadPartitionOptions options) {

            }
        });
        connectorFunctions.supportQueryFieldMinMaxValueFunction(new QueryFieldMinMaxValueFunction() {
            @Override
            public FieldMinMaxValue minMaxValue(TapConnectorContext connectorContext, TapTable table, TapAdvanceFilter filter, String fieldName) {
                return null;
            }
        });
        connectorFunctions.supportCountByPartitionFilterFunction(new CountByPartitionFilterFunction() {
            @Override
            public long countByPartitionFilter(TapConnectorContext connectorContext, TapTable table, TapAdvanceFilter filter) {
                return 0;
            }
        });
        connectorFunctions.supportRawDataCallbackFilterFunctionV2(new RawDataCallbackFilterFunctionV2() {
            @Override
            public List<TapEvent> filter(TapConnectorContext context, List<String> tables, Map<String, Object> rawData) {
                return null;
            }
        });
        connectorFunctions.supportRawDataCallbackFilterFunction(new RawDataCallbackFilterFunction() {
            @Override
            public List<TapEvent> filter(TapConnectorContext context, Map<String, Object> rawData) {
                return null;
            }
        });
        for (Capability capability : connectorFunctions.getCapabilities()) {
            System.out.println("array " + capability);
        }
    }

    public ConnectorFunctions supportReleaseExternalFunction(ReleaseExternalFunction function) {
        releaseExternalFunction = function;
        return this;
    }
    public ConnectorFunctions supportQueryIndexes(QueryIndexesFunction function) {
        queryIndexesFunction = function;
        return this;
    }

    public ConnectorFunctions supportCreateIndex(CreateIndexFunction function) {
        createIndexFunction = function;
        return this;
    }

    public ConnectorFunctions supportDeleteIndex(DeleteIndexFunction function) {
        deleteIndexFunction = function;
        return this;
    }

    public ConnectorFunctions supportControl(ControlFunction function) {
        controlFunction = function;
        return this;
    }

    /**
     * Flow engine may get current stream offset at any time.
     * To continue stream read for the stream offset when job resumed from pause or stopped accidentally.
     *
     * @param function
     * @return
     */
    public ConnectorFunctions supportTimestampToStreamOffset(TimestampToStreamOffsetFunction function) {
        timestampToStreamOffsetFunction = function;
        return this;
    }

    /**
     * Flow engine will call this method when batch read is started.
     * You need implement the batch feature in this method synchronously, once this method finished, Flow engine will consider the batch read is ended.
     *
     * Exception can be throw in this method, Flow engine will consider there is an error occurred in batch read.
     *
     * @param function
     * @return
     */
    public ConnectorFunctions supportBatchRead(BatchReadFunction function) {
        batchReadFunction = function;
        return this;
    }

    /**
     *
     */
    public ConnectorFunctions supportStreamRead(StreamReadFunction function) {
        streamReadFunction = function;
        return this;
    }

    public ConnectorFunctions supportBatchCount(BatchCountFunction function) {
        this.batchCountFunction = function;
        return this;
    }

    public ConnectorFunctions supportWriteRecord(WriteRecordFunction function) {
        this.writeRecordFunction = function;
        return this;
    }

    /**
     * @deprecated
     *
     * Please use supportCreateTableV2 instead.
     * CreateTableOptions is required as return value to tell engine the table is exists already or not.
     *
     * @param function
     * @return
     */
    @Deprecated
    public ConnectorFunctions supportCreateTable(CreateTableFunction function) {
        this.createTableFunction = function;
        return this;
    }

    public ConnectorFunctions supportCreateTableV2(CreateTableV2Function function) {
        this.createTableV2Function = function;
        return this;
    }


    public ConnectorFunctions supportClearTable(ClearTableFunction function) {
        this.clearTableFunction = function;
        return this;
    }

    public ConnectorFunctions supportDropTable(DropTableFunction function) {
        this.dropTableFunction = function;
        return this;
    }

    public ConnectorFunctions supportQueryByFilter(QueryByFilterFunction function) {
        this.queryByFilterFunction = function;
        return this;
    }

    public ConnectorFunctions supportQueryByAdvanceFilter(QueryByAdvanceFilterFunction function) {
        this.queryByAdvanceFilterFunction = function;
        return this;
    }

    public WriteRecordFunction getWriteRecordFunction() {
        return writeRecordFunction;
    }

    public QueryByAdvanceFilterFunction getQueryByAdvanceFilterFunction() {
        return queryByAdvanceFilterFunction;
    }

    public BatchReadFunction getBatchReadFunction() {
        return batchReadFunction;
    }

    public StreamReadFunction getStreamReadFunction() {
        return streamReadFunction;
    }

    public BatchCountFunction getBatchCountFunction() {
        return batchCountFunction;
    }

    public TimestampToStreamOffsetFunction getTimestampToStreamOffsetFunction() {
        return timestampToStreamOffsetFunction;
    }

    public QueryByFilterFunction getQueryByFilterFunction() {
        return queryByFilterFunction;
    }

    public CreateTableFunction getCreateTableFunction() {
        return createTableFunction;
    }

    public ClearTableFunction getClearTableFunction() {
        return clearTableFunction;
    }

    public DropTableFunction getDropTableFunction() {
        return dropTableFunction;
    }

    public ControlFunction getControlFunction() {
        return controlFunction;
    }

    public CreateIndexFunction getCreateIndexFunction() {
        return createIndexFunction;
    }

    public DeleteIndexFunction getDeleteIndexFunction() {
        return deleteIndexFunction;
    }

    public QueryIndexesFunction getQueryIndexesFunction() {
        return queryIndexesFunction;
    }

    public ReleaseExternalFunction getReleaseExternalFunction() {
        return releaseExternalFunction;
    }

    public AlterDatabaseTimeZoneFunction getAlterDatabaseTimeZoneFunction() {
        return alterDatabaseTimeZoneFunction;
    }

    public AlterFieldAttributesFunction getAlterFieldAttributesFunction() {
        return alterFieldAttributesFunction;
    }

    public AlterFieldNameFunction getAlterFieldNameFunction() {
        return alterFieldNameFunction;
    }

    public AlterTableCharsetFunction getAlterTableCharsetFunction() {
        return alterTableCharsetFunction;
    }

    public DropFieldFunction getDropFieldFunction() {
        return dropFieldFunction;
    }

    public NewFieldFunction getNewFieldFunction() {
        return newFieldFunction;
    }

    public CreateTableV2Function getCreateTableV2Function() {
        return createTableV2Function;
    }

    public RawDataCallbackFilterFunction getRawDataCallbackFilterFunction() {
        return rawDataCallbackFilterFunction;
    }

    public RawDataCallbackFilterFunctionV2 getRawDataCallbackFilterFunctionV2() {
        return rawDataCallbackFilterFunctionV2;
    }

    public ExecuteCommandFunction getExecuteCommandFunction() {
        return executeCommandFunction;
    }

    public CountByPartitionFilterFunction getCountByPartitionFilterFunction() {
        return countByPartitionFilterFunction;
    }

    public GetReadPartitionsFunction getGetReadPartitionsFunction() {
        return getReadPartitionsFunction;
    }

    public QueryFieldMinMaxValueFunction getQueryFieldMinMaxValueFunction() {
        return queryFieldMinMaxValueFunction;
    }

    public RunRawCommandFunction getRunRawCommandFunction() {
        return runRawCommandFunction;
    }

    public StreamReadMultiConnectionFunction getStreamReadMultiConnectionFunction() {
        return streamReadMultiConnectionFunction;
    }

    public TransactionBeginFunction getTransactionBeginFunction() {
        return transactionBeginFunction;
    }

    public TransactionCommitFunction getTransactionCommitFunction() {
        return transactionCommitFunction;
    }

    public TransactionRollbackFunction getTransactionRollbackFunction() {
        return transactionRollbackFunction;
    }

    public CountRawCommandFunction getCountRawCommandFunction() {
        return countRawCommandFunction;
    }
}
