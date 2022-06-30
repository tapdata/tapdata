package io.tapdata.pdk.apis.functions;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.TapFunction;
import io.tapdata.pdk.apis.functions.connector.common.ReleaseExternalFunction;
import io.tapdata.pdk.apis.functions.connector.source.*;
import io.tapdata.pdk.apis.functions.connector.target.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ConnectorFunctions extends CommonFunctions<ConnectorFunctions> {
    private ReleaseExternalFunction releaseExternalFunction;
    private BatchReadFunction batchReadFunction;
    private StreamReadFunction streamReadFunction;
    private BatchCountFunction batchCountFunction;
    private TimestampToStreamOffsetFunction timestampToStreamOffsetFunction;
    private WriteRecordFunction writeRecordFunction;
    private QueryByFilterFunction queryByFilterFunction;
    private QueryByAdvanceFilterFunction queryByAdvanceFilterFunction;
    private CreateTableFunction createTableFunction;
    private ClearTableFunction clearTableFunction;
    private DropTableFunction dropTableFunction;
    private ControlFunction controlFunction;
    private CreateIndexFunction createIndexFunction;
    private DeleteIndexFunction deleteIndexFunction;
    private QueryIndexesFunction queryIndexesFunction;
    private AlterDatabaseTimeZoneFunction alterDatabaseTimeZoneFunction;
    private AlterFieldCheckFunction alterFieldCheckFunction;
    private AlterFieldCommentFunction alterFieldCommentFunction;
    private AlterFieldConstraintFunction alterFieldConstraintFunction;
    private AlterFieldDataTypeFunction alterFieldDataTypeFunction;
    private AlterFieldDefaultFunction alterFieldDefaultFunction;
    private AlterFieldNameFunction alterFieldNameFunction;
    private AlterFieldNotNullFunction alterFieldNotNullFunction;
    private AlterFieldPrimaryKeyFunction alterFieldPrimaryKeyFunction;
    private AlterTableCharsetFunction alterTableCharsetFunction;
    private DropFieldFunction dropFieldFunction;
    private NewFieldFunction newFieldFunction;

    public ConnectorFunctions supportAlterDatabaseTimeZoneFunction(AlterDatabaseTimeZoneFunction function) {
        alterDatabaseTimeZoneFunction = function;
        return this;
    }
    public ConnectorFunctions supportAlterFieldCheckFunction(AlterFieldCheckFunction function) {
        alterFieldCheckFunction = function;
        return this;
    }
    public ConnectorFunctions supportAlterFieldCommentFunction(AlterFieldCommentFunction function) {
        alterFieldCommentFunction = function;
        return this;
    }
    public ConnectorFunctions supportAlterFieldConstraintFunction(AlterFieldConstraintFunction function) {
        alterFieldConstraintFunction = function;
        return this;
    }
    public ConnectorFunctions supportAlterFieldDataTypeFunction(AlterFieldDataTypeFunction function) {
        alterFieldDataTypeFunction = function;
        return this;
    }
    public ConnectorFunctions supportAlterFieldDefaultFunction(AlterFieldDefaultFunction function) {
        alterFieldDefaultFunction = function;
        return this;
    }
    public ConnectorFunctions supportAlterFieldNameFunction(AlterFieldNameFunction function) {
        alterFieldNameFunction = function;
        return this;
    }
    public ConnectorFunctions supportAlterFieldNotNullFunction(AlterFieldNotNullFunction function) {
        alterFieldNotNullFunction = function;
        return this;
    }
    public ConnectorFunctions supportAlterFieldPrimaryKeyFunction(AlterFieldPrimaryKeyFunction function) {
        alterFieldPrimaryKeyFunction = function;
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

    public List<String> getCapabilities() {
        Field[] fields = ConnectorFunctions.class.getDeclaredFields();
        List<String> fieldArray = new ArrayList<>();
        for(Field field : fields) {
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
                            fieldNameBuilder.append("-");
                            c += 32;
                        }
                        fieldNameBuilder.append(c);
                    }
                    fieldArray.add(fieldNameBuilder.toString());
                }
            } catch (Throwable ignored) {}
        }
        return fieldArray;
    }

    public static void main(String[] args) {
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
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
        System.out.println("array " + connectorFunctions.getCapabilities());
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

    public ConnectorFunctions supportCreateTable(CreateTableFunction function) {
        this.createTableFunction = function;
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

    public AlterFieldCheckFunction getAlterFieldCheckFunction() {
        return alterFieldCheckFunction;
    }

    public AlterFieldCommentFunction getAlterFieldCommentFunction() {
        return alterFieldCommentFunction;
    }

    public AlterFieldConstraintFunction getAlterFieldConstraintFunction() {
        return alterFieldConstraintFunction;
    }

    public AlterFieldDataTypeFunction getAlterFieldDataTypeFunction() {
        return alterFieldDataTypeFunction;
    }

    public AlterFieldDefaultFunction getAlterFieldDefaultFunction() {
        return alterFieldDefaultFunction;
    }

    public AlterFieldNameFunction getAlterFieldNameFunction() {
        return alterFieldNameFunction;
    }

    public AlterFieldNotNullFunction getAlterFieldNotNullFunction() {
        return alterFieldNotNullFunction;
    }

    public AlterFieldPrimaryKeyFunction getAlterFieldPrimaryKeyFunction() {
        return alterFieldPrimaryKeyFunction;
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
}
