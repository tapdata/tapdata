package io.tapdata.connector.tidb.ddl.ccj;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.Capability;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.pdk.apis.entity.ConnectionOptions.DDL_NEW_FIELD_EVENT;

public class TidbAddColumnDDLWrapper extends TidbDDLWrapper {

    @Override
    public List<Capability> getCapabilities() {
        return Collections.singletonList(Capability.create(DDL_NEW_FIELD_EVENT).type(Capability.TYPE_DDL));
    }

    @Override
    public void wrap(Alter ddl, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) {
        verifyAlter(ddl);
        String tableName = getTableName(ddl);
        TapTable tapTable = null == tableMap ? null : tableMap.get(tableName);
        List<AlterExpression> alterExpressions = ddl.getAlterExpressions();
        for (AlterExpression alterExpression : alterExpressions) {
            if (alterExpression.getOperation() != AlterOperation.ADD) {
                continue;
            }
            List<AlterExpression.ColumnDataType> colDataTypeList = alterExpression.getColDataTypeList();
            if (null == colDataTypeList) {
                continue;
            }
            TapNewFieldEvent tapNewFieldEvent = new TapNewFieldEvent();
            tapNewFieldEvent.setTableId(tableName);
            for (AlterExpression.ColumnDataType columnDataType : colDataTypeList) {
                String columnName = StringKit.removeHeadTail(columnDataType.getColumnName(), ccjddlWrapperConfig.getSplit(), null);
                ColDataType colDataType = columnDataType.getColDataType();
                String dataType = getDataType(colDataType);
                TapField tapField = new TapField(columnName, dataType);
                List<String> columnSpecs = columnDataType.getColumnSpecs();
                String preSpec = "";
                for (String columnSpec : columnSpecs) {
                    String lowerColumnSpec = columnSpec.toLowerCase();
                    switch (lowerColumnSpec) {
                        case "not":
                        case "default":
                        case "comment":
                            preSpec = lowerColumnSpec;
                            break;
                        case "null":
                            tapField.nullable(!"not".equals(preSpec));
                            preSpec = "";
                            break;
                        case "auto_increment":
                            tapField.autoInc(true);
                            preSpec = "";
                            break;
                        default:
                            if ("default".equals(preSpec)) {
                                tapField.defaultValue(StringKit.removeHeadTail(columnSpec, "'", null));
                                preSpec = "";
                            } else if ("comment".equals(preSpec)) {
                                tapField.comment(StringKit.removeHeadTail(columnSpec, "'", null));
                                preSpec = "";
                            }
                            break;
                    }
                }
                setColumnPos(tapTable, tapField);
                tapNewFieldEvent.field(tapField);
                consumer.accept(tapNewFieldEvent);
            }
        }
    }
}
