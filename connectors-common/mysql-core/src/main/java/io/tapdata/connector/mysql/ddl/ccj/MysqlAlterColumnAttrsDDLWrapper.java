package io.tapdata.connector.mysql.ddl.ccj;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-04 17:45
 **/
public class MysqlAlterColumnAttrsDDLWrapper extends MysqlDDLWrapper {

    @Override
    public List<Capability> getCapabilities() {
        return Collections.singletonList(Capability.create(ConnectionOptions.DDL_ALTER_FIELD_ATTRIBUTES_EVENT).type(Capability.TYPE_DDL));
    }

    @Override
    public void wrap(Alter ddl, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) {
        verifyAlter(ddl);
        String tableName = getTableName(ddl);
        TapTable tapTable = null != tableMap ? tableMap.get(tableName) : null;
        List<AlterExpression> alterExpressions = ddl.getAlterExpressions();
        if (null == alterExpressions || alterExpressions.size() <= 0) {
            return;
        }
        for (AlterExpression alterExpression : alterExpressions) {
            if (alterExpression.getOperation() != AlterOperation.CHANGE
                    && alterExpression.getOperation() != AlterOperation.MODIFY
                    && alterExpression.getOperation() != AlterOperation.ALTER) {
                continue;
            }
            List<AlterExpression.ColumnDataType> colDataTypeList = alterExpression.getColDataTypeList();
            for (AlterExpression.ColumnDataType columnDataType : colDataTypeList) {
                TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = new TapAlterFieldAttributesEvent();
                tapAlterFieldAttributesEvent.setTableId(tableName);
                String columnName = StringKit.removeHeadTail(columnDataType.getColumnName(), ccjddlWrapperConfig.getSplit(), null);
                tapAlterFieldAttributesEvent.fieldName(columnName);
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
                            boolean nullable = !"not".equals(preSpec);
                            tapAlterFieldAttributesEvent.nullable(ValueChange.create(nullable));
                            preSpec = "";
                            break;
                        case "key":
                            if (EmptyKit.isBlank(preSpec)) {
                                int primaryPos = 1;
                                if (null != tapTable) {
                                    primaryPos = tapTable.getMaxPKPos() + 1;
                                }
                                tapAlterFieldAttributesEvent.primaryChange(ValueChange.create(primaryPos));
                                preSpec = "";
                            }
                            break;
                        default:
                            if ("default".equals(preSpec)) {
                                tapAlterFieldAttributesEvent.defaultChange(ValueChange.create(StringKit.removeHeadTail(columnSpec, "'", null)));
                                preSpec = "";
                            } else if ("comment".equals(preSpec)) {
                                tapAlterFieldAttributesEvent.comment(ValueChange.create(StringKit.removeHeadTail(columnSpec, "'", null)));
                                preSpec = "";
                            }
                            break;
                    }
                }
                ColDataType colDataType = columnDataType.getColDataType();
                String dataType = getDataType(colDataType);
                tapAlterFieldAttributesEvent.dataType(ValueChange.create(dataType));
                consumer.accept(tapAlterFieldAttributesEvent);
            }
        }
    }
}
