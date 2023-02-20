package io.tapdata.connector.tidb.ddl.ccj;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class TidbAlterColumnNameDDLWrapper extends TidbDDLWrapper {

    @Override
    public List<Capability> getCapabilities() {
        return Collections.singletonList(Capability.create(ConnectionOptions.DDL_ALTER_FIELD_NAME_EVENT).type(Capability.TYPE_DDL));
    }

    @Override
    public void wrap(Alter ddl, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) {
        verifyAlter(ddl);
        TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
        String tableName = getTableName(ddl);
        tapAlterFieldNameEvent.setTableId(tableName);
        List<AlterExpression> alterExpressions = ddl.getAlterExpressions();
        if (null == alterExpressions || alterExpressions.size() <= 0) {
            return;
        }
        AlterExpression alterExpression = alterExpressions.get(0);
        if (alterExpression.getOperation() == AlterOperation.RENAME) {
            String columnName = alterExpression.getColumnName();
            String columnOldName = alterExpression.getColumnOldName();
            if (EmptyKit.isBlank(columnName) || EmptyKit.isBlank(columnOldName)) {
                return;
            }
            tapAlterFieldNameEvent.nameChange(ValueChange.create(StringKit.removeHeadTail(columnOldName, ccjddlWrapperConfig.getSplit(), null), StringKit.removeHeadTail(columnName, ccjddlWrapperConfig.getSplit(), null)));
            consumer.accept(tapAlterFieldNameEvent);
        }
    }
}
