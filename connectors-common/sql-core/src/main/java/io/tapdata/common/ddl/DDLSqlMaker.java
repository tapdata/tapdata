package io.tapdata.common.ddl;

import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2022-07-11 17:25
 **/
public interface DDLSqlMaker {
    default List<String> addColumn(TapConnectorContext tapConnectorContext, TapNewFieldEvent tapNewFieldEvent) {
        throw new UnsupportedOperationException();
    }

    default List<String> alterColumnAttr(TapConnectorContext tapConnectorContext, TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent) {
        throw new UnsupportedOperationException();
    }

    default List<String> alterColumnName(TapConnectorContext tapConnectorContext, TapAlterFieldNameEvent tapAlterFieldNameEvent) {
        throw new UnsupportedOperationException();
    }

    default List<String> dropColumn(TapConnectorContext tapConnectorContext, TapDropFieldEvent tapDropFieldEvent) {
        throw new UnsupportedOperationException();
    }
}
