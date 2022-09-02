package io.tapdata.common.ddl;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;

public interface DDLSqlGenerator {

    default List<String> addColumn(CommonDbConfig config, TapNewFieldEvent tapNewFieldEvent) {
        throw new UnsupportedOperationException();
    }

    default List<String> alterColumnAttr(CommonDbConfig config, TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent) {
        throw new UnsupportedOperationException();
    }

    default List<String> alterColumnName(CommonDbConfig config, TapAlterFieldNameEvent tapAlterFieldNameEvent) {
        throw new UnsupportedOperationException();
    }

    default List<String> dropColumn(CommonDbConfig config, TapDropFieldEvent tapDropFieldEvent) {
        throw new UnsupportedOperationException();
    }

}
