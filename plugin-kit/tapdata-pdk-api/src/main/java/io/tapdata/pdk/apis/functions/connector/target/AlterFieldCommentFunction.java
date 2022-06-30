package io.tapdata.pdk.apis.functions.connector.target;

import io.tapdata.entity.event.ddl.entity.FieldAttrChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldCommentEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;

public interface AlterFieldCommentFunction {
    void alterFieldComment(TapConnectorContext connectorContext, TapAlterFieldCommentEvent alterFieldCommentEvent) throws Throwable;
}
