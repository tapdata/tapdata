package com.tapdata.connector.mariadb;


import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author lemon
 */
@TapConnectorClass("spec_redis.json")
public class MariadbConnector extends ConnectorBase {

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
//        this.mysqlJdbcContext = new MysqlJdbcContext(tapConnectionContext);
//        if (tapConnectionContext instanceof TapConnectorContext) {
//            this.mysqlWriter = new MysqlJdbcOneByOneWriter(mysqlJdbcContext);
//            this.mysqlReader = new MysqlReader(mysqlJdbcContext);
//            this.version = mysqlJdbcContext.getMysqlVersion();
//            this.connectionTimezone = tapConnectionContext.getConnectionConfig().getString("timezone");
//            if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
//                this.connectionTimezone = mysqlJdbcContext.timezone();
//            }
//        }
//        ddlSqlMaker = new MysqlDDLSqlMaker(version);
//        fieldDDLHandlers = new BiClassHandlers<>();
//        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
//        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
//        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
//        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);

    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {

    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {

    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {

    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        return null;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return 0;
    }
}
