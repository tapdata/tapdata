package io.tapdata.bigquery.service;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public class OpenApiWriteRecoder {
    public static final String projectId = "";
    public static final String tableSetId = "";
    public static final String openApi = "";


    public boolean tableExist(String tableName){
        return true;
    }

    public String[] createTable(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent, String version){
        return null;
    }
}
