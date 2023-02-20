package io.tapdata.bigquery.service.command;

import io.tapdata.bigquery.entity.ContextConfig;
import io.tapdata.bigquery.service.bigQuery.BigQueryResult;
import io.tapdata.bigquery.service.bigQuery.BigQueryStart;
import io.tapdata.bigquery.service.bigQuery.SqlMarker;
import io.tapdata.bigquery.util.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

import java.util.*;

import static io.tapdata.base.ConnectorBase.entry;
import static io.tapdata.base.ConnectorBase.map;

/**
 * command  -> SchemaSetList
 * io  -> https://cloud.google.com/bigquery/docs/listing-datasets
 */
public class SchemaSetList implements Command {
    public static final String SQL = "SELECT schema_name FROM `#{projectId}`.INFORMATION_SCHEMA.SCHEMATA;";

    @Override
    public CommandResult commandResult(TapConnectionContext tapConnectionContext, CommandInfo commandInfo) {
        Map<String, Object> connectionConfig = commandInfo.getConnectionConfig();
        if (Checker.isEmptyCollection(connectionConfig)) {
            throw new CoreException("Connection Config must be null or empty.");
        }
        tapConnectionContext.setConnectionConfig(DataMap.create(connectionConfig));
        ContextConfig config = BigQueryStart.contextConfig(tapConnectionContext);
        String credentialsJson = config.serviceAccount();
        if (Checker.isEmpty(connectionConfig)) {
            throw new CoreException("Please sure your credentialsJson is accurate.");
        }
        String projectId = config.projectId();
        if (Checker.isEmpty(connectionConfig)) {
            throw new CoreException("Please sure your credentialsJson is accurate.and ensure the credentialsJson embody project_id.");
        }
        SqlMarker sqlMarker = SqlMarker.create(credentialsJson);
        BigQueryResult bigQueryResult = sqlMarker.executeOnce(SchemaSetList.SQL.replace("#{projectId}", projectId));
        if (Checker.isEmpty(bigQueryResult)) {
            throw new CoreException("Sql query exception, data set cannot be obtained.");
        }
        List<Map<String, Object>> result = bigQueryResult.result();

        Map<String, Object> pageResult = new HashMap<>();
        pageResult.put("page", 1);
        pageResult.put("size", bigQueryResult.getTotalRows());
        pageResult.put("total", bigQueryResult.getTotalRows());
        List<Map<String, Object>> resultList = new ArrayList<>();
        if (Checker.isNotEmpty(pageResult)) {
            result.forEach(map -> resultList.add(map(entry("label", map.get("schema_name")), entry("value", map.get("schema_name")))));
        }
        pageResult.put("items", resultList);
        return new CommandResult().result(pageResult);
    }
}
