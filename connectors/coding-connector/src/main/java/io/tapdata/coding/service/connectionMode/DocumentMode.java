package io.tapdata.coding.service.connectionMode;

import cn.hutool.http.HttpRequest;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.service.loader.CodingStarter;
import io.tapdata.coding.service.loader.IssuesLoader;
import io.tapdata.coding.service.schema.SchemaStart;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * {
 * "label": "${document}",
 * "value": "DocumentMode"
 * }
 */
public class DocumentMode implements ConnectionMode {
    TapConnectionContext connectionContext;
    IssuesLoader loader;
    ContextConfig contextConfig;
    AtomicReference<String> accessToken;

    @Override
    public ConnectionMode config(TapConnectionContext connectionContext, AtomicReference<String> accessToken) {
        this.connectionContext = connectionContext;
        this.loader = IssuesLoader.create(connectionContext, accessToken);
        this.contextConfig = loader.veryContextConfigAndNodeConfig();
        this.accessToken = accessToken;
        return this;
    }

    @Override
    public List<TapTable> discoverSchema(List<String> tables, int tableSize, AtomicReference<String> accessToken) {
        List<SchemaStart> schemaStart = SchemaStart.getAllSchemas(connectionContext, accessToken);
        if (tables == null || tables.isEmpty()) {
            List<TapTable> tapTables = list();
            schemaStart.forEach(schema -> {
                TapTable documentTable = schema.document(connectionContext);
                if (Checker.isNotEmpty(documentTable)) {
                    tapTables.add(documentTable);
                }
            });
            return tapTables;
        }
        return null;
    }

    @Override
    public Map<String, Object> attributeAssignment(Map<String, Object> stringObjectMap) {
        Object code = stringObjectMap.get("Code");
        HttpEntity<String, String> header = HttpEntity.create().builder("Authorization", accessToken.get());
        String projectName = contextConfig.getProjectName();
        HttpEntity<String, Object> issueDetialBody = HttpEntity.create()
                .builder("Action", "DescribeIssue")
                .builder("ProjectName", projectName);
        String teamName = contextConfig.getTeamName();
        CodingHttp authorization = CodingHttp.create(header.getEntity(), String.format(CodingStarter.OPEN_API_URL, teamName));
        HttpRequest requestDetail = authorization.createHttpRequest();
        Map<String, Object> issueDetail = loader.readIssueDetail(
                issueDetialBody,
                authorization,
                requestDetail,
                (code instanceof Integer) ? (Integer) code : Integer.parseInt(code.toString()),
                projectName,
                teamName);
        loader.composeIssue(projectName, teamName, issueDetail);
        return issueDetail;
    }
}
