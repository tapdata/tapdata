package com.tapdata.tm.mcp.tools;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.mcp.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.tapdata.tm.mcp.Utils.sendPostRequest;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 09:49
 */
@Slf4j
//@Component
public class SampleData {
    private final McpToolSupport toolSupport;
    private final int serverPort;

    public SampleData(McpToolSupport toolSupport, ServletWebServerApplicationContext webServerAppCtxt) {
        this.toolSupport = toolSupport;
        this.serverPort = webServerAppCtxt.getWebServer().getPort();
    }

    @McpTool(name = "sampleData", description = "Query sample data using the specified database connection id and data schema name. Returns a maximum of 100 rows of data.")
    public Object sampleData(
            McpSyncRequestContext context,
            @McpToolParam(description = "The id of the connection to query sample data.") String connectionId,
            @McpToolParam(description = "The name of the data schema to query sample data.") String schemaName) {
        if (StringUtils.isBlank(connectionId))
            throw new RuntimeException("Parameter connectionId is required.");

        if (StringUtils.isBlank(schemaName))
            throw new RuntimeException("Parameter schemaName is required.");

        Map<String, Object> data = new HashMap<>();
        data.put("className", "QueryDataBaseDataService");
        data.put("method", "getData");
        data.put("args", Arrays.asList(connectionId, schemaName));

        try {
            String response = sendPostRequest(
                    String.format("http://localhost:%d/api/proxy/call?access_token=%s", serverPort, toolSupport.getAccessToken(context)), data);

            ResponseMessage<Object> responseMessage = Utils.parseJson(response, new TypeReference<ResponseMessage<Object>>() {
            });
            if (ResponseMessage.OK.equals(responseMessage.getCode()))
                return responseMessage.getData();
            else
                throw new RuntimeException(String.format("%s: %s", responseMessage.getCode(), responseMessage.getMessage()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
