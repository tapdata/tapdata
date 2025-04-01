package com.tapdata.tm.mcp.tools;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.tapdata.tm.mcp.Utils.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/3/26 09:49
 */
@Slf4j
//@Component
public class SampleData extends Tool{
    private int serverPort;
    public SampleData(SessionAttribute sessionAttribute, UserService userService, ServletWebServerApplicationContext webServerAppCtxt) {
        super("sampleData", "Query sample data using the specified database connection id and data schema name. Returns a maximum of 100 rows of data.",
                readJsonSchema("SampleData.json"), sessionAttribute, userService);
        this.serverPort = webServerAppCtxt.getWebServer().getPort();
    }

    public McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params) {

        String userId = getUserId(exchange);
        if (userId == null) {
            log.error("Not found userId in session");
            throw new RuntimeException("Not found userId in current session");
        }

        String connectionId = getStringValue(params, "connectionId");
        String schemaName = getStringValue(params, "schemaName");

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
                    String.format("http://localhost:%d/api/proxy/call?access_token=%s", serverPort, getAccessToken(exchange)), data);

            ResponseMessage<Object> responseMessage = JsonUtil.parseJsonUseJackson(response, new TypeReference<ResponseMessage<Object>>() {
            });
            if (ResponseMessage.OK.equals(responseMessage.getCode()))
                return makeCallToolResult(responseMessage.getData());
            else
                throw new RuntimeException(String.format("%s: %s", responseMessage.getCode(), responseMessage.getMessage()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
