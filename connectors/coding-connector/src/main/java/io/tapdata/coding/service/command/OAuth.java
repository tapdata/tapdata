package io.tapdata.coding.service.command;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;


public class OAuth implements Command {
    @Override
    public CommandResult commandResult(TapConnectionContext tapConnectionContext, CommandInfo commandInfo, AtomicReference<String> accessToken) {
        Map<String, Object> connectionConfig = commandInfo.getConnectionConfig();
        Object tapdataConfig = connectionConfig.get("__TAPDATA_CONFIG");
        if (Checker.isEmpty(connectionConfig)) {
            throw new IllegalArgumentException("ConnectionConfig cannot be null");
        }
        Object codeObj = connectionConfig.get("code");
        if (Checker.isEmpty(codeObj)) {
            throw new IllegalArgumentException("OAuth' code cannot be null");
        }
        Object teamNameObj = connectionConfig.get("teamName");
        if (Checker.isEmpty(teamNameObj)) {
            if (tapdataConfig instanceof Map){
                Map<String, Object> config = (Map<String, Object>) tapdataConfig;
                teamNameObj = config.get("teamName");
            }
            if (Checker.isEmpty(teamNameObj)){
                throw new IllegalArgumentException("Team name cannot be null");
            }
        }
        Object clientIdObj = connectionConfig.get("clientId");
        if (Checker.isEmpty(clientIdObj)) {
            if (tapdataConfig instanceof Map){
                Map<String, Object> config = (Map<String, Object>) tapdataConfig;
                clientIdObj = config.get("clientId");
            }
            if (Checker.isEmpty(clientIdObj)){
                throw new IllegalArgumentException("App client_id cannot be null");
            }
        }
        Object clientSecretObj = connectionConfig.get("clientSecret");
        if (Checker.isEmpty(clientSecretObj)) {
            if (tapdataConfig instanceof Map){
                Map<String, Object> config = (Map<String, Object>) tapdataConfig;
                clientSecretObj = config.get("clientSecret");
            }
            if (Checker.isEmpty(clientSecretObj)){
                throw new IllegalArgumentException("App client_secret cannot be null");
            }
        }
        String clientId = (String) clientIdObj;
        String clientSecret = (String) clientSecretObj;
        String code = (String) codeObj;
        String teamName = (String) teamNameObj;
        String url = String.format("https://%s.coding.net/api/oauth/access_token?" +
                "client_id=%s" +
                "&code=%s" +
                "&client_secret=%s" +
                "&grant_type=authorization_code",
                teamName,
                clientId,
                code,
                clientSecret);
        HttpRequest request = HttpUtil.createPost(url);
        try {
            HttpResponse execute = request.execute();
            String body = execute.body();
            Map<String, Object> dataResult = (Map<String, Object>) fromJson(body);
            connectionConfig.put("token",dataResult.get("access_token"));
            connectionConfig.put("refreshToken",dataResult.get("refresh_token"));
            connectionConfig.put("teamName",dataResult.get("team"));
            connectionConfig.put("isOAuth",true);
            return new CommandResult().result(connectionConfig);
        }catch (Exception e){
            return new CommandResult().result(Collections.EMPTY_MAP);
        }
    }
}
