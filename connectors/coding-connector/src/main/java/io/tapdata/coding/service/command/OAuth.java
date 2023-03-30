package io.tapdata.coding.service.command;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.fromJson;


public class OAuth implements Command {
    static final String TAG = OAuth.class.getSimpleName();
    @Override
    public CommandResult commandResult(TapConnectionContext tapConnectionContext, CommandInfo commandInfo, AtomicReference<String> accessToken) {
        Map<String, Object> connectionConfig = commandInfo.getConnectionConfig();
        Object tapConfig = connectionConfig.get("__TAPDATA_CONFIG");
        if (Checker.isEmpty(connectionConfig)) {
            TapLogger.info(TAG,"ConnectionConfig cannot be null");
            return new CommandResult().result(connectionConfig);
        }
        Object codeObj = connectionConfig.get("code");
        if (Checker.isEmpty(codeObj)) {
            TapLogger.info(TAG,"OAuth' code cannot be null");
            return new CommandResult().result(connectionConfig);
        }
        Object teamNameObj = connectionConfig.get("teamName");
        if (Checker.isEmpty(teamNameObj)) {
            if (tapConfig instanceof Map){
                Map<?, ?> config = (Map<?, ?>) tapConfig;
                teamNameObj = config.get("teamName");
            }
            if (Checker.isEmpty(teamNameObj)){
                TapLogger.info(TAG,"Team name cannot be null");
            }
        }
        Object clientIdObj = connectionConfig.get("clientId");
        if (Checker.isEmpty(clientIdObj)) {
            if (tapConfig instanceof Map){
                Map<?, ?> config = (Map<?, ?>) tapConfig;
                clientIdObj = config.get("clientId");
            }
            if (Checker.isEmpty(clientIdObj)){
                TapLogger.info(TAG,"App client_id cannot be null");
                return new CommandResult().result(connectionConfig);
            }
        }
        Object clientSecretObj = connectionConfig.get("clientSecret");
        if (Checker.isEmpty(clientSecretObj)) {
            if (tapConfig instanceof Map){
                Map<?, ?> config = (Map<?, ?>) tapConfig;
                clientSecretObj = config.get("clientSecret");
            }
            if (Checker.isEmpty(clientSecretObj)){
                TapLogger.info(TAG,"App client_secret cannot be null");
                return new CommandResult().result(connectionConfig);
            }
        }
        String clientId = (String) clientIdObj;
        String clientSecret = (String) clientSecretObj;
        String code = (String) codeObj;
        String teamName = (String) Optional.ofNullable(teamNameObj).orElse("login");
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
            Map<?, ?> dataResult = fromJson(body, Map.class);
            connectionConfig.put("token",dataResult.get("access_token"));
            connectionConfig.put("refreshToken",dataResult.get("refresh_token"));
            connectionConfig.put("teamName",dataResult.get("team"));
            connectionConfig.put("isOAuth",true);
            return new CommandResult().result(connectionConfig);
        }catch (Exception e){
            return new CommandResult().result(connectionConfig);
        }
    }
}
