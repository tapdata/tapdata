package io.tapdata.zoho.service.commandMode;

import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.zoho.entity.CommandResultV2;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.TokenEntity;
import io.tapdata.zoho.service.zoho.TokenLoader;
import io.tapdata.zoho.utils.Checker;

import java.util.HashMap;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

//command -> TokenCommand
public class TokenCommand extends ConfigContextChecker<TokenEntity> implements CommandMode {
    String clientID;
    String clientSecret;
    String generateCode;
    @Override
    public CommandResult command(TapConnectionContext connectionContext, CommandInfo commandInfo) {
        this.checkerConfig(commandInfo.getConnectionConfig());
        TokenEntity token = TokenLoader.create(connectionContext).getToken(this.clientID, this.clientSecret, this.generateCode);
        return this.command(token);
    }

    @Override
    protected boolean checkerConfig(Map<String, Object> context) {
        if (Checker.isEmpty(context) || context.isEmpty()){
            throw new CoreException("ConnectionConfig can not be null or not be empty.");
        }
        Object clientIDObj = context.get("clientID");
        Object clientSecretObj = context.get("clientSecret");
        Object generateCodeObj = context.get("generateCode");
        if (Checker.isEmpty(clientIDObj)){
            throw new CoreException("ClientID can not be null or not be empty.");
        }
        if (Checker.isEmpty(clientSecretObj)){
            throw new CoreException("ClientSecret can not be null or not be empty.");
        }
        if (Checker.isEmpty(generateCodeObj)){
            throw new CoreException("GenerateCode can not be null or not be empty.");
        }
        this.clientID = (String)clientIDObj;
        this.clientSecret = (String)clientSecretObj;
        this.generateCode = (String)generateCodeObj;
        return true;
    }

    @Override
    protected CommandResultV2 command(TokenEntity token) {
        Map<String, Object> stringObjectHashMap = new HashMap<>();
        stringObjectHashMap.put("accessToken", map(entry("data",token.accessToken())));
        stringObjectHashMap.put("refreshToken",map(entry("data",token.refreshToken())));
        stringObjectHashMap.put("getTokenMsg", map(entry("data",token.message())));
        return CommandResultV2.create(map(entry("setValue",stringObjectHashMap)));
//        Map<String, Object> stringObjectHashMap = new HashMap<>();
//        stringObjectHashMap.put("accessToken", map(entry("data",token.accessToken())));
//        stringObjectHashMap.put("refreshToken",map(entry("data",token.refreshToken()));
//        stringObjectHashMap.put("getTokenMsg", HttpEntity.create().build("data",token.message()));
//        CommandResultV2 commandResultV2 = CommandResultV2.create(new HashMap<String,Object>(){{put("setValue",stringObjectHashMap);}});
//        return commandResultV2;
    }
}
