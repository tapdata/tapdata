package io.tapdata.zoho.service.commandMode;

import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.zoho.service.zoho.TokenLoader;
import io.tapdata.zoho.utils.Checker;

import java.util.Map;
//command -> TokenCommand
public class TokenCommand extends ConfigContextChecker implements CommandMode {
    String clientID;
    String clientSecret;
    String generateCode;
    @Override
    public CommandResult command(TapConnectionContext connectionContext, CommandInfo commandInfo) {
        this.checkerConfig(commandInfo.getConnectionConfig());
        return new CommandResult().result(TokenLoader.create(connectionContext)
                .getToken(this.clientID,this.clientSecret,this.generateCode)
                .map());
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
}
