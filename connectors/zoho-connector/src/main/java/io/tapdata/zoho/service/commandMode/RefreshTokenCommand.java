package io.tapdata.zoho.service.commandMode;

import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.zoho.service.zoho.TokenLoader;
import io.tapdata.zoho.utils.Checker;

import java.util.Map;

//command -> RefreshTokenCommand
public class RefreshTokenCommand extends ConfigContextChecker implements CommandMode {
    String clientID;
    String clientSecret;
    String refreshToken;
    @Override
    public CommandResult command(TapConnectionContext connectionContext, CommandInfo commandInfo) {
        this.checkerConfig(commandInfo.getConnectionConfig());
        return new CommandResult().result(TokenLoader.create(connectionContext)
                .refreshToken(this.refreshToken,this.clientID,this.clientSecret)
                .map());
    }

    @Override
    protected boolean checkerConfig(Map<String, Object> context) {
        if (Checker.isEmpty(context) || context.isEmpty()){
            throw new CoreException("ConnectionConfig can not be null or not be empty.");
        }
        Object clientIDObj = context.get("clientID");
        Object clientSecretObj = context.get("clientSecret");
        Object refreshTokenObj = context.get("refreshToken");
        if (Checker.isEmpty(clientIDObj)){
            throw new CoreException("ClientID can not be null or not be empty.");
        }
        if (Checker.isEmpty(clientSecretObj)){
            throw new CoreException("ClientSecret can not be null or not be empty.");
        }
        if (Checker.isEmpty(refreshTokenObj)){
            throw new CoreException("RefreshToken can not be null or not be empty.");
        }
        this.clientID = (String)clientIDObj;
        this.clientSecret = (String)clientSecretObj;
        this.refreshToken = (String)refreshTokenObj;
        return true;
    }
}
