package io.tapdata.zoho.service.commandMode.impl;

import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.zoho.annonation.LanguageEnum;
import io.tapdata.zoho.entity.CommandResultV2;
import io.tapdata.zoho.entity.RefreshTokenEntity;
import io.tapdata.zoho.enums.HttpCode;
import io.tapdata.zoho.service.commandMode.CommandMode;
import io.tapdata.zoho.service.commandMode.ConfigContextChecker;
import io.tapdata.zoho.service.zoho.loader.TokenLoader;
import io.tapdata.zoho.utils.Checker;

import java.util.HashMap;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

//command -> RefreshTokenCommand
public class RefreshTokenCommand extends ConfigContextChecker<RefreshTokenEntity> implements CommandMode {
    String clientID;
    String clientSecret;
    String refreshToken;
    @Override
    public CommandResult command(TapConnectionContext connectionContext, CommandInfo commandInfo) {
        this.checkerConfig(commandInfo.getConnectionConfig());
        String language = commandInfo.getLocale();
        this.language(Checker.isEmpty(language)? LanguageEnum.EN.getLanguage():language);
        RefreshTokenEntity refreshTokenEntity = TokenLoader.create(connectionContext)
                .refreshToken(this.refreshToken, this.clientID, this.clientSecret);
        return this.commandResult(refreshTokenEntity);
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

    @Override
    protected CommandResultV2 commandResult(RefreshTokenEntity entity) {
        Map<String, Object> stringObjectHashMap = new HashMap<>();
        stringObjectHashMap.put("accessToken", map(entry("data",entity.accessToken())));
        stringObjectHashMap.put("getRefreshMsg", map(entry("data", HttpCode.message(this.language,entity.code()))));
        return CommandResultV2.create(map(entry("setValue",stringObjectHashMap)));
    }
}
