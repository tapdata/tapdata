package io.tapdata.zoho.service.commandMode.impl;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.zoho.annonation.LanguageEnum;
import io.tapdata.zoho.entity.CommandResultV2;
import io.tapdata.zoho.enums.HttpCode;
import io.tapdata.zoho.service.commandMode.CommandMode;
import io.tapdata.zoho.service.commandMode.ConfigContextChecker;
import io.tapdata.zoho.service.zoho.loader.WebHookOpenApi;
import io.tapdata.zoho.utils.Checker;

import java.util.HashMap;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

//command -> WebHookCreate
public class WebHookCreateCommand extends ConfigContextChecker<Object> implements CommandMode {
    @Override
    public CommandResult command(TapConnectionContext connectionContext, CommandInfo commandInfo) {
        String language = commandInfo.getLocale();
        this.language(Checker.isEmpty(language)? LanguageEnum.EN.getLanguage():language);
        Map<String, Object> webHook = WebHookOpenApi.create(connectionContext).create(commandInfo.getArgMap());
        return this.commandResult(webHook);
    }

    @Override
    protected boolean checkerConfig(Map<String, Object> context) {
        return false;
    }

    @Override
    protected CommandResultV2 commandResult(Object entity) {
        Map<String, Object> stringObjectHashMap = new HashMap<>();
//        stringObjectHashMap.put("accessToken", map(entry("data", token.accessToken())));
//        stringObjectHashMap.put("refreshToken",map(entry("data", token.refreshToken())));
//        stringObjectHashMap.put("getTokenMsg", map(entry("data", HttpCode.message(this.language,token.code()))));
        return CommandResultV2.create(map(entry("setValue",stringObjectHashMap)));
    }
}
