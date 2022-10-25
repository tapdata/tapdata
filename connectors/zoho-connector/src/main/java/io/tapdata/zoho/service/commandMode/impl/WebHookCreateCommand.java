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
    Map<String,Object> defaultSubscriptions = new HashMap<>();
    {
        defaultSubscriptions.put("Contact_Delete",null);
        defaultSubscriptions.put("Agent_Delete",null);
        defaultSubscriptions.put("Account_Delete",null);
        defaultSubscriptions.put("Agent_Add",null);
        defaultSubscriptions.put("Ticket_Delete",null);
        defaultSubscriptions.put("Ticket_Comment_Add",null);
        defaultSubscriptions.put("Ticket_Attachment_Update",null);
        defaultSubscriptions.put("Ticket_Add",null);
        defaultSubscriptions.put("Contact_Add",null);
        defaultSubscriptions.put("Department_Add",null);
        defaultSubscriptions.put("Ticket_Comment_Update",null);
        defaultSubscriptions.put("Ticket_Attachment_Add",null);
        defaultSubscriptions.put("Ticket_Attachment_Delete",null);
        defaultSubscriptions.put("Department_Update",map(entry("includePrevState",true)));
        defaultSubscriptions.put("Ticket_Update",map(entry("includePrevState",true)));
        defaultSubscriptions.put("Agent_Update",map(entry("includePrevState",true)));
        defaultSubscriptions.put("Contact_Update",map(entry("includePrevState",true)));
        defaultSubscriptions.put("Ticket_Thread_Add",map(entry("includePrevState",true)));
    }
    @Override
    public CommandResult command(TapConnectionContext connectionContext, CommandInfo commandInfo) {
        String language = commandInfo.getLocale();
        this.language(Checker.isEmpty(language)? LanguageEnum.EN.getLanguage():language);
        Map<String, Object> webHook = WebHookOpenApi.create(connectionContext).create(commandInfo.getArgMap());
        return this.commandResult(webHook);
    }

    @Override
    protected boolean checkerConfig(Map<String, Object> context) {
        /**
         *   "subscriptions": {
         *     "Contact_Delete": null,
         *     "Department_Update": {
         *       "includePrevState": true
         *     },
         *     "Agent_Delete": null,
         *     "Ticket_Update": {
         *       "includePrevState": true
         *     },
         *     "Account_Delete": null,
         *     "Contact_Update": {
         *       "includePrevState": true
         *     },
         *     "Agent_Update": {
         *       "includePrevState": true
         *     },
         *     "Agent_Add": null,
         *     "Ticket_Delete": null,
         *     "Ticket_Comment_Add": null,
         *     "Ticket_Attachment_Update": null,
         *     "Ticket_Add": null,
         *     "Contact_Add": null,
         *     "Department_Add": null,
         *     "Ticket_Comment_Update": null,
         *     "Ticket_Thread_Add": {
         *       "direction": "in"
         *     },
         *     "Ticket_Attachment_Add": null,
         *     "Ticket_Attachment_Delete": null
         *   },
         *   "url": "http://175.178.127.39/api/proxy/callback/zIh1SiyQScDto-Bf2TiXz7LPF6nEElZXW4W_z9xcW0Jk_7cmSYEVwB3sSlPtm0W2YA==",
         *   "includeEventsFrom": [
         *     "AUTOMATION"
         *   ],
         *   "name": "ZoHo8082",
         *   "description": ""
         * */
        Object subscriptions = context.get("subscriptions");

        Object url = context.get("url");
        return Checker.isNotEmpty(url);
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
