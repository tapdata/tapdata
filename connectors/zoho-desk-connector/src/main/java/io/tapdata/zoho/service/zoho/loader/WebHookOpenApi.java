package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class WebHookOpenApi extends ZoHoStarter implements ZoHoBase  {

    private static final String TAG = WebHookOpenApi.class.getSimpleName();
    public WebHookOpenApi(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static WebHookOpenApi create(TapConnectionContext tapConnectionContext){
        return new WebHookOpenApi(tapConnectionContext);
    }

    public static final String LIST_URL = "/api/v1/webhooks";
    public static final String CREATE_URL = "/api/v1/webhooks";
    public static final String GET_URL = "/api/v1/webhooks/{webhook_id}";
    public static final String DELETE_URL = "/api/v1/webhooks/{webhooks_id}";
    public static final String UPDATE_URL = "/api/v1/webhooks/{webhooks_id}";

    //list of WebHook is GET
    public List<Map<String,Object>> list(){
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,LIST_URL), HttpType.GET,requestHeard());
        HttpResult httpResult = this.readyAccessToken(http);
        Object data = ((Map<String,Object>)httpResult.getResult()).get("data");
        return Checker.isEmpty(data)?new ArrayList<>():(List<Map<String,Object>>)data;
    }

    //create of WebHook is POST
    public Map<String,Object> create(Map<String,Object> webHook){
        if (Checker.isEmpty(webHook)) return null;
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,CREATE_URL), HttpType.POST,requestHeard())
                .body(HttpEntity.create().addAll(webHook));
        Object data = this.readyAccessToken(http).getResult();
        return Checker.isEmpty(data)? Collections.emptyMap():((Map<String,Object>)data);
    }

    //get of WebHook is GET
    public Map<String,Object> get(String webHookId){
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,GET_URL), HttpType.GET,requestHeard())
                .resetFull(HttpEntity.create().build("webhook_id",webHookId));
        Object data = this.readyAccessToken(http).getResult();
        return Checker.isEmpty(data)? Collections.emptyMap():((Map<String,Object>)data);
    }

    //Delete of WebHook is DELETE
    public boolean delete(String webHookId){
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,DELETE_URL), HttpType.GET,requestHeard())
                .resetFull(HttpEntity.create().build("webhook_id",webHookId));
        HttpResult httpResult = this.readyAccessToken(http);
        Integer httpCode = httpResult.httpCode();
        return Checker.isEmpty(httpCode) && httpCode.equals(200);
    }

    //update of WebHook is PATCH
    public Map<String,Object> update(Map<String,Object> webHook){
        if (Checker.isEmpty(webHook)) return null;
        Object webHookId = webHook.get("id");
        if (Checker.isEmpty(webHookId)) return null;
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,UPDATE_URL), HttpType.POST,requestHeard())
                .resetFull(HttpEntity.create().build("webhook_id",String.valueOf(webHookId)))
                .body(HttpEntity.create().addAll(webHook));
        Object data = this.readyAccessToken(http).getResult();
        return Checker.isEmpty(data)? Collections.emptyMap():((Map<String,Object>)data);
    }
}
