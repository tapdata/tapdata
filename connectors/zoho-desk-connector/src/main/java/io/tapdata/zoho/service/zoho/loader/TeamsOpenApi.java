package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.entity.logger.TapLogger;
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

public class TeamsOpenApi extends ZoHoStarter implements ZoHoBase {
    private static final String TAG = TeamsOpenApi.class.getSimpleName();
    protected TeamsOpenApi(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static TeamsOpenApi create(TapConnectionContext tapConnectionContext){
        return new TeamsOpenApi(tapConnectionContext);
    }

    public static final String LIST_URL = "/api/v1/teams";
    public static final String GET_URL = "/api/v1/teams/{team_id}";

    public List<Map<String,Object>> page(){
        HttpEntity<String, String> header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,LIST_URL), HttpType.GET,header).header(header);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get all teams succeed.");
        Object data = ((Map<String,Object>)httpResult.getResult()).get("teams");
        return Checker.isEmpty(data)?new ArrayList<>():(List<Map<String,Object>>)data;
    }

    public Map<String,Object> get(String teamId){
        HttpEntity<String, String> header = requestHeard();
        HttpEntity<String,String> resetFull = HttpEntity.create().build("team_id",teamId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,GET_URL), HttpType.GET,header).header(header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get all teams succeed.");
        Object result = httpResult.getResult();
        return Checker.isEmpty(result)? Collections.emptyMap():(Map<String, Object>) result;
    }
}
