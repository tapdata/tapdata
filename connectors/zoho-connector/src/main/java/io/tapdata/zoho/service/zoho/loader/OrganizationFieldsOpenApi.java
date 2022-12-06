package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.enums.ModuleEnums;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrganizationFieldsOpenApi extends ZoHoStarter implements ZoHoBase  {
    public static final String GET_ORGANIZATION_FIELDS_URL = "/api/v1/organizationFields";
    public static final String GET_FIELDS_COUNT_URL = "/api/v1/customFieldCount";
    public static final String GET_FIELD_URL = "/api/v1/organizationFields/{field_id}";
    protected OrganizationFieldsOpenApi(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static OrganizationFieldsOpenApi create(TapConnectionContext tapConnectionContext){
        return new OrganizationFieldsOpenApi(tapConnectionContext);
    }

    private static final String TAG = OrganizationFieldsOpenApi.class.getSimpleName();
    public TapConnectionContext getContext(){
        return this.tapConnectionContext;
    }

    public Map<String,Object> get(String fieldId){
        if (Checker.isEmpty(fieldId)){
            TapLogger.debug(TAG,"Department Id can not be null or not be empty.");
        }
        HttpEntity<String, String> header = requestHeard();
        HttpEntity<String,String> resetFull = HttpEntity.create().build("field_id",fieldId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,GET_FIELD_URL), HttpType.GET,header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get organization field list succeed.");
        Map<String,Object> data = (Map<String,Object>)httpResult.getResult();
        return Checker.isEmpty(data)? new HashMap<>():data;
    }

    public List<Map<String,Object>> list(ModuleEnums module, String apiNames, Long departmentId){
        if (Checker.isEmpty(module)) return new ArrayList<>();
        HttpEntity<String,Object> form = HttpEntity.create().build("module",module.getName());
        if (Checker.isNotEmpty(apiNames)){
            form.build("apiNames",apiNames);
        }
        if (Checker.isNotEmpty(departmentId)){
            form.build("departmentId",departmentId);
        }
        return list(form);
    }
    private List<Map<String,Object>> list(HttpEntity<String,Object> form){
        HttpEntity<String, String> header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,GET_ORGANIZATION_FIELDS_URL), HttpType.GET,header).header(header).form(form);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get organization fields succeed.");
        Object data = ((Map<String,Object>)httpResult.getResult()).get("data");
        return Checker.isEmpty(data)?new ArrayList<>():(List<Map<String,Object>>)data;
    }
    public int count(ModuleEnums module){
        HttpEntity<String, String> header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,GET_FIELDS_COUNT_URL), HttpType.GET,header)
                .header(header)
                .form(HttpEntity.create().build("module",module.getName()));
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get organization fields succeed.");
        Object data = ((Map<String,Object>)httpResult.getResult()).get("data");
        Object count = null;
        return Checker.isEmpty(data)|| Checker.isEmpty(count = ( (Map<String,Object>) data).get("totalAvailableCount")) ?0:(int)count;
    }
}
