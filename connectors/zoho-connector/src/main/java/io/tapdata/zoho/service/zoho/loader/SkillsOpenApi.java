package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SkillsOpenApi extends ZoHoStarter implements ZoHoBase  {

    private static final String TAG = SkillsOpenApi.class.getSimpleName();
    protected SkillsOpenApi(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static SkillsOpenApi create(TapConnectionContext tapConnectionContext){
        return new SkillsOpenApi(tapConnectionContext);
    }
    public static final String LIST_URL = "/api/v1/skills";

    public static final int MAX_PAGE_LIMIT = 100;
    public static final int MIX_PAGE_LIMIT = 1;
    public static final int MIN_PAGE_FROM = 0;
    public static final int DEFAULT_PAGE_LIMIT = 50;
    public List<Map<String,Object>> page(String departmentId, Integer from, Integer limit){
        return page(departmentId,from, limit,null,null,null);
    }
    public List<Map<String,Object>> page(String departmentId,Integer from,Integer limit,String status,String searchString,String skillTypeId){
        HttpEntity<String,Object> form = HttpEntity.create();
        if (Checker.isNotEmpty(departmentId)) {
            TapLogger.info(TAG,"When you request a Skill page ,the department id must be not null or empty.");
            throw new CoreException("When you request a Skill page ,the department id must be not null or empty.");
        }
        form.build("departmentId",departmentId);
        if (Checker.isNotEmpty(status)) form.build("status",status);
        if (Checker.isNotEmpty(searchString)) form.build("searchString",searchString);
        if (Checker.isNotEmpty(skillTypeId)) form.build("skillTypeId",skillTypeId);
        if (Checker.isEmpty(from) || from < MIN_PAGE_FROM) from = MIN_PAGE_FROM;
        if (Checker.isEmpty(limit) || limit < MIX_PAGE_LIMIT || limit > MAX_PAGE_LIMIT) limit = DEFAULT_PAGE_LIMIT;
        return page(form.build("from",from).build("limit",limit));
    }

    private List<Map<String,Object>> page(HttpEntity<String,Object> form){
        HttpEntity<String, String> header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,LIST_URL), HttpType.GET,header).header(header).form(form);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get product page succeed.");
        Object data = ((Map<String,Object>)httpResult.getResult()).get("data");
        return Checker.isEmpty(data)?new ArrayList<>():(List<Map<String,Object>>)data;
    }
}
