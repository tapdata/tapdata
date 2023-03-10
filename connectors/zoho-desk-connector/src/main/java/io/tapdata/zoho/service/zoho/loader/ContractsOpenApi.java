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

public class ContractsOpenApi extends ZoHoStarter implements ZoHoBase  {
    private static final String TAG = TicketCommentsOpenApi.class.getSimpleName();
    protected ContractsOpenApi(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static ContractsOpenApi create(TapConnectionContext tapConnectionContext){
        return new ContractsOpenApi(tapConnectionContext);
    }
    public static final String LIST_URL = "api/v1/contracts";
    public static final String GET_URL = "api/v1/contracts/{contract_id}";

    public static final int MAX_PAGE_LIMIT = 100;
    public static final int MIX_PAGE_LIMIT = 1;
    public static final int MIN_PAGE_FROM = 0;
    public static final int DEFAULT_PAGE_LIMIT = 50;
    public List<Map<String,Object>> page(Integer from, Integer limit,String sortBy){
        return page(from, limit,null,null,null,sortBy,null,null,null);
    }
    public List<Map<String,Object>> page(Integer from, Integer limit){
        return page(from, limit,null,null,null,null,null,null,null);
    }
    public List<Map<String,Object>> page(Integer from,Integer limit,String viewId,String departmentId,String accountId,String sortBy,String ownerId,String contractName,String include){
        HttpEntity<String,Object> form = HttpEntity.create();
        if (Checker.isNotEmpty(viewId)) form.build("viewId",viewId);
        if (Checker.isNotEmpty(departmentId)) form.build("departmentId",departmentId);
        if (Checker.isNotEmpty(accountId)) form.build("accountId",accountId);
        if (Checker.isNotEmpty(sortBy)) form.build("sortBy",sortBy);
        if (Checker.isNotEmpty(ownerId)) form.build("ownerId",ownerId);
        if (Checker.isNotEmpty(contractName)) form.build("contractName",contractName);
        if (Checker.isNotEmpty(include)) form.build("include",include);
        if (Checker.isEmpty(from) || from < MIN_PAGE_FROM) from = MIN_PAGE_FROM;
        if (Checker.isEmpty(limit) || limit < MIX_PAGE_LIMIT || limit > MAX_PAGE_LIMIT) limit = DEFAULT_PAGE_LIMIT;
        return page(form.build("from",from).build("limit",limit));
    }

    private List<Map<String,Object>> page(HttpEntity<String,Object> form){
        HttpEntity<String, String> header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,LIST_URL), HttpType.GET,header).header(header).form(form);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get Contracts page succeed.");
        Object data = ((Map<String,Object>)httpResult.getResult()).get("data");
        return Checker.isEmpty(data)?new ArrayList<>():(List<Map<String,Object>>)data;
    }

    public Map<String,Object> get(String contractId){
        HttpEntity<String, String> header = requestHeard();
        HttpEntity<String,String> resetFull = HttpEntity.create().build("contract_id",contractId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,GET_URL), HttpType.GET,header).header(header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get a Contract succeed.");
        Object result = httpResult.getResult();
        return Checker.isEmpty(result)? Collections.emptyMap():(Map<String, Object>) result;
    }
}
