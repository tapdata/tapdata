package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TicketCommentsOpenApi extends ZoHoStarter implements ZoHoBase {
    private static final String TAG = TicketCommentsOpenApi.class.getSimpleName();

    public static final String LIST_URL = "api/v1/tickets/{ticket_id}/comments";

    protected TicketCommentsOpenApi(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static TicketCommentsOpenApi create(TapConnectionContext tapConnectionContext){
        return new TicketCommentsOpenApi(tapConnectionContext);
    }

    public static final int MAX_PAGE_LIMIT = 100;
    public static final int MIX_PAGE_LIMIT = 1;
    public static final int MIN_PAGE_FROM = 0;
    public static final int DEFAULT_PAGE_LIMIT = 50;
    public List<Map<String,Object>> page(String ticketId,Integer from,Integer limit){
        return page(ticketId,from, limit,null,null);
    }
    public List<Map<String,Object>> page(String ticketId,Integer from,Integer limit,String sortBy,String include){
        HttpEntity<String,Object> form = HttpEntity.create();
        if (Checker.isNotEmpty(sortBy)) form.build("sortBy",sortBy);
        if (Checker.isNotEmpty(include)) form.build("include",include);
        if (Checker.isEmpty(from) || from < MIN_PAGE_FROM) from = MIN_PAGE_FROM;
        if (Checker.isEmpty(limit) || limit < MIX_PAGE_LIMIT || limit > MAX_PAGE_LIMIT) limit = DEFAULT_PAGE_LIMIT;
        return page(form.build("from",from).build("limit",limit),ticketId);
    }

    private List<Map<String,Object>> page(HttpEntity<String,Object> form,String ticketId){
        HttpEntity<String, String> header = requestHeard();
        HttpEntity<String,String> resetFull = HttpEntity.create().build("ticket_id",ticketId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,LIST_URL), HttpType.GET,header).header(header).resetFull(resetFull).form(form);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get product page succeed.");
        Object data = ((Map<String,Object>)httpResult.getResult()).get("data");
        return Checker.isEmpty(data)?new ArrayList<>():(List<Map<String,Object>>)data;
    }
}
