package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.context.TapContext;
import io.tapdata.zoho.entity.*;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.toJson;

public class TicketLoader extends ZoHoStarter implements ZoHoBase {
    private static final String TAG = TicketLoader.class.getSimpleName();
    public TapConnectionContext getContext(){
        return this.tapConnectionContext;
    }

    private TicketLoader(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static TicketLoader create(TapConnectionContext tapConnectionContext){
        return new TicketLoader(tapConnectionContext);
    }

    public static final String GET_COUNT_URL = "/api/v1/ticketsCount";
    /**
     * 根据工单ID获取一个工单详情
     * */
    public Map<String,Object> getOne(String ticketId){
        if (Checker.isEmpty(ticketId)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
        }
        String url = "/api/v1/tickets/{ticketID}";
        HttpEntity<String, String> header = requestHeard();
        HttpEntity<String,String> resetFull = HttpEntity.create().build("ticketID",ticketId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,url), HttpType.GET,header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get ticket list succeed.");
        Map<String,Object> data = (Map<String,Object>)httpResult.getResult();
        return Checker.isEmpty(data)? new HashMap<>():data;
    }

    /**
     * 获取查询条件下的全部工单列表
     *
     * */
    public List<Map<String,Object>> list(HttpEntity<String,Object> form){
        String url = "/api/v1/tickets";
        HttpEntity<String, String> header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,url), HttpType.GET,header).header(header).form(form);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get ticket list succeed.");
        Object data = ((Map<String,Object>)httpResult.getResult()).get("data");
        return Checker.isEmpty(data)?new ArrayList<>():(List<Map<String,Object>>)data;
    }

    /**
     * 获取查询条件下的全部工单数
     *
     * */
    public Integer count(){
        HttpEntity<String, String> header = requestHeard();
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,GET_COUNT_URL), HttpType.GET,header)
                .header(header);
        try {
            HttpResult httpResult = this.readyAccessToken(http);
            TapLogger.debug(TAG,"Get ticket count succeed.");
            Object count = ((Map<String,Object>)httpResult.getResult()).get("count");
            return Checker.isEmpty(count) ?0:(int)count;
        } catch (Exception e){
            return 1;
        }
    }

    public static void fun(){
        String url = "/api/v1/tickets/{ticketID}";
        System.out.println(String.format(ZO_HO_BASE_URL,url));

        HttpEntity<String,String> header = HttpEntity.create()
                .build("Authorization","1");
        HttpEntity<String,String> resetFull = HttpEntity.create().build("ticketID","815");
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,url), HttpType.POST,header).resetFull(resetFull);
        http.post();
        System.out.println(http.getUrl());
    }

    public HttpEntity<String,Object> getTickPageParam(){

        HttpEntity<String,Object> form = HttpEntity.create();;//.build("include", "contacts,assignee,departments,team,isRead");
        if (Checker.isNotEmpty(this.tapConnectionContext) && this.tapConnectionContext instanceof TapConnectorContext) {
            TapConnectorContext connectorContext = (TapConnectorContext)this.tapConnectionContext;
            DataMap nodeConfig = connectorContext.getNodeConfig();
            //@TODO 构建查询条件
            return form.build("limit",100);
        }else {
            return form;
        }
    }
}
