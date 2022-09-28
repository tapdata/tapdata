package io.tapdata.zoho.service.zoho;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.zoho.entity.*;
import io.tapdata.zoho.enums.HttpCode;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TicketLoader extends ZoHoStarter implements ZoHoBase {
    private static final String TAG = TicketLoader.class.getSimpleName();

    private TicketLoader(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static TicketLoader create(TapConnectionContext tapConnectionContext){
        return new TicketLoader(tapConnectionContext);
    }

    /**
     * 根据工单ID获取一个工单详情
     * */
    public Map<String,Object> getOne(String ticketId){
        if (Checker.isEmpty(ticketId)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
        }
        String url = "/api/v1/tickets/{ticketID}";
        String accessToken = this.accessTokenFromConfig();
        HttpEntity<String,String> header = HttpEntity.create()
                .build("Authorization",accessToken);
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
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        String accessToken = this.accessTokenFromConfig();
        HttpEntity<String,String> header = HttpEntity.create().build("Authorization",accessToken);
        String orgId = contextConfig.orgId();
        if (Checker.isNotEmpty(orgId)){
            header.build("orgId",orgId);
        }
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,url), HttpType.GET,header).header(header).form(form);
        HttpResult httpResult = this.readyAccessToken(http);
//        HttpResult httpResult = http.get();
//        if (Checker.isEmpty(httpResult) ){
//            TapLogger.debug(TAG,"Try to get ticket list , but AccessToken is.");
//        }
//        String code = httpResult.getCode();
//        if (HttpCode.INVALID_OAUTH.getCode().equals(code)){
//            //重新获取超时的AccessToken，并添加到stateMap
//            String newAccessToken = this.refreshAndBackAccessToken();
//            this.addNewAccessTokenToStateMap(newAccessToken);
//            header.build("Authorization",newAccessToken);
//            httpResult = http.get();
//            if (Checker.isEmpty(httpResult) || Checker.isEmpty(httpResult.getResult()) || Checker.isEmpty(httpResult.getResult().get("data"))){
//                throw new CoreException("Try to get ticket list , but faild.");
//            }
//        }
        TapLogger.debug(TAG,"Get ticket list succeed.");
        Object data = ((Map<String,Object>)httpResult.getResult()).get("data");
        return Checker.isEmpty(data)?new ArrayList<>():(List<Map<String,Object>>)data;
    }

    /**
     * 获取查询条件下的全部工单数
     *
     * */
    public Integer count(){
        int totalCount = 0;
        final int pageMaxSize = 100;
        int startPage = 0;
        String url = "/api/v1/tickets";
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        String accessToken = this.accessTokenFromConfig();
        String orgId = contextConfig.orgId();
        HttpEntity<String,Object> form = this.getTickPageParam().build("limit",pageMaxSize);
        HttpEntity<String,String> heards = HttpEntity.create().build("orgId",orgId).build("Authorization",accessToken);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,url), HttpType.GET,heards).header(heards).form(form);
        List<Map<String,Object>> list = new ArrayList<>();
        while (true){
            form.build("from",startPage);
            HttpResult httpResult = this.readyAccessToken(http);
            //HttpResult httpResult = http.get();
            //if (Checker.isEmpty(httpResult) ){
            //    TapLogger.debug(TAG,"Try to get ticket list , but AccessToken is.");
            //}
            //String code = httpResult.getCode();
            //if (HttpCode.INVALID_OAUTH.getCode().equals(code)){
            //    //重新获取超时的AccessToken，并添加到stateMap
            //    String newAccessToken = this.refreshAndBackAccessToken();
            //    this.addNewAccessTokenToStateMap(newAccessToken);
            //    heards.build("Authorization",newAccessToken);
            //    httpResult = http.get();
            //    if (Checker.isEmpty(httpResult) || Checker.isEmpty(httpResult.getResult()) || Checker.isEmpty(httpResult.getResult().get("data"))){
            //        throw new CoreException("Try to get ticket list , but faild.");
            //    }
            //}
            Object data = ((Map<String,Object>)httpResult.getResult()).get("data");
            list = Checker.isEmpty(data)?null:(List<Map<String,Object>>)data;
            int pageSizeBatch = 0;
            if (Checker.isEmpty(list) || list.isEmpty() || (pageSizeBatch = list.size()) < pageMaxSize){
                totalCount += pageSizeBatch;
                break;
            }
            totalCount += pageMaxSize;
            startPage += pageMaxSize;
        }
        return totalCount;
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
            return form
                    .build("limit",100)
                    .build("","");
        }else {
            return form;
        }
    }
}
