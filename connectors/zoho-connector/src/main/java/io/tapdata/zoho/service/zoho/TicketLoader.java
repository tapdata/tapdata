package io.tapdata.zoho.service.zoho;

import cn.hutool.core.util.NumberUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.zoho.entity.*;
import io.tapdata.zoho.enums.HttpCode;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

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
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,url), HttpType.POST,header).resetFull(resetFull);
        HttpResult httpResult = http.post();
        if (Checker.isEmpty(httpResult) ){
            TapLogger.debug(TAG,"Try to get ticket list , but AccessToken is.");
        }
        String code = httpResult.getCode();
        if (HttpCode.INVALID_OAUTH.getCode().equals(code)){
            //重新获取超时的AccessToken，并添加到stateMap
            String newAccessToken = this.refreshAndBackAccessToken();
            this.addNewAccessTokenToStateMap(newAccessToken);
            header.build("Authorization",newAccessToken);
            httpResult = http.get();
            if (Checker.isEmpty(httpResult) || Checker.isEmpty(httpResult.getResult()) || Checker.isEmpty(httpResult.getResult().get("data"))){
                throw new CoreException("Try to get ticket list , but faild.");
            }
        }
        TapLogger.debug(TAG,"Get ticket list succeed.");
        return (Map<String,Object>)httpResult.getResult().get("data");
    }

    /**
     * 获取工单列表
     *
     * */
    public List<Map<String,Object>> list(){
        String url = "/api/v1/tickets";
        ContextConfig contextConfig = this.veryContextConfigAndNodeConfig();
        String accessToken = contextConfig.getAccessToken();
        String orgId = contextConfig.getOrgId();
        HttpEntity<String,Object> form = HttpEntity.create().build("include","contacts,assignee,departments,team,isRead");
        HttpEntity<String,String> heards = HttpEntity.create().build("orgId",orgId).build("Authorization",accessToken);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,url), HttpType.GET,heards).header(heards).form(form);
        HttpResult httpResult = http.get();
        if (Checker.isEmpty(httpResult) ){
            TapLogger.debug(TAG,"Try to get ticket list , but AccessToken is.");
        }
        String code = httpResult.getCode();
        if (HttpCode.INVALID_OAUTH.getCode().equals(code)){
            //重新获取超时的AccessToken，并添加到stateMap
            String newAccessToken = this.refreshAndBackAccessToken();
            this.addNewAccessTokenToStateMap(newAccessToken);
            heards.build("Authorization",newAccessToken);
            httpResult = http.get();
            if (Checker.isEmpty(httpResult) || Checker.isEmpty(httpResult.getResult()) || Checker.isEmpty(httpResult.getResult().get("data"))){
                throw new CoreException("Try to get ticket list , but faild.");
            }
        }
        TapLogger.debug(TAG,"Get ticket list succeed.");
        return (List<Map<String,Object>>)httpResult.getResult().get("data");
    }

    /**
     * 获取全部工单数
     *
     * */
    public Integer count(){
        List<Map<String, Object>> list = this.list();
        if (Checker.isNotEmpty(list) && !list.isEmpty()){
            return list.size();
        }else {
            TapLogger.debug(TAG,"Try to get all ticketa count , but not result,count: {}.",0);
            return 0;
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

}
