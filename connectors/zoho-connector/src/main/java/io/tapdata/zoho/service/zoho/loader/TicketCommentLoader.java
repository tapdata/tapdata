package io.tapdata.zoho.service.zoho.loader;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoHttp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TicketCommentLoader extends ZoHoStarter implements ZoHoBase {
    private static final String TAG = TicketCommentLoader.class.getSimpleName();
    protected TicketCommentLoader(TapConnectionContext tapConnectionContext) {
        super(tapConnectionContext);
    }
    public static TicketCommentLoader create(TapConnectionContext tapConnectionContext){
        return new TicketCommentLoader(tapConnectionContext);
    }

    public static final String TICKET_ID_KEY = "ticket_id";
    public static final String COMMENT_ID_KEY = "comment_id";
    public static final String ADD_COMMENT_URL = "/api/v1/tickets/{"+TICKET_ID_KEY+"}/comments";
    public static final String UPDATE_COMMENT_URL = "/api/v1/tickets/{"+TICKET_ID_KEY+"}/comments/{"+COMMENT_ID_KEY+"}";
    public static final String DEL_COMMENT_URL = "/api/v1/tickets/{"+TICKET_ID_KEY+"}/comments/{"+COMMENT_ID_KEY+"}";
    public static final String GET_ONE_TICKET_COMMENT_URL = "/api/v1/tickets/{"+TICKET_ID_KEY+"}/comments/{"+COMMENT_ID_KEY+"}";
    public static final String LIST_TICKET_COMMENT_URL = "/api/v1/tickets/{"+TICKET_ID_KEY+"}/comments";
    public static final String ONE_TICKET_COMMENT_HISTORY_URL = "/api/v1/tickets/{"+TICKET_ID_KEY+"}/comments/{"+COMMENT_ID_KEY+"}/history";

    public Map<String,Object> addComment(String ticketId,Map<String,Object> commentBody){
        if (Checker.isEmpty(ticketId)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
        }
        HttpEntity<String, String> header = requestHeard();
        HttpEntity<String,Object> body = HttpEntity.create(commentBody);
        HttpEntity<String,String> resetFull = HttpEntity.create().build(TICKET_ID_KEY,ticketId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,ADD_COMMENT_URL), HttpType.POST,header)
                .body(body)
                .resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Create ticket comment succeed.");
        Map<String,Object> data = (Map<String,Object>)httpResult.getResult();
        return Checker.isEmpty(data)? new HashMap<>():data;
    }

    public Map<String,Object> updateComment(String ticketId,String commentId,Map<String,Object> commentBody){
        if (Checker.isEmpty(ticketId)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
        }
        if (Checker.isEmpty(commentId)){
            TapLogger.debug(TAG,"Comment Id can not be null or not be empty.");
        }
        HttpEntity<String, String> header = requestHeard();
        HttpEntity<String,Object> body = HttpEntity.create(commentBody);
        HttpEntity<String,String> resetFull = HttpEntity.create().build(TICKET_ID_KEY,ticketId).build(COMMENT_ID_KEY,commentId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,UPDATE_COMMENT_URL), HttpType.PATCH,header)
                .body(body)
                .resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Update ticket succeed.");
        Map<String,Object> data = (Map<String,Object>)httpResult.getResult();
        return Checker.isEmpty(data)? new HashMap<>():data;
    }

    public Integer delComment(String ticketId,String commentId){
        if (Checker.isEmpty(ticketId)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
        }
        if (Checker.isEmpty(commentId)){
            TapLogger.debug(TAG,"Comment Id can not be null or not be empty.");
        }
        HttpEntity<String, String> header = requestHeard();
        HttpEntity<String,String> resetFull = HttpEntity.create().build(TICKET_ID_KEY,ticketId).build(COMMENT_ID_KEY,commentId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,DEL_COMMENT_URL), HttpType.DELETE,header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Update ticket succeed.");
        Integer data = (Integer) httpResult.getResult();
        return data;
    }
    public Map<String,Object> getOneTicketComment(String ticketId,String commentId){
        if (Checker.isEmpty(ticketId)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
        }
        if (Checker.isEmpty(commentId)){
            TapLogger.debug(TAG,"Comment Id can not be null or not be empty.");
        }
        HttpEntity<String, String> header = requestHeard();
        HttpEntity<String,String> resetFull = HttpEntity.create().build(TICKET_ID_KEY,ticketId).build(COMMENT_ID_KEY,commentId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,GET_ONE_TICKET_COMMENT_URL), HttpType.GET,header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get ticket comment list succeed.");
        Map<String,Object> data = (Map<String,Object>)httpResult.getResult();
        return Checker.isEmpty(data)? new HashMap<>() : data;
    }
    public List<Map<String,Object>> listTicketComment(String ticketId){
        if (Checker.isEmpty(ticketId)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
        }
        HttpEntity<String, String> header = requestHeard();
        HttpEntity<String,String> resetFull = HttpEntity.create().build(TICKET_ID_KEY,ticketId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,LIST_TICKET_COMMENT_URL), HttpType.GET,header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get ticket comment list succeed.");
        Map<String,Object> data = (Map<String,Object>)httpResult.getResult();
        return Checker.isEmpty(data)?
                new ArrayList<>() : (
                        Checker.isEmpty(data.get("data")) ? new ArrayList<>(): (List<Map<String,Object>>)data.get("data"));
    }
    public List<Map<String,Object>> oneTicketCommentHistory(String ticketId,String commentId){
        if (Checker.isEmpty(ticketId)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
        }
        if (Checker.isEmpty(commentId)){
            TapLogger.debug(TAG,"Comment Id can not be null or not be empty.");
        }
        HttpEntity<String, String> header = requestHeard();
        HttpEntity<String,String> resetFull = HttpEntity.create().build(TICKET_ID_KEY,ticketId).build(COMMENT_ID_KEY,commentId);
        ZoHoHttp http = ZoHoHttp.create(String.format(ZO_HO_BASE_URL,ONE_TICKET_COMMENT_HISTORY_URL), HttpType.GET,header).resetFull(resetFull);
        HttpResult httpResult = this.readyAccessToken(http);
        TapLogger.debug(TAG,"Get ticket comment list succeed.");
        Map<String,Object> data = (Map<String,Object>)httpResult.getResult();
        return Checker.isEmpty(data)?
                new ArrayList<>() : (
                Checker.isEmpty(data.get("data")) ? new ArrayList<>(): (List<Map<String,Object>>)data.get("data"));
    }

    @Override
    public TapConnectionContext getContext() {
        return this.tapConnectionContext;
    }
}
