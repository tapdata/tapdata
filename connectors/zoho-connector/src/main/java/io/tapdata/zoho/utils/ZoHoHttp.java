package io.tapdata.zoho.utils;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpStatus;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpResult;
import io.tapdata.zoho.entity.HttpType;
import io.tapdata.zoho.enums.HttpCode;

import java.util.Collections;
import java.util.Map;

public class ZoHoHttp {
    private static final String TAG = ZoHoHttp.class.getSimpleName();
    private HttpEntity<String,String> heard;
    private HttpEntity<String,Object> body;
    private HttpEntity<String,Object> form;
    private HttpType httpType;
    private String url;
    private HttpEntity<String,String> resetFull;
    private String refreshToken;
    private final HttpResult EMPTY = HttpResult.create(HttpCode.ERROR,HttpEntity.create().build(HttpCode.ERROR.getCode(), HttpCode.EMPTY.getMessage()).entity());

    private ZoHoHttp(){}
    public static ZoHoHttp create(){
        return new ZoHoHttp();
    }
    public static ZoHoHttp create(String url,HttpType httpType){
        return new ZoHoHttp().url(url).httpType(httpType);
    }
    public static ZoHoHttp create(String url,HttpType httpType,HttpEntity<String,String> heard){
        return new ZoHoHttp().url(url).httpType(httpType).header(heard);
    }
    public static ZoHoHttp create(String url,HttpType httpType,HttpEntity<String,String> heard,HttpEntity<String,Object> body){
        return new ZoHoHttp().url(url).httpType(httpType).header(heard).body(body);
    }
    public ZoHoHttp body(String refreshToken){
        this.refreshToken = refreshToken;
        return this;
    }
    public ZoHoHttp body(HttpEntity<String,Object> body){
        this.body = body;
        return this;
    }
    public ZoHoHttp header(HttpEntity<String,String> header){
        this.heard = header;
        return this;
    }
    public ZoHoHttp url(String url){
        this.url = url;
        return this;
    }
    public ZoHoHttp httpType(HttpType httpType){
        this.httpType = httpType;
        return this;
    }
    public ZoHoHttp httpType(String httpType){
        this.httpType = HttpType.set(httpType);
        return this;
    }
    public ZoHoHttp resetFull(HttpEntity<String,String> resetFull){
        this.resetFull = resetFull;
        return this;
    }
    public ZoHoHttp form(HttpEntity<String,Object> form){
        this.form = form;
        return this;
    }

    private void beforeSend(){
        if (Checker.isEmpty(this.refreshToken)){
//            TapLogger.debug(TAG,"refresh_token is empty.");
        }
        if (Checker.isEmpty(this.httpType)){
            throw new RuntimeException("HTTP Method is not define Type :[POST | GET]");
        }
        if (Checker.isEmpty(this.url)){
            throw new RuntimeException("HTTP URL must be not null or not be empty.");
        }
        if (Checker.isNotEmpty(this.resetFull)){
            for (Map.Entry<String,String> entity : this.resetFull.entity().entrySet()){
                this.url = this.url.replaceAll("\\{"+entity.getKey()+"}",entity.getValue());
            }
        }
    }
    private HttpResult afterSend(HttpResponse execute){
        if (Checker.isEmpty(execute) ){
            return EMPTY;
        }
        String body = execute.body();
        if (Checker.isEmpty(body)){
            return EMPTY;
        }
        JSONObject executeObject = JSONUtil.parseObj(body);
        String executeResult = executeObject.getStr("errorCode");
        if (Checker.isNotEmpty(executeResult) || Checker.isNotEmpty(executeResult = executeObject.getStr("error"))) {
            HttpCode httpCode = HttpCode.code(executeResult);
            if (null == httpCode){
                return HttpResult.create(
                        HttpCode.ERROR,
                        HttpEntity.create()
                                .build(HttpCode.ERROR.getCode(), executeObject.get("message")).entity());
            }
            return HttpResult.create(
                    httpCode,
                    HttpEntity.create()
                            .build(httpCode.getCode(),httpCode.getMessage()).entity());
        }
        return HttpResult.create(HttpCode.SUCCEED,executeObject);
    }
//    public static void main(String[] args) {
//        HttpEntity<String,Object> form = HttpEntity.create()
//                .build("code","1000.ad37cdc0e742873d79755a3c0aa46cde.f8bd1ce9b2d63db913a0b09f78810e1e")
//                .build("client_id","1000.IIOULMMPS1V8C0YYNKI70TPI7EW2GX")
//                .build("client_secret","35854927862d2abda4c1ef31457048213ad5f95675")
//                .build("redirect_uri","https://www.zylker.com/oauthgrant")
//                .build("grant_type","authorization_code");
//        ZoHoHttp http = ZoHoHttp.create(String.format("https://accounts.zoho.com.cn/oauth/v2/token"), HttpType.GET).form(form);
//        //TapLogger.debug(TAG,"Try to get AccessToken and RefreshToken.");
//        HttpResult post = http.get();
//        System.out.println(post.getResult().get("asscessToken")+"\n"+post.getResult().get("refreshToken"));
//    }
    public HttpResult post(){
        this.beforeSend();
        HttpRequest request = HttpUtil.createPost(url);
        if (Checker.isNotEmpty(heard)){
            request.addHeaders(heard.entity());
        }
        if (Checker.isNotEmpty(form)){
            request.form(form.entity());
        }
        if (Checker.isNotEmpty(body)){
            request.body(JSONUtil.toJsonStr(body.entity()));
        }
        HttpResponse execute = null;
        try {
            execute = request.execute();
        }catch (Exception e){
//            TapLogger.info(TAG,"Http[POST] read timed out:{}",e.getMessage());
            return EMPTY;
        }

        return null == execute ?
                EMPTY:this.afterSend(execute);
    }
    public HttpResult get(){
        this.beforeSend();
        HttpRequest request = HttpUtil.createGet(url);
        if (Checker.isNotEmpty(heard)){
            request.addHeaders(heard.entity());
        }
        if (Checker.isNotEmpty(form)){
            request.form(form.entity());
        }
        HttpResponse execute = null;
        try {
            execute = request.execute();
        }catch (Exception e){
            TapLogger.info(TAG,"Http[Get] read timed out:{}",e.getMessage());
            return EMPTY;
        }
        return null == execute ?
                EMPTY:this.afterSend(execute);
    }
    public HttpEntity<String, String> getHeard() {
        return heard;
    }
    public HttpEntity<String, Object> getBody() {
        return body;
    }
    public HttpType getHttpType() {
        return httpType;
    }
    public String getUrl() {
        return url;
    }
    public HttpEntity<String, String> getResetFull() {
        return resetFull;
    }

//    public static void main(String[] args) {
//        HttpEntity<String,Object> form = HttpEntity.create()
//                .build("refresh_token","1000.a664d08e653ce402c62b609f7ab6051a.bf5b49d68b7fc8428561b304ef1f4874")
//                .build("client_id","1000.RXERF0BIW3RBP7NOJMK615YT9ATRFB")
//                .build("client_secret","2d5d8f1518a0232cfa33ff45b8ac9566d9c5344cc5")
//                .build("scope","Desk.tickets.ALL,Desk.contacts.READ,Desk.contacts.WRITE,Desk.contacts.UPDATE,Desk.contacts.CREATE,Desk.tasks.ALL,Desk.basic.READ,Desk.basic.CREATE,Desk.settings.ALL,Desk.events.ALL,Desk.articles.READ,Desk.articles.CREATE,Desk.articles.UPDATE,Desk.articles.DELETE")
//                .build("redirect_uri","https://www.zylker.com/oauthgrant")
//                .build("grant_type","refresh_token");
//        ZoHoHttp http = ZoHoHttp.create(String.format("https://accounts.zoho.com.cn%s","/oauth/v2/token"), HttpType.POST).form(form);
//        HttpResult post = http.post();
//        String code = post.getCode();
//        if (HttpCode.SUCCEED.getCode().equals(code)){
//            RefreshTokenEntity refreshTokenEntity = RefreshTokenEntity.create(post.getResult());
//            System.out.println(refreshTokenEntity.getAccessToken());
//        }else {
////            TapLogger.error(TAG,"{} | {}",code,post.getResult().get(HttpCode.ERROR.getCode()));
//            throw new RuntimeException(code+"|"+post.getResult().get(HttpCode.ERROR.getCode()));
//        }
//    }

    static class HttpAfter{
        HttpEntity<String,Object> after;
        HttpResponse execute;
        private HttpAfter( HttpResponse execute){
            this.execute = execute;
        }
        public static HttpAfter create(HttpResponse execute){
            return new HttpAfter(execute);
        }
        public Map<String,Object> after(){
            if (Checker.isEmpty(execute)){
                return Collections.emptyMap();
            }
            String body = execute.body();
            if (Checker.isEmpty(body)){
                return Collections.emptyMap();
            }
            JSONObject executeObject = JSONUtil.parseObj(body);
            String executeResult = executeObject.getStr("");
            if (Checker.isNotEmpty(executeResult) && HttpCode.INVALID_OAUTH.getCode().equals(executeResult)){
                TapLogger.debug(TAG,"{},start refresh token...",HttpCode.INVALID_OAUTH.getMessage());
                return null;
            }
            return executeObject;
        }
        public HttpEntity<String,Object> result(){
            return this.after;
        }
    }
    /**
     * Ticket detail
     * {
     *     "modifiedTime": "2022-09-23T08:59:08.000Z",
     *     "subCategory": null,
     *     "statusType": "Open",
     *     "subject": "GavinTest",
     *     "dueDate": "2022-09-23T08:10:26.000Z",
     *     "departmentId": "10504000000007057",
     *     "channel": "Email",
     *     "onholdTime": null,
     *     "language": "Chinese (Simplified)",
     *     "source": {
     *         "appName": null,
     *         "extId": null,
     *         "permalink": null,
     *         "type": "SYSTEM",
     *         "appPhotoURL": null
     *     },
     *     "resolution": null,
     *     "sharedDepartments": [],
     *     "closedTime": null,
     *     "approvalCount": "0",
     *     "isOverDue": true,
     *     "isTrashed": false,
     *     "contact": {
     *         "firstName": null,
     *         "lastName": "Gavin",
     *         "phone": "15770674965",
     *         "mobile": null,
     *         "id": "10504000000165005",
     *         "isSpam": false,
     *         "type": null,
     *         "email": "2749984520@qq.com",
     *         "account": null
     *     },
     *     "createdTime": "2022-09-23T02:10:27.000Z",
     *     "id": "10504000000165033",
     *     "isResponseOverdue": false,
     *     "customerResponseTime": "2022-09-23T02:10:26.000Z",
     *     "productId": null,
     *     "contactId": "10504000000165005",
     *     "threadCount": "1",
     *     "secondaryContacts": [],
     *     "priority": "High",
     *     "classification": "Problem",
     *     "commentCount": "1",
     *     "taskCount": "0",
     *     "accountId": null,
     *     "phone": "15770674965",
     *     "webUrl": "https://desk.zoho.com.cn/support/gavinhome/ShowHomePage.do#Cases/dv/10504000000165033",
     *     "assignee": {
     *         "photoURL": "https://desk.zoho.com.cn/api/v1/agents/10504000000083001/photo?orgId=41960353",
     *         "firstName": "",
     *         "lastName": "",
     *         "id": "10504000000083001",
     *         "email": "2749984520@qq.com"
     *     },
     *     "isSpam": false,
     *     "status": "Open",
     *     "entitySkills": [],
     *     "ticketNumber": "102",
     *     "sentiment": null,
     *     "customFields": {},
     *     "isArchived": false,
     *     "description": "<div style=\"font-size: 13px; font-family: Regular, Lato, Arial, Helvetica, sans-serif\"><img src=\"https://desk.zoho.com.cn:443/support/ImageDisplay?downloadType=uploadedFile&amp;fileName=1663898942936.png&amp;blockId=d4a8372ab5238046b67fd491ee52fd690ad7e16f5bb4b7e7&amp;zgId=7dd0ebf87c596be4dc91a236738b3d70&amp;mode=view\" style=\"padding: 0px; max-width: 100%; box-sizing: border-box\" />watch watch.you img is very big.<br /><div><br /></div></div>",
     *     "timeEntryCount": "0",
     *     "channelRelatedInfo": null,
     *     "responseDueDate": null,
     *     "isDeleted": false,
     *     "modifiedBy": "10504000000083001",
     *     "department": {
     *         "name": "gavinhome",
     *         "id": "10504000000007057"
     *     },
     *     "followerCount": "0",
     *     "email": "2749984520@qq.com",
     *     "layoutDetails": {
     *         "id": "10504000000018011",
     *         "layoutName": "gavinhome"
     *     },
     *     "channelCode": null,
     *     "product": null,
     *     "isFollowing": false,
     *     "cf": {},
     *     "slaId": "10504000000007503",
     *     "team": null,
     *     "layoutId": "10504000000018011",
     *     "assigneeId": "10504000000083001",
     *     "createdBy": "10504000000083001",
     *     "teamId": null,
     *     "tagCount": "0",
     *     "attachmentCount": "0",
     *     "isEscalated": false,
     *     "category": null
     * }
     * */
    /**
     * Ticket list
     * {
     *     "data": [
     *         {
     *             "id": "10504000000165033",
     *             "ticketNumber": "102",
     *             "layoutId": "10504000000018011",
     *             "email": "2749984520@qq.com",
     *             "phone": "15770674965",
     *             "subject": "GavinTest",
     *             "status": "Open",
     *             "statusType": "Open",
     *             "createdTime": "2022-09-23T02:10:27.000Z",
     *             "category": null,
     *             "language": "Chinese (Simplified)",
     *             "subCategory": null,
     *             "priority": "High",
     *             "channel": "Email",
     *             "dueDate": "2022-09-23T08:10:26.000Z",
     *             "responseDueDate": null,
     *             "commentCount": "1",
     *             "sentiment": null,
     *             "threadCount": "1",
     *             "closedTime": null,
     *             "onholdTime": null,
     *             "departmentId": "10504000000007057",
     *             "contactId": "10504000000165005",
     *             "productId": null,
     *             "assigneeId": "10504000000083001",
     *             "teamId": null,
     *             "department": {
     *                 "id": "10504000000007057",
     *                 "name": "gavinhome"
     *             },
     *             "contact": {
     *                 "firstName": null,
     *                 "lastName": "Gavin",
     *                 "email": "2749984520@qq.com",
     *                 "mobile": null,
     *                 "phone": "15770674965",
     *                 "type": null,
     *                 "id": "10504000000165005",
     *                 "account": null
     *             },
     *             "team": null,
     *             "assignee": {
     *                 "id": "10504000000083001",
     *                 "email": "2749984520@qq.com",
     *                 "photoURL": "https://desk.zoho.com.cn/api/v1/agents/10504000000083001/photo?orgId=41960353",
     *                 "firstName": "",
     *                 "lastName": ""
     *             },
     *             "webUrl": "https://desk.zoho.com.cn/support/gavinhome/ShowHomePage.do#Cases/dv/10504000000165033",
     *             "channelCode": null,
     *             "isRead": true,
     *             "lastThread": {
     *                 "channel": "EMAIL",
     *                 "isDraft": true,
     *                 "isForward": false,
     *                 "direction": "out"
     *             },
     *             "customerResponseTime": "2022-09-23T02:10:26.000Z",
     *             "isArchived": false,
     *             "isSpam": false,
     *             "source": {
     *                 "appName": null,
     *                 "appPhotoURL": null,
     *                 "permalink": null,
     *                 "type": "SYSTEM",
     *                 "extId": null
     *             }
     *         },
     *         {
     *             "id": "10504000000161077",
     *             "ticketNumber": "101",
     *             "layoutId": "10504000000018011",
     *             "email": "support@zohocorp.com.cn",
     *             "phone": "1 888 900 9646",
     *             "subject": "Here's your first ticket.",
     *             "status": "Open",
     *             "statusType": "Open",
     *             "createdTime": "2022-09-22T03:17:04.000Z",
     *             "category": null,
     *             "language": "Chinese (Traditional)",
     *             "subCategory": null,
     *             "priority": null,
     *             "channel": "Email",
     *             "dueDate": "2022-09-24T03:17:04.000Z",
     *             "responseDueDate": null,
     *             "commentCount": "0",
     *             "sentiment": null,
     *             "threadCount": "1",
     *             "closedTime": null,
     *             "onholdTime": null,
     *             "departmentId": "10504000000007057",
     *             "contactId": "10504000000161029",
     *             "productId": null,
     *             "assigneeId": "10504000000083001",
     *             "teamId": null,
     *             "department": {
     *                 "id": "10504000000007057",
     *                 "name": "gavinhome"
     *             },
     *             "contact": {
     *                 "firstName": null,
     *                 "lastName": "Lawrence",
     *                 "email": "support@zohocorp.com.cn",
     *                 "mobile": null,
     *                 "phone": "1 888 900 9646",
     *                 "type": null,
     *                 "id": "10504000000161029",
     *                 "account": {
     *                     "accountName": "Zoho",
     *                     "website": "https://www.zoho.com/",
     *                     "id": "10504000000161001"
     *                 }
     *             },
     *             "team": null,
     *             "assignee": {
     *                 "id": "10504000000083001",
     *                 "email": "2749984520@qq.com",
     *                 "photoURL": "https://desk.zoho.com.cn/api/v1/agents/10504000000083001/photo?orgId=41960353",
     *                 "firstName": "",
     *                 "lastName": ""
     *             },
     *             "webUrl": "https://desk.zoho.com.cn/support/gavinhome/ShowHomePage.do#Cases/dv/10504000000161077",
     *             "channelCode": null,
     *             "isRead": true,
     *             "lastThread": null,
     *             "customerResponseTime": "2022-09-22T03:17:03.000Z",
     *             "isArchived": false,
     *             "isSpam": false,
     *             "source": {
     *                 "appName": null,
     *                 "appPhotoURL": null,
     *                 "permalink": null,
     *                 "type": "SYSTEM",
     *                 "extId": null
     *             }
     *         }
     *     ]
     * }
     *
     * */
}
