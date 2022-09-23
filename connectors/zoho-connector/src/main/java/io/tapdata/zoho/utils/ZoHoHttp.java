package io.tapdata.zoho.utils;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.Match;
import io.tapdata.entity.error.CoreException;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.entity.HttpType;

import java.util.Map;

public class ZoHoHttp {
    HttpEntity<String,String> heard;
    HttpEntity<String,Object> body;
    HttpType httpType;
    String url;
    HttpEntity<String,String> resetFull;

    private ZoHoHttp(){}
    public static ZoHoHttp create(){
        return new ZoHoHttp();
    }
    public static ZoHoHttp create(String url,HttpType httpType,HttpEntity<String,String> heard){
        return new ZoHoHttp().url(url).httpType(httpType).header(heard);
    }
    public static ZoHoHttp create(String url,String httpType,HttpEntity<String,String> heard){
        return new ZoHoHttp().url(url).httpType(httpType).header(heard);
    }
    public static ZoHoHttp create(String url,String httpType,HttpEntity<String,String> heard,HttpEntity<String,Object> body){
        return new ZoHoHttp().url(url).httpType(httpType).header(heard).body(body);
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

    private void beforeSend(){
        Checker checker = Checker.create();
        if (checker.isEmpty(this.httpType)){
            throw new RuntimeException("HTTP Method is not define Type :[POST | GET]");
        }
        if (checker.isEmpty(this.url)){
            throw new RuntimeException("HTTP URL must be not null or not be empty.");
        }
        if (checker.isNotEmpty(this.resetFull)){
            for (Map.Entry<String,String> entity : this.resetFull.entity().entrySet()){
                int start = this.url.indexOf(entity.getKey());
                int end = start + entity.getKey().length();
                this.url.replaceAll("{"+entity.getKey()+"}",entity.getValue());
            }
        }
    }


    public Map<String,Object> post(){
        this.beforeSend();

        return null;
    }

    public Map<String,Object> get(){
        this.beforeSend();

        return null;
    }

    public static void main(String[] args) {
        StringBuilder resetParam = new StringBuilder();
        resetParam.append("x").append("j").append("h");
        System.out.println(resetParam.toString());
        //resetParam.delete(0,resetParam.length());
        System.out.println(resetParam.toString());
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
