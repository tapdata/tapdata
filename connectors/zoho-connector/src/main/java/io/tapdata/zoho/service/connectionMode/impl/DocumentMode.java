package io.tapdata.zoho.service.connectionMode.impl;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.impl.TicketLoader;
import io.tapdata.zoho.utils.Checker;

import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;

public class DocumentMode implements ConnectionMode {
    private static final String TAG = DocumentMode.class.getSimpleName();
    TapConnectionContext connectionContext;

    @Override
    public ConnectionMode config(TapConnectionContext connectionContext) {
        this.connectionContext = connectionContext;
        return this;
    }
    @Override
    public List<TapTable> discoverSchema(List<String> tables, int tableSize ) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    table("Tickets")
                            .add(field("id","Long").isPrimaryKey(true).primaryKeyPos(1))
                            .add(field("modifiedTime","DateTime"))
                            .add(field("description","Textarea"))
                            .add(field("subCategory","Map"))//Object
                            .add(field("statusType","StringMinor"))
                            .add(field("subject","StringNormal"))
                            .add(field("dueDate","DateTime"))
                            .add(field("departmentId","Long"))
                            .add(field("channel","StringMinor"))
                            .add(field("onholdTime","DateTime"))//Object
                            .add(field("language","StringMinor"))
                            .add(field("source","Map"))
                            .add(field("resolution","StringNormal"))//Object
                            .add(field("sharedDepartments","JAVA_Array"))
                            .add(field("closedTime","DateTime"))//Object
                            .add(field("approvalCount","Integer"))
                            .add(field("isOverDue","Boolean"))//Object
                            .add(field("isTrashed","Boolean"))//Boolean
                            .add(field("contact","Map"))
                            .add(field("createdTime","DateTime"))
                            .add(field("isResponseOverdue","Boolean"))//Object
                            .add(field("customerResponseTime","DateTime"))
                            .add(field("productId","Long"))//Object
                            .add(field("contactId","Long"))
                            .add(field("threadCount","Integer"))
                            .add(field("secondaryContacts","JAVA_Array"))
                            .add(field("priority","StringMinor"))
                            .add(field("classification","StringNormal"))
                            .add(field("commentCount","Integer"))
                            .add(field("taskCount","Integer"))
                            .add(field("accountId","Long"))//Object
                            .add(field("phone","Phone"))
                            .add(field("webUrl","URL"))
                            .add(field("assignee","Map"))
                            .add(field("isSpam","Boolean"))//Object
                            .add(field("status","StringMinor"))
                            .add(field("entitySkills","JAVA_Array"))
                            .add(field("ticketNumber","Integer"))
                            .add(field("sentiment","Map"))//Object
                            .add(field("customFields","Map"))
                            .add(field("isArchived","Boolean"))//Object
                            .add(field("Textarea","Textarea"))
                            .add(field("timeEntryCount","Integer"))
                            .add(field("channelRelatedInfo","Map"))//Object
                            .add(field("responseDueDate","Date"))//Object
                            .add(field("isDeleted","Boolean"))//Object
                            .add(field("modifiedBy","Long"))
                            .add(field("department","Map"))
                            .add(field("followerCount","Integer"))
                            .add(field("email","Email"))
                            .add(field("layoutDetails","Map"))
                            .add(field("channelCode","StringMinor"))//Object
                            .add(field("product","Map"))//Object
                            .add(field("isFollowing","Boolean"))//Object
                            .add(field("cf","Map"))
                            .add(field("slaId","Long"))
                            .add(field("team","Map"))//Object
                            .add(field("layoutId","Long"))
                            .add(field("assigneeId","Long"))
                            .add(field("createdBy","Long"))
                            .add(field("teamId","Long"))//Object
                            .add(field("tagCount","Integer"))
                            .add(field("attachmentCount","Integer"))
                            .add(field("isEscalated","Boolean"))//Object
                            .add(field("category","Map"))//Object
                            .add(field("department","Map"))
                            .add(field("contact","Map"))//Object
                            .add(field("assignee","Map"))//Object
            );
        }
        return null;
    }

    @Override
    public Map<String,Object> attributeAssignment(Map<String,Object> stringObjectMap) {
        Object ticketIdObj = stringObjectMap.get("id");
        if (Checker.isEmpty(ticketIdObj)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
        }
        Map<String, Object> ticketDetail = TicketLoader.create(connectionContext).getOne((String) ticketIdObj);
        ticketDetail.put("department",stringObjectMap.get("department"));
        ticketDetail.put("contact",stringObjectMap.get("contact"));
        ticketDetail.put("assignee",stringObjectMap.get("assignee"));
        this.removeJsonNull(ticketDetail);
        return ticketDetail;
    }
    /***
    public static void main(String[] args) {
        String json = ticket;
        final String start = "\n.add(field(";
        final String end = "\"))";
        JSONObject object = JSONUtil.parseObj(json);
        StringBuilder builder = new StringBuilder(start);
        object.forEach((key,value)->{
            builder.append("\"").append(key).append("\",\"");
            if (value instanceof String){
                try {
                    Integer.valueOf((String)value);
                    builder.append("Integer").append(end);
                }catch (Exception e){
                    try {
                        Long.valueOf((String)value);
                        builder.append("Long").append(end);
                    }catch (Exception e1){
                        builder.append("String").append(end);
                    }
                }
            }else if (value instanceof JSONObject || value instanceof Map){
                builder.append("Map").append(end);
            }else if (value instanceof JSONArray || value instanceof List){
                builder.append("JAVA_Array").append(end);
            }else {
                builder.append("Object").append(end);
            }
        });
        System.out.println(builder.toString());
    }
    public static final String ticket = "{\n" +
            "         \"modifiedTime\": \"2022-09-23T08:59:08.000Z\",\n" +
            "         \"subCategory\": null,\n" +
            "         \"statusType\": \"Open\",\n" +
            "         \"subject\": \"GavinTest\",\n" +
            "         \"dueDate\": \"2022-09-23T08:10:26.000Z\",\n" +
            "         \"departmentId\": \"10504000000007057\",\n" +
            "         \"channel\": \"Email\",\n" +
            "         \"onholdTime\": null,\n" +
            "         \"language\": \"Chinese (Simplified)\",\n" +
            "         \"source\": {\n" +
            "             \"appName\": null,\n" +
            "             \"extId\": null,\n" +
            "             \"permalink\": null,\n" +
            "             \"type\": \"SYSTEM\",\n" +
            "             \"appPhotoURL\": null\n" +
            "         },\n" +
            "         \"resolution\": null,\n" +
            "         \"sharedDepartments\": [],\n" +
            "         \"closedTime\": null,\n" +
            "         \"approvalCount\": \"0\",\n" +
            "         \"isOverDue\": true,\n" +
            "         \"isTrashed\": false,\n" +
            "         \"contact\": {\n" +
            "             \"firstName\": null,\n" +
            "             \"lastName\": \"Gavin\",\n" +
            "             \"phone\": \"15770674965\",\n" +
            "             \"mobile\": null,\n" +
            "             \"id\": \"10504000000165005\",\n" +
            "             \"isSpam\": false,\n" +
            "             \"type\": null,\n" +
            "             \"email\": \"2749984520@qq.com\",\n" +
            "             \"account\": null\n" +
            "         },\n" +
            "         \"createdTime\": \"2022-09-23T02:10:27.000Z\",\n" +
            "         \"id\": \"10504000000165033\",\n" +
            "         \"isResponseOverdue\": false,\n" +
            "         \"customerResponseTime\": \"2022-09-23T02:10:26.000Z\",\n" +
            "         \"productId\": null,\n" +
            "         \"contactId\": \"10504000000165005\",\n" +
            "         \"threadCount\": \"1\",\n" +
            "         \"secondaryContacts\": [],\n" +
            "         \"priority\": \"High\",\n" +
            "         \"classification\": \"Problem\",\n" +
            "         \"commentCount\": \"1\",\n" +
            "         \"taskCount\": \"0\",\n" +
            "         \"accountId\": null,\n" +
            "         \"phone\": \"15770674965\",\n" +
            "         \"webUrl\": \"https://desk.zoho.com.cn/support/gavinhome/ShowHomePage.do#Cases/dv/10504000000165033\",\n" +
            "         \"assignee\": {\n" +
            "             \"photoURL\": \"https://desk.zoho.com.cn/api/v1/agents/10504000000083001/photo?orgId=41960353\",\n" +
            "             \"firstName\": \"\",\n" +
            "             \"lastName\": \"\",\n" +
            "             \"id\": \"10504000000083001\",\n" +
            "             \"email\": \"2749984520@qq.com\"\n" +
            "         },\n" +
            "         \"isSpam\": false,\n" +
            "         \"status\": \"Open\",\n" +
            "         \"entitySkills\": [],\n" +
            "         \"ticketNumber\": \"102\",\n" +
            "         \"sentiment\": null,\n" +
            "         \"customFields\": {},\n" +
            "         \"isArchived\": false,\n" +
            "         \"description\": \"<div style=\\\"font-size: 13px; font-family: Regular, Lato, Arial, Helvetica, sans-serif\\\"><img src=\\\"https://desk.zoho.com.cn:443/support/ImageDisplay?downloadType=uploadedFile&amp;fileName=1663898942936.png&amp;blockId=d4a8372ab5238046b67fd491ee52fd690ad7e16f5bb4b7e7&amp;zgId=7dd0ebf87c596be4dc91a236738b3d70&amp;mode=view\\\" style=\\\"padding: 0px; max-width: 100%; box-sizing: border-box\\\" />watch watch.you img is very big.<br /><div><br /></div></div>\",\n" +
            "         \"timeEntryCount\": \"0\",\n" +
            "         \"channelRelatedInfo\": null,\n" +
            "         \"responseDueDate\": null,\n" +
            "         \"isDeleted\": false,\n" +
            "         \"modifiedBy\": \"10504000000083001\",\n" +
            "         \"department\": {\n" +
            "             \"name\": \"gavinhome\",\n" +
            "             \"id\": \"10504000000007057\"\n" +
            "         },\n" +
            "         \"followerCount\": \"0\",\n" +
            "         \"email\": \"2749984520@qq.com\",\n" +
            "         \"layoutDetails\": {\n" +
            "             \"id\": \"10504000000018011\",\n" +
            "             \"layoutName\": \"gavinhome\"\n" +
            "         },\n" +
            "         \"channelCode\": null,\n" +
            "         \"product\": null,\n" +
            "         \"isFollowing\": false,\n" +
            "         \"cf\": {},\n" +
            "         \"slaId\": \"10504000000007503\",\n" +
            "         \"team\": null,\n" +
            "         \"layoutId\": \"10504000000018011\",\n" +
            "         \"assigneeId\": \"10504000000083001\",\n" +
            "         \"createdBy\": \"10504000000083001\",\n" +
            "         \"teamId\": null,\n" +
            "         \"tagCount\": \"0\",\n" +
            "         \"attachmentCount\": \"0\",\n" +
            "         \"isEscalated\": false,\n" +
            "         \"category\": null\n" +
            "     }";
     ***/
}
