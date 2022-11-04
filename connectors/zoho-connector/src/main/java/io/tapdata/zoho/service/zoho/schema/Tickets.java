package io.tapdata.zoho.service.zoho.schema;

import cn.hutool.json.JSONObject;
import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.enums.Constants;
import io.tapdata.zoho.enums.FieldModelType;
import io.tapdata.zoho.service.zoho.loader.OrganizationFieldLoader;
import io.tapdata.zoho.service.zoho.loader.TicketLoader;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.MapUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.list;
import static io.tapdata.base.ConnectorBase.table;
import static io.tapdata.entity.simplify.TapSimplify.field;

/**
 * 工单表 ，Schema 的 工单策略
 * */
public class Tickets implements Schema {
    private static final String TAG = Tickets.class.getSimpleName();

    private TicketLoader ticketLoader;
    @Override
    public Schema config(ZoHoBase openApi) {
        if (Checker.isNotEmpty(openApi) && openApi instanceof TicketLoader) ticketLoader = (TicketLoader)openApi;
        return this;
    }

    @Override
    public String schemaName() {
        return Schemas.Tickets.getTableName();
    }

    @Override
    public List<TapTable> document(List<String> tables, int tableSize) {
        if(tables == null || tables.isEmpty()) {
            return list(
                    /** customFields  department product*/
                    // sentiment     channelRelatedInfo  layoutDetails   team category
                    // subCategory   source    sharedDepartments   contact  secondaryContacts assignee   entitySkills
                    table(Schemas.Tickets.getTableName())
                            .add(field("id","StringMinor").isPrimaryKey(true).primaryKeyPos(1))
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
                            .add(field("category","StringNormal"))//Object
                            .add(field("contact","Map"))//Object
            );
        }
        return null;
    }

    @Override
    public List<TapTable> csv(List<String> tables, int tableSize, TapConnectionContext connectionContext) {
        if(tables == null || tables.isEmpty()) {
            TapTable tapTable = table(Schemas.Tickets.getTableName())
                    .add(ConnectorBase.field("id","Long").isPrimaryKey(true).primaryKeyPos(1))    //"Ticket Reference Id"
                    .add(ConnectorBase.field("ticketNumber","Long"))
                    .add(ConnectorBase.field("departmentName","StringNormal"))    //"Department"     department（需要从分页中需添加到详情） ->  department.name（掉方法打平）
                    .add(ConnectorBase.field("departmentId","Long"))
                    .add(ConnectorBase.field("contactAccountAccountName","StringNormal"))//contact      contact（需要从分页中需添加到详情）->contact.account.accountName（掉方法打平）
                    .add(ConnectorBase.field("contactAccountId","Long"))//contactID      contact（需要从分页中需添加到详情）->contact.account.id（掉方法打平）
                    .add(ConnectorBase.field("contactLastName","StringNormal"))//联系人名称      contact（需要从分页中需添加到详情）->contact.lastName（掉方法打平）
                    .add(ConnectorBase.field("contactId","Long"))//联系人名称 Id      contact（需要从分页中需添加到详情）->contact.id（掉方法打平）
                    .add(ConnectorBase.field("email","Email"))
                    .add(ConnectorBase.field("phone","Phone"))
                    .add(ConnectorBase.field("subject","StringNormal"))
                    .add(ConnectorBase.field("description","Textarea"))
                    .add(ConnectorBase.field("status","StringMinor"))
                    .add(ConnectorBase.field("productName","StringNormal"))//产品名称
                    .add(ConnectorBase.field("productId","Long"))//产品名称 Id
                    .add(ConnectorBase.field("assigneeName","StringNormal"))//工单所有者   assignee（需要从分页中需添加到详情）->assignee.lastName（掉方法打平）
                    .add(ConnectorBase.field("assigneeId","Long"))//工单所有者 Id    assignee（需要从分页中需添加到详情）->assignee.id（掉方法打平）
                    .add(ConnectorBase.field("createdByName","StringNormal"))
                    .add(ConnectorBase.field("createdBy","Long"))
                    .add(ConnectorBase.field("modifiedByName","StringNormal"))
                    .add(ConnectorBase.field("modifiedBy","Long"))
                    .add(ConnectorBase.field("createdTime","DateTime"))
                    .add(ConnectorBase.field("modifiedTime","DateTime"))
                    .add(ConnectorBase.field("closedTime","DateTime"))
                    .add(ConnectorBase.field("dueDate","DateTime"))
                    .add(ConnectorBase.field("priority","StringMinor"))
                    .add(ConnectorBase.field("channel","StringMinor"))//Model
                    .add(ConnectorBase.field("isOverDue","Boolean"))
                    .add(ConnectorBase.field("isEscalated","Boolean"))
                    .add(ConnectorBase.field("classification","StringNormal"))
                    .add(ConnectorBase.field("resolution","StringNormal"))//object
                    .add(ConnectorBase.field("category","StringNormal"))//object
                    .add(ConnectorBase.field("subCategory","StringNormal"))//object
                    .add(ConnectorBase.field("customerResponseTime","DateTime"))
                    .add(ConnectorBase.field("teamId","Long"))
                    .add(ConnectorBase.field("teamName","StringNormal"))//object工单所有者 Id    team（需要从分页中需添加到详情）->team.name  （掉方法打平）
                    .add(ConnectorBase.field("tags","StringNormal")) //@TODO
                    .add(ConnectorBase.field("language","StringMinor"))
                    .add(ConnectorBase.field("timeEntryCount","Integer"))//@TODO 工单搁置时间

//                    .add(field("statusType","String"))
//                    .add(field("onholdTime","Object"))
//                    .add(field("source","Map"))
//                    .add(field("sharedDepartments","JAVA_Array"))
//                    .add(field("approvalCount","Integer"))
//                    .add(field("isTrashed","Boolean"))
//                    .add(field("isResponseOverdue","Boolean"))
//                    .add(field("contactId","Long"))
//                    .add(field("threadCount","Integer"))
//                    .add(field("secondaryContacts","JAVA_Array"))
//                    .add(field("commentCount","Integer"))
//                    .add(field("taskCount","Integer"))
//                    .add(field("accountId","Long"))
//                    .add(field("webUrl","String"))
//                    .add(field("isSpam","Boolean"))
//                    .add(field("entitySkills","JAVA_Array"))
//                    .add(field("sentiment","Object"))
//                    .add(field("customFields","Map"))
//                    .add(field("isArchived","Boolean"))
//                    .add(field("channelRelatedInfo","Object"))
//                    .add(field("responseDueDate","Object"))
//                    .add(field("isDeleted","Boolean"))
//                    .add(field("modifiedBy","Long"))
//                    .add(field("followerCount","Integer"))
//                    .add(field("layoutDetails","Map"))
//                    .add(field("channelCode","Object"))
//                    .add(field("isFollowing","Boolean"))
//                    .add(field("cf","Map"))
//                    .add(field("slaId","Long"))
//                    .add(field("layoutId","Long"))
//                    .add(field("assigneeId","Long"))
//                    .add(field("createdBy","Long"))
//                    .add(field("tagCount","Integer"))
//                    .add(field("attachmentCount","Integer"))

                    ;
            // 查询自定义属性列表
            Map<String, Map<String, Object>> customFieldMap = OrganizationFieldLoader.create(connectionContext)
                    .customFieldMap(FieldModelType.TICKETS);
            //accessToken过期 @TODO
            if (Checker.isEmpty(customFieldMap) || customFieldMap.isEmpty()){
                throw new CoreException("Get custom fields error.");
            }
            customFieldMap.forEach((fieldName,field)->{
                if (null != field && field instanceof JSONObject) {
                    Object filedName = field.get("Name");
                    if (Checker.isNotEmpty(filedName)) {
                        //@TODO 根据type属性匹配对应tapdata类型
                        Object fieldTypeObj = field.get("type");
                        tapTable.add(ConnectorBase.field(
                                Constants.CUSTOM_FIELD_SUFFIX + filedName,
                                Checker.isEmpty(fieldTypeObj)?"NULL":(String)fieldTypeObj));
                        //CustomFieldType.type(Checker.isEmpty(fieldTypeObj)?null:String.valueOf(fieldTypeObj))));
                    }
                }
            });
            return list(tapTable);
        }
        return null;
    }

//    @Override
//    public Map<String, Object> attributeAssignmentDocument(Map<String, Object> obj, TapConnectionContext connectionContext) {
//        Object ticketIdObj = obj.get("id");
//        if (Checker.isEmpty(ticketIdObj)){
//            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
//        }
//        if (Checker.isEmpty(ticketLoader)){
//            ticketLoader = TicketLoader.create(connectionContext);
//        }
//        Map<String, Object> ticketDetail = ticketLoader.getOne((String) ticketIdObj);
//        ticketDetail.put("department",obj.get("department"));
//        ticketDetail.put("contact",obj.get("contact"));
//        ticketDetail.put("assignee",obj.get("assignee"));
//        return this.attributeAssignmentSelfDocument(ticketDetail);
//    }

    @Override
    public Map<String, Object> getDetail(Map<String, Object> obj, TapConnectionContext connectionContext) {
        Object ticketIdObj = obj.get("id");
        if (Checker.isEmpty(ticketIdObj)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
        }
        if (Checker.isEmpty(ticketLoader)){
            ticketLoader = TicketLoader.create(connectionContext);
        }
        Map<String, Object> ticketDetail = ticketLoader.getOne((String) ticketIdObj);
        if (Checker.isEmpty(ticketDetail)) ticketDetail = new HashMap<>();
        ticketDetail.put("department",obj.get("department"));
        ticketDetail.put("contact",obj.get("contact"));
        ticketDetail.put("assignee",obj.get("assignee"));
        return ticketDetail;
    }

    @Override
    public Map<String, Object> attributeAssignmentSelfCsv(Map<String, Object> obj, ContextConfig contextConfig) {
        Map<String,Object> ticketCSVDetail = new HashMap<>();
        //加入自定义属性
        Map<String, Object> customFieldMap = this.setCustomField(obj);
        if (Checker.isNotEmpty(customFieldMap)){
            ticketCSVDetail.putAll(customFieldMap);
        }
        //@TODO 统计需要操作的属性,子属性用点分割；数据组结果会以|分隔返回，大文本会以""包裹
        MapUtil.putMapSplitByDotKeyNameFirstUpper(obj, ticketCSVDetail,
                "id",    //"Ticket Reference Id"
                "ticketNumber",
                "department.name",//"Department"     department（需要从分页中需添加到详情） ->  department.name（掉方法打平）
                "departmentId",
                "contact.account.accountName",//contact      contact（需要从分页中需添加到详情）->contact.account.accountName（掉方法打平）
                "contact.account.id",//contactID      contact（需要从分页中需添加到详情）->contact.account.id（掉方法打平）
                "contact.lastName",//联系人名称      contact（需要从分页中需添加到详情）->contact.lastName（掉方法打平）
                "contact.id",//联系人名称 Id      contact（需要从分页中需添加到详情）->contact.id（掉方法打平）
                "email",
                "phone",
                "subject",
                "description",
                "status",
                "productName",//产品名称
                "productId",//产品名称 Id
                "assignee.lastName",//工单所有者   assignee（需要从分页中需添加到详情）->assignee.lastName（掉方法打平）
                "assignee.id",//工单所有者 Id    assignee（需要从分页中需添加到详情）->assignee.id（掉方法打平）
                "createdByName",
                "createdBy",
                "modifiedByName",
                "modifiedBy",
                "createdTime",
                "modifiedTime",
                "closedTime",
                "dueDate",
                "priority",
                "channel",
                "isOverDue",
                "isEscalated",
                "classification",
                "resolution",
                "category",
                "subCategory",
                "customerResponseTime",
                "teamId",
                "teamName",//工单所有者 Id    team（需要从分页中需添加到详情）->team.name  （掉方法打平）
                "tags", //@TODO
                "language",
                "timeEntryCount"//@TODO 工单搁置时间
        );
        this.removeJsonNull(ticketCSVDetail);
        return ticketCSVDetail;
    }

//    @Override
//    public Map<String, Object> attributeAssignmentCsv(Map<String, Object> obj, TapConnectionContext connectionContext, ContextConfig contextConfig) {
//        Object ticketIdObj = obj.get("id");
//        if (Checker.isEmpty(ticketIdObj)){
//            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
//            return null;
//        }
//        if (Checker.isEmpty(ticketLoader)){
//            ticketLoader = TicketLoader.create(connectionContext);
//        }
//        Map<String, Object> ticketDetail = ticketLoader.getOne((String) ticketIdObj);
//        //把分页结果中具有但详情结果中不具有切CSV结构数据需要的结构进行提取
//        ticketDetail.put("department",obj.get("department"));
//        ticketDetail.put("contact",obj.get("contact"));
//        ticketDetail.put("assignee",obj.get("assignee"));
//        return this.attributeAssignmentSelfCsv(ticketDetail,contextConfig);
//    }

    private Map<String,Object> setCustomField(Map<String,Object> stringObjectMap){
        Map<String, Object> result = new HashMap<>();
        Object customFieldsObj = stringObjectMap.get("cf");
        if (null == customFieldsObj){
            return result;
        }
        Map<String,Object> customFields = (Map<String,Object>)customFieldsObj;
        if (null == customFields || customFields.size()<=0){
            return result;
        }
        customFields.forEach(result::put);
        /**
         customFields.forEach((field,value)->{
         String customField = Constants.CUSTOM_FIELD_SUFFIX + field;

         if (Checker.isNotEmpty(value)) {
         Object jsonObject = null;
         try {
         jsonObject = JSONUtil.parseObj(valueString.toString());
         Object nameObj = ((Map<String,Object>)jsonObject).get("Name");
         if (Checker.isNotEmpty(nameObj)){
         result.put(customField, nameObj);
         }else {
         result.put(customField, jsonObject);
         }
         }catch (Exception e){
         try {
         jsonObject = JSONUtil.parseArray(valueString.toString());
         StringJoiner joiner = new StringJoiner(Constants.ARRAY_SPLIT_CHAR);
         ((List<Object>)jsonObject).forEach(obj->{
         if (Checker.isNotEmpty(obj)) {
         if (obj instanceof Map || obj instanceof JSONObject) {
         Object name = ((Map) obj).get("Name");
         if (Checker.isNotEmpty(name)) {
         joiner.add(String.valueOf(name));
         } else {
         joiner.add(obj.toString());
         }
         } else {
         joiner.add(obj.toString());
         }
         }
         });
         result.put(customField, joiner.toString());
         }catch (Exception e1){
         jsonObject = String.valueOf(valueString);
         result.put(customField, jsonObject);
         }
         }
         }
         else {
         result.put(customField, jsonObject);
         }
         });**/
        if (result.size()>0) {
            stringObjectMap.putAll(result);
        }
        return result;
    }
}
