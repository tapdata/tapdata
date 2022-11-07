package io.tapdata.zoho.service.connectionMode.impl;

import cn.hutool.json.JSONObject;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.enums.Constants;
import io.tapdata.zoho.enums.FieldModelType;
import io.tapdata.zoho.service.connectionMode.ConnectionMode;
import io.tapdata.zoho.service.zoho.loader.OrganizationFieldLoader;
import io.tapdata.zoho.service.zoho.loader.TicketLoader;
import io.tapdata.zoho.service.zoho.loader.ZoHoBase;
import io.tapdata.zoho.service.zoho.schema.Schema;
import io.tapdata.zoho.service.zoho.schema.Schemas;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.MapUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.base.ConnectorBase.*;

/**
 *
 *  {
 *    "label": "${csv}",
 *    "value": "CSVMode"
 *  }
 * */
public class CSVMode implements ConnectionMode {
    private static final String TAG = TicketLoader.class.getSimpleName();
    TapConnectionContext connectionContext;
    ContextConfig contextConfig;
    @Override
    public ConnectionMode config(TapConnectionContext connectionContext) {
        this.connectionContext = connectionContext;
        return this;
    }

    @Override
    public List<TapTable> discoverSchema(List<String> tables, int tableSize) {
        /**
        if(tables == null || tables.isEmpty()) {
            //
            //"To Address",
            // "Time to Respond",Team,"Team Id",
            // Tags,工单搁置时间,Language
            TapTable tapTable = table("Tickets")
                    .add(field("id","Long").isPrimaryKey(true).primaryKeyPos(1))    //"Ticket Reference Id"
                    .add(field("ticketNumber","Long"))
                    .add(field("departmentName","StringNormal"))    //"Department"     department（需要从分页中需添加到详情） ->  department.name（掉方法打平）
                    .add(field("departmentId","Long"))
                    .add(field("contactAccountAccountName","StringNormal"))//contact      contact（需要从分页中需添加到详情）->contact.account.accountName（掉方法打平）
                    .add(field("contactAccountId","Long"))//contactID      contact（需要从分页中需添加到详情）->contact.account.id（掉方法打平）
                    .add(field("contactLastName","StringNormal"))//联系人名称      contact（需要从分页中需添加到详情）->contact.lastName（掉方法打平）
                    .add(field("contactId","Long"))//联系人名称 Id      contact（需要从分页中需添加到详情）->contact.id（掉方法打平）
                    .add(field("email","Email"))
                    .add(field("phone","Phone"))
                    .add(field("subject","StringNormal"))
                    .add(field("description","Textarea"))
                    .add(field("status","StringMinor"))
                    .add(field("productName","StringNormal"))//产品名称
                    .add(field("productId","Long"))//产品名称 Id
                    .add(field("assigneeName","StringNormal"))//工单所有者   assignee（需要从分页中需添加到详情）->assignee.lastName（掉方法打平）
                    .add(field("assigneeId","Long"))//工单所有者 Id    assignee（需要从分页中需添加到详情）->assignee.id（掉方法打平）
                    .add(field("createdByName","StringNormal"))
                    .add(field("createdBy","Long"))
                    .add(field("modifiedByName","StringNormal"))
                    .add(field("modifiedBy","Long"))
                    .add(field("createdTime","DateTime"))
                    .add(field("modifiedTime","DateTime"))
                    .add(field("closedTime","DateTime"))
                    .add(field("dueDate","DateTime"))
                    .add(field("priority","StringMinor"))
                    .add(field("channel","StringMinor"))//Model
                    .add(field("isOverDue","Boolean"))
                    .add(field("isEscalated","Boolean"))
                    .add(field("classification","StringNormal"))
                    .add(field("resolution","StringNormal"))//object
                    .add(field("category","StringNormal"))//object
                    .add(field("subCategory","StringNormal"))//object
                    .add(field("customerResponseTime","DateTime"))
                    .add(field("teamId","Long"))
                    .add(field("teamName","StringNormal"))//object工单所有者 Id    team（需要从分页中需添加到详情）->team.name  （掉方法打平）
                    .add(field("tags","StringNormal")) //@TODO
                    .add(field("language","StringMinor"))
                    .add(field("timeEntryCount","Integer"))//@TODO 工单搁置时间

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
                        tapTable.add(field(
                                Constants.CUSTOM_FIELD_SUFFIX + filedName,
                                Checker.isEmpty(fieldTypeObj)?"NULL":(String)fieldTypeObj));
                                //CustomFieldType.type(Checker.isEmpty(fieldTypeObj)?null:String.valueOf(fieldTypeObj))));
                    }
                }
            });
            return list(tapTable);
        }
        return null;
        **/
        List<Schema> schemas = Schemas.allSupportSchemas();
        if (null != schemas && !schemas.isEmpty()){
            List<TapTable> tapTables = new ArrayList<>();
            schemas.forEach(schema -> tapTables.addAll(schema.csv(tables,tableSize,connectionContext)));
            return null != tapTables && !tapTables.isEmpty()? tapTables : null;
        }
        return null;
    }
    @Override
    public List<TapTable> discoverSchemaV1(List<String> tables, int tableSize) {
        if(tables == null || tables.isEmpty()) {
            //
            //"To Address",
            // "Time to Respond",Team,"Team Id",
            // Tags,工单搁置时间,Language
            TapTable tapTable = table("Tickets")
                    .add(field("id","Long").isPrimaryKey(true).primaryKeyPos(1))    //"Ticket Reference Id"
                    .add(field("ticketNumber","Long"))
                    .add(field("departmentName","StringNormal"))    //"Department"     department（需要从分页中需添加到详情） ->  department.name（掉方法打平）
                    .add(field("departmentId","Long"))
                    .add(field("contactAccountAccountName","StringNormal"))//contact      contact（需要从分页中需添加到详情）->contact.account.accountName（掉方法打平）
                    .add(field("contactAccountId","Long"))//contactID      contact（需要从分页中需添加到详情）->contact.account.id（掉方法打平）
                    .add(field("contactLastName","StringNormal"))//联系人名称      contact（需要从分页中需添加到详情）->contact.lastName（掉方法打平）
                    .add(field("contactId","Long"))//联系人名称 Id      contact（需要从分页中需添加到详情）->contact.id（掉方法打平）
                    .add(field("email","Email"))
                    .add(field("phone","Phone"))
                    .add(field("subject","StringNormal"))
                    .add(field("description","Textarea"))
                    .add(field("status","StringMinor"))
                    .add(field("productName","StringNormal"))//产品名称
                    .add(field("productId","Long"))//产品名称 Id
                    .add(field("assigneeName","StringNormal"))//工单所有者   assignee（需要从分页中需添加到详情）->assignee.lastName（掉方法打平）
                    .add(field("assigneeId","Long"))//工单所有者 Id    assignee（需要从分页中需添加到详情）->assignee.id（掉方法打平）
                    .add(field("createdByName","StringNormal"))
                    .add(field("createdBy","Long"))
                    .add(field("modifiedByName","StringNormal"))
                    .add(field("modifiedBy","Long"))
                    .add(field("createdTime","DateTime"))
                    .add(field("modifiedTime","DateTime"))
                    .add(field("closedTime","DateTime"))
                    .add(field("dueDate","DateTime"))
                    .add(field("priority","StringMinor"))
                    .add(field("channel","StringMinor"))//Model
                    .add(field("isOverDue","Boolean"))
                    .add(field("isEscalated","Boolean"))
                    .add(field("classification","StringNormal"))
                    .add(field("resolution","StringNormal"))//object
                    .add(field("category","StringNormal"))//object
                    .add(field("subCategory","StringNormal"))//object
                    .add(field("customerResponseTime","DateTime"))
                    .add(field("teamId","Long"))
                    .add(field("teamName","StringNormal"))//object工单所有者 Id    team（需要从分页中需添加到详情）->team.name  （掉方法打平）
                    .add(field("tags","StringNormal")) //@TODO
                    .add(field("language","StringMinor"))
                    .add(field("timeEntryCount","Integer"))//@TODO 工单搁置时间

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
                        tapTable.add(field(
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

    public Map<String,Object> attributeAssignmentSelfV2(Map<String,Object> ticketDetail,String tableName) {
        Map<String,Object> ticketCSVDetail = new HashMap<>();
        //加入自定义属性
        Map<String, Object> customFieldMap = this.setCustomField(ticketDetail, contextConfig);
        if (Checker.isNotEmpty(customFieldMap)){
            ticketCSVDetail.putAll(customFieldMap);
        }
        //@TODO 统计需要操作的属性,子属性用点分割；数据组结果会以|分隔返回，大文本会以""包裹
        MapUtil.putMapSplitByDotKeyNameFirstUpper(ticketDetail, ticketCSVDetail,
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

    @Override
    public Map<String,Object> attributeAssignmentSelf(Map<String,Object> objectMap,String tableName){
        return Schema.schema(tableName).attributeAssignmentSelfCsv(objectMap,contextConfig);
    }

    public Map<String,Object> attributeAssignmentV2(Map<String,Object> stringObjectMap,String tableName) {
        Object ticketIdObj = stringObjectMap.get("id");
        if (Checker.isEmpty(ticketIdObj)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
            return null;
        }
        TicketLoader ticketLoader = TicketLoader.create(connectionContext);
        Map<String, Object> ticketDetail = ticketLoader.getOne((String) ticketIdObj);
        //把分页结果中具有但详情结果中不具有切CSV结构数据需要的结构进行提取
        ticketDetail.put("department",stringObjectMap.get("department"));
        ticketDetail.put("contact",stringObjectMap.get("contact"));
        ticketDetail.put("assignee",stringObjectMap.get("assignee"));
        return this.attributeAssignmentSelf(ticketDetail,tableName);
    }

    @Override
    public Map<String,Object> attributeAssignment(Map<String,Object> stringObjectMap, String tableName, ZoHoBase openApi){
        return Schema.schema(tableName).config(openApi).attributeAssignmentCsv(stringObjectMap,connectionContext,contextConfig);
    }
    //CustomFields
    private Map<String,Object> setCustomField(Map<String,Object> stringObjectMap,ContextConfig contextConfig){
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
