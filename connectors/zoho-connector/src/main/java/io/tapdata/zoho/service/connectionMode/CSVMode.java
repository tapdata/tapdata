package io.tapdata.zoho.service.connectionMode;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.entity.ContextConfig;
import io.tapdata.zoho.entity.HttpEntity;
import io.tapdata.zoho.enums.Constants;
import io.tapdata.zoho.enums.CustomFieldType;
import io.tapdata.zoho.enums.FieldModelType;
import io.tapdata.zoho.service.zoho.OrganizationFieldLoader;
import io.tapdata.zoho.service.zoho.TicketLoader;
import io.tapdata.zoho.utils.Checker;
import io.tapdata.zoho.utils.ZoHoString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static io.tapdata.base.ConnectorBase.*;

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
        if(tables == null || tables.isEmpty()) {
            //
            //"To Address",
            // "Time to Respond",Team,"Team Id",
            // Tags,工单搁置时间,Language
            TapTable tapTable = table("Issues")
//                    .add(field("Code",              "Integer").isPrimaryKey(true).primaryKeyPos(3))        //事项 Code
//                    .add(field("ProjectName",       "StringMinor").isPrimaryKey(true).primaryKeyPos(2))    //项目名称
//                    .add(field("TeamName",          "StringMinor").isPrimaryKey(true).primaryKeyPos(1))    //团队名称
                    .add(field("id","Long").isPrimaryKey(true).primaryKeyPos(1))    //"Ticket Reference Id"
                    .add(field("ticketNumber","Long"))
                    .add(field("departmentName","String"))    //"Department"   SSS  department（需要从分页中需添加到详情） ->  department.name（掉方法打平）
                    .add(field("departmentId","Long"))
                    .add(field("contactAccountAccountName","Long"))//contact   SSS   contact（需要从分页中需添加到详情）->contact.account.accountName（掉方法打平）
                    .add(field("contactAccountId","Long"))//contactID   SSS   contact（需要从分页中需添加到详情）->contact.account.id（掉方法打平）
                    .add(field("contactLastName","Long"))//联系人名称   SSS   contact（需要从分页中需添加到详情）->contact.lastName（掉方法打平）
                    .add(field("contactId","Long"))//联系人名称 Id   SSS   contact（需要从分页中需添加到详情）->contact.id（掉方法打平）
                    .add(field("email","String"))
                    .add(field("phone","Long"))
                    .add(field("subject","String"))
                    .add(field("description","String"))
                    .add(field("status","String"))
                    .add(field("productName","String"))//产品名称
                    .add(field("productId","Long"))//产品名称 Id
                    .add(field("assigneeName","String"))//工单所有者 SSS  assignee（需要从分页中需添加到详情）->assignee.lastName（掉方法打平）
                    .add(field("assigneeId","Long"))//工单所有者 Id  SSS  assignee（需要从分页中需添加到详情）->assignee.id（掉方法打平）
                    .add(field("createdByName","String"))
                    .add(field("createdBy","Long"))
                    .add(field("modifiedByName","String"))
                    .add(field("modifiedBy","Long"))
                    .add(field("createdTime","String"))
                    .add(field("modifiedTime","String"))
                    .add(field("closedTime","String"))
                    .add(field("dueDate","String"))
                    .add(field("priority","String"))
                    .add(field("channel","String"))//Model
                    .add(field("isOverDue","Boolean"))
                    .add(field("isEscalated","Boolean"))
                    .add(field("classification","String"))
                    .add(field("resolution","Object"))
                    .add(field("category","Object"))
                    .add(field("subCategory","Object"))
                    .add(field("customerResponseTime","String"))
                    .add(field("teamId","Long"))
                    .add(field("teamName","Object"))//工单所有者 Id  SSS  team（需要从分页中需添加到详情）->team.name  （掉方法打平）
                    .add(field("tags","String")) //@TODO
                    .add(field("language","String"))
                    .add(field("timeEntryCount","Integer"))//@TODO 工单搁置时间
/**
                    .add(field("statusType","String"))
                    .add(field("onholdTime","Object"))
                    .add(field("source","Map"))
                    .add(field("sharedDepartments","JAVA_Array"))
                    .add(field("approvalCount","Integer"))
                    .add(field("isTrashed","Boolean"))
                    .add(field("isResponseOverdue","Boolean"))
                    .add(field("contactId","Long"))
                    .add(field("threadCount","Integer"))
                    .add(field("secondaryContacts","JAVA_Array"))
                    .add(field("commentCount","Integer"))
                    .add(field("taskCount","Integer"))
                    .add(field("accountId","Long"))
                    .add(field("webUrl","String"))
                    .add(field("isSpam","Boolean"))
                    .add(field("entitySkills","JAVA_Array"))
                    .add(field("sentiment","Object"))
                    .add(field("customFields","Map"))
                    .add(field("isArchived","Boolean"))
                    .add(field("channelRelatedInfo","Object"))
                    .add(field("responseDueDate","Object"))
                    .add(field("isDeleted","Boolean"))
                    .add(field("modifiedBy","Long"))
                    .add(field("followerCount","Integer"))
                    .add(field("layoutDetails","Map"))
                    .add(field("channelCode","Object"))
                    .add(field("isFollowing","Boolean"))
                    .add(field("cf","Map"))
                    .add(field("slaId","Long"))
                    .add(field("layoutId","Long"))
                    .add(field("assigneeId","Long"))
                    .add(field("createdBy","Long"))
                    .add(field("tagCount","Integer"))
                    .add(field("attachmentCount","Integer"))
 **/
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
                                Checker.isEmpty(fieldTypeObj)?"":(String)fieldTypeObj));
                                //CustomFieldType.type(Checker.isEmpty(fieldTypeObj)?null:String.valueOf(fieldTypeObj))));
                    }
                }
            });
            return list(tapTable);
        }
        return null;
    }

    @Override
    public Map<String,Object> attributeAssignment(Map<String,Object> stringObjectMap) {
        Object ticketIdObj = stringObjectMap.get("id");
        if (Checker.isEmpty(ticketIdObj)){
            TapLogger.debug(TAG,"Ticket Id can not be null or not be empty.");
        }
        TicketLoader ticketLoader = TicketLoader.create(connectionContext);
        Map<String, Object> ticketDetail = ticketLoader.getOne((String) ticketIdObj);
        //把分页结果中具有但详情结果中不具有切CSV结构数据需要的结构进行提取
        ticketDetail.put("department",stringObjectMap.get("department"));
        ticketDetail.put("contact",stringObjectMap.get("contact"));
        ticketDetail.put("assignee",stringObjectMap.get("assignee"));

        Map<String,Object> ticketCSVDetail = new HashMap<>();
        //加入自定义属性
        Map<String, Object> customFieldMap = this.setCustomField(ticketDetail, contextConfig);
        if (Checker.isNotEmpty(customFieldMap)){
            ticketCSVDetail.putAll(customFieldMap);
        }
        //@TODO 统计需要操作的属性,子属性用点分割；数据组结果会以|分隔返回，大文本会以""包裹
        putMap(ticketDetail,ticketCSVDetail,
                "id",    //"Ticket Reference Id"
                "ticketNumber",
                "department.name",//"Department"   SSS  department（需要从分页中需添加到详情） ->  department.name（掉方法打平）
                "departmentId",
                "contact.account.accountName",//contact   SSS   contact（需要从分页中需添加到详情）->contact.account.accountName（掉方法打平）
                "contact.account.id",//contactID   SSS   contact（需要从分页中需添加到详情）->contact.account.id（掉方法打平）
                "contact.lastName",//联系人名称   SSS   contact（需要从分页中需添加到详情）->contact.lastName（掉方法打平）
                "contact.id",//联系人名称 Id   SSS   contact（需要从分页中需添加到详情）->contact.id（掉方法打平）
                "email",
                "phone",
                "subject",
                "description",
                "status",
                "productName",//产品名称
                "productId",//产品名称 Id
                "assignee.lastName",//工单所有者 SSS  assignee（需要从分页中需添加到详情）->assignee.lastName（掉方法打平）
                "assignee.id",//工单所有者 Id  SSS  assignee（需要从分页中需添加到详情）->assignee.id（掉方法打平）
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
                "teamName",//工单所有者 Id  SSS  team（需要从分页中需添加到详情）->team.name  （掉方法打平）
                "tags", //@TODO
                "language",
                "timeEntryCount"//@TODO 工单搁置时间
        );
        return ticketCSVDetail;
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
        result.putAll(customFields);
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

    private void putMap(Map<String,Object> map,Map<String,Object> targetMap,String ... keys){
        for (String key : keys) {
            Map<String, Object> entry = getEntry(ZoHoString.fistUpper(key,"."),key, map);
            if (Checker.isNotEmpty(entry)) {
                targetMap.putAll(entry);
            }
        }
    }
    private Map<String,Object> getEntry(final String finalKey,String key,Map<String,Object> map){
        int index = key.contains(".")?key.indexOf("."):key.length();
        String currentKey = key.substring(0, index);
        Object value = map.get(currentKey);
        if (Checker.isEmpty(value)){
            return null;
        }
        if (index<key.length()-1){
            String nextKey = key.substring(index + 1);
            if ((value instanceof JSONObject) || (value instanceof Map)){
                Map<String,Object> obj = (Map<String,Object>)value;
                return getEntry(finalKey,nextKey,obj);
            }else if(value instanceof JSONArray || value instanceof List){
                try {
                    List<Map<String, Object>> list = (List<Map<String, Object>>) value;
                    StringJoiner joiner = new StringJoiner(Constants.ARRAY_SPLIT_CHAR);
                    list.forEach(l->{
                        Object nextKeyObj = l.get(nextKey);
                        if (Checker.isNotEmpty(nextKeyObj)) {
                            joiner.add(String.valueOf(nextKeyObj));
                        }
                    });
                    return map(entry(finalKey,joiner.toString()));
                }catch (Exception e){
                    //@TODO 多层list嵌套时目前不支持解析，返回null
                    return null;
                    //List<List<Object>> catchList = (List<List<Object>>) value;
                    //catchList.forEach(list->{
                    //
                    //});
                }
            }else {
                return null;
            }
        }
        return map(entry(finalKey,value));
    }
}
