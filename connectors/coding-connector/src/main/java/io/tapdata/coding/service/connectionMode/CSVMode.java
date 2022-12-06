package io.tapdata.coding.service.connectionMode;

import cn.hutool.http.HttpRequest;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.enums.Constants;
import io.tapdata.coding.service.loader.CodingStarter;
import io.tapdata.coding.service.loader.IssuesLoader;
import io.tapdata.coding.service.schema.SchemaStart;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.util.*;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.*;

/**
 * {
 *   "label": "${csv}",
 *   "value": "CSVMode"
 * }
 * */
public class CSVMode implements ConnectionMode {
    TapConnectionContext connectionContext;
    IssuesLoader loader;
    ContextConfig contextConfig;
    @Override
    public ConnectionMode config(TapConnectionContext connectionContext) {
        this.connectionContext = connectionContext;
        this.loader = IssuesLoader.create(connectionContext);
        this.contextConfig = loader.veryContextConfigAndNodeConfig();
        return this;
    }

    @Override
    public List<TapTable> discoverSchema(List<String> tables, int tableSize) {
        List<SchemaStart> schemaStart = SchemaStart.getAllSchemas(connectionContext);
        if (tables == null || tables.isEmpty()){
            List<TapTable> tapTables = list();
            schemaStart.forEach(schema -> {
                TapTable csvTable = schema.csv(connectionContext);
                if (Checker.isNotEmpty(csvTable)) {
                    tapTables.add(csvTable);
                }
            });
            return tapTables;
        }
        /**
         * ContextConfig contextConfig = IssueLoader.create(connectionContext).veryContextConfigAndNodeConfig();
        if(tables == null || tables.isEmpty()) {
            //ID,事项类型,
            //标题,描述,状态,创建时间,创建人,更新时间,所属迭代,处理人,
            //缺陷类型,优先级,开始日期,截止日期,模块,标签,关注人,预估工时,进度,已登记工时,分数,次数,页面链接
            TapTable tapTable = table("Issues")
                    .add(field("Code",              "Integer").isPrimaryKey(true).primaryKeyPos(3))        //事项 Code
                    .add(field("ProjectName",       "StringMinor").isPrimaryKey(true).primaryKeyPos(2))    //项目名称
                    .add(field("TeamName",          "StringMinor").isPrimaryKey(true).primaryKeyPos(1))    //团队名称
                    .add(field("Type",              "StringMinor"))                                        //事项类型：DEFECT - 缺陷;REQUIREMENT - 需求;MISSION - 任务;EPIC - 史诗;SUB_TASK - 子工作项
                    .add(field("Name",              "StringMinor"))                                        //名称
                    .add(field("Description",       "StringLonger"))                                       //描述
                    .add(field("IssueStatusName",   "StringMinor"))                                        //事项状态名称
                    .add(field("CreatedAt",               JAVA_Long))                                             //创建时间
                    .add(field("CreatorId",         "Integer"))                                            //创建人Id
                    .add(field("UpdatedAt",               JAVA_Long))                                             //修改时间
                    .add(field("IterationName",     "StringNormal"))                                       //所属迭代
                    .add(field("AssigneeName",      "StringNormal"))                                       //处理人
                    .add(field("DefectTypeName",    "StringNormal"))                                       //缺陷类型
                    .add(field("ParentCode",        "Integer"))                                            //缺陷类型
                    .add(field("StartDate",               JAVA_Long))                                             //开始日期时间戳
                    .add(field("DueDate",                 JAVA_Long))                                             //截止日期时间戳
                    .add(field("Priority",          "StringNormal"))                                       //优先级
                    .add(field("ProjectModuleName", "StringNormal"))                                       //项目模块
                    .add(field("LabelName",         "StringNormal"))                                       //标签s
                    .add(field("WatcherName",       "StringNormal"))                                       //关注人s
                    .add(field("WorkingHours",      "WorkingHours"))                                       //工时（小时）
                    ;
            // 查询自定义属性列表
            Map<Integer,Map<String,Object>> customFields = new HashMap<>();
            List<Map<String, Object>> allIssueType = IssueLoader.create(connectionContext).getAllIssueType();
            if (Checker.isEmpty(allIssueType)){
                throw new CoreException("Get issue type list error.");
            }
            //查询全部事项类型，根据事项类型获取全部自定义属性
            for (Map<String, Object> issueType : allIssueType) {
                Object type = issueType.get("IssueType");
                if (Checker.isNotEmpty(type)) {
                    Map<Integer, Map<String, Object>> issueCustomFieldMap = this.getIssueCustomFieldMap(String.valueOf(type), contextConfig);
                    if (null != issueCustomFieldMap) {
                        customFields.putAll(issueCustomFieldMap);
                    }
                }
            }
            customFields.forEach((fieldId,obj)->{
                Object issueFieldObj = obj.get("IssueField");
                if (null != issueFieldObj && issueFieldObj instanceof JSONObject) {
                    Map<String, Object> issueField = (Map<String, Object>) obj.get("IssueField");
                    Object filedName = issueField.get("Name");
                    if (Checker.isNotEmpty(filedName)) {
                        //@TODO 根据ComponentType属性匹配对应tapdata类型
                        Object componentTypeObj = issueField.get("ComponentType");
                        tapTable.add(field(
                                Constants.CUSTOM_FIELD_SUFFIX + String.valueOf(filedName),
                                CustomFieldType.type(Checker.isEmpty(componentTypeObj)?null:String.valueOf(componentTypeObj))));
                    }
                }
            });
            return list(tapTable);
        }
        */

        return null;
    }

    @Override
    public Map<String,Object> attributeAssignment(Map<String,Object> stringObjectMap) {
        Object code = stringObjectMap.get("Code");
        HttpEntity<String,String> header = HttpEntity.create().builder("Authorization",contextConfig.getToken());
        String projectName = contextConfig.getProjectName();
        HttpEntity<String,Object> issueDetialBody = HttpEntity.create()
                .builder("Action","DescribeIssue")
                .builder("ProjectName",projectName);
        String teamName = contextConfig.getTeamName();
        CodingHttp authorization = CodingHttp.create(header.getEntity(), String.format(CodingStarter.OPEN_API_URL, teamName ));
        HttpRequest requestDetail = authorization.createHttpRequest();
        stringObjectMap = loader.readIssueDetail(
                issueDetialBody,
                authorization,
                requestDetail,
                (code instanceof Integer)?(Integer)code:Integer.parseInt(code.toString()),
                projectName,
                teamName);

        Map<String,Object> issueDetail = new HashMap<>();
        //加入自定义属性
        Map<String, Object> customFieldMap = this.setCustomField(stringObjectMap, contextConfig);
        if (Checker.isNotEmpty(customFieldMap)){
            issueDetail.putAll(customFieldMap);
        }
        //@TODO 统计需要操作的属性,子属性用点分割；数据组结果会以|分隔返回，大文本会以""包裹
        putMap(stringObjectMap,issueDetail,
                "Code",
                //"ProjectName",
                //"TeamName",
                "Type",
                "Name",
                "Description",
                "IssueStatusName",
                "CreatedAt",
                "CreatorId",
                "UpdatedAt",
                "Iteration.Name",
                "Assignee.Name",
                "DefectType.Name",
                "Parent.Code",
                "StartDate",
                "DueDate",
                "Priority",
                "ProjectModule.Name",
                "WorkingHours",
                "Labels.Name",
                "Watchers.Name"
        );
        loader.composeIssue(contextConfig.getProjectName(), contextConfig.getTeamName(), issueDetail);
        return issueDetail;
    }

    private Map<Integer,Map<String,Object>> getIssueCustomFieldMap(String issueType, ContextConfig contextConfig){
        HttpEntity<String,String> heard = HttpEntity.create().builder("Authorization",contextConfig.getToken());
        HttpEntity<String,Object> body = HttpEntity.create()
                .builder("Action","DescribeProjectIssueFieldList")
                .builder("ProjectName",contextConfig.getProjectName())
                .builder("IssueType",issueType);
        Map<String, Object> post = CodingHttp.create(heard.getEntity(), body.getEntity(), String.format(CodingStarter.OPEN_API_URL, contextConfig.getTeamName())).post();
        Object response = post.get("Response");
        Map<String,Object> responseMap = (Map<String, Object>) response;
        if (null == response ){
            throw new CoreException("HTTP request exception, Issue CustomField acquisition failed: " + CodingStarter.OPEN_API_URL+"?Action=DescribeProjectIssueFieldList");
        }
        Object data = responseMap.get("ProjectIssueFieldList");
        if (null != data && data instanceof JSONArray){
            if (((JSONArray)data).size()>0) {
                List<Map<String, Object>> list = (List<Map<String, Object>>) data;
                return list.stream().collect(Collectors.toMap(item -> (Integer) item.get("IssueFieldId"), item -> item));
            }
        }
        return null;
    }

    //CustomFields
    private Map<String,Object> setCustomField(Map<String,Object> stringObjectMap,ContextConfig contextConfig){
        Object issueType = stringObjectMap.get("Type");
        if (Checker.isEmpty(issueType)){
            return null;
        }
        Map<Integer, Map<String, Object>> issueCustomFieldMap = this.getIssueCustomFieldMap((String) issueType, contextConfig);
        if (Checker.isEmpty(issueCustomFieldMap)){
            return null;
        }
        Object customFieldsObj = stringObjectMap.get("CustomFields");
        if (null == customFieldsObj){
            return null;
        }
        List<Map<String,Object>> customFields = (List<Map<String,Object>>)customFieldsObj;
        if (null == customFields || customFields.size()<=0){
            return null;
        }
        Map<String,Object> result = new HashMap<>();
        customFields.forEach(field->{
            // @TODO
            Object id = field.get("Id");
            if (Checker.isNotEmpty(id)) {
                Map<String,Object> fieldDetialMap = issueCustomFieldMap.get(id);
                if (Checker.isNotEmpty(fieldDetialMap)){
                    Map<String, Object> fieldMap = (Map<String,Object>)fieldDetialMap.get("IssueField");
                    //Object componentTypeObj = issueCustomFieldMap.get("ComponentType");
                    //Class feature = CustomFieldType.feature(Checker.isEmpty(componentTypeObj)?null:String.valueOf(componentTypeObj));
                    Object valueString = field.get("ValueString");
                    if (Checker.isNotEmpty(valueString)) {
                        Object jsonObject = null;
                        try {
                            jsonObject = JSONUtil.parseObj(valueString.toString());
                            Object nameObj = ((Map<String,Object>)jsonObject).get("Name");
                            if (Checker.isNotEmpty(nameObj)){
                                result.put(Constants.CUSTOM_FIELD_SUFFIX + fieldMap.get("Name"), nameObj);
                            }else {
                                result.put(Constants.CUSTOM_FIELD_SUFFIX + fieldMap.get("Name"), jsonObject);
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
                                result.put(Constants.CUSTOM_FIELD_SUFFIX + fieldMap.get("Name"), joiner.toString());
                            }catch (Exception e1){
                                jsonObject = String.valueOf(valueString);
                                result.put(Constants.CUSTOM_FIELD_SUFFIX + fieldMap.get("Name"), jsonObject);
                            }
                        }
                    }
                }
            }
        });
        if (result.size()>0) {
            stringObjectMap.putAll(result);
        }
        return result;
    }

    private void putMap(Map<String,Object> map,Map<String,Object> targetMap,String ... keys){
        for (String key : keys) {
            Map<String, Object> entry = getEntry(key.replaceAll("\\.",""),key, map);
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
