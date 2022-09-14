package io.tapdata.coding.service.connectionMode;

import cn.hutool.http.HttpRequest;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.tapdata.coding.entity.ContextConfig;
import io.tapdata.coding.enums.Constants;
import io.tapdata.coding.enums.CustomFieldType;
import io.tapdata.coding.service.CodingStarter;
import io.tapdata.coding.service.IssueLoader;
import io.tapdata.coding.utils.http.CodingHttp;
import io.tapdata.coding.utils.http.HttpEntity;
import io.tapdata.coding.utils.tool.Checker;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class CSVMode implements ConnectionMode {
    @Override
    public List<TapTable> discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize) {
        ContextConfig contextConfig = IssueLoader.create(connectionContext).veryContextConfigAndNodeConfig();

        if(tables == null || tables.isEmpty()) {
//            ID,事项类型,
//            标题,描述,状态,创建时间,创建人,更新时间,所属迭代,处理人,
//            缺陷类型,优先级,开始日期,截止日期,模块,标签,关注人,预估工时,进度,已登记工时,分数,次数,页面链接
            TapTable tapTable = table("Issues")
                    .add(field("Code", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(3))        //事项 Code
                    .add(field("ProjectName", "StringMinor").isPrimaryKey(true).primaryKeyPos(2))   //项目名称
                    .add(field("TeamName", "StringMinor").isPrimaryKey(true).primaryKeyPos(1))      //团队名称
                    .add(field("Type", "StringMinor"))                                         //事项类型：DEFECT - 缺陷;REQUIREMENT - 需求;MISSION - 任务;EPIC - 史诗;SUB_TASK - 子工作项
                    .add(field("Name", "StringMinor"))                                              //名称
                    .add(field("Description", "StringLonger"))                                       //描述
                    .add(field("IssueStatusName", "StringMinor"))                                   //事项状态名称
                    .add(field("CreatedAt", JAVA_Long))                                             //创建时间
                    .add(field("CreatorId", JAVA_Integer))                                       //创建人Id
                    .add(field("UpdatedAt", JAVA_Long))                                             //修改时间

                    .add(field("IterationName", JAVA_String))                                               //所属迭代
                    .add(field("AssigneeName", JAVA_String))                                                //处理人
                    .add(field("DefectTypeName", JAVA_String))                                              //缺陷类型
                    .add(field("ParentCode", JAVA_Integer))                                              //缺陷类型

                    .add(field("StartDate", JAVA_Long))                                             //开始日期时间戳
                    .add(field("DueDate", JAVA_Long))                                               //截止日期时间戳
                    .add(field("Priority", JAVA_String))                                               //优先级

                    .add(field("ProjectModuleName", JAVA_String))                                           //项目模块
                    .add(field("LabelName", JAVA_String))                                          //标签s
                    .add(field("WatcherName", JAVA_String))                                            //关注人s


                    .add(field("WorkingHours", "WorkingHours"))                                      //工时（小时）
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
                /***
                 *  {
                 *      "Id": 63054716,
                 *      "IssueFieldId": 36719601,
                 *      "NeedDefault": false,
                 *      "ValueString": "",
                 *      "IssueType": "REQUIREMENT",
                 *      "Required": false,
                 *      "IssueField": {
                 *          "Id": 36719601,
                 *          "Name": "处理人",
                 *          "IconUrl": "",
                 *          "Type": "ASSIGNEE",
                 *          "ComponentType": "SELECT_MEMBER_SINGLE",
                 *          "Description": "",
                 *          "Options": [],
                 *          "Unit": "",
                 *          "Selectable": false,
                 *          "Required": false,
                 *          "Editable": false,
                 *          "Deletable": false,
                 *          "Sortable": true,
                 *          "CreatedBy": 0,
                 *          "CreatedAt": 1661745452000,
                 *          "UpdatedAt": 1661745452000
                 *      },
                 *      "CreatedAt": 1663063209000,
                 *      "UpdatedAt": 1663063209000
                 *  }
                 */
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
        return null;
    }

    @Override
    public Map<String,Object> attributeAssignment(TapConnectorContext nodeContext, Map<String,Object> stringObjectMap) {
        IssueLoader loader = IssueLoader.create(nodeContext);
        ContextConfig contextConfig = loader.veryContextConfigAndNodeConfig();
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
        //@TODO 统计需要操作的属性
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
                    //@TODO
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
    /**
     "Issue": {
     "Epic": {
     "Code": 0,
     "Type": "",
     "Name": "",
     "IssueStatusId": 0,
     "IssueStatusName": "",
     "Priority": "",
     "Assignee": {
     "Id": 0,
     "Status": 0,
     "Avatar": "",
     "Name": "",
     "Email": "",
     "TeamId": 0,
     "Phone": "",
     "GlobalKey": "",
     "TeamGlobalKey": ""
     }
     },
     "ParentType": "REQUIREMENT",
     "Code": 66,
     "Type": "REQUIREMENT",
     "Name": "计算",
     "Description": "\n计算\n",
     "IterationId": 0,
     "IssueStatusId": 43900887,
     "IssueStatusName": "未开始",
     "IssueStatusType": "TODO",
     "Priority": "1",
     "Assignee": {
     "Id": 0,
     "Status": 0,
     "Avatar": "",
     "Name": "",
     "Email": "",
     "TeamId": 0,
     "Phone": "",
     "GlobalKey": "",
     "TeamGlobalKey": ""
     },
     "StartDate": 0,
     "DueDate": 0,
     "WorkingHours": 0,
     "Creator": {
     "Id": 8613060,
     "Status": 1,
     "Avatar": "https://coding-net-production-static-ci.codehub.cn/3b111f6b-f929-4f16-838c-f29ff2c97eb6.jpg?imageView2/1/w/0/h/0",
     "Name": "TestHookGavin",
     "Email": "",
     "TeamId": 0,
     "Phone": "",
     "GlobalKey": "",
     "TeamGlobalKey": ""
     },
     "StoryPoint": "",
     "CreatedAt": 1663128089000,
     "UpdatedAt": 1663131435000,
     "CompletedAt": 0,
     "ProjectModule": {
     "Id": 0,
     "Name": ""
     },
     "Watchers": [
     {
     "Id": 8613060,
     "Status": 1,
     "Avatar": "https://coding-net-production-static-ci.codehub.cn/3b111f6b-f929-4f16-838c-f29ff2c97eb6.jpg?imageView2/1/w/0/h/0",
     "Name": "TestHookGavin",
     "Email": "",
     "TeamId": 0,
     "Phone": "",
     "GlobalKey": "",
     "TeamGlobalKey": ""
     }
     ],
     "Labels": [],
     "Files": [],
     "RequirementType": {
     "Id": 0,
     "Name": ""
     },
     "DefectType": {
     "Id": 0,
     "Name": "",
     "IconUrl": ""
     },
     "CustomFields": [
     {
     "Id": 36756582,
     "ValueString": "100"
     }
     ],
     "ThirdLinks": [],
     "SubTasks": [],
     "Parent": {
     "Code": 0,
     "Type": "",
     "Name": "",
     "IssueStatusId": 0,
     "IssueStatusName": "",
     "Priority": "",
     "Assignee": {
     "Id": 0,
     "Status": 0,
     "Avatar": "",
     "Name": "",
     "Email": "",
     "TeamId": 0,
     "Phone": "",
     "GlobalKey": "",
     "TeamGlobalKey": ""
     },
     "IssueStatusType": "",
     "IssueTypeDetail": {
     "Id": 0,
     "Name": "",
     "IssueType": "",
     "Description": "",
     "IsSystem": false
     }
     },
     "Iteration": {
     "Code": 0,
     "Name": "",
     "Status": "",
     "Id": 0
     },
     "IssueTypeDetail": {
     "Id": 8085488,
     "Name": "需求",
     "IssueType": "REQUIREMENT",
     "Description": "需求是指用户解决某一个问题或达到某一目标所需的软件功能。",
     "IsSystem": true
     },
     "IssueTypeId": 8085488
     }
     * */

/**
//                    .add(field("Code", JAVA_Integer).isPrimaryKey(true).primaryKeyPos(3))        //事项 Code
//                    .add(field("ProjectName", "StringMinor").isPrimaryKey(true).primaryKeyPos(2))   //项目名称
//                    .add(field("TeamName", "StringMinor").isPrimaryKey(true).primaryKeyPos(1))      //团队名称
//                    .add(field("ParentType", "StringMinor"))                                       //父事项类型
//                    .add(field("Type", "StringMinor"))                                         //事项类型：DEFECT - 缺陷;REQUIREMENT - 需求;MISSION - 任务;EPIC - 史诗;SUB_TASK - 子工作项
//
//                    .add(field("Name", "StringMinor"))                                              //名称
//                    .add(field("Description", "StringLonger"))                                       //描述
//
//                    .add(field("IssueStatusId", JAVA_Integer))                                   //事项状态 Id
//                    .add(field("IssueStatusName", "StringMinor"))                                   //事项状态名称
//                    .add(field("IssueStatusType", "StringMinor"))                                   //事项状态类型
//                    .add(field("CreatorId", JAVA_Integer))                                       //创建人Id
//                    .add(field("Creator", JAVA_Map))                                                 //创建人
//                    .add(field("CreatedAt", JAVA_Long))                                             //创建时间
//                    .add(field("UpdatedAt", JAVA_Long))                                             //修改时间
//                    .add(field("CompletedAt", JAVA_Long))                                           //完成时间
//                    .add(field("IssueTypeDetailId", JAVA_Integer))                               //事项类型ID
//                    .add(field("IssueTypeDetail", JAVA_Map))                                         //事项类型具体信息
//                    .add(field("IterationId", JAVA_Integer))                                     //迭代 Id
//                    .add(field("Priority", "StringBit"))                                          //优先级:"0" - 低;"1" - 中;"2" - 高;"3" - 紧急;"" - 未指定
//                    .add(field("IterationCode", JAVA_Integer))                                   //所属迭代Code
//                    .add(field("Iteration", JAVA_Map))                                               //所属迭代
//
//                    .add(field("AssigneeId", JAVA_Integer))                                      //Assignee.Id 等于 0 时表示未指定
//                    .add(field("Assignee", JAVA_Map))                                                //处理人
//
//                    .add(field("StartDate", JAVA_Long))                                             //开始日期时间戳
//                    .add(field("DueDate", JAVA_Long))                                               //截止日期时间戳
//                    .add(field("WorkingHours", "WorkingHours"))                                      //工时（小时）
//
//                    .add(field("StoryPoint", "StringMinor"))                                        //故事点
//
//
//                    .add(field("ProjectModuleId", JAVA_Integer))                                 //ProjectModule.Id 等于 0 时表示未指定
//                    .add(field("ProjectModule", JAVA_Map))                                           //项目模块
//
//                    .add(field("WatcherIdArr", JAVA_Array))                                        //关注人Id列表
//                    .add(field("Watchers", JAVA_Array))                                            //关注人
//
////                            .add(field("LabelIdArr", JAVA_Array))                                          //标签Id列表
////                            .add(field("Labels", JAVA_Array))                                              //标签列表
//
////                            .add(field("FileIdArr", JAVA_Array))                                           //附件Id列表
////                            .add(field("Files", JAVA_Array))                                               //附件列表
//                    .add(field("RequirementType", "StringSmaller"))                                   //需求类型
//
//                    .add(field("DefectType", JAVA_Map))                                              //缺陷类型
//                    .add(field("CustomFields", JAVA_Array))                                        //自定义字段列表
//                    .add(field("ThirdLinks", JAVA_Array))                                          //第三方链接列表
//
////                            .add(field("SubTaskCodeArr", JAVA_Array))                                      //子工作项Code列表
////                            .add(field("SubTasks", JAVA_Array))                                            //子工作项列表
//
//                    .add(field("ParentCode", JAVA_Integer))                                      //父事项Code
//                    .add(field("Parent", JAVA_Map))                                                  //父事项
//
//                    .add(field("EpicCode", JAVA_Integer))                                        //所属史诗Code
//                    .add(field("Epic", JAVA_Map))
 */
}
