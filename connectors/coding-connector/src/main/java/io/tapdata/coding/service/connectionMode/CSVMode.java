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
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.*;

/**
 * {
 * "label": "${csv}",
 * "value": "CSVMode"
 * }
 */
public class CSVMode implements ConnectionMode {
    TapConnectionContext connectionContext;
    IssuesLoader loader;
    ContextConfig contextConfig;
    AtomicReference<String> accessToken;

    @Override
    public ConnectionMode config(TapConnectionContext connectionContext, AtomicReference<String> accessToken) {
        this.connectionContext = connectionContext;
        this.loader = IssuesLoader.create(connectionContext, accessToken);
        this.contextConfig = loader.veryContextConfigAndNodeConfig();
        this.accessToken = accessToken;
        return this;
    }

    @Override
    public List<TapTable> discoverSchema(List<String> tables, int tableSize, AtomicReference<String> accessToken) {
        List<SchemaStart> schemaStart = SchemaStart.getAllSchemas(connectionContext, accessToken);
        if (tables == null || tables.isEmpty()) {
            List<TapTable> tapTables = list();
            schemaStart.forEach(schema -> {
                TapTable csvTable = schema.csv(connectionContext);
                if (Checker.isNotEmpty(csvTable)) {
                    tapTables.add(csvTable);
                }
            });
            return tapTables;
        }
        return null;
    }

    @Override
    public Map<String, Object> attributeAssignment(Map<String, Object> stringObjectMap) {
        Object code = stringObjectMap.get("Code");
        HttpEntity<String, String> header = HttpEntity.create().builder("Authorization", accessToken.get());
        String projectName = contextConfig.getProjectName();
        HttpEntity<String, Object> issueDetialBody = HttpEntity.create()
                .builder("Action", "DescribeIssue")
                .builder("ProjectName", projectName);
        String teamName = contextConfig.getTeamName();
        CodingHttp authorization = CodingHttp.create(header.getEntity(), String.format(CodingStarter.OPEN_API_URL, teamName));
        HttpRequest requestDetail = authorization.createHttpRequest();
        stringObjectMap = loader.readIssueDetail(
                issueDetialBody,
                authorization,
                requestDetail,
                (code instanceof Integer) ? (Integer) code : Integer.parseInt(code.toString()),
                projectName,
                teamName);

        Map<String, Object> issueDetail = new HashMap<>();
        //加入自定义属性
        Map<String, Object> customFieldMap = this.setCustomField(stringObjectMap, contextConfig);
        if (Checker.isNotEmpty(customFieldMap)) {
            issueDetail.putAll(customFieldMap);
        }
        //@TODO 统计需要操作的属性,子属性用点分割；数据组结果会以|分隔返回，大文本会以""包裹
        putMap(stringObjectMap, issueDetail,
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

    private Map<Integer, Map<String, Object>> getIssueCustomFieldMap(String issueType, ContextConfig contextConfig) {
        HttpEntity<String, String> heard = HttpEntity.create().builder("Authorization", accessToken.get());
        HttpEntity<String, Object> body = HttpEntity.create()
                .builder("Action", "DescribeProjectIssueFieldList")
                .builder("ProjectName", contextConfig.getProjectName())
                .builder("IssueType", issueType);
        Map<String, Object> post = CodingHttp.create(heard.getEntity(), body.getEntity(), String.format(CodingStarter.OPEN_API_URL, contextConfig.getTeamName())).post();
        Object response = post.get("Response");
        Map<String, Object> responseMap = (Map<String, Object>) response;
        if (null == response) {
            throw new CoreException("HTTP request exception, Issue CustomField acquisition failed: " + CodingStarter.OPEN_API_URL + "?Action=DescribeProjectIssueFieldList. " + Optional.ofNullable(post.get(CodingHttp.ERROR_KEY)).orElse(""));
        }
        Object data = responseMap.get("ProjectIssueFieldList");
        if (null != data && data instanceof JSONArray) {
            if (((JSONArray) data).size() > 0) {
                List<Map<String, Object>> list = (List<Map<String, Object>>) data;
                return list.stream().collect(Collectors.toMap(item -> (Integer) item.get("IssueFieldId"), item -> item));
            }
        }
        return null;
    }

    //CustomFields
    private Map<String, Object> setCustomField(Map<String, Object> stringObjectMap, ContextConfig contextConfig) {
        Object issueType = stringObjectMap.get("Type");
        if (Checker.isEmpty(issueType)) {
            return null;
        }
        Map<Integer, Map<String, Object>> issueCustomFieldMap = this.getIssueCustomFieldMap((String) issueType, contextConfig);
        if (Checker.isEmpty(issueCustomFieldMap)) {
            return null;
        }
        Object customFieldsObj = stringObjectMap.get("CustomFields");
        if (null == customFieldsObj) {
            return null;
        }
        List<Map<String, Object>> customFields = (List<Map<String, Object>>) customFieldsObj;
        if (null == customFields || customFields.size() <= 0) {
            return null;
        }
        Map<String, Object> result = new HashMap<>();
        customFields.forEach(field -> {
            // @TODO
            Object id = field.get("Id");
            if (Checker.isNotEmpty(id)) {
                Map<String, Object> fieldDetialMap = issueCustomFieldMap.get(id);
                if (Checker.isNotEmpty(fieldDetialMap)) {
                    Map<String, Object> fieldMap = (Map<String, Object>) fieldDetialMap.get("IssueField");
                    //Object componentTypeObj = issueCustomFieldMap.get("ComponentType");
                    //Class feature = CustomFieldType.feature(Checker.isEmpty(componentTypeObj)?null:String.valueOf(componentTypeObj));
                    Object valueString = field.get("ValueString");
                    if (Checker.isNotEmpty(valueString)) {
                        Object jsonObject = null;
                        try {
                            jsonObject = JSONUtil.parseObj(valueString.toString());
                            Object nameObj = ((Map<String, Object>) jsonObject).get("Name");
                            if (Checker.isNotEmpty(nameObj)) {
                                result.put(Constants.CUSTOM_FIELD_SUFFIX + fieldMap.get("Name"), nameObj);
                            } else {
                                result.put(Constants.CUSTOM_FIELD_SUFFIX + fieldMap.get("Name"), jsonObject);
                            }
                        } catch (Exception e) {
                            try {
                                jsonObject = JSONUtil.parseArray(valueString.toString());
                                StringJoiner joiner = new StringJoiner(Constants.ARRAY_SPLIT_CHAR);
                                ((List<Object>) jsonObject).forEach(obj -> {
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
                            } catch (Exception e1) {
                                jsonObject = String.valueOf(valueString);
                                result.put(Constants.CUSTOM_FIELD_SUFFIX + fieldMap.get("Name"), jsonObject);
                            }
                        }
                    }
                }
            }
        });
        if (result.size() > 0) {
            stringObjectMap.putAll(result);
        }
        return result;
    }

    private void putMap(Map<String, Object> map, Map<String, Object> targetMap, String... keys) {
        for (String key : keys) {
            Map<String, Object> entry = getEntry(key.replaceAll("\\.", ""), key, map);
            if (Checker.isNotEmpty(entry)) {
                targetMap.putAll(entry);
            }
        }
    }

    private Map<String, Object> getEntry(final String finalKey, String key, Map<String, Object> map) {
        int index = key.contains(".") ? key.indexOf(".") : key.length();
        String currentKey = key.substring(0, index);
        Object value = map.get(currentKey);
        if (Checker.isEmpty(value)) {
            return null;
        }
        if (index < key.length() - 1) {
            String nextKey = key.substring(index + 1);
            if ((value instanceof JSONObject) || (value instanceof Map)) {
                Map<String, Object> obj = (Map<String, Object>) value;
                return getEntry(finalKey, nextKey, obj);
            } else if (value instanceof JSONArray || value instanceof List) {
                try {
                    List<Map<String, Object>> list = (List<Map<String, Object>>) value;
                    StringJoiner joiner = new StringJoiner(Constants.ARRAY_SPLIT_CHAR);
                    list.forEach(l -> {
                        Object nextKeyObj = l.get(nextKey);
                        if (Checker.isNotEmpty(nextKeyObj)) {
                            joiner.add(String.valueOf(nextKeyObj));
                        }
                    });
                    return map(entry(finalKey, joiner.toString()));
                } catch (Exception e) {
                    //@TODO 多层list嵌套时目前不支持解析，返回null
                    return null;
                    //List<List<Object>> catchList = (List<List<Object>>) value;
                    //catchList.forEach(list->{
                    //
                    //});
                }
            } else {
                return null;
            }
        }
        return map(entry(finalKey, value));
    }
}
