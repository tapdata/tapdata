package com.tapdata.tm.utils;

import com.mongodb.BasicDBObject;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: Zed
 * @Date: 2021/9/22
 * @Description:
 */
@Slf4j
@Component
public class MetadataUtil {

    @Autowired
    private MetadataInstancesService metadataInstancesService;

    public static final String  MODIFY_PROPERTY = "Modify.property";
    public static final String  ADD_PROPERTY = "Add.property";
    public static final String  REMOVE_PROPERTY = "Remove.property";
    public static final String  MODIFY_FIELD_PROPERTY = "Modify.field";
    public static final String  ADD_NEW_FIELD = "Add.new.field";
    public static final String  REMOVE_FIELD = "Remove.field";
    public static final String  EQUALS = "eq";
    public static final String  ADD = "add";
    public static final String  MODIFY = "mod";
    public static final String  DELETE = "del";
    public static final String  BASE_PREFIX = "base_";
    public static final String  COMPARE_PREFIX = "compare_";
    public static final String  RESULT_SUFFIX = "_result";

    public static final String [] compareFieldKeys = {
            "alias_name",
            "field_name",
            "java_type",
            "columnSize",
            "is_nullable",
            "unique",
            "primary_key_position",
            "foreign_key_position",
            "precision",
            "scale",
            "autoincrement"
    };

    /**
     * 比较结果是第一个参数相对于第二个参数
     * @param firstMeta 第一个模型
     * @param secondMeta 第二个模型
     * @return
     */
    public static CompareResult compare(MetadataInstancesDto firstMeta, MetadataInstancesDto secondMeta) {

        if (firstMeta == null || secondMeta == null) {
            return null;
        }

        List<String> descriptions = new ArrayList<>();
        CompareResult compareResult = new CompareResult();
        Map<String, Object> baseProperties = buildProperties(firstMeta);
        Map<String, Object> compareProperties = buildProperties(secondMeta);
        List<ComparePropertiesResult> compareResults = compareProperties(baseProperties, compareProperties, descriptions);
        if (CollectionUtils.isNotEmpty(compareResults)) {
            compareResult.setProperties(compareResults);
        }

        // compare fields
        List<Map<String, Object>> compareFieldsResult = compareFields(firstMeta, secondMeta, descriptions);
        if (CollectionUtils.isNotEmpty(compareFieldsResult)) {
            compareResult.setFields(compareFieldsResult);
        }

        String  versionDescription = "";
        if (CollectionUtils.isNotEmpty(descriptions)) {
            StringBuilder sb = new StringBuilder("");
            for (String description : descriptions) {
                sb.append(description).append(";");
            }
            versionDescription = sb.toString();
            versionDescription = versionDescription.substring(0, versionDescription.length() - 1);
        }
        compareResult.setVersionDescription(versionDescription);

        return compareResult;
    };


    public static Map<String, Object> buildProperties(MetadataInstancesDto metadata)  {
        Map<String, Object> properties = new HashMap<>();
        properties.put("name", metadata.getName());
        properties.put("comment", metadata.getComment());

        Map<String, Object> customProperties = metadata.getCustomProperties();
        if (customProperties != null) {
            properties.putAll(customProperties);
        }
        return properties;
    };



    public static List<ComparePropertiesResult>  compareProperties (Map<String, Object> baseProperties, Map<String, Object> compareProperties, List<String> descriptions) {
        List<ComparePropertiesResult> compareResults = new ArrayList<>();
        for(Map.Entry<String, Object> entry : baseProperties.entrySet()) {
            Object compareValue = compareProperties.get(entry.getKey());

            String result = EQUALS;

            if (compareValue != null && !compareValue.equals(entry.getValue())) {
                result = MODIFY;

                if (!descriptions.contains(MODIFY_PROPERTY)) {
                    descriptions.add(MODIFY_PROPERTY);
                }
            } else {
                result = ADD;
                if (!descriptions.contains(ADD_PROPERTY)) {
                    descriptions.add(ADD_PROPERTY);
                }
            }

            ComparePropertiesResult compareResult = new ComparePropertiesResult();
            compareResult.setKey(entry.getKey());
            compareResult.setBaseValue(entry.getValue());
            compareResult.setCompareValue(compareValue);
            compareResult.setResult(result);
            boolean isCustom = !"name".equals(entry.getKey()) && !"comment".equals(entry.getKey());
            compareResult.setCustom(isCustom);
            compareResults.add(compareResult);
        }

        for(Map.Entry<String, Object> entry : compareProperties.entrySet()) {
            if (baseProperties.get(entry.getKey()) == null) {
                ComparePropertiesResult compareResult = new ComparePropertiesResult();
                compareResult.setKey(entry.getKey());
                compareResult.setBaseValue("");
                compareResult.setCompareValue(entry.getValue());
                compareResult.setResult(DELETE);
                boolean isCustom = !"name".equals(entry.getKey()) && !"comment".equals(entry.getKey());
                compareResult.setCustom(isCustom);
                compareResults.add(compareResult);

                if (!descriptions.contains(REMOVE_PROPERTY)) {
                    descriptions.add(REMOVE_PROPERTY);
                }
            }
        }

        return compareResults;

    }


    public static List<Map<String, Object>>  compareFields (MetadataInstancesDto firstMeta, MetadataInstancesDto secondMeta, List<String> descriptions) {
        List<Field> baseFields = new ArrayList<>();
        List<Field> compareFields = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(firstMeta.getFields())) {
            baseFields = firstMeta.getFields().stream().filter(f -> !f.isDeleted()).collect(Collectors.toList());
        }

        if (CollectionUtils.isNotEmpty(secondMeta.getFields())) {
            compareFields = secondMeta.getFields().stream().filter(f -> !f.isDeleted()).collect(Collectors.toList());
        }

        List<Map<String, Object>> compareResult = new ArrayList<>();

        for (Field baseField : baseFields) {
            String baseOriginalFieldName = StringUtils.isNotBlank(baseField.getOriginalFieldName()) ? baseField.getOriginalFieldName()
                    : StringUtils.isNotBlank(baseField.getFieldName()) ? baseField.getFieldName() : "";
            Field compareField = findFieldByOriginalFieldName(compareFields, baseOriginalFieldName);
            Map<String, Object> fieldCompare = new HashMap<>();
            fieldCompare.put("result", EQUALS);

            if (compareField != null) {
                for (String fieldKey : compareFieldKeys) {
                    Object baseValue = getValueByKey(baseField, fieldKey);
                    Object compareValue = getValueByKey(compareField, fieldKey);
                    String fieldResult = EQUALS;

                    if (baseValue != null && !baseValue.equals(compareValue)) {
                        fieldResult = MODIFY;
                        fieldCompare.put("result", MODIFY);

                        if (!descriptions.contains(MODIFY_FIELD_PROPERTY)) {
                            descriptions.add(MODIFY_FIELD_PROPERTY);
                        }
                    }

                    fieldCompare.put(BASE_PREFIX + fieldKey, baseValue);
                    fieldCompare.put(COMPARE_PREFIX + fieldKey, compareValue);
                    fieldCompare.put(fieldKey + RESULT_SUFFIX, fieldResult);
                }

            } else {
                fieldCompare.put("result", ADD);
                for (String baseKey : compareFieldKeys) {
                    Object baseValue = getValueByKey(baseField, baseKey);
                    fieldCompare.put(BASE_PREFIX + baseKey, baseValue);
                }

                if (!descriptions.contains(ADD_NEW_FIELD)) {
                    descriptions.add(ADD_NEW_FIELD);
                }
            }

            compareResult.add(fieldCompare);

        }

        for (Field compareField : compareFields) {
            String compareOriginalFieldName = StringUtils.isNotBlank(compareField.getOriginalFieldName()) ? compareField.getOriginalFieldName()
                    : StringUtils.isNotBlank(compareField.getFieldName()) ? compareField.getFieldName() : "";

            Field baseField = findFieldByOriginalFieldName(baseFields, compareOriginalFieldName);
            Map<String, Object> fieldCompare = new HashMap<>();

            if (baseField == null) {
                fieldCompare.put("result", DELETE);
                for (String fieldKey : compareFieldKeys) {
                    Object compareValue = getValueByKey(compareField, fieldKey);
                    fieldCompare.put(COMPARE_PREFIX + fieldKey, compareValue);
                }

                if (!descriptions.contains(REMOVE_FIELD)) {
                    descriptions.add(REMOVE_FIELD);
                }

                compareResult.add(fieldCompare);

            }
        }

        return compareResult;
    }

    public static Field findFieldByOriginalFieldName(List<Field> compareFields, String baseOriginalFieldName) {
        if (CollectionUtils.isEmpty(compareFields)) {
            return null;
        }


        for (Field compareField : compareFields) {
            if ((compareField.getOriginalFieldName() != null && compareField.getOriginalFieldName().equals(baseOriginalFieldName))
                    || (compareField.getFieldName() != null && compareField.getFieldName().equals(baseOriginalFieldName))) {
                return compareField;
            }
        }

        return null;
    }


    public static Object getValueByKey(Object obj, String key) {
        java.lang.reflect.Field[] declaredFields = obj.getClass().getDeclaredFields();
        for (java.lang.reflect.Field declaredField : declaredFields) {
            if (declaredField.getName().equals(key)) {
                declaredField.setAccessible(true);
                try {
                    return declaredField.get(obj);
                } catch (IllegalAccessException e) {
                    return null;
                }
            }
        }
        return null;
    }


    /**
     * 生成模型历史，最多保留 5个历史版本，每次更新模型时都需要调用此方法生成历史
     * @param id
     * @param newMeta
     * @param oldMeta
     * @param user
     * @param input 是否入口
     */
    public void addHistory(ObjectId id, MetadataInstancesDto newMeta, MetadataInstancesDto oldMeta, UserDetail user, boolean input) {
        CompareResult compare = compare(newMeta, oldMeta);
        if (compare == null) {
            return;
        }

        // 根据比对结果，判断是否生成历史版本
        boolean needAddHistory = false;
        if (CollectionUtils.isNotEmpty(compare.getProperties())) {
            for (ComparePropertiesResult property : compare.getProperties()) {
                if (!EQUALS.equals(property.getResult())) {
                    needAddHistory = true;
                    break;
                }
            }
        }

        if (CollectionUtils.isNotEmpty(compare.getFields())) {
            for (Map<String, Object> field : compare.getFields()) {
                if (!EQUALS.equals(field.get("result"))) {
                    needAddHistory = true;
                    break;
                }
            }
        }

        if (needAddHistory) {
            int newVersion = 2;
            MetadataInstancesDto historyModel = new MetadataInstancesDto();
            BeanUtils.copyProperties(oldMeta, historyModel);
            historyModel.setId(null);
            historyModel.setVersionUserId(user.getUserId());
            historyModel.setVersionUserName(user.getUsername());
            historyModel.setVersionDescription(compare.getVersionDescription());
            historyModel.setHistories(null);

            if (oldMeta.getVersion() != null) {
                newVersion = oldMeta.getVersion() + 1;
            } else {
                historyModel.setVersion(1);
            }

            if (input) {
                Update update = new Update();
                update.set("version", newVersion);
                ArrayList<MetadataInstancesDto> hisModels = new ArrayList<>();
                hisModels.add(historyModel);
                BasicDBObject basicDBObject = new BasicDBObject("$each", hisModels);
                basicDBObject.append("$slice", -5);
                update.push("histories", basicDBObject);
                Criteria criteria = Criteria.where("id").is(id);
                metadataInstancesService.update(new Query(criteria), update, user);
            } else {
                List<MetadataInstancesDto> histories = newMeta.getHistories() == null ? new ArrayList<>() : newMeta.getHistories();
                histories.add(historyModel);
                while (histories.size() > 5) {
                    histories.remove(0);
                }
            }
        }
    }


    public MetadataInstancesDto modelNext(MetadataInstancesDto newModel, DataSourceConnectionDto connection, String databaseId, UserDetail user) {
        List<MetadataInstancesDto> metadataInstancesDtos = modelNext(Lists.of(newModel), connection, databaseId, user);
        if (CollectionUtils.isNotEmpty(metadataInstancesDtos)) {
            return metadataInstancesDtos.get(0);
        }
        return null;
    }
    public List<MetadataInstancesDto> modelNext(List<MetadataInstancesDto> newModels, DataSourceConnectionDto connection, String databaseId, UserDetail user) {
        if (newModels == null || connection == null) {
            log.info("Finished update new models, newModels = {}, connection = {}", newModels == null ? "" : newModels.size(), connection);
            return Lists.newArrayList();
        }


        Map<String, String> newModelMap = newModels.stream().collect(Collectors.toMap(MetadataInstancesDto::getOriginalName
                , m -> MetaDataBuilderUtils.generateQualifiedName(m.getMetaType(), connection, m.getOriginalName())));
        Criteria criteria = Criteria.where("qualified_name").in(newModelMap.values());
        List<MetadataInstancesDto> metadataInstancesDtos = metadataInstancesService.findAllDto(new Query(criteria), user);

        Map<String, MetadataInstancesDto> oldModeMap = metadataInstancesDtos.stream().collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName, m -> m));


        List<MetadataInstancesDto> newModelList = new ArrayList<>();
        for (MetadataInstancesDto newModel : newModels) {

            MetadataInstancesDto oldModel = oldModeMap.get(newModelMap.get(newModel.getOriginalName()));

            if (oldModel != null && CollectionUtils.isNotEmpty(oldModel.getFields()) && CollectionUtils.isNotEmpty(newModel.getFields())) {
                newModel.setName(StringUtils.isNotBlank(oldModel.getName()) ? oldModel.getName() : "");
                //newModel.setComment(StringUtils.isNotBlank(oldModel.getComment()) ? oldModel.getComment() : "");
                oldModel.setVersionTime(new Date());

                newModel.setHistories(oldModel.getHistories());
                addHistory(oldModel.getId(), newModel, oldModel, user, false);

            }

            if (CollectionUtils.isNotEmpty(newModel.getFields())) {
                for (Field field : newModel.getFields()) {
                    if (field.getIsAutoAllowed() == null) {
                        field.setIsAutoAllowed(true);
                    }
                    if (field.getSource() == null) {
                        field.setSource("auto");
                    }
                }
            }

            if (oldModel != null && CollectionUtils.isNotEmpty(oldModel.getFields())) {
                for (Field field : oldModel.getFields()) {
                    if (field.getIsAutoAllowed() == null) {
                        field.setIsAutoAllowed(true);
                    }
                    if (field.getSource() == null) {
                        field.setSource("auto");
                    }
                }
            }

            newModel = MetaDataBuilderUtils.build(newModel.getMetaType(), connection, user.getUserId(), user.getUsername(), newModel.getOriginalName(), newModel, oldModel, databaseId, null, newModelMap);
            newModelList.add(newModel);
        }

        return newModelList;
    }



    @Data
    @EqualsAndHashCode(callSuper=false)
    public static class ComparePropertiesResult {
        private String key;
        private Object baseValue;
        private Object compareValue;
        private String result;
        private boolean isCustom;
    }

    @Data
    @EqualsAndHashCode(callSuper=false)
    public static class CompareResult {
        private List<ComparePropertiesResult> properties;
        private List<Map<String, Object>> fields;
        private String versionDescription;
    }


}
