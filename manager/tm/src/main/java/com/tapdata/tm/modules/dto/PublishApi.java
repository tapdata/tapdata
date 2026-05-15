package com.tapdata.tm.modules.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Tag;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.Param;
import com.tapdata.tm.module.dto.PathSetting;
import com.tapdata.tm.module.dto.Sort;
import com.tapdata.tm.module.dto.Where;
import com.tapdata.tm.module.entity.Path;
import lombok.Data;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/11 10:43 Create
 * @description
 */
@Data
public class PublishApi {
    private String id;

    private String name;

    @JsonProperty("datasource")
    private String dataSource;

    @JsonProperty("tableName")
    private String tableName;

    private String apiVersion;

    private String basePath;

    private String readPreference;
    private String readPreferenceTag;

    private String readConcern;


    private String description;

    private String describtion;

    private String prefix;

    private String path;

    private String apiType;

    private String status;

    private List<SimplifyPath> paths;
    private List<SimplifyField> fields;

    private List<Tag> listtags;

    private String project;
    private String createType;

    private String connectionId;

    private ObjectId connection;

    private String connectionType;


    /**
     * 访问路径方式  默认值 default  自定义 customize
     */
    private String pathAccessMethod;

    /**
     * 限制条数
     */
    private Integer limit;

    /**
     * 用户可以自定义路径最后关键字，没有设置按默认值处理
     */
    private List<PathSetting> pathSetting;

    private String publishStatus;

    public static PublishApi from(ModulesDto dto) {
        if (null == dto) {
            return null;
        }
        PublishApi api = new PublishApi();
        api.setName(dto.getName());
        api.setDataSource(dto.getDataSource());
        api.setTableName(dto.getTableName());
        api.setApiVersion(dto.getApiVersion());
        api.setBasePath(dto.getBasePath());
        api.setReadPreference(dto.getReadPreference());
        api.setReadPreferenceTag(dto.getReadPreferenceTag());
        api.setReadConcern(dto.getReadConcern());
        api.setDescription(dto.getDescription());
        api.setDescribtion(dto.getDescribtion());
        api.setPrefix(dto.getPrefix());
        api.setPath(dto.getPath());
        api.setApiType(dto.getApiType());
        api.setStatus(dto.getStatus());
        api.setPaths(SimplifyPath.from(dto.getPaths()));
        api.setFields(SimplifyField.from(dto.getFields()));
        api.setListtags(dto.getListtags());
        api.setProject(dto.getProject());
        api.setCreateType(dto.getCreateType());
        api.setConnectionId(dto.getConnectionId());
        api.setConnection(dto.getConnection());
        api.setConnectionType(dto.getConnectionType());
        api.setPathAccessMethod(dto.getPathAccessMethod());
        api.setLimit(dto.getLimit());
        api.setPathSetting(dto.getPathSetting());
        api.setPublishStatus(dto.getPublishStatus());
        return api;
    }

    @Data
    public static class SimplifyPath {
        String name;
        private String method;
        private String result;
        private Object condition;
        private Object filter;

        private String createType;

        private List<SimplifyField> fields;

        private String type;

        private List<String > acl;


        private List<Param> params;

        private String path;
        private String description;

        private List<Where> where;
        private List<Sort> sort;

        @JsonProperty("customWhere")
        private String customWhere;

        public static List<SimplifyPath> from(List<Path> paths) {
            List<SimplifyPath> simplifyPaths = new ArrayList<>();
            if (null != paths) {
                paths.stream().map(SimplifyPath::from).filter(Objects::nonNull).forEach(simplifyPaths::add);
            }
            return simplifyPaths;
        }

        public static SimplifyPath from(Path path) {
            if (path == null) {
                return null;
            }
            SimplifyPath simplifyPath = new SimplifyPath();
            simplifyPath.setName(path.getName());
            simplifyPath.setMethod(path.getMethod());
            simplifyPath.setResult(path.getResult());
            simplifyPath.setCondition(path.getCondition());
            simplifyPath.setFilter(path.getFilter());
            simplifyPath.setCreateType(path.getCreateType());
            if (null != path.getFields()) {
                simplifyPath.setFields(path.getFields().stream().map(SimplifyField::from).filter(Objects::nonNull).toList());
            }
            simplifyPath.setType(path.getType());
            simplifyPath.setAcl(path.getAcl());
            simplifyPath.setParams(path.getParams());
            simplifyPath.setPath(path.getPath());
            simplifyPath.setDescription(path.getDescription());
            simplifyPath.setWhere(path.getWhere());
            simplifyPath.setSort(path.getSort());
            simplifyPath.setCustomWhere(path.getCustomWhere());
            return simplifyPath;
        }
    }

    @Data
    public static class SimplifyField {
        private String tapType;

        private String simpleTypeName;

        @JsonProperty("data_type")
        private String dataType;

        @JsonProperty("field_name")
        private String fieldName;

        @JsonProperty("field_alias")
        private String fieldAlias;

        private Boolean primaryKey;

        @JsonProperty("primary_key_position")
        private Integer primaryKeyPosition;

        private String source;

        private String key;

        @JsonProperty("alias_name")
        private String aliasName;

        private String comment;


        private String originalDataType;

        private String field_type;

        private String sourceDbType;

        /**
         * @see com.tapdata.tm.commons.schema.enums.TableFieldTag
         * （API Server）Currently, only when users add new fields on the front end, they are marked as follows: USER_CREATE
         * */
        private String tag;

        /**
         * Sensitive Fields - Encryption Rule List
         * */
        private List<String> textEncryptionRuleIds;

        public static List<SimplifyField> from(List<Field> field) {
            List<SimplifyField> simplifyFields = new ArrayList<>();
            if (null != field) {
                field.forEach(field1 -> simplifyFields.add(SimplifyField.from(field1)));
            }
            return simplifyFields;
        }

        public static SimplifyField from(Field field) {
            if (field == null) {return null;}
            SimplifyField simplifyField = new SimplifyField();
            simplifyField.setTapType(field.getTapType());
            simplifyField.setSimpleTypeName(field.getSimpleTypeName());
            simplifyField.setDataType(field.getDataType());
            simplifyField.setFieldName(field.getFieldName());
            simplifyField.setFieldAlias(field.getFieldAlias());
            simplifyField.setPrimaryKey(field.getPrimaryKey());
            simplifyField.setPrimaryKeyPosition(field.getPrimaryKeyPosition());
            simplifyField.setSource(field.getSource());
            simplifyField.setKey(field.getKey());
            simplifyField.setAliasName(field.getAliasName());
            simplifyField.setComment(field.getComment());
            simplifyField.setOriginalDataType(field.getOriginalDataType());
            simplifyField.setField_type(field.getField_type());
            simplifyField.setSourceDbType(field.getSourceDbType());
            simplifyField.setTag(field.getTag());
            simplifyField.setTextEncryptionRuleIds(field.getTextEncryptionRuleIds());
            return simplifyField;
        }
    }
}
