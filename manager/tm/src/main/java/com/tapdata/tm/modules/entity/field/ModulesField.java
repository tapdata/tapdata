package com.tapdata.tm.modules.entity.field;

import com.tapdata.tm.base.field.CollectionField;

/**
 * @see com.tapdata.tm.modules.entity.ModulesEntity
 */
public enum ModulesField implements CollectionField {
    NAME("name"),
    DATA_SOURCE("datasource"),
    TABLE_NAME("tableName"),
    API_VERSION("apiVersion"),
    BASE_PATH("basePath"),
    READ_PREFERENCE("readPreference"),
    READ_CONCERN("readConcern"),
    DESCRIPTION_0("describtion"),
    PREFIX("prefix"),
    PROJECT("project"),
    API_TYPE("apiType"),
    STATUS("status"),
    PATHS("paths"),
    FIELDS("fields"),
    LIST_TAGS("listtags"),
    CREATE_TYPE("createType"),
    IS_DELETED("is_deleted"),
    RES_ROWS("res_rows"),
    VISIT_COUNT("visitCount"),
    CONNECTION("connection"),
    FAIL_RATE("failRate"),
    RESPONSE_TIME("responseTime"),
    LATENCY("latency"),
    REQ_BYTES("req_bytes"),
    CONNECTION_ID("connectionId"),
    OPERATION_TYPE("operationType"),
    CONNECTION_TYPE("connectionType"),
    CONNECTION_NAME("connectionName"),
    DESCRIPTION("description"),
    PATH_ACCESS_METHOD("pathAccessMethod"),
    LIMIT("limit"),
    PATH_SETTING("pathSetting"),
    API_ALARM_CONFIG("apiAlarmConfig");
    final String field;

    ModulesField(String field) {
        this.field = field;
    }


    @Override
    public String field() {
        return this.field;
    }

    public enum Field implements CollectionField {
        AUTOINCREMENT(ModulesField.FIELDS.field.concat(".autoincrement")),
        AUTO_INC_START_VALUE(ModulesField.FIELDS.field.concat(".auto_inc_start_value")),
        AUTO_INCREMENT_VALUE(ModulesField.FIELDS.field.concat(".auto_increment_value")),
        AUTO_INC_CACHE_VALUE(ModulesField.FIELDS.field.concat(".auto_inc_cache_value")),
        SEQUENCE_NAME(ModulesField.FIELDS.field.concat(".sequence_name")),
        COLUMN_SIZE(ModulesField.FIELDS.field.concat(".columnSize")),
        DATA_CODE(ModulesField.FIELDS.field.concat(".data_code")),
        DATA_TYPE(ModulesField.FIELDS.field.concat(".dataType")),
        TAP_TYPE(ModulesField.FIELDS.field.concat(".tapType")),
        PURE_DATA_TYPE(ModulesField.FIELDS.field.concat(".pure_data_type")),
        DATA_LENGTH(ModulesField.FIELDS.field.concat(".data_length")),
        PREVIOUS_DATA_TYPE(ModulesField.FIELDS.field.concat(".previousDataType")),
        SELECT_DATA_TYPE(ModulesField.FIELDS.field.concat(".selectDataType")),
        DATA_TYPE_TEMP(ModulesField.FIELDS.field.concat(".dataTypeTemp")),
        DEFAULT_VALUE(ModulesField.FIELDS.field.concat(".default_value")),
        DEFAULT_FUNCTION(ModulesField.FIELDS.field.concat(".default_function")),
        ORIGINAL_DEFAULT_VALUE(ModulesField.FIELDS.field.concat(".originalDefaultValue")),
        FIELD_NAME(ModulesField.FIELDS.field.concat(".field_name")),
        FIELD_ALIAS(ModulesField.FIELDS.field.concat(".field_alias")),
        PREVIOUS_FIELD_NAME(ModulesField.FIELDS.field.concat(".previousFieldName")),
        FOREIGN_KEY_POSITION(ModulesField.FIELDS.field.concat(".foreign_key_position")),
        ID(ModulesField.FIELDS.field.concat(".id")),
        IS_ANALYZE(ModulesField.FIELDS.field.concat(".isAnalyze")),
        IS_AUTO_ALLOWED(ModulesField.FIELDS.field.concat(".is_auto_allowed")),
        IS_DELETED(ModulesField.FIELDS.field.concat(".is_deleted")),
        IS_NULLABLE(ModulesField.FIELDS.field.concat(".is_nullable")),
        IS_PRECISION_EDIT(ModulesField.FIELDS.field.concat(".isPrecisionEdit")),
        IS_SCALE_EDIT(ModulesField.FIELDS.field.concat(".isScaleEdit")),
        JAVA_TYPE(ModulesField.FIELDS.field.concat(".java_type")),
        ORI_PRECISION(ModulesField.FIELDS.field.concat(".oriPrecision")),
        ORI_SCALE(ModulesField.FIELDS.field.concat(".oriScale")),
        ORIGINAL_FIELD_NAME(ModulesField.FIELDS.field.concat(".original_field_name")),
        ORIGINAL_JAVA_TYPE(ModulesField.FIELDS.field.concat(".original_java_type")),
        PARENT(ModulesField.FIELDS.field.concat(".parent")),
        DATA_PRECISION(ModulesField.FIELDS.field.concat(".data_precision")),
        ORIGINAL_PRECISION(ModulesField.FIELDS.field.concat(".originalPrecision")),
        PRIMARY_KEY(ModulesField.FIELDS.field.concat(".primaryKey")),
        PRIMARY_KEY_POSITION(ModulesField.FIELDS.field.concat(".primary_key_position")),
        DATA_SCALE(ModulesField.FIELDS.field.concat(".data_scale")),
        ORIGINAL_SCALE(ModulesField.FIELDS.field.concat(".originalScale")),
        UNIQUE(ModulesField.FIELDS.field.concat(".unique")),
        KEY(ModulesField.FIELDS.field.concat(".key")),
        PK_CONSTRAINT_NAME(ModulesField.FIELDS.field.concat(".pk_constraint_name")),
        PK_CONSTRAINT_NAME_1(ModulesField.FIELDS.field.concat(".pkConstraintName")),
        FOREIGN_KEY(ModulesField.FIELDS.field.concat(".foreign_key")),
        FOREIGN_KEY_TABLE(ModulesField.FIELDS.field.concat(".foreign_key_table")),
        FOREIGN_KEY_COLUMN(ModulesField.FIELDS.field.concat(".foreign_key_column")),
        NODE_DATA_TYPE(ModulesField.FIELDS.field.concat(".node_data_type")),
        TABLE_NAME(ModulesField.FIELDS.field.concat(".table_name")),
        ALIAS_NAME(ModulesField.FIELDS.field.concat(".alias_name")),
        VISIBLE(ModulesField.FIELDS.field.concat(".visible")),
        COMMENT(ModulesField.FIELDS.field.concat(".comment")),
        DESCRIPTION(ModulesField.FIELDS.field.concat(".description")),
        COLUMN_POSITION(ModulesField.FIELDS.field.concat(".columnPosition")),
        ORIGINAL_DATA_TYPE(ModulesField.FIELDS.field.concat(".originalDataType")),
        FIELD_TYPE(ModulesField.FIELDS.field.concat(".field_type")),
        REQUIRED(ModulesField.FIELDS.field.concat(".required")),
        EXAMPLE(ModulesField.FIELDS.field.concat(".example")),
        DICTIONARY(ModulesField.FIELDS.field.concat(".dictionary")),
        SOURCE_DB_TYPE(ModulesField.FIELDS.field.concat(".sourceDbType")),
        FIXED(ModulesField.FIELDS.field.concat(".fixed")),
        DATA_TYPE_SUPPORT(ModulesField.FIELDS.field.concat(".dataTypeSupport")),
        CREATE_SOURCE(ModulesField.FIELDS.field.concat(".createSource")),
        CHANGE_RULE_ID(ModulesField.FIELDS.field.concat(".changeRuleId")),
        use_default_value(ModulesField.FIELDS.field.concat(".useDefaultValue")),
        TEXT_ENCRYPTION_RULE_IDS(ModulesField.FIELDS.field.concat(".textEncryptionRuleIds"));
        final String field;

        Field(String field) {
            this.field = field;
        }


        @Override
        public String field() {
            return this.field;
        }
    }

    public enum Tag implements CollectionField {
        ID(ModulesField.LIST_TAGS.field.concat(".id")),
        VALUE(ModulesField.LIST_TAGS.field.concat(".value"));
        final String field;

        Tag(String field) {
            this.field = field;
        }


        @Override
        public String field() {
            return this.field;
        }
    }

    public enum PathSetting implements CollectionField {
        TYPE(ModulesField.PATH_SETTING.field.concat(".type")),
        PATH(ModulesField.PATH_SETTING.field.concat(".path")),
        METHOD(ModulesField.PATH_SETTING.field.concat(".method"));
        final String field;

        PathSetting(String field) {
            this.field = field;
        }

        @Override
        public String field() {
            return this.field;
        }
    }

    public enum ApiAlarmConfig implements CollectionField {
        ALARM_SETTINGS("alarmSettings"),
        ALARM_RULES("alarmRules"),
        EMAIL_RECEIVERS("emailReceivers");

        final String field;

        ApiAlarmConfig(String field) {
            this.field = field;
        }


        @Override
        public String field() {
            return this.field;
        }

        public enum AlarmSetting implements CollectionField {
            TYPE(ApiAlarmConfig.ALARM_SETTINGS.field.concat(".type")),
            open(ApiAlarmConfig.ALARM_SETTINGS.field.concat(".open")),
            KEY(ApiAlarmConfig.ALARM_SETTINGS.field.concat(".key")),
            SORT(ApiAlarmConfig.ALARM_SETTINGS.field.concat(".sort")),
            NOTIFY(ApiAlarmConfig.ALARM_SETTINGS.field.concat(".notify")),
            INTERVAL(ApiAlarmConfig.ALARM_SETTINGS.field.concat(".interval")),
            UNIT(ApiAlarmConfig.ALARM_SETTINGS.field.concat(".unit")),
            PARAMS(ApiAlarmConfig.ALARM_SETTINGS.field.concat(".params"));
            final String field;

            AlarmSetting(String field) {
                this.field = field;
            }

            @Override
            public String field() {
                return this.field;
            }
        }

        public enum AlarmRule implements CollectionField {
            KEY(ApiAlarmConfig.ALARM_RULES.field.concat(".key")),
            POINT(ApiAlarmConfig.ALARM_RULES.field.concat(".point")),
            EQUALS_FLAG(ApiAlarmConfig.ALARM_RULES.field.concat(".equalsFlag")),
            MS(ApiAlarmConfig.ALARM_RULES.field.concat(".ms")),
            TIMES(ApiAlarmConfig.ALARM_RULES.field.concat(".times")),
            VALUE(ApiAlarmConfig.ALARM_RULES.field.concat(".value")),
            UNIT(ApiAlarmConfig.ALARM_RULES.field.concat(".unit"));
            final String field;

            AlarmRule(String field) {
                this.field = field;
            }


            @Override
            public String field() {
                return this.field;
            }
        }
    }
}
