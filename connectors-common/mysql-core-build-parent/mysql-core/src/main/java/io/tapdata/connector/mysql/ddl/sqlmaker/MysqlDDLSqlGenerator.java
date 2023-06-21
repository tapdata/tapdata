package io.tapdata.connector.mysql.ddl.sqlmaker;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.ddl.DDLSqlGenerator;
import io.tapdata.connector.mysql.util.MysqlUtil;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * @author samuel
 * @Description
 * @create 2022-07-11 17:44
 **/
public class MysqlDDLSqlGenerator implements DDLSqlGenerator {

    protected final static String ALTER_TABLE_PREFIX = "alter table `%s`.`%s`";
    private static final String TAG = MysqlDDLSqlGenerator.class.getSimpleName();
    protected final String version;
    protected KVReadOnlyMap<TapTable> tableMap;

    public MysqlDDLSqlGenerator(String version, KVReadOnlyMap<TapTable> tableMap) {
        this.version = version;
        this.tableMap = tableMap;
    }

    @Override
    public List<String> addColumn(CommonDbConfig config, TapNewFieldEvent tapNewFieldEvent) {
        List<String> sqls = new ArrayList<>();
        if (null == tapNewFieldEvent) {
            return null;
        }
        List<TapField> newFields = tapNewFieldEvent.getNewFields();
        if (null == newFields) {
            return null;
        }
        String tableId = tapNewFieldEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append add column ddl sql failed, table name is blank");
        }
        for (TapField newField : newFields) {
            StringBuilder sql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, config.getDatabase(), tableId)).append(" add");
            String fieldName = newField.getName();
            if (StringUtils.isNotBlank(fieldName)) {
                sql.append(" `").append(fieldName).append("`");
            } else {
                throw new RuntimeException("Append add column ddl sql failed, field name is blank");
            }
            String dataType = newField.getDataType();
            try {
                dataType = MysqlUtil.fixDataType(dataType, version);
            } catch (Exception e) {
                TapLogger.warn(TAG, e.getMessage());
            }
            if (StringUtils.isNotBlank(dataType)) {
                sql.append(" ").append(dataType);
            } else {
                throw new RuntimeException("Append add column ddl sql failed, data type is blank");
            }
            Boolean nullable = newField.getNullable();
            if (null != nullable) {
                if (nullable) {
                    sql.append(" null");
                } else {
                    sql.append(" not null");
                }
            }
            Object defaultValue = newField.getDefaultValue();
            if (null != defaultValue) {
                sql.append(" default '").append(defaultValue).append("'");
            }
            String comment = newField.getComment();
            if (StringUtils.isNotBlank(comment)) {
                sql.append(" comment '").append(comment).append("'");
            }
            Boolean primaryKey = newField.getPrimaryKey();
            if (null != primaryKey && primaryKey) {
                sql.append(" key");
            }
            sqls.add(sql.toString());
        }
        return sqls;
    }

    @Override
    public List<String> alterColumnAttr(CommonDbConfig config, TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent) {
        if (null == tapAlterFieldAttributesEvent) {
            return null;
        }
        String tableId = tapAlterFieldAttributesEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append alter column attr ddl sql failed, table name is blank");
        }
        StringBuilder sql = new StringBuilder(String.format(ALTER_TABLE_PREFIX, config.getDatabase(), tableId)).append(" modify");
        String fieldName = tapAlterFieldAttributesEvent.getFieldName();
        if (StringUtils.isNotBlank(fieldName)) {
            sql.append(" `").append(fieldName).append("`");
        } else {
            throw new RuntimeException("Append alter column attr ddl sql failed, field name is blank");
        }
        ValueChange<String> dataTypeChange = tapAlterFieldAttributesEvent.getDataTypeChange();
        if (StringUtils.isNotBlank(dataTypeChange.getAfter())) {
            String dataTypeChangeAfter = dataTypeChange.getAfter();
            try {
                dataTypeChangeAfter = MysqlUtil.fixDataType(dataTypeChangeAfter, version);
            } catch (Exception e) {
                TapLogger.warn(TAG, e.getMessage());
            }
            sql.append(" ").append(dataTypeChangeAfter);
        } else {
            throw new RuntimeException("Append alter column attr ddl sql failed, data type is blank");
        }
        ValueChange<Boolean> nullableChange = tapAlterFieldAttributesEvent.getNullableChange();
        if (null != nullableChange && null != nullableChange.getAfter()) {
            if (nullableChange.getAfter()) {
                sql.append(" null");
            } else {
                sql.append(" not null");
            }
        }
        ValueChange<String> commentChange = tapAlterFieldAttributesEvent.getCommentChange();
        if (null != commentChange && StringUtils.isNotBlank(commentChange.getAfter())) {
            sql.append(" comment '").append(commentChange.getAfter()).append("'");
        }
        ValueChange<Integer> primaryChange = tapAlterFieldAttributesEvent.getPrimaryChange();
        if (null != primaryChange && null != primaryChange.getAfter() && primaryChange.getAfter() > 0) {
            sql.append(" key");
        }
        return Collections.singletonList(sql.toString());
    }

    @Override
    public List<String> alterColumnName(CommonDbConfig config, TapAlterFieldNameEvent tapAlterFieldNameEvent) {
        if (null == tapAlterFieldNameEvent) {
            return null;
        }
        String tableId = tapAlterFieldNameEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append alter column name ddl sql failed, table name is blank");
        }
        ValueChange<String> nameChange = tapAlterFieldNameEvent.getNameChange();
        if (null == nameChange) {
            throw new RuntimeException("Append alter column name ddl sql failed, change name object is null");
        }
        String before = nameChange.getBefore();
        String after = nameChange.getAfter();
        if (StringUtils.isBlank(before)) {
            throw new RuntimeException("Append alter column name ddl sql failed, old column name is blank");
        }
        if (StringUtils.isBlank(after)) {
            throw new RuntimeException("Append alter column name ddl sql failed, new column name is blank");
        }
        Integer subVersion = MysqlUtil.getSubVersion(version, 1);
        String sql = String.format(ALTER_TABLE_PREFIX, config.getDatabase(), tableId);
        if (subVersion != null && subVersion >= 8) {
            return Collections.singletonList(sql + " rename column `" + before + "` to `" + after + "`");
        } else {
            TapTable tapTable = tableMap.get(tableId);
            if (tapTable == null) {
                throw new RuntimeException("Append alter column name ddl sql failed, tapTable is blank");
            }
            Optional<TapField> tapFieldOptional = tapTable.getNameFieldMap().entrySet().stream()
                    .filter(e -> StringUtils.equals(e.getKey(), after)).map(Map.Entry::getValue).findFirst();
            if (!tapFieldOptional.isPresent()) {
                throw new RuntimeException("Append alter column name ddl sql failed, field is blank");
            }
            TapField field = tapFieldOptional.get();
            sql += " change `" + before + "` " + "`" + after + "` " + field.getDataType();
            if (null != field.getAutoInc() && field.getAutoInc()) {
                if (field.getPrimaryKeyPos() == 1) {
                    sql += " auto_increment";
                } else {
                    TapLogger.warn(TAG, "Field \"{}\" cannot be auto increment in mysql, there can be only one auto column and it must be defined the first key", field.getName());
                }
            }
            if (field.getNullable()) {
                sql += " null";
            } else {
                sql += " not null";
            }
            // default value
            String defaultValue = field.getDefaultValue() == null ? "" : field.getDefaultValue().toString();
            if (StringUtils.isNotBlank(defaultValue)) {
                sql += " default '" + defaultValue + "'";
            }

            // comment
            String comment = field.getComment();
            if (StringUtils.isNotBlank(comment)) {
                // try to escape the single quote in comments
                comment = comment.replace("'", "''");
                sql += " comment '" + comment + "'";
            }

            Boolean primaryKey = field.getPrimaryKey();
            if (null != primaryKey && primaryKey) {
                sql += " key";
            }
            return Collections.singletonList(sql);
        }
    }

    @Override
    public List<String> dropColumn(CommonDbConfig config, TapDropFieldEvent tapDropFieldEvent) {
        if (null == tapDropFieldEvent) {
            return null;
        }
        String tableId = tapDropFieldEvent.getTableId();
        if (StringUtils.isBlank(tableId)) {
            throw new RuntimeException("Append drop column ddl sql failed, table name is blank");
        }
        String fieldName = tapDropFieldEvent.getFieldName();
        if (StringUtils.isBlank(fieldName)) {
            throw new RuntimeException("Append drop column ddl sql failed, field name is blank");
        }
        return Collections.singletonList(String.format(ALTER_TABLE_PREFIX, config.getDatabase(), tableId) + " drop `" + fieldName + "`");
    }
}
