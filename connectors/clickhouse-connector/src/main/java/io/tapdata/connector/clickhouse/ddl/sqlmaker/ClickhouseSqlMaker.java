package io.tapdata.connector.clickhouse.ddl.sqlmaker;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

public class ClickhouseSqlMaker extends CommonSqlMaker {

    private String clickhouseVersion;

    public ClickhouseSqlMaker withVersion(String clickhouseVersion) {
        this.clickhouseVersion = clickhouseVersion;
        return this;
    }

    public String buildColumnDefinition(TapTable tapTable, boolean needComment) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        //no primary key,need judge logic primary key
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
//        Collection<String> logicPrimaryKeys = EmptyKit.isNotEmpty(primaryKeys) ? Collections.emptyList() : tapTable.primaryKeys(true);
        return nameFieldMap.entrySet().stream().sorted(Comparator.comparing(v ->
                EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(v -> { //pos may be null
            StringBuilder builder = new StringBuilder();
            TapField tapField = v.getValue();
            //ignore those which has no dataType
            if (tapField.getDataType() == null) {
                return "";
            }
            String dataType = tapField.getDataType();
            dataType = dataType.replace("unsigned", "").replace("UNSIGNED", "");
            if (Double.parseDouble(clickhouseVersion) <= 21.8) {
                if (dataType.startsWith("DateTime(")) {
                    dataType = "DateTime";
                }
                if (dataType.contains("DateTime64(")) {
                    dataType = "DateTime64";
                }
            }
            builder.append('\"').append(tapField.getName()).append("\" ");
            //null to omit
            if (tapField.getNullable() != null && tapField.getNullable() && !primaryKeys.contains(tapField.getName())) {
                builder.append("Nullable(").append(dataType).append(")").append(' ');
            } else {
                builder.append(dataType).append(' ');
            }


            //null to omit
            if (tapField.getDefaultValue() != null && !"".equals(tapField.getDefaultValue())) {
                builder.append("DEFAULT").append(' ');
                if (tapField.getDefaultValue() instanceof Number) {
                    builder.append(tapField.getDefaultValue()).append(' ');
                } else {
                    builder.append("'").append(tapField.getDefaultValue()).append("' ");
                }
            }
            if (needComment && EmptyKit.isNotBlank(tapField.getComment())) {
                String comment = tapField.getComment();
                comment = comment.replace("'", "''");
                builder.append("comment '").append(comment).append("' ");
            }
            return builder.toString();
        }).collect(Collectors.joining(", "));
    }
}
