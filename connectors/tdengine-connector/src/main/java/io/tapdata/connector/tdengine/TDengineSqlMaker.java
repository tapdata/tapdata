package io.tapdata.connector.tdengine;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

public class TDengineSqlMaker {

    public static String buildColumnDefinition(TapTable tapTable, String timestampField) {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();

        return nameFieldMap.entrySet().stream()
                .peek(v -> {
                    if (StringUtils.equals(timestampField, v.getValue().getName())) {
                        v.getValue().setPos(0);
                    }
                })
                .sorted(Comparator.comparing(v -> EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos()))
                .map(v -> {
                    StringBuilder builder = new StringBuilder();
                    TapField tapField = v.getValue();
                    //ignore those which has no dataType
                    if (tapField.getDataType() == null) {
                        return "";
                    }
                    builder.append('`').append(tapField.getName()).append("` ").append(tapField.getDataType()).append(' ');
                    return builder.toString();
                }).collect(Collectors.joining(", "));
    }

}
