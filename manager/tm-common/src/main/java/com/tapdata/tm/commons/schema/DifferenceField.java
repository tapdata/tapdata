package com.tapdata.tm.commons.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DifferenceField {
    public static final String APPLY_TYPE_AUTO = "auto";
    public static final String APPLY_TYPE_MANUAL = "manual";
    private String columnName;
    private Field sourceField;
    private Field targetField;
    private DifferenceTypeEnum type;
    private String applyType;


    public static DifferenceField buildMissingField(String columnName,Field sourceField) {
        return DifferenceField.builder().columnName(columnName).sourceField(sourceField).type(DifferenceTypeEnum.Missing).build();
    }

    public static DifferenceField buildAdditionalField(String columnName,Field targetField) {
        return DifferenceField.builder().columnName(columnName).targetField(targetField).type(DifferenceTypeEnum.Additional).build();
    }

    public static DifferenceField buildDifferentField(String columnName,Field sourceField,Field targetField) {
        return DifferenceField.builder().columnName(columnName).sourceField(sourceField).targetField(targetField).type(DifferenceTypeEnum.Different).build();
    }

    public static DifferenceField buildCannotWriteField(String columnName,Field sourceField,Field targetField) {
        return DifferenceField.builder().columnName(columnName).sourceField(sourceField).targetField(targetField).type(DifferenceTypeEnum.CannotWrite).build();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (obj instanceof DifferenceField that) {
            return switch (type) {
                case Missing -> columnName.equals(that.columnName) && type.equals(that.type) && sourceField.toString().equals(that.sourceField.toString());
                case Additional -> columnName.equals(that.columnName) && type.equals(that.type) && targetField.toString().equals(that.targetField.toString());
                case Different,CannotWrite ->
                        columnName.equals(that.columnName) && type.equals(that.type) && sourceField.toString().equals(that.sourceField.toString()) && targetField.toString().equals(that.targetField.toString());
                default -> false;
            };

        }
        return false;
    }

}
