package com.tapdata.tm.commons.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

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

    public static DifferenceField buildCannotWriteField(String columnName,Field sourceField) {
        return DifferenceField.builder().columnName(columnName).sourceField(sourceField).type(DifferenceTypeEnum.CannotWrite).build();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DifferenceField that = (DifferenceField) obj;

        if (!Objects.equals(columnName, that.columnName) || !Objects.equals(type, that.type)) {
            return false;
        }

        if (type == null) {
            return false;
        }

        return switch (type) {
            case Missing,CannotWrite -> Objects.equals(sourceField, that.sourceField);
            case Additional -> Objects.equals(targetField, that.targetField);
            case Different ->
                Objects.equals(sourceField, that.sourceField) &&
                Objects.equals(targetField, that.targetField);
            default -> false;
        };
    }

    @Override
    public int hashCode() {
        if (type == null) {
            return Objects.hash(columnName);
        }

        return switch (type) {
            case Missing,CannotWrite -> Objects.hash(columnName, type, sourceField);
            case Additional -> Objects.hash(columnName, type, targetField);
            case Different -> Objects.hash(columnName, type, sourceField, targetField);
            default -> Objects.hash(columnName, type);
        };
    }

}
