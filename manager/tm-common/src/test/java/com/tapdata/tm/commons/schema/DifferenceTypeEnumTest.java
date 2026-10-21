package com.tapdata.tm.commons.schema;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.util.PdkSchemaConvert;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DifferenceTypeEnumTest {
    private static final String PARTITION_FIELD = "pt_YEARPA_MONTPA";

    @ParameterizedTest
    @EnumSource(value = DifferenceTypeEnum.class, names = {"Different", "Precision"})
    void preservesNonPrimaryKeyWhenApplyingTypeDifference(DifferenceTypeEnum type) {
        Field source = field(PARTITION_FIELD, "INTEGER", false, null);
        Field target = field(PARTITION_FIELD, "INT", true, 4);
        List<Field> fields = new ArrayList<>(List.of(source));

        type.processDifferenceField(source, fields, difference(type, source, target));

        assertSame(target, fields.get(0));
        assertEquals("INT", target.getDataType());
        assertEquals("INT", target.getDataTypeTemp());
        assertFalse(target.getPrimaryKey());
        assertNull(target.getPrimaryKeyPosition());

        TapTable table = PdkSchemaConvert.toPdk(metadata(fields));
        assertTrue(assertDoesNotThrow(() -> table.primaryKeys()).isEmpty());
    }

    @ParameterizedTest
    @MethodSource("primaryKeyDefinitions")
    void preservesPrimaryKeyDefinitionWhenApplyingTypeDifference(
            DifferenceTypeEnum type, Boolean primaryKey, Integer position) {
        Field source = field(PARTITION_FIELD, "INTEGER", primaryKey, position);
        Field target = field(PARTITION_FIELD, "INT", !Boolean.TRUE.equals(primaryKey), 9);
        List<Field> fields = new ArrayList<>(List.of(source));

        type.processDifferenceField(source, fields, difference(type, source, target));

        assertEquals(primaryKey, target.getPrimaryKey());
        assertEquals(position, target.getPrimaryKeyPosition());
        assertEquals("INT", target.getDataType());
        assertDoesNotThrow(() -> PdkSchemaConvert.toPdk(metadata(fields)).primaryKeys());
    }

    static Stream<Arguments> primaryKeyDefinitions() {
        return Stream.of(DifferenceTypeEnum.Different, DifferenceTypeEnum.Precision)
                .flatMap(type -> Stream.of(
                        Arguments.of(type, false, null),
                        Arguments.of(type, false, 0),
                        Arguments.of(type, null, null),
                        Arguments.of(type, true, 0),
                        Arguments.of(type, true, 1),
                        Arguments.of(type, true, 3)));
    }

    @ParameterizedTest
    @EnumSource(value = DifferenceTypeEnum.class, names = {"Different", "Precision"})
    void supportsLogicUpdateKeysAfterApplyingDifference(DifferenceTypeEnum type) {
        Field addedField = field(PARTITION_FIELD, "INTEGER", false, null);
        Field targetField = field(PARTITION_FIELD, "INT", true, 4);
        MetadataInstancesDto metadata = metadata(new ArrayList<>(List.of(
                field("ACCTPA", "DECIMAL(9, 0)", true, 1),
                field("YEARPA", "DECIMAL(4, 0)", true, 2),
                field("MONTPA", "DECIMAL(2, 0)", true, 3),
                addedField)));
        metadata.setQualifiedName("repro-qualified-name");
        DAG.Options options = new DAG.Options();
        options.setDifferenceFields(Map.of(metadata.getQualifiedName(),
                List.of(difference(type, addedField, targetField))));

        options.processDifferenceField(metadata);
        TapTable table = PdkSchemaConvert.toPdk(metadata);

        List<String> sourceKeys = List.of("ACCTPA", "YEARPA", "MONTPA");
        assertEquals(sourceKeys, new ArrayList<>(assertDoesNotThrow(() -> table.primaryKeys())));
        assertFalse(table.getNameFieldMap().get(PARTITION_FIELD).getPrimaryKey());
        assertNull(table.getNameFieldMap().get(PARTITION_FIELD).getPrimaryKeyPos());

        List<String> updateConditions = List.of("ACCTPA", "YEARPA", "MONTPA", PARTITION_FIELD);
        table.setLogicPrimaries(updateConditions);
        assertEquals(updateConditions, new ArrayList<>(table.primaryKeys(true)));
        assertEquals(sourceKeys, new ArrayList<>(table.primaryKeys()));
    }

    private static Field field(String name, String dataType, Boolean primaryKey, Integer position) {
        Field field = new Field();
        field.setFieldName(name);
        field.setOriginalFieldName(name);
        field.setDataType(dataType);
        field.setPrimaryKey(primaryKey);
        field.setPrimaryKeyPosition(position);
        field.setIsNullable(!Boolean.TRUE.equals(primaryKey));
        field.setAutoincrement("NO");
        return field;
    }

    private static DifferenceField difference(DifferenceTypeEnum type, Field source, Field target) {
        return DifferenceField.builder()
                .columnName(source.getFieldName())
                .sourceField(source)
                .targetField(target)
                .type(type)
                .build();
    }

    private static MetadataInstancesDto metadata(List<Field> fields) {
        MetadataInstancesDto metadata = new MetadataInstancesDto();
        metadata.setOriginalName("tap13093-repro");
        metadata.setFields(fields);
        return metadata;
    }
}
