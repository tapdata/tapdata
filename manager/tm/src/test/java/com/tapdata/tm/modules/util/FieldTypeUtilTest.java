package com.tapdata.tm.modules.util;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.enums.TableFieldTag;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.entity.Path;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FieldTypeUtilTest {

    @Test
    void testParseTapType() {
        Field field = new Field();
        field.setTapType(FieldTypeUtil.FILED_TYPE.get("Array"));
        FieldTypeUtil.parseTapType(field);
        assertEquals("Array", field.getSimpleTypeName());
    }

    @Test
    void testParseTapTypeListEmptyAndNotEmpty() {
        FieldTypeUtil.parseTapType((List<Field>) null);
        FieldTypeUtil.parseTapType(new ArrayList<>());

        Field field = new Field();
        field.setTapType(FieldTypeUtil.FILED_TYPE.get("Map"));
        List<Field> fields = new ArrayList<>();
        fields.add(field);
        FieldTypeUtil.parseTapType(fields);
        assertEquals("Map", field.getSimpleTypeName());
    }

    @Test
    void testGenericFieldIfNeedGenerateTapTypeAndOriginal() {
        Field field = new Field();
        field.setTag(TableFieldTag.USER_CREATE.getType());
        field.setTapType(null);
        field.setSimpleTypeName(" String ");
        field.setFieldName("a");
        field.setOriginalFieldName(null);

        FieldTypeUtil.genericFieldIfNeed(field);
        assertEquals(FieldTypeUtil.FILED_TYPE.get("String"), field.getTapType());
        assertEquals("a", field.getOriginalFieldName());
    }

    @Test
    void testGenericFieldIfNeedTypeEmpty() {
        Field field = new Field();
        field.setTag(TableFieldTag.USER_CREATE.getType());
        field.setTapType(null);
        field.setSimpleTypeName("  ");
        field.setFieldName("a");

        BizException ex = assertThrows(BizException.class, () -> FieldTypeUtil.genericFieldIfNeed(field));
        assertEquals("schema.field.type.empty", ex.getErrorCode());
    }

    @Test
    void testGenericFieldIfNeedTypeNoSupport() {
        Field field = new Field();
        field.setTag(TableFieldTag.USER_CREATE.getType());
        field.setTapType(null);
        field.setSimpleTypeName("Unknown");
        field.setFieldName("a");

        BizException ex = assertThrows(BizException.class, () -> FieldTypeUtil.genericFieldIfNeed(field));
        assertEquals("schema.field.type.noSupport", ex.getErrorCode());
        assertNotNull(ex.getArgs());
        assertTrue(ex.getArgs().length >= 2);
    }

    @Test
    void testGenericFieldIfNeedNameEmpty() {
        Field field = new Field();
        field.setTag(TableFieldTag.USER_CREATE.getType());
        field.setTapType(FieldTypeUtil.FILED_TYPE.get("Any"));
        field.setFieldName(" ");

        BizException ex = assertThrows(BizException.class, () -> FieldTypeUtil.genericFieldIfNeed(field));
        assertEquals("schema.field.name.empty", ex.getErrorCode());
    }

    @Test
    void testGenericFieldIfNeedKeepTapTypeAndSetOriginal() {
        Field field = new Field();
        field.setTag(TableFieldTag.NORMAL.getType());
        field.setTapType(FieldTypeUtil.FILED_TYPE.get("Any"));
        field.setFieldName("b");
        field.setOriginalFieldName(null);

        FieldTypeUtil.genericFieldIfNeed(field);
        assertEquals("b", field.getOriginalFieldName());
    }

    @Test
    void testValidCustomWhereIfNeedNullAndNested() {
        FieldTypeUtil.validCustomWhereIfNeed(null);

        ModulesDto modulesDto = new ModulesDto();

        Field root = new Field();
        root.setTag(TableFieldTag.USER_CREATE.getType());
        root.setTapType(null);
        root.setSimpleTypeName("Integer");
        root.setFieldName("f1");

        Field inPath = new Field();
        inPath.setTag(TableFieldTag.NORMAL.getType());
        inPath.setTapType(FieldTypeUtil.FILED_TYPE.get("Any"));
        inPath.setFieldName("f2");

        modulesDto.setFields(List.of(root));
        Path path = new Path();
        path.setFields(List.of(inPath));
        modulesDto.setPaths(List.of(path));

        FieldTypeUtil.validCustomWhereIfNeed(modulesDto);

        assertEquals(FieldTypeUtil.FILED_TYPE.get("Integer"), root.getTapType());
        assertEquals("f1", root.getOriginalFieldName());
        assertEquals("f2", inPath.getOriginalFieldName());
    }
}

