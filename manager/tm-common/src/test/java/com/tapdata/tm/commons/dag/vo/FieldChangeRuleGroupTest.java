package com.tapdata.tm.commons.dag.vo;

import com.tapdata.tm.commons.schema.Field;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FieldChangeRuleGroupTest {

    private static final String NODE_ID = "node-1";
    private static final String ANCESTORS_NAME = "schema.tab";
    private static final String FIELD_NAME = "col";

    private FieldChangeRuleGroup group;

    @BeforeEach
    void setUp() {
        group = new FieldChangeRuleGroup();
    }

    private FieldChangeRule fieldRule(String ancestorsName, String fieldName, String dataType, String tapType) {
        Map<String, String> result = new HashMap<>();
        result.put("dataType", dataType);
        result.put("tapType", tapType);
        return new FieldChangeRule("rid", FieldChangeRule.Scope.Field,
                new String[]{NODE_ID, ancestorsName, fieldName},
                FieldChangeRule.Type.DataType, "varchar(6)", null, result);
    }

    @Nested
    class GetRuleTest {
        @Test
        void matchByFieldNameAndAncestorsName() {
            FieldChangeRule rule = fieldRule(ANCESTORS_NAME, FIELD_NAME, "varchar(3)", "{}");
            group.add(NODE_ID, rule);
            FieldChangeRule got = group.getRule(NODE_ID, ANCESTORS_NAME, FIELD_NAME, "varchar(6)", null);
            assertEquals(rule, got);
        }

        @Test
        void matchWhenRuleAncestorsNameIsNull() {
            FieldChangeRule rule = fieldRule(null, FIELD_NAME, "varchar(3)", "{}");
            group.add(NODE_ID, rule);
            // even when caller passes a non-null ancestorsName, the rule still matches
            FieldChangeRule got = group.getRule(NODE_ID, "any.table", FIELD_NAME, "varchar(6)", null);
            assertEquals(rule, got);
        }

        @Test
        void noMatchWhenAncestorsNameDiffers() {
            FieldChangeRule rule = fieldRule(ANCESTORS_NAME, FIELD_NAME, "varchar(3)", "{}");
            group.add(NODE_ID, rule);
            FieldChangeRule got = group.getRule(NODE_ID, "other.table", FIELD_NAME, "varchar(6)", null);
            assertNull(got);
        }

        @Test
        void noMatchWhenFieldNameDiffers() {
            FieldChangeRule rule = fieldRule(ANCESTORS_NAME, FIELD_NAME, "varchar(3)", "{}");
            group.add(NODE_ID, rule);
            FieldChangeRule got = group.getRule(NODE_ID, ANCESTORS_NAME, "other_col", "varchar(6)", null);
            assertNull(got);
        }

        @Test
        void noMatchWhenNodeIdAbsent() {
            FieldChangeRule rule = fieldRule(ANCESTORS_NAME, FIELD_NAME, "varchar(3)", "{}");
            group.add(NODE_ID, rule);
            FieldChangeRule got = group.getRule("other-node", ANCESTORS_NAME, FIELD_NAME, "varchar(6)", null);
            assertNull(got);
        }
    }

    @Nested
    class ProcessTest {
        @Test
        void appliesDataTypeChangeWhenMatched() {
            FieldChangeRule rule = fieldRule(ANCESTORS_NAME, FIELD_NAME, "varchar(3)", "tap-type-3");
            group.add(NODE_ID, rule);

            Field field = new Field();
            field.setFieldName(FIELD_NAME);
            field.setDataType("varchar(6)");

            DefaultExpressionMatchingMap map = mock(DefaultExpressionMatchingMap.class);
            when(map.get(anyString())).thenReturn(null);

            group.process(NODE_ID, ANCESTORS_NAME, field, map);

            assertEquals("varchar(3)", field.getDataType());
            assertEquals("tap-type-3", field.getTapType());
            assertEquals("rid", field.getChangeRuleId());
        }

        @Test
        void appliesWhenRuleAncestorsNameIsNullRegardlessOfTable() {
            FieldChangeRule rule = fieldRule(null, FIELD_NAME, "varchar(3)", "tap-type-3");
            group.add(NODE_ID, rule);

            Field field = new Field();
            field.setFieldName(FIELD_NAME);
            field.setDataType("varchar(6)");

            DefaultExpressionMatchingMap map = mock(DefaultExpressionMatchingMap.class);
            when(map.get(anyString())).thenReturn(null);

            group.process(NODE_ID, "renamed.table", field, map);

            assertEquals("varchar(3)", field.getDataType());
        }

        @Test
        void doesNothingWhenAncestorsNameDiffers() {
            FieldChangeRule rule = fieldRule(ANCESTORS_NAME, FIELD_NAME, "varchar(3)", "tap-type-3");
            group.add(NODE_ID, rule);

            Field field = new Field();
            field.setFieldName(FIELD_NAME);
            field.setDataType("varchar(6)");

            DefaultExpressionMatchingMap map = mock(DefaultExpressionMatchingMap.class);
            when(map.get(anyString())).thenReturn(null);

            group.process(NODE_ID, "other.table", field, map);

            assertEquals("varchar(6)", field.getDataType());
            assertNull(field.getTapType());
            assertNull(field.getChangeRuleId());
            assertNotNull(field);
        }
    }
}
