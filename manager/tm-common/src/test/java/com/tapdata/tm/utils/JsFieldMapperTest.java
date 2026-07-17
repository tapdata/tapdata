package com.tapdata.tm.utils;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsFieldMapperTest {

    @Nested
    class EmptyResultScenarios {
        @Test
        void returnsEmptyMapForNullBlankOrMissingSimpleReturn() {
            assertTrue(JsFieldMapper.parseMapping(null).isEmpty());
            assertTrue(JsFieldMapper.parseMapping("").isEmpty());
            assertTrue(JsFieldMapper.parseMapping("   \n\t ").isEmpty());
            assertTrue(JsFieldMapper.parseMapping("var ret = { fieldA: record.field_a };").isEmpty());
        }

        @Test
        void returnsEmptyMapWhenReturningRecordDirectly() {
            String script = """
                    return record;
                    """;

            assertTrue(JsFieldMapper.parseMapping(script).isEmpty());
        }

        @Test
        void returnsEmptyMapForUnsupportedReturnExpression() {
            String script = """
                    return { fieldA: record.field_a };
                    """;

            assertTrue(JsFieldMapper.parseMapping(script).isEmpty());
        }
    }

    @Nested
    class ObjectLiteralScenarios {
        @Test
        void parsesSimpleObjectLiteralWithQuotedAndUnquotedKeys() {
            String script = """
                    const ret = {
                        departmentCode: record.dept_cde,
                        "paymentMethodCode": record.pymt_mthd_cde,
                        'statusCode': record.status_code
                    };
                    return ret;
                    """;

            assertEquals(Map.of(
                    "departmentCode", "dept_cde",
                    "paymentMethodCode", "pymt_mthd_cde",
                    "statusCode", "status_code"
            ), JsFieldMapper.parseMapping(script));
        }

        @Test
        void parsesDirectObjectAssignmentWithoutDeclaration() {
            String script = """
                    ret = {
                        fieldA: record.field_a
                    };
                    return ret;
                    """;

            assertEquals(Map.of("fieldA", "field_a"), JsFieldMapper.parseMapping(script));
        }

        @Test
        void usesLastSimpleReturnVariable() {
            String script = """
                    var first = { fieldA: record.first_a };
                    var second = { fieldA: record.second_a };
                    return first;
                    return second;
                    """;

            assertEquals(Map.of("fieldA", "second_a"), JsFieldMapper.parseMapping(script));
        }

        @Test
        void parsesReturnInsideFunctionBody() {
            String script = """
                    function process(record) {
                        var ret = { fieldA: record.field_a };
                        return ret;
                    }
                    """;

            assertEquals(Map.of("fieldA", "field_a"), JsFieldMapper.parseMapping(script));
        }

        @Test
        void ignoresComplexObjectValuesThatAreNotDirectRecordFieldReads() {
            String script = """
                    var ret = {
                        direct: record.direct_field,
                        computed: record.left_field + record.right_field,
                        wrapped: String(record.wrapped_field)
                    };
                    return ret;
                    """;

            assertEquals(Map.of("direct", "direct_field"), JsFieldMapper.parseMapping(script));
        }

        @Test
        void doesNotMatchReturnedVariableNameInsideAnotherIdentifier() {
            String script = """
                    var myret = { fieldA: record.wrong_field };
                    var ret = { fieldA: record.right_field };
                    return ret;
                    """;

            assertEquals(Map.of("fieldA", "right_field"), JsFieldMapper.parseMapping(script));
        }
    }

    @Nested
    class AssignmentScenarios {
        @Test
        void parsesReturnedObjectAssignmentsWithoutObjectLiteralMappings() {
            String script = """
                    let ret = {};
                    ret.fieldA = record.field_a;
                    ret.fieldB = record.field_b
                    return ret;
                    """;

            assertEquals(Map.of(
                    "fieldA", "field_a",
                    "fieldB", "field_b"
            ), JsFieldMapper.parseMapping(script));
        }

        @Test
        void laterAssignmentOverridesObjectLiteralMapping() {
            String script = """
                    var ret = { fieldA: record.old_field_a };
                    ret.fieldA = record.new_field_a;
                    return ret;
                    """;

            assertEquals(Map.of("fieldA", "new_field_a"), JsFieldMapper.parseMapping(script));
        }

        @Test
        void parsesAliasAssignmentWhenSourceMappingIsKnown() {
            String script = """
                    var ret = {
                        sourceField: record.source_field
                    };
                    ret.aliasField = ret.sourceField;
                    return ret;
                    """;

            assertEquals(Map.of(
                    "sourceField", "source_field",
                    "aliasField", "source_field"
            ), JsFieldMapper.parseMapping(script));
        }

        @Test
        void ignoresAliasAssignmentWhenSourceMappingIsUnknown() {
            String script = """
                    var ret = {};
                    ret.aliasField = ret.sourceField;
                    return ret;
                    """;

            assertTrue(JsFieldMapper.parseMapping(script).isEmpty());
        }

        @Test
        void deleteStatementRemovesReturnedObjectMapping() {
            String script = """
                    var ret = {
                        fieldA: record.field_a,
                        fieldB: record.field_b
                    };
                    delete ret.fieldA;
                    return ret;
                    """;

            assertEquals(Map.of("fieldB", "field_b"), JsFieldMapper.parseMapping(script));
        }

        @Test
        void ignoresUnsupportedBracketAccessAndComplexAssignments() {
            String script = """
                    var ret = {};
                    ret.bracketTarget = record['field_a'];
                    ret['bracketSource'] = record.field_b;
                    ret.computed = record.left_field + record.right_field;
                    return ret;
                    """;

            assertTrue(JsFieldMapper.parseMapping(script).isEmpty());
        }
    }

    @Nested
    class CommentScenarios {
        @Test
        void lineCommentDoesNotChangeReturnedVariable() {
            String script = """
                    var ret = { fieldA: record.safe_field };
                    return ret;
                    // return fake;
                    var fake = { fieldA: record.comment_field };
                    """;

            assertEquals(Map.of("fieldA", "safe_field"), JsFieldMapper.parseMapping(script));
        }

        @Test
        void blockCommentDoesNotChangeReturnedVariable() {
            String script = """
                    var ret = { fieldA: record.safe_field };
                    /*
                    return fake;
                    var fake = { fieldA: record.comment_field };
                    */
                    return ret;
                    """;

            assertEquals(Map.of("fieldA", "safe_field"), JsFieldMapper.parseMapping(script));
        }

        @Test
        void commentedAssignmentsAndDeletesDoNotChangeMapping() {
            String script = """
                    var ret = {
                        fieldA: record.safe_a
                    };
                    // ret.fieldB = record.comment_b;
                    // delete ret.fieldA;
                    /*
                    ret.fieldC = record.comment_c;
                    delete ret.fieldA;
                    */
                    return ret;
                    """;

            assertEquals(Map.of("fieldA", "safe_a"), JsFieldMapper.parseMapping(script));
        }

        @Test
        void commentMarkersInsideStringLiteralsAreNotTreatedAsComments() {
            String script = """
                    var text = "keep // this is not a comment";
                    var blockText = 'keep /* this is not a comment */ too';
                    var ret = { fieldA: record.safe_a };
                    return ret;
                    """;

            assertEquals(Map.of("fieldA", "safe_a"), JsFieldMapper.parseMapping(script));
        }
    }
}
