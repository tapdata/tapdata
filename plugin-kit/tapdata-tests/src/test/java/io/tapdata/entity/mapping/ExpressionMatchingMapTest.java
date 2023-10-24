package io.tapdata.entity.mapping;

import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.mapping.type.*;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static org.junit.jupiter.api.Assertions.*;

class ExpressionMatchingMapTest {

    @Test
    void testGet() {
        String str = "{\n" +
                "    \"tinyint[($length)][unsigned][zerofill]\": {\"bit\": 1, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"smallint[($length)][unsigned][zerofill]\": {\"bit\": 4, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"mediumint[($length)][unsigned][zerofill]\": {\"bit\": 8, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"int[($length)][unsigned][zerofill]\": {\"bit\": 32, \"unsigned\": \"unsigned\", \"zerofill\": \"zerofill\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint($length)[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"bigint[unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"float[($length)][unsigned][zerofill]\": {\"bit\": 16, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"double[($length)][unsigned][zerofill]\": {\"bit\": 256, \"unsigned\": \"unsigned\", \"to\": \"TapNumber\"},\n" +
                "    \"decimal($precision, $scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [0, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"precisionDefault\": 10, \"scaleDefault\": 0, \"to\": \"TapNumber\"},\n" +
                "    \"date\": {\"range\": [\"1000-01-01\", \"9999-12-31\"], \"to\": \"TapDate\"},\n" +
                "    \"time\": {\"range\": [\"-838:59:59\",\"838:59:59\"], \"to\": \"TapTime\"},\n" +
                "    \"year\": {\"range\": [1901, 2155], \"to\": \"TapYear\"},\n" +
                "    \"datetime\": {\"range\": [\"1000-01-01 00:00:00\", \"9999-12-31 23:59:59\"], \"to\": \"TapDateTime\"},\n" +
                "    \"timestamp\": {\"to\": \"TapDateTime\"},\n" +
                "    \"char[($byte)]\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"varchar[($byte)]\": {\"byte\": \"64k\", \"fixed\": false, \"to\": \"TapString\"},\n" +
                "    \"tinyblob\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"tinytext\": {\"byte\": 255, \"to\": \"TapString\"},\n" +
                "    \"blob\": {\"byte\": \"64k\", \"to\": \"TapBinary\"},\n" +
                "    \"text\": {\"byte\": \"64k\", \"to\": \"TapString\"},\n" +
                "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
                "    \"mediumtext\": {\"byte\": \"16m\", \"to\": \"TapString\"},\n" +
                "    \"longblob\": {\"byte\": \"4g\", \"to\": \"TapBinary\"},\n" +
                "    \"longtext\": {\"byte\": \"4g\", \"to\": \"TapString\"},\n" +
                "    \"bit($byte)\": {\"byte\": 8, \"to\": \"TapBinary\"},\n" +
                "    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
                "    \"varbinary($byte)\": {\"byte\": 255, \"fixed\": false, \"to\": \"TapBinary\"},\n" +
                "    \"[varbinary]($byte)[ABC$hi]aaaa[DDD[AAA|BBB]]\": {\"byte\": 33333, \"fixed\": false, \"to\": \"TapBinary\"}\n" +
                "}";

//        String str1 = "{\n" +
//                "  \"[tinyint]f[unsigned$ggg][23213$ggg zero$fill]\" : {\"to\" : \"tapString\"}\n" +
//                "}";

//        ExpressionMatchingMap<TapMapping> map1 = ExpressionMatchingMap.map(str, new TypeHolder<Map<String, TapMapping>>(){});
//        map1.get("");

        DefaultExpressionMatchingMap matchingMap = DefaultExpressionMatchingMap.map(str);
//        matchingMap.setValueFilter(defaultMap -> {
//            TapMapping tapMapping = (TapMapping) defaultMap.get(TapMapping.FIELD_TYPE_MAPPING);
//            if(tapMapping == null) {
//                defaultMap.put(TapMapping.FIELD_TYPE_MAPPING, TapMapping.build(defaultMap));
//            }
//        });
        //"    \"binary($byte)\": {\"byte\": 255, \"to\": \"TapBinary\"},\n" +
        validateTapMapping(matchingMap, "Binary(4)", TapBinaryMapping.class, TapBinary.class, exprResult -> {
            Assertions.assertNotNull(exprResult, "Expression is not matched");
            Assertions.assertEquals(exprResult.getParams().get("byte"), "4");
        }, tapBinaryMapping -> {
            assertEquals(tapBinaryMapping.getBytes(), 255 );
        }, tapBinary -> {
            assertEquals(tapBinary.getBytes(), 4);
        });

        validateTapMapping(matchingMap, "binary(4)", TapBinaryMapping.class, TapBinary.class, exprResult -> {
            Assertions.assertNotNull(exprResult, "Expression is not matched");
            Assertions.assertEquals(exprResult.getParams().get("byte"), "4");
        }, tapBinaryMapping -> {
            assertEquals(tapBinaryMapping.getBytes(), 255 );
        }, tapBinary -> {
            assertEquals(tapBinary.getBytes(), 4);
        });

        validateTapMapping(matchingMap, "binary", TapBinaryMapping.class, TapBinary.class, exprResult -> {
            Assertions.assertNull(exprResult, "Expression should not be matched");
        }, null, null);

        // "    \"mediumblob\": {\"byte\": \"16m\", \"to\": \"TapBinary\"},\n" +
        validateTapMapping(matchingMap, "mediumblob", TapBinaryMapping.class, TapBinary.class, exprResult -> {
            Assertions.assertNotNull(exprResult, "Expression is not matched");
            assertNull(exprResult.getParams());
        }, tapBinaryMapping -> {
            assertEquals(tapBinaryMapping.getBytes(), 16777215 );
        }, tapBinary -> {
            assertEquals(tapBinary.getBytes(), 16777215);
        });

        validateTapMapping(matchingMap, "MEDIUMBLOB", TapBinaryMapping.class, TapBinary.class, exprResult -> {
            Assertions.assertNotNull(exprResult, "Expression is not matched");
            assertNull(exprResult.getParams());
        }, tapBinaryMapping -> {
            assertEquals(tapBinaryMapping.getBytes(), 16777215 );
        }, tapBinary -> {
            assertEquals(tapBinary.getBytes(), 16777215);
        });

        //"    \"decimal($precision, $scale)[theUnsigned][theZerofill]\": {\"precision\":[1, 65], \"scale\": [0, 30], \"unsigned\": \"theUnsigned\", \"zerofill\": \"theZerofill\", \"to\": \"TapNumber\"},\n" +

        Consumer<TapNumberMapping> decimal4020Consumer = tapNumberMapping -> {
            assertEquals(tapNumberMapping.getMinPrecision(), 1 );
            assertEquals(tapNumberMapping.getMaxPrecision(), 65 );
            assertEquals(tapNumberMapping.getMinScale(), 0 );
            assertEquals(tapNumberMapping.getMaxScale(), 30 );
        };
        Consumer<TapNumber> decimal4020TapTypeConsumer = tapNumber -> {
            assertEquals(tapNumber.getPrecision(), 40);
            assertEquals(tapNumber.getScale(), 20);
            assertNull(tapNumber.getUnsigned());
            assertNull(tapNumber.getZerofill());
        };
        Consumer<TypeExprResult<DataMap>> decimal4020ExpreConsumer = exprResult -> {
            assertNotNull(exprResult, "Expression is not matched");
            assertEquals(exprResult.getParams().get("precision"), "40");
            assertEquals(exprResult.getParams().get("scale"), "20");
        };
        validateTapMapping(matchingMap, "decimal(40, 20)", TapNumberMapping.class, TapNumber.class,
                decimal4020ExpreConsumer, decimal4020Consumer, decimal4020TapTypeConsumer);

        validateTapMapping(matchingMap, "DECIMAL(40, 20)", TapNumberMapping.class, TapNumber.class,
                decimal4020ExpreConsumer, decimal4020Consumer, decimal4020TapTypeConsumer);

        validateTapMapping(matchingMap, "DECIMAL(40, 20) theUnsigned", TapNumberMapping.class, TapNumber.class,
                decimal4020ExpreConsumer, decimal4020Consumer, tapNumber -> {
                    assertEquals(tapNumber.getUnsigned(), true);
                    assertNull(tapNumber.getZerofill());
                    assertEquals(tapNumber.getPrecision(), 40);
                    assertEquals(tapNumber.getScale(), 20);
                });

        validateTapMapping(matchingMap, "DECIMAL(40, 20) theUnsigned theZerofill", TapNumberMapping.class, TapNumber.class,
                decimal4020ExpreConsumer, decimal4020Consumer, tapNumber -> {
                    assertEquals(tapNumber.getUnsigned(), true);
                    assertEquals(tapNumber.getZerofill(), true);
                    assertEquals(tapNumber.getPrecision(), 40);
                    assertEquals(tapNumber.getScale(), 20);
                });
    }

    @Test
    void testEnumSet() {
        String str = "{\n" +
                "    \"enum($values)\": {\"byte\": \"64\", \"to\": \"TapString\"},\n" +
                "    \"set($values)\": {\"byte\": \"32\", \"to\": \"TapString\"},\n" +
                "}";

        DefaultExpressionMatchingMap matchingMap = DefaultExpressionMatchingMap.map(str);

        validateTapMapping(matchingMap, "enum('1','2','3')", TapStringMapping.class, TapString.class, exprResult -> {
            assertNotNull(exprResult, "Expression is not matched");
            assertEquals(exprResult.getParams().get("values"), "'1','2','3'");
        }, tapStringMapping -> {
            assertEquals(tapStringMapping.getBytes(), 64 );
        }, tapString -> {
            assertEquals(tapString.getBytes(), 64);
        });
        validateTapMapping(matchingMap, "set('a','b','c')", TapStringMapping.class, TapString.class, exprResult -> {
            assertNotNull(exprResult, "Expression is not matched");
            assertEquals(exprResult.getParams().get("values"), "'a','b','c'");
        }, tapStringMapping -> {
            assertEquals(tapStringMapping.getBytes(), 32 );
        }, tapString -> {
            assertEquals(tapString.getBytes(), 32);
        });
    }

    @Test
    void testNumberWithStar() {
        String str = "{\n" +
                "    \"number(*,$scale)\": {\"scale\": 64, \"to\": \"TapNumber\"},\n" +
                "}";

        DefaultExpressionMatchingMap matchingMap = DefaultExpressionMatchingMap.map(str);

        validateTapMapping(matchingMap, "number(*,3)", TapNumberMapping.class, TapNumber.class, exprResult -> {
            assertNotNull(exprResult, "Expression is not matched");
            assertEquals(exprResult.getParams().get("scale"), "3");
        }, tapNumberMapping -> {
            assertEquals(tapNumberMapping.getMaxScale(), 64 );
        }, tapNumber -> {
            assertEquals(tapNumber.getScale(), 3);
        });

    }

    @Test
    void testMappingGbase8sDateTime() {
        String str = "{\n" +

                "    \"DATETIME HOUR TO SECOND\": {\"to\": \"TapTime\"},\n" +
                "    \"DATETIME $start TO FRACTION[($fraction)]\": {\"to\": \"TapDateTime\"},\n" +
                "    \"DATETIME $start TO $end\": {\"to\": \"TapDateTime\"},\n" +
                "}";

        DefaultExpressionMatchingMap matchingMap = DefaultExpressionMatchingMap.map(str);

        validateTapMapping(matchingMap, "DATETIME YEAR TO FRACTION(5)", TapDateTimeMapping.class, TapDateTime.class, exprResult -> {
            assertNotNull(exprResult, "Expression is not matched");
            assertEquals(exprResult.getParams().get("fraction"), "5");
        }, tapNumberMapping -> {
            assertNotNull(tapNumberMapping);
        }, tapNumber -> {
            assertEquals(5, tapNumber.getFraction());
        });
        validateTapMapping(matchingMap, "DATETIME YEAR TO SECOND", TapDateTimeMapping.class, TapDateTime.class, exprResult -> {
            assertNotNull(exprResult, "Expression is not matched");
            assertEquals(exprResult.getParams().get("end"), "SECOND");
        }, tapNumberMapping -> {
            assertNotNull(tapNumberMapping);
        }, tapNumber -> {
            assertNotNull(tapNumber);
        });

        validateTapMapping(matchingMap, "DATETIME HOUR TO SECOND", TapTimeMapping.class, TapTime.class, exprResult -> {
            assertNotNull(exprResult, "Expression is not matched");
        }, tapNumberMapping -> {
            assertNotNull(tapNumberMapping);
        }, tapNumber -> {
            assertNotNull(tapNumber);
        });
    }

    private <T extends TapMapping, R extends TapType> void validateTapMapping(DefaultExpressionMatchingMap matchingMap, String dataType, Class<T> tapMappingClass, Class<R> tapTypeClass, Consumer<TypeExprResult<DataMap>> paramValidator, Consumer<T> tapMappingValidator, Consumer<R> tapTypeValidator) {
        TypeExprResult<DataMap> exprResult = matchingMap.get(dataType);
        if(paramValidator != null)
            paramValidator.accept(exprResult);

        if(exprResult != null) {
            T tapMapping = (T) exprResult.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
            assertNotNull(tapMapping, "TapMapping not found");
            assertEquals(tapMapping.getClass(), tapMappingClass);

            if(tapMappingValidator != null)
                tapMappingValidator.accept(tapMapping);
            if(tapTypeValidator != null)
                tapTypeValidator.accept((R) tapMapping.toTapType(dataType, exprResult.getParams()));
        }
    }

}
