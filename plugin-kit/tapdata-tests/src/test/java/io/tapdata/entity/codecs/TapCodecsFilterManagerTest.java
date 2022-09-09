package io.tapdata.entity.codecs;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.detector.impl.NewFieldDetector;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TapCodecsFilterManagerTest {
    @Test
    public void testValueConversion() {
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        Map<String, Object> map = map(
                entry("string", "string"),
                entry("int", 5555),
                entry("long", 34324L),
                entry("float", 324.3f),
                entry("double", 343.324d)
                );

        Map<String, TapField> sourceNameFieldMap = new HashMap<>();
        sourceNameFieldMap.put("string", field("string", "varchar").tapType(tapString().bytes(50L)));
        sourceNameFieldMap.put("int", field("int", "number(32)").tapType(tapNumber().bit(32).maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))));
        sourceNameFieldMap.put("long", field("long", "number(64)").tapType(tapNumber().bit(64).minValue(BigDecimal.valueOf(Long.MIN_VALUE)).maxValue(BigDecimal.valueOf(Long.MAX_VALUE))));
        sourceNameFieldMap.put("float", field("float", "number(32)").tapType(tapNumber().bit(32).scale(3).minValue(BigDecimal.valueOf(Float.MIN_VALUE)).maxValue(BigDecimal.valueOf(Float.MAX_VALUE))));
        sourceNameFieldMap.put("double", field("double", "double").tapType(tapNumber().scale(3).bit(64).minValue(BigDecimal.valueOf(Double.MIN_VALUE)).maxValue(BigDecimal.valueOf(Double.MAX_VALUE))));

        //read from source, transform to TapValue out from source connector.
        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);

        //before enter a processor, transform to value from TapValue.
        Map<String, TapField> nameFieldMap = codecsFilterManager.transformFromTapValueMap(map);

        //Processor add a new field.
        map.put("dateTime", new Date());

        //transform to TapValue out from processor. nameFieldMap will add new field.
        codecsFilterManager.transformToTapValueMap(map, nameFieldMap);
        assertNotNull(map.get("dateTime"));
        TapValue tapValue = (TapValue) map.get("dateTime");
        assertEquals(tapValue.getTapType().getClass().getSimpleName(), "TapDateTime");

        nameFieldMap = codecsFilterManager.transformFromTapValueMap(map);
        assertNotNull(nameFieldMap.get("dateTime"));
        TapField dateTimeField = nameFieldMap.get("dateTime");
        assertEquals(dateTimeField.getTapType().getClass().getSimpleName(), "TapDateTime");
    }

    @Test
    public void testAllLayerMap() {
        TapCodecsRegistry tapCodecsRegistry = TapCodecsRegistry.create();
        tapCodecsRegistry.registerToTapValue(TDDUser.class, (ToTapValueCodec<TapValue<?, ?>>) (value, tapType) -> new TapStringValue(InstanceFactory.instance(JsonParser.class).toJson(value)));
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(tapCodecsRegistry);
        Map<String, Object> map = map(
                entry("id", "id_1"),
                entry("tapString", "123"),
                entry("tddUser", new TDDUser("uid_" + 1, "name_" + 1, "desp_" + 1, (int) 1, TDDUser.GENDER_FEMALE)),
                entry("tapString10", "1234567890"),
                entry("tapString10Fixed", "1"),
                entry("tapInt", 123123),
                entry("tapBoolean", true),
                entry("tapDate", new Date()),

                entry("tapArrayString", list("1", "2", "3")),
                entry("tapArrayDouble", list(1.1, 2.2, 3.3)),
                entry("tapArrayTDDUser", list(new TDDUser("a", "n", "d", 1, TDDUser.GENDER_MALE), new TDDUser("b", "a", "b", 2, TDDUser.GENDER_FEMALE))),
                entry("tapRawTDDUser", new TDDUser("a1", "n1", "d1", 11, TDDUser.GENDER_MALE)),
                entry("tapNumber", 123.0),
//                        entry("tapNumber(8)", 1111),
                entry("tapNumber52", 343.22),
                entry("tapBinary", new byte[]{123, 21, 3, 2}),
                entry("tapTime", new Date()),
                entry("tapMapStringString", map(entry("a", "a"), entry("b", "b"))),
                entry("tapMapStringDouble", map(entry("a", 1.0), entry("b", 2.0))),
                entry("tapMapStringList", map(entry("a", list("a", "b", map(entry("1", "1")))), entry("b", list("1", "2", list("m", "n"))))),
                entry("tapArrayMap", list("1", map(entry("n", list("1", "2", map(entry("k", "v"))))), "3")),
                entry("tapMapStringTDDUser", map(entry("a", new TDDUser("a1", "n1", "d1", 11, TDDUser.GENDER_MALE)))),
                entry("tapDateTime", new Date()),
                entry("tapDateTimeTimeZone", new Date())
        );

        Map<String, TapField> sourceNameFieldMap = new HashMap<>();
//        sourceNameFieldMap.put("string", field("string", "varchar").tapType(tapString().bytes(50L)));
//        sourceNameFieldMap.put("int", field("int", "number(32)").tapType(tapNumber().bit(32).maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))));
//        sourceNameFieldMap.put("long", field("long", "number(64)").tapType(tapNumber().bit(64).minValue(BigDecimal.valueOf(Long.MIN_VALUE)).maxValue(BigDecimal.valueOf(Long.MAX_VALUE))));
//        sourceNameFieldMap.put("double", field("double", "double").tapType(tapNumber().scale(3).bit(64).minValue(BigDecimal.valueOf(Double.MIN_VALUE)).maxValue(BigDecimal.valueOf(Double.MAX_VALUE))));

        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);

        Map<String, TapField> nameFieldMap = codecsFilterManager.transformFromTapValueMap(map);

        assertEquals("v", ((Map)((List)((Map)((List)map.get("tapArrayMap")).get(1)).get("n")).get(2)).get("k"));
    }

    @Test
    public void testNewFieldDetector() {
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        Map<String, Object> map = map(
                entry("string1", "string"),
                entry("int1", 5555),
                entry("long1", 34324L),
                entry("double1", 343.324d)
        );

        Map<String, TapField> sourceNameFieldMap = new HashMap<>();
        sourceNameFieldMap.put("string1", field("string", "varchar").tapType(tapString().bytes(50L)));
        sourceNameFieldMap.put("int1", field("int", "number(32)").tapType(tapNumber().bit(32).maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))));
        sourceNameFieldMap.put("long1", field("long", "number(64)").tapType(tapNumber().bit(64).minValue(BigDecimal.valueOf(Long.MIN_VALUE)).maxValue(BigDecimal.valueOf(Long.MAX_VALUE))));
        sourceNameFieldMap.put("double1", field("double", "double").tapType(tapNumber().scale(3).bit(64).minValue(BigDecimal.valueOf(Double.MIN_VALUE)).maxValue(BigDecimal.valueOf(Double.MAX_VALUE))));

        //read from source, transform to TapValue out from source connector.
        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);

        //before enter a processor, transform to value from TapValue.
        Map<String, TapField> nameFieldMap = codecsFilterManager.transformFromTapValueMap(map);

        //Processor add a new field.
        map.put("dateTime", new Date());
        map.put("double", 11.3d);
        map.put("bigDecimal", BigDecimal.ONE);
        map.put("string", "hello");
        map.put("map", map(entry("1", 1)));
        map.put("array", list("1"));
        map.put("boolean", true);
        map.put("bytes", new byte[]{'1'});

        Map<String, TapField> newFieldMap = new HashMap<>();
        NewFieldDetector newFieldDetector = newField -> newFieldMap.put(newField.getName(), newField);
        //transform to TapValue out from processor. nameFieldMap will add new field.
        codecsFilterManager.transformToTapValueMap(map, nameFieldMap, newFieldDetector);

        assertEquals(8, newFieldMap.size());
        assertNotNull(newFieldMap.get("dateTime"));
        assertEquals(newFieldMap.get("dateTime").getDataType(), "TapDateTime");

        assertNotNull(newFieldMap.get("double"));
        assertEquals(newFieldMap.get("double").getDataType(), "TapNumber");

        assertNotNull(newFieldMap.get("bigDecimal"));
        assertEquals(newFieldMap.get("bigDecimal").getDataType(), "TapNumber");

        assertNotNull(newFieldMap.get("string"));
        assertEquals(newFieldMap.get("string").getDataType(), "TapString");

        assertNotNull(newFieldMap.get("map"));
        assertEquals(newFieldMap.get("map").getDataType(), "TapMap");

        assertNotNull(newFieldMap.get("array"));
        assertEquals(newFieldMap.get("array").getDataType(), "TapArray");

        assertNotNull(newFieldMap.get("boolean"));
        assertEquals(newFieldMap.get("boolean").getDataType(), "TapBoolean");

        assertNotNull(newFieldMap.get("bytes"));
        assertEquals(newFieldMap.get("bytes").getDataType(), "TapBinary");

    }

    @Test
    public void testScaleForFloatDouble() {
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        Map<String, Object> map = map(
                entry("float", 324.3f),
                entry("floatMax", Float.MAX_VALUE),
                entry("floatNegativeMax", -Float.MAX_VALUE),
                entry("floatMin", Float.MIN_VALUE),
                entry("floatOverflow", Float.MAX_VALUE + 1),
                entry("double", 343.324d)
        );

        Map<String, TapField> sourceNameFieldMap = new HashMap<>();
        sourceNameFieldMap.put("float", field("float", "number(32)").tapType(tapNumber().bit(32).scale(3).minValue(BigDecimal.valueOf(Float.MIN_VALUE)).maxValue(BigDecimal.valueOf(Float.MAX_VALUE))));
        sourceNameFieldMap.put("floatMax", field("floatMax", "number(32)").tapType(tapNumber().bit(32).scale(3).minValue(BigDecimal.valueOf(Float.MIN_VALUE)).maxValue(BigDecimal.valueOf(Float.MAX_VALUE))));
        sourceNameFieldMap.put("floatNegativeMax", field("floatNegativeMax", "number(32)").tapType(tapNumber().bit(32).scale(3).minValue(BigDecimal.valueOf(Float.MIN_VALUE)).maxValue(BigDecimal.valueOf(Float.MAX_VALUE))));
        sourceNameFieldMap.put("floatMin", field("floatMin", "number(32)").tapType(tapNumber().bit(32).scale(3).minValue(BigDecimal.valueOf(Float.MIN_VALUE)).maxValue(BigDecimal.valueOf(Float.MAX_VALUE))));
        sourceNameFieldMap.put("floatOverflow", field("floatOverflow", "double").tapType(tapNumber().scale(3).bit(64).minValue(BigDecimal.valueOf(Double.MIN_VALUE)).maxValue(BigDecimal.valueOf(Double.MAX_VALUE))));
        sourceNameFieldMap.put("double", field("double", "double").tapType(tapNumber().scale(3).fixed(true).bit(64).minValue(BigDecimal.valueOf(Double.MIN_VALUE)).maxValue(BigDecimal.valueOf(Double.MAX_VALUE))));

        //read from source, transform to TapValue out from source connector.
        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);

        //before enter a processor, transform to value from TapValue.
        Map<String, TapField> nameFieldMap = codecsFilterManager.transformFromTapValueMap(map);
        assertEquals(map.get("float"), 324.3f);
        assertEquals(map.get("floatMax"), Float.MAX_VALUE);
        assertEquals(map.get("floatNegativeMax"), -Float.MAX_VALUE);
        assertEquals(map.get("floatMin"), Float.MIN_VALUE);
        assertEquals(map.get("floatOverflow"), Double.valueOf(String.valueOf(Float.MAX_VALUE + 1)));
        assertEquals(map.get("double"), 343.324d);
    }

    @Test
    public void testFractionValue() {
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        long time = 1660792574472L;
        Map<String, Object> map = map(
                entry("datetime", time),
                entry("nano", Instant.ofEpochSecond(time / 1000, 123123213))
        );

        Map<String, TapField> sourceNameFieldMap = new HashMap<>();
        sourceNameFieldMap.put("datetime", field("datetime", "datetime").tapType(tapDateTime().fraction(3)));
        sourceNameFieldMap.put("nano", field("nano", "nano").tapType(tapDateTime().fraction(9)));

        //read from source, transform to TapValue out from source connector.
        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);

        //before enter a processor, transform to value from TapValue.
        codecsFilterManager.transformFromTapValueMap(map);
        assertEquals(((DateTime)map.get("datetime")).getNano(), 472000000);
        assertEquals(((DateTime)map.get("nano")).getNano(), 123123213);
    }

    @Test
    public void testTapMapInMap() {
        //TODO need pass this case.
//        TapCodecsRegistry codecsRegistry = TapCodecsRegistry.create();
//        codecsRegistry.registerFromTapValue(TapMapValue.class, tapValue -> toJson(tapValue.getValue()));
//        codecsRegistry.registerFromTapValue(TapArrayValue.class, tapValue -> toJson(tapValue.getValue()));
//        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(codecsRegistry);
//        long time = 1660792574472L;
//        Map<String, Object> map = map(
////                entry("map", map(entry("map", map(entry("a", 1), entry("a", list(map(entry("aaa", "bbb")))))))),
////                entry("list", list(map(entry("11", "aa"), entry("aaa", "a"))))
//                entry("list", list("1", "12"))
//        );
//
//        Map<String, TapField> sourceNameFieldMap = new HashMap<>();
//        sourceNameFieldMap.put("map", field("map", "map").tapType(tapMap()));
//        sourceNameFieldMap.put("list", field("list", "list").tapType(tapArray()));
//
//        //read from source, transform to TapValue out from source connector.
//        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);
//
//        //before enter a processor, transform to value from TapValue.
//        codecsFilterManager.transformFromTapValueMap(map);
//        assertEquals(((DateTime)map.get("datetime")).getNano(), 472000000);
//        assertEquals(((DateTime)map.get("nano")).getNano(), 123123213);
    }
}
