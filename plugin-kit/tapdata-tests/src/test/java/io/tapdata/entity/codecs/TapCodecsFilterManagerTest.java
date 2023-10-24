package io.tapdata.entity.codecs;

import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.codec.detector.impl.NewFieldDetector;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.codec.filter.ToTapValueCheck;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Array;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_BigDecimal;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Binary;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Boolean;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Date;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Double;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Float;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Integer;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Long;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Map;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_String;
import static org.junit.jupiter.api.Assertions.*;

public class TapCodecsFilterManagerTest {

    public List<Map<String,Object>> testRecord(final Map<String,TapField> fieldMap, final int recordCount){
        List<Map<String,Object>> records = new ArrayList<>();
        Random random = new Random();
        for (int index = 0; index < recordCount; index++) {
            Map<String, Object> record = new HashMap<>();
            fieldMap.forEach((key, field) -> {
                String type = field.getDataType();
                String keyName = field.getName();
                switch (type) {
                    case JAVA_Array: {
                        List<String> list = new ArrayList<>();
                        list.add(UUID.randomUUID().toString());
                        list.add(UUID.randomUUID().toString());
                        record.put(keyName, list);
                    }
                    break;
                    case JAVA_Binary: {
                        record.put(keyName, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                    }
                    break;
                    case JAVA_Integer: {
                        record.put(keyName, random.nextInt(Integer.MAX_VALUE));
                    }
                    break;
                    case JAVA_Map: {
                        Map<String, Object> map = new HashMap<>();
                        map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
                        map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
                        record.put(keyName, map);
                    }
                    break;
                    case JAVA_BigDecimal: {
                        BigDecimal bd = BigDecimal.valueOf(Math.random() * 10 + 50);
                        record.put(keyName, bd.setScale(4, RoundingMode.HALF_UP));
                    }
                    break;
                    case JAVA_Boolean: {
                        record.put(keyName, Math.random() * 10 + 50 > 55);
                    }
                    break;
                    case JAVA_Float: {
                        BigDecimal bd = BigDecimal.valueOf(Math.random() * 10 + 50);
                        record.put(keyName, bd.setScale(4, RoundingMode.HALF_UP).floatValue());//Float.parseFloat("" + (Math.random() * 10 + 50)));
                    }
                    break;
                    case JAVA_Long:
                    case "INT64": {
                        record.put(keyName, random.nextLong());
                    }
                    break;
                    case JAVA_Double: {
                        BigDecimal bd = BigDecimal.valueOf(Math.random() * 10 + 50);
                        record.put(keyName, bd.setScale(4, RoundingMode.HALF_UP).doubleValue());
                    }
                    break;
                    case JAVA_String:
                    case "STRING(100)":
                        record.put(keyName, UUID.randomUUID().toString());
                        break;
                    case JAVA_Date: {
                        record.put(keyName, new Date(random.nextInt(Integer.MAX_VALUE)));
                    }
                    break;
                    case "Date_Time": {
                        TapDateTime tapType = (TapDateTime)field.getTapType();
                        TimeZone.setDefault( null == tapType.getWithTimeZone() || !tapType.getWithTimeZone() ? null : TimeZone.getTimeZone("GMT+0"));
                        Date date = new Date((long) (1293861599 + new Random().nextDouble() * 60 * 60 * 24 * 365));
                        record.put(keyName, date);
                    }
                    break;
                    case "Time":
                        TapTime tapType = (TapTime)field.getTapType();
                        TimeZone.setDefault( null == tapType.getWithTimeZone() || !tapType.getWithTimeZone() ? null : TimeZone.getTimeZone("GMT+0"));
                        Date date = new Date(random.nextInt());
                        record.put(keyName, date);
                        break;
                    case "Year":
                        record.put(keyName, new Date(random.nextInt()));
                        break;
                    default:
                        record.put(keyName, null);
                }
            });
            records.add(record);
        }
        return records;
    }
    public Map<String,TapField> testTable(final int fieldCount){
        Map<String, TapField> map = new HashMap<>();

        for (int index = 0; index < fieldCount; index++) {
            String keyName = "FIELD_" + index;
            if (index % 19 == 0) {
                map.put(keyName, field(keyName, JAVA_Long).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))));
            } else if (index % 19 == 1) {
                map.put(keyName, field(keyName, JAVA_Array).tapType(tapArray()));
            } else if (index % 19 == 3) {
                map.put(keyName, field(keyName, JAVA_Binary).tapType(tapBinary().bytes(100L)));
            } else if (index % 19 == 4) {
                map.put(keyName, field(keyName, JAVA_Boolean).tapType(tapBoolean()));
            } else if (index % 19 == 5) {
                map.put(keyName, field(keyName, JAVA_Date).tapType(tapDate()));
            } else if (index % 19 == 6) {
                map.put(keyName, field(keyName, "Date_Time").tapType(tapDateTime().fraction(3)));
            } else if (index % 19 == 7) {
                map.put(keyName, field(keyName, "Date_Time").tapType(tapDateTime().fraction(3).withTimeZone(true)));
            } else if (index % 19 == 8) {
                map.put(keyName, field(keyName, JAVA_Map).tapType(tapMap()));
            } else if (index % 19 == 9) {
                map.put(keyName, field(keyName, JAVA_Long).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))));
            } else if (index % 19 == 10) {
                map.put(keyName, field(keyName, JAVA_Integer).tapType(tapNumber().maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))));
            } else if (index % 19 == 11) {
                map.put(keyName, field(keyName, JAVA_BigDecimal).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).precision(200).scale(4).fixed(true)));
            } else if (index % 19 == 12) {
                map.put(keyName, field(keyName, JAVA_Float).tapType(tapNumber().maxValue(BigDecimal.valueOf(Float.MAX_VALUE)).minValue(BigDecimal.valueOf(-Float.MAX_VALUE)).precision(200).scale(4).fixed(false)));
            } else if (index % 19 == 13) {
                map.put(keyName, field(keyName, JAVA_Double).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).precision(200).scale(4).fixed(false)));
            } else if (index % 19 == 14) {
                map.put(keyName, field(keyName, JAVA_String).tapType(tapString().bytes(50L)));
            } else if (index % 19 == 15) {
                map.put(keyName, field(keyName, JAVA_String).tapType(tapString().bytes(50L)));
            } else if (index % 19 == 16) {
                map.put(keyName, field(keyName, "INT64").tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))));
            } else if (index % 19 == 17) {
                map.put(keyName, field(keyName, "Time").tapType(tapTime().withTimeZone(false)));
            } else if (index % 19 == 18) {
                map.put(keyName, field(keyName, "Time").tapType(tapTime().withTimeZone(true)));
            } else {
                map.put(keyName, field(keyName, "Year").tapType(tapYear()));
            }
        }
        return map;
    }


    @Test
    public void test(){
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        Map<String, TapField> sourceNameFieldMap = testTable(5);
        List<Map<String,Object>> maps = testRecord(sourceNameFieldMap, 10000);
        long start = System.currentTimeMillis();
        for (Map<String, Object> map : maps) {
            codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);
            Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
            codecsFilterManager.transformFromTapValueMap(map, nameFieldMap);
            //codecsFilterManager.transformToTapValueMap(map, nameFieldMap);
        }
        long end = System.currentTimeMillis();
        //In 5 fields and table has 10000 records, qps only: 1.4705882 w/s
        //In 5 fields and table has 10000 records, qps only: 1.6447369 w/s
        assertFalse((10000.00F / (end-start))*1000> 30000.00F ,"In 5 fields and table has 10000 records, qps only: " + (1000.00F / (end-start)) + " w/s");

        sourceNameFieldMap = testTable(50);
        maps = testRecord(sourceNameFieldMap, 10000);
        start = System.currentTimeMillis();
        for (Map<String, Object> map : maps) {
            codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);
            Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
            codecsFilterManager.transformFromTapValueMap(map, nameFieldMap);
            //codecsFilterManager.transformToTapValueMap(map, nameFieldMap);
        }
        end = System.currentTimeMillis();
        //In 50 fields and table has 10000 records, qps only: 3.4129694 w/s
        //In 50 fields and table has 10000 records, qps only: 3.267974 w/s ==>
        assertTrue((10000.00F / (end-start))*1000 > 30000.00F ,"In 50 fields and table has 10000 records, qps only: " + (1000.00F / (end-start)) + " w/s");

        sourceNameFieldMap = testTable(100);
        maps = testRecord(sourceNameFieldMap, 10000);
        start = System.currentTimeMillis();
        for (Map<String, Object> map : maps) {
            codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);
            Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
            codecsFilterManager.transformFromTapValueMap(map, nameFieldMap);
            //codecsFilterManager.transformToTapValueMap(map, nameFieldMap);
        }
        end = System.currentTimeMillis();
        //In 100 fields and table has 10000 records, qps only: 1.7241379 w/s
        //In 100 fields and table has 10000 records, qps only: 1.8867924 w/s
        assertFalse((10000.00F / (end-start))*1000 > 30000.00F ,"In 100 fields and table has 10000 records, qps only: " + (1000.00F / (end-start)) + " w/s");

        sourceNameFieldMap = testTable(500);
        maps = testRecord(sourceNameFieldMap, 10000);
        start = System.currentTimeMillis();
        for (Map<String, Object> map : maps) {
            codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);
            Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
            codecsFilterManager.transformFromTapValueMap(map, nameFieldMap);
            //codecsFilterManager.transformToTapValueMap(map, nameFieldMap);
        }
        end = System.currentTimeMillis();
        //In 500 fields and table has 10000 records, qps only: 0.6896552 w/s
        //In 500 fields and table has 10000 records, qps only: 0.47619048 w/s
        assertTrue((10000.00F / (end-start))*1000 > 30000.00F ,"In 500 fields and table has 10000 records, qps only: " + (1000.00F / (end-start)) + " w/s");
    }

    @Test
    public void testValueConversion() {
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        //Map<String, Object> map = map(
        //        entry("string", "string"),
        //        entry("int", 5555),
        //        entry("long", 34324L),
        //        entry("float", 324.3f),
        //        entry("double", 343.324d)
        //        );
        Map<String, TapField> sourceNameFieldMap = testTable(500);
        List<Map<String,Object>> maps = testRecord(sourceNameFieldMap, 100000);
        Map<String,Object> map = maps.get(0);

        //Map<String, TapField> sourceNameFieldMap = new HashMap<>();
        //sourceNameFieldMap.put("string", field("string", "varchar").tapType(tapString().bytes(50L)));
        //sourceNameFieldMap.put("int", field("int", "number(32)").tapType(tapNumber().bit(32).maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))));
        //sourceNameFieldMap.put("long", field("long", "number(64)").tapType(tapNumber().bit(64).minValue(BigDecimal.valueOf(Long.MIN_VALUE)).maxValue(BigDecimal.valueOf(Long.MAX_VALUE))));
        //sourceNameFieldMap.put("float", field("float", "number(32)").tapType(tapNumber().bit(32).scale(3).minValue(BigDecimal.valueOf(Float.MIN_VALUE)).maxValue(BigDecimal.valueOf(Float.MAX_VALUE))));
        //sourceNameFieldMap.put("double", field("double", "double").tapType(tapNumber().scale(3).bit(64).minValue(BigDecimal.valueOf(Double.MIN_VALUE)).maxValue(BigDecimal.valueOf(Double.MAX_VALUE))));

        //read from source, transform to TapValue out from source connector.
        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);

        //before enter a processor, transform to value from TapValue.
        Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
        codecsFilterManager.transformFromTapValueMap(map, nameFieldMap);

        //Processor add a new field.
        map.put("dateTime", new Date());

        //transform to TapValue out from processor. nameFieldMap will add new field.
        codecsFilterManager.transformToTapValueMap(map, nameFieldMap);
        assertNotNull(map.get("dateTime"));
        TapValue tapValue = (TapValue) map.get("dateTime");
        assertEquals(tapValue.getTapType().getClass().getSimpleName(), "TapDateTime");

        nameFieldMap = new ConcurrentHashMap<>();
        codecsFilterManager.transformFromTapValueMap(map, nameFieldMap);
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

        Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
        codecsFilterManager.transformFromTapValueMap(map, nameFieldMap);

        assertEquals("v", ((Map)((List)((Map)((List)map.get("tapArrayMap")).get(1)).get("n")).get(2)).get("k"));
    }
    @Test
    public void testNewFieldWithoutDetector() {
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

        //Add fields outside of fields in Table.
        map.put("dateTime", new Date());
        map.put("double", 11.3d);
        map.put("bigDecimal", BigDecimal.ONE);
        map.put("string", "hello");
        map.put("map", map(entry("1", 1)));
        map.put("array", list("1"));
        map.put("boolean", true);
        map.put("bytes", new byte[]{'1'});
        map.put("arrayMap", list(map(entry("1", 1))));

        //read from source, transform to TapValue out from source connector.
        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);

        //before enter a processor, transform to value from TapValue.
        Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
        codecsFilterManager.transformFromTapValueMap(map ,nameFieldMap);

        assertEquals("hello", map.get("string"));
        assertEquals(true, map.get("boolean"));
        assertEquals(11.3d, map.get("double"));

        assertEquals(13, map.size());
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
        Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
        codecsFilterManager.transformFromTapValueMap(map ,nameFieldMap);

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
        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap, newFieldDetector);

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
                entry("double", 343.324d),
                entry("int", 5)
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
        Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
        codecsFilterManager.transformFromTapValueMap(map, nameFieldMap);
        assertEquals(map.get("float"), 324.3f);
        assertEquals(map.get("floatMax"), Float.MAX_VALUE);
        assertEquals(map.get("floatNegativeMax"), -Float.MAX_VALUE);
        assertEquals(map.get("floatMin"), Float.MIN_VALUE);
        assertEquals(map.get("floatOverflow"), Float.valueOf(String.valueOf(Float.MAX_VALUE + 1)));
        assertEquals(map.get("double"), 343.324d);
        assertEquals(map.get("int"), 5);
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
        TapCodecsRegistry codecsRegistry = TapCodecsRegistry.create();
        codecsRegistry.registerFromTapValue(TapMapValue.class, tapValue -> {
            return toJson(tapValue.getValue());
        });
        codecsRegistry.registerFromTapValue(TapArrayValue.class, tapValue -> {
            return toJson(tapValue.getValue());
        });
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(codecsRegistry);
        long time = 1660792574472L;
        Map<String, Object> map = map(
                entry("map", map(entry("map", map(entry("a", 1), entry("a", list(map(entry("aaa", "bbb")))))))),
                entry("list", list(map(entry("11", "aa"), entry("aaa", "a")))),
                entry("list1", list("1", "12"))
        );

        Map<String, TapField> sourceNameFieldMap = new HashMap<>();
        sourceNameFieldMap.put("map", field("map", "map").tapType(tapMap()));
        sourceNameFieldMap.put("list", field("list", "list").tapType(tapArray()));

        //read from source, transform to TapValue out from source connector.
        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);

        //before enter a processor, transform to value from TapValue.
        codecsFilterManager.transformFromTapValueMap(map);
        assertEquals("[{\"11\":\"aa\",\"aaa\":\"a\"}]", map.get("list"));
        assertEquals("[\"1\",\"12\"]", map.get("list1"));
        assertEquals("{\"map\":\"{\\\"a\\\":\\\"[{\\\\\\\"aaa\\\\\\\":\\\\\\\"bbb\\\\\\\"}]\\\"}\"}", map.get("map"));
    }

    @Test
    public void testOnlyNecessaryTapValue() {
        TapCodecsRegistry codecsRegistry = TapCodecsRegistry.create();
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(codecsRegistry);
        codecsRegistry.registerToTapValue(ObjectId.class, (ToTapValueCodec<TapValue<?, ?>>) (value, tapType) -> {
            ObjectId objValue = (ObjectId) value;
            return new TapStringValue(objValue.toHexString());
        });
        codecsRegistry.registerFromTapValue(TapStringValue.class, tapValue -> {
            Object originValue = tapValue.getOriginValue();
            if (originValue instanceof ObjectId) {
                return originValue;
            }
            //If not ObjectId, use default TapValue Codec to convert.
            return codecsRegistry.getValueFromDefaultTapValueCodec(tapValue);
        });
        long time = 1660792574472L;
        ObjectId objectId = new ObjectId();
        Map<String, Object> map = map(
                entry("datetime", time),
                entry("string", "hello"),
                entry("map", map(entry("map", map(entry("a", 1), entry("a", list(map(entry("aaa", "bbb")))))))),
                entry("list", list(map(entry("11", "aa"), entry("aaa", "a"), entry("objectId", new ObjectId())))),
                entry("list1", list("1", "12")),
                entry("datetime1", new DateTime(new Date(time))),
                entry("objectId", objectId),
                entry("float", 213.23f),
                entry("double", 3423.234234324d),
                entry("int", 123),
                entry("nano", Instant.ofEpochSecond(time / 1000, 123123213))
        );

        Map<String, TapField> sourceNameFieldMap = new HashMap<>();
        sourceNameFieldMap.put("float", field("float", "float").tapType(tapNumber().scale(3).bit(32).minValue(BigDecimal.valueOf(-Float.MAX_VALUE)).maxValue(BigDecimal.valueOf(Float.MAX_VALUE))));
        sourceNameFieldMap.put("double", field("double", "double").tapType(tapNumber().scale(17).bit(64).maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE))));
        sourceNameFieldMap.put("int", field("int", "int").tapType(tapNumber().minValue(BigDecimal.valueOf(-Integer.MAX_VALUE)).maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).bit(32)));
        sourceNameFieldMap.put("datetime", field("datetime", "datetime").tapType(tapDateTime().fraction(3)));
        sourceNameFieldMap.put("datetime1", field("datetime1", "datetime").tapType(tapDateTime().fraction(3)));
        sourceNameFieldMap.put("nano", field("nano", "nano").tapType(tapDateTime().fraction(9)));
        sourceNameFieldMap.put("map", field("map", "map").tapType(tapMap()));
        sourceNameFieldMap.put("string", field("string", "string").tapType(tapString().bytes(1024L)));
        sourceNameFieldMap.put("list", field("list", "list").tapType(tapArray()));
        sourceNameFieldMap.put("list1", field("list1", "list").tapType(tapArray()));
        sourceNameFieldMap.put("objectId", field("objectId", "string").tapType(tapString().bytes(24L)));

        //read from source, transform to TapValue out from source connector.
        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);

        TapCodecsFilterManager newCodecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        Map<String, TapValue<?, ?>> valueMap = newCodecsFilterManager.transformFromTapValueMap(map, sourceNameFieldMap);

        newCodecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap, valueMap);

        //before enter a processor, transform to value from TapValue.
        codecsFilterManager.transformFromTapValueMap(map);
        assertEquals(472000000, ((DateTime)map.get("datetime")).getNano());
        assertEquals(123123213, ((DateTime)map.get("nano")).getNano());

        assertEquals("hello", map.get("string"));
        assertEquals("aa", ((Map)((List)map.get("list")).get(0)).get("11"));
        assertEquals("1", ((List)map.get("list1")).get(0));
        assertEquals(objectId, map.get("objectId"));
        assertEquals(123, map.get("int"));
        assertEquals(213.23f, map.get("float"));
        assertEquals(3423.234234324d, map.get("double"));


    }


    @Test
    public void testMySQL2ClickHouseLossScale() {
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());

        String sourceTypeExpression = "{" +
                "    \"double[($precision,$scale)][unsigned]\": {\n" +
                "\t  \"to\": \"TapNumber\",\n" +
                "\t  \"precision\": [\n" +
                "\t\t1,\n" +
                "\t\t255\n" +
                "\t  ],\n" +
                "\t  \"scale\": [\n" +
                "\t\t0,\n" +
                "\t\t30\n" +
                "\t  ],\n" +
                "\t  \"value\": [\n" +
                "\t\t\"-1.7976931348623157E+308\",\n" +
                "\t\t\"1.7976931348623157E+308\"\n" +
                "\t  ],\n" +
                "\t  \"unsigned\": \"unsigned\",\n" +
                "\t  \"fixed\": false\n" +
                "\t}\n"
                + "}";

        String targetTypeExpression = "{\n" +
                "\"Float32[($precision,$scale)]\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"name\": \"float\",\n" +
                "      \"precision\": [\n" +
                "        1,\n" +
                "        30\n" +
                "      ],\n" +
                "      \"scale\": [\n" +
                "        0,\n" +
                "        30\n" +
                "      ],\n" +
                "      \"unsigned\": \"unsigned\",\n" +
                "      \"fixed\": false\n" +
                "    },\n" +
                "    \"Float64\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"precision\": [\n" +
                "        0,\n" +
                "        30\n" +
                "      ],\n" +
                "      \"scale\": [\n" +
                "        0,\n" +
                "        30\n" +
                "      ],\n" +
                "      \"fixed\": false\n" +
                "    },\n" +
                "    \"Decimal[($precision,$scale)]\": {\n" +
                "      \"to\": \"TapNumber\",\n" +
                "      \"precision\": [\n" +
                "        1,\n" +
                "        76\n" +
                "      ],\n" +
                "      \"scale\": [\n" +
                "        0,\n" +
                "        38\n" +
                "      ],\n" +
                "      \"defaultPrecision\": 10,\n" +
                "      \"defaultScale\": 0\n" +
                "    }" +
                "}";

        TapTable sourceTable = table("test");
        sourceTable
                .add(field("double(15,4)", "double(15,4)"))

        ;

        TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
        TargetTypesGenerator targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);
        TapCodecsRegistry codecRegistry = TapCodecsRegistry.create();
        TapCodecsFilterManager targetCodecFilterManager = TapCodecsFilterManager.create(codecRegistry);

        tableFieldTypesGenerator.autoFill(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(sourceTypeExpression));
        TapResult<LinkedHashMap<String, TapField>> tapResult = targetTypesGenerator.convert(sourceTable.getNameFieldMap(), DefaultExpressionMatchingMap.map(targetTypeExpression), targetCodecFilterManager);

        LinkedHashMap<String, TapField> nameFieldMap = tapResult.getData();

        Map<String, Object> map = map(
                entry("double(15,4)", 12345678900.5678)
        );

        //read from source, transform to TapValue out from source connector.
        codecsFilterManager.transformToTapValueMap(map, sourceTable.getNameFieldMap());

        //before enter a processor, transform to value from TapValue.
        codecsFilterManager.transformFromTapValueMap(map);
        assertEquals(12345678900.5678, map.get("double(15,4)"));
//        assertEquals(((DateTime)map.get("nano")).getNano(), 123123213);
    }

    @Test
    public void testTapMapArrayFromAndTo() {
        TapCodecsRegistry codecsRegistry = TapCodecsRegistry.create();
//        codecsRegistry.registerFromTapValue(TapMapValue.class, tapValue -> {
//            return toJson(tapValue.getValue());
//        });
//        codecsRegistry.registerFromTapValue(TapArrayValue.class, tapValue -> {
//            return toJson(tapValue.getValue());
//        });
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(codecsRegistry);
        long time = 1660792574472L;
        Map<String, Object> map = map(
                entry("map", map(entry("map", map(entry("a", new Date()), entry("list", list(map(entry("aa", "bb")))))))),
                entry("list", list(map(entry("11", "aa"), entry("aaa", new Date())))),
                entry("list1", list("1", "12"))
        );

        Map<String, TapField> sourceNameFieldMap = new HashMap<>();
        sourceNameFieldMap.put("map", field("map", "map").tapType(tapMap()));
        sourceNameFieldMap.put("list", field("list", "list").tapType(tapArray()));

        //read from source, transform to TapValue out from source connector.
        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);

        //before enter a processor, transform to value from TapValue.
        codecsFilterManager.transformFromTapValueMap(map);
        assertTrue(map.get("list") instanceof List);
        assertTrue(map.get("list1") instanceof List);
        assertTrue(map.get("map") instanceof Map);
        assertTrue(((Map<?, ?>)((Map<?, ?>) map.get("map")).get("map")).get("a") instanceof DateTime);
    }

    @Test
    public void testToTapValueCheck() {
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
        Map<String, Object> map = map(
                entry("string1", "string"),
                entry("int1", 5555),
                entry("long1", 34324L),
                entry("double1", 343.324d)
        );
        map.put("dateTime", new Date());
        map.put("double", 11.3d);
        map.put("bigDecimal", BigDecimal.ONE);
        map.put("string", "hello");
        map.put("map", map(entry("1", 1)));
        map.put("array", list("1"));
        map.put("boolean", true);
        map.put("bytes", new byte[]{'1'});
        map.put("arrayMap", list(map(entry("1", 1))));

        Map<String, TapField> sourceNameFieldMap = new HashMap<>();
        sourceNameFieldMap.put("string1", field("string", "varchar").tapType(tapString().bytes(50L)));
        sourceNameFieldMap.put("int1", field("int", "number(32)").tapType(tapNumber().bit(32).maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))));
        sourceNameFieldMap.put("long1", field("long", "number(64)").tapType(tapNumber().bit(64).minValue(BigDecimal.valueOf(Long.MIN_VALUE)).maxValue(BigDecimal.valueOf(Long.MAX_VALUE))));
        sourceNameFieldMap.put("double1", field("double", "double").tapType(tapNumber().scale(3).bit(64).minValue(BigDecimal.valueOf(Double.MIN_VALUE)).maxValue(BigDecimal.valueOf(Double.MAX_VALUE))));


        //read from source, transform to TapValue out from source connector.
        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap, (ToTapValueCheck) (key, value) -> {
            if(key.equals("dateTime")) {
                assertEquals(value.getClass(), DateTime.class);
            }
            return true;
        });

        assertEquals(map.get("arrayMap").getClass(), ArrayList.class);
        assertEquals(map.get("dateTime").getClass(), Date.class);
    }

    @Test
    public void testToTapValueCheckStopFilter() {
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


        AtomicInteger counter = new AtomicInteger(0);
        //read from source, transform to TapValue out from source connector.
        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap, (ToTapValueCheck) (key, value) -> {
            counter.incrementAndGet();
            if(key.equals("int1") && (int)value == 5555) {
                return false;
            }
            assertFalse(key.equals("long1") || key.equals("double1"));
            return true;
        });

        assertEquals(2, counter.get());
    }

    @Test
    public void testSupportWrongModel() {
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
        sourceNameFieldMap.put("tapArrayMap", field("string", "varchar").tapType(tapString().bytes(50L)));
        sourceNameFieldMap.put("tapMapStringString", field("int", "number(32)").tapType(tapNumber().bit(32).maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))));
        sourceNameFieldMap.put("tapMapStringDouble", field("long", "number(64)").tapType(tapNumber().bit(64).minValue(BigDecimal.valueOf(Long.MIN_VALUE)).maxValue(BigDecimal.valueOf(Long.MAX_VALUE))));
        sourceNameFieldMap.put("tapTime", field("double", "double").tapType(tapNumber().scale(3).bit(64).minValue(BigDecimal.valueOf(Double.MIN_VALUE)).maxValue(BigDecimal.valueOf(Double.MAX_VALUE))));

        codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);

        assertEquals(map.get("tapArrayMap").getClass(), TapArrayValue.class);
        assertEquals(map.get("tapMapStringString").getClass(), TapMapValue.class);
        assertEquals(map.get("tapMapStringDouble").getClass(), TapMapValue.class);
        assertEquals(map.get("tapTime").getClass(), TapDateTimeValue.class);
    }
}
