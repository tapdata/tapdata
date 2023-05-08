package io.tapdata.pdk.tdd.tests.v4;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.list;
import static io.tapdata.entity.simplify.TapSimplify.tapArray;
import static io.tapdata.entity.simplify.TapSimplify.tapBinary;
import static io.tapdata.entity.simplify.TapSimplify.tapBoolean;
import static io.tapdata.entity.simplify.TapSimplify.tapDate;
import static io.tapdata.entity.simplify.TapSimplify.tapDateTime;
import static io.tapdata.entity.simplify.TapSimplify.tapMap;
import static io.tapdata.entity.simplify.TapSimplify.tapNumber;
import static io.tapdata.entity.simplify.TapSimplify.tapString;
import static io.tapdata.entity.simplify.TapSimplify.tapTime;
import static io.tapdata.entity.simplify.TapSimplify.tapYear;
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

/**
 * @author GavinXiao
 * @description CodecsFilterMannagerTest create by Gavin
 * @create 2023/5/8 16:00
 **/
@DisplayName("codecsFilterManager")
@TapGo(tag = "V4", sort = 20010, debug = true)
public class CodecsFilterManagerTest extends PDKTestBaseV2 {
    {
        if (PDKTestBaseV2.testRunning) {
            System.out.println(langUtil.formatLang("codecsFilterManager.wait"));
        }
    }
    public static List<SupportFunction> testFunctions() {
        return list(supportAny(
                langUtil.formatLang(anyOneFunFormat, "QueryByAdvanceFilterFunction,QueryByFilterFunction"),
                QueryByAdvanceFilterFunction.class, QueryByFilterFunction.class),
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction"))
        );
    }

    /**
     * 用例1，混合字段的编解码性能对比5字段、50字段、100字段、500字段
     */
    @DisplayName("codecsFilterManager.batch")
    @TapTestCase(sort = 1)
    @Test
    public void batch() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("codecsFilterManager.batch.wait"));
        super.execTestConnection((node, testCase) -> {
            TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());
            Map<String, TapField> sourceNameFieldMap = testTable(5);
            List<Map<String,Object>> maps = testRecord(sourceNameFieldMap, 10000);
            long start = System.currentTimeMillis();
            for (Map<String, Object> map : maps) {
                codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);
                Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
                codecsFilterManager.transformFromTapValueMap(map, nameFieldMap);
            }
            long end = System.currentTimeMillis();
            TapAssert.succeed(testCase, langUtil.formatLang("codecsFilterManager.batch.v", 5, 10000, 1000.00F / (end-start)));

            sourceNameFieldMap = testTable(50);
            maps = testRecord(sourceNameFieldMap, 10000);
            start = System.currentTimeMillis();
            for (Map<String, Object> map : maps) {
                codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);
                Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
                codecsFilterManager.transformFromTapValueMap(map, nameFieldMap);
            }
            end = System.currentTimeMillis();
            TapAssert.succeed(testCase, langUtil.formatLang("codecsFilterManager.batch.v", 50, 10000, 1000.00F / (end-start)));

            sourceNameFieldMap = testTable(100);
            maps = testRecord(sourceNameFieldMap, 10000);
            start = System.currentTimeMillis();
            for (Map<String, Object> map : maps) {
                codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);
                Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
                codecsFilterManager.transformFromTapValueMap(map, nameFieldMap);
            }
            end = System.currentTimeMillis();
            TapAssert.succeed(testCase, langUtil.formatLang("codecsFilterManager.batch.v", 100, 10000, 1000.00F / (end-start)));

            sourceNameFieldMap = testTable(500);
            maps = testRecord(sourceNameFieldMap, 10000);
            start = System.currentTimeMillis();
            for (Map<String, Object> map : maps) {
                codecsFilterManager.transformToTapValueMap(map, sourceNameFieldMap);
                Map<String, TapField> nameFieldMap = new ConcurrentHashMap<>();
                codecsFilterManager.transformFromTapValueMap(map, nameFieldMap);
            }
            end = System.currentTimeMillis();
            TapAssert.succeed(testCase, langUtil.formatLang("codecsFilterManager.batch.v", 500, 10000, 1000.00F / (end-start)));
        });
    }

    public List<Map<String,Object>> testRecord(final Map<String, TapField> fieldMap, final int recordCount){
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

    /**
     * 用例2，混合字段的编解码性能对比，不做值转换直接写入
     */
    @DisplayName("codecsFilterManager.read")
    @TapTestCase(sort = 2)
    @Test
    public void read() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("codecsFilterManager.read.wait"));
        //super.execTest((node, testCase) -> {

        //});
    }
    /**
     * 用例2，混合字段的编解码性能对比，做值转换后写入
     */
    @DisplayName("codecsFilterManager.read2")
    @TapTestCase(sort = 3)
    @Test
    public void read2() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("codecsFilterManager.read2.wait"));
        //super.execTest((node, testCase) -> {

        //});
    }
}