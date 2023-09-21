package io.tapdata.pdk.tdd.tests.support;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.DateTime;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;
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
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class Record extends HashMap<String, Object> {
    public static TapTable testTable(String testTableId){
        return table(testTableId)
                .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("Type_ARRAY", JAVA_Array).tapType(tapArray()))
                .add(field("Type_BINARY", JAVA_Binary).tapType(tapBinary().bytes(100L)))
                .add(field("Type_BOOLEAN", JAVA_Boolean).tapType(tapBoolean()))
                .add(field("Type_DATE", JAVA_Date).tapType(tapDate()))
                .add(field("Type_DATETIME", "Date_Time").tapType(tapDateTime().fraction(3)))
                .add(field("Type_DATETIME_WITH_TIME_ZONE", "Date_Time").tapType(tapDateTime().fraction(3).withTimeZone(true)))
                .add(field("Type_MAP", JAVA_Map).tapType(tapMap()))
                .add(field("Type_NUMBER_Long", JAVA_Long).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("Type_NUMBER_INTEGER", JAVA_Integer).tapType(tapNumber().maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))))
                .add(field("Type_NUMBER_BigDecimal", JAVA_BigDecimal).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).precision(200).scale(4).fixed(true)))
                .add(field("Type_NUMBER_Float", JAVA_Float).tapType(tapNumber().maxValue(BigDecimal.valueOf(Float.MAX_VALUE)).minValue(BigDecimal.valueOf(-Float.MAX_VALUE)).precision(200).scale(4).fixed(false)))
                .add(field("Type_NUMBER_Double", JAVA_Double).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).precision(200).scale(4).fixed(false)))
                .add(field("Type_STRING_1", JAVA_String).tapType(tapString().bytes(50L)))
                .add(field("Type_STRING_2", JAVA_String).tapType(tapString().bytes(50L)))
                .add(field("Type_INT64", "INT64").tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("Type_TIME", "Time").tapType(tapTime().withTimeZone(false)))
                .add(field("Type_TIME_WITH_TIME_ZONE", "Time").tapType(tapTime().withTimeZone(true)))
                .add(field("Type_YEAR", "Year").tapType(tapYear()));
    }

    public static Record create() {
        return new Record();
    }

    private Record() {
        super();
    }

    public Record builder(String key, Object value) {
        this.put(key, value);
        return this;
    }

    public Record reset() {
        return new Record();
    }

    @Deprecated()
    public static Record[] testStart(int needCount) {
        if (needCount < 1) return new Record[0];
        Record[] records = new Record[needCount];
        for (int i = 0; i < needCount; i++) {
            records[i] = Record.create()
                    .builder("id", System.nanoTime())
                    .builder("name", "Test-" + i)
                    .builder("text", "Test-" + System.currentTimeMillis());
        }
        return records;
    }

    public static Record[] testRecordWithTapTable(TapTable table, int needCount) {
        Record[] records = new Record[needCount];
        if (needCount < 1 || null == table) return records;
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        for (int i = 0; i < needCount; i++) {
            Record record = new Record();
            builderKey(record, nameFieldMap, field -> true);
            records[i] = record;
        }
        return records;
    }

    /**
     * 修改记录值
     * table ： 表结构
     * records ： 待修改的记录
     * modifyNums： 需要修改字段的数量，小于等于0时表示修改全部，否则修改指定部分数，除非大于表字段数
     * needModifyPrimaryKey ： 是否修改主键
     */
    public static Record[] modifyRecordWithTapTable(TapTable table, Record[] records, int modifyNums, boolean needModifyPrimaryKey) {
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        AtomicInteger num = new AtomicInteger((modifyNums <= 0 ? nameFieldMap.size() : modifyNums) + 1);
        for (int i = 0; i < records.length; i++) {
            Record record = records[i];
            builderKey(record, nameFieldMap, field -> needModifyPrimaryKey ? (!field.getPrimaryKey() && num.decrementAndGet() > 0) : (!field.getPrimaryKey() && num.decrementAndGet() > 0));
            //records[i] = record;
        }
        return records;
    }

    public static Record[] modifyAsNewRecordWithTapTable(TapTable table, Record[] records, int modifyNums, boolean needModifyPrimaryKey) {
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        AtomicInteger num = new AtomicInteger((modifyNums <= 0 ? nameFieldMap.size() : modifyNums) + 1);
        Record[] newRecords = new Record[records.length];
        for (int i = 0; i < records.length; i++) {
            Record record = new Record();
            Collection<String> strings = table.primaryKeys(true);
            if (Objects.nonNull(strings)) {
                for (String key : strings) {
                    record.builder(key, records[i].get(key));
                }
            }
            builderKey(record, nameFieldMap, field -> needModifyPrimaryKey ? (!field.getPrimaryKey() && num.decrementAndGet() > 0) : (!field.getPrimaryKey() && num.decrementAndGet() > 0));
            newRecords[i] = record;
        }
        return newRecords;
    }

    private static Record builderKey(Record record, LinkedHashMap<String, TapField> nameFieldMap, Checker... checker) {
        //Record item = (Objects.nonNull(checker[0]) && checker[0].check())? ;
        Random random = new Random();
        nameFieldMap.forEach((key, field) -> {
            if (Objects.nonNull(checker[0]) && checker[0].check(field)) {
                String type = field.getDataType();
                String keyName = field.getName();
                switch (type) {
                    case JAVA_Array: {
                        List<String> list = new ArrayList<>();
                        list.add(UUID.randomUUID().toString());
                        list.add(UUID.randomUUID().toString());
                        record.builder(keyName, list);
                    }
                    break;
                    case JAVA_Binary: {
                        record.builder(keyName, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                    }
                    break;
                    case JAVA_Integer: {
                        record.builder(keyName, random.nextInt(Integer.MAX_VALUE));
                    }
                    break;
                    case JAVA_Map: {
                        Map<String, Object> map = new HashMap<>();
                        map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
                        map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
                        record.builder(keyName, map);
                    }
                    break;
                    case JAVA_BigDecimal: {
                        BigDecimal bd = BigDecimal.valueOf(Math.random() * 10 + 50);
                        record.builder(keyName, bd.setScale(4, RoundingMode.HALF_UP));
                    }
                    break;
                    case JAVA_Boolean: {
                        record.builder(keyName, Math.random() * 10 + 50 > 55);
                    }
                    break;
                    case JAVA_Float: {
                        BigDecimal bd = BigDecimal.valueOf(Math.random() * 10 + 50);
                        record.builder(keyName, bd.setScale(4, RoundingMode.HALF_UP).floatValue());//Float.parseFloat("" + (Math.random() * 10 + 50)));
                    }
                    break;
                    case JAVA_Long:
                    case "INT64": {
                        record.builder(keyName, random.nextLong());
                    }
                    break;
                    case JAVA_Double: {
                        BigDecimal bd = BigDecimal.valueOf(Math.random() * 10 + 50);
                        record.builder(keyName, bd.setScale(4, RoundingMode.HALF_UP).doubleValue());
                    }
                    break;
                    case JAVA_String:
                    case "STRING(100)":
                        record.builder(keyName, UUID.randomUUID().toString());
                        break;
                    case JAVA_Date: {
                        record.builder(keyName, new Date(random.nextInt(Integer.MAX_VALUE)));
                    }
                    break;
                    case "Date_Time": {
                        TapDateTime tapType = (TapDateTime)field.getTapType();
                        TimeZone.setDefault( null == tapType.getWithTimeZone() || !tapType.getWithTimeZone() ? null : TimeZone.getTimeZone("GMT+0"));
                        Date date = new Date((long) (1293861599 + new Random().nextDouble() * 60 * 60 * 24 * 365));
                        record.builder(keyName, date);
                    }
                    break;
                    case "Time":
                        TapTime tapType = (TapTime)field.getTapType();
                        TimeZone.setDefault( null == tapType.getWithTimeZone() || !tapType.getWithTimeZone() ? null : TimeZone.getTimeZone("GMT+0"));
                        Date date = new Date(random.nextInt());
                        record.builder(keyName, date);
                        break;
                    case "Year":
                        record.builder(keyName, new Date(random.nextInt()));
                        break;
                    default:
                        record.builder(keyName, null);
                }
            }
        });
        return record;
    }

    interface Checker {
        //!field.getPrimaryKey()
        public boolean check(TapField field);

        public default boolean check() {
            return true;
        }
    }
}
