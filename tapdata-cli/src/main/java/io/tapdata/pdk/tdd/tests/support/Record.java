package io.tapdata.pdk.tdd.tests.support;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class Record extends HashMap {
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
     * */
    public static Record[] modifyRecordWithTapTable(TapTable table, Record[] records,int modifyNums, boolean needModifyPrimaryKey) {
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        AtomicInteger num = new AtomicInteger(modifyNums <= 0 ? nameFieldMap.size() : modifyNums);
        for (int i = 0; i < records.length; i++) {
            Record record = records[i];
            builderKey(record, nameFieldMap, field -> needModifyPrimaryKey ? (!field.getPrimaryKey() && num.decrementAndGet() > 0) : num.decrementAndGet() > 0 );
            records[i] = record;
        }
        return records;
    }

    private static Record builderKey(Record record, LinkedHashMap<String, TapField> nameFieldMap, Checker... checker) {
        //Record item = (Objects.nonNull(checker[0]) && checker[0].check())? ;
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
                    ;
                    break;
                    case JAVA_Binary: {
                        record.builder(keyName, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                    }
                    ;
                    break;
                    case JAVA_Integer: {
                        Date date = new Date();
                        record.builder(keyName, date.getSeconds());
                    }
                    ;
                    break;
                    case JAVA_Date: {
                        record.builder(keyName, DateUtil.dateToStr());
                    }
                    ;
                    break;
                    case JAVA_Map: {
                        Map<String, Object> map = new HashMap<>();
                        map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
                        map.put(UUID.randomUUID().toString(), UUID.randomUUID().toString());
                        record.builder(keyName, map);
                    }
                    ;
                    break;
                    case JAVA_String: {
                        record.builder(keyName, UUID.randomUUID().toString());
                    }
                    ;
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
                        record.builder(keyName, Float.parseFloat("" + (Math.random() * 10 + 50)));
                    }
                    break;
                    case JAVA_Long: {
                        record.builder(keyName, System.nanoTime());
                    }
                    break;
                    case JAVA_Double: {
                        record.builder(keyName, Double.parseDouble("" + (Math.random() * 10 + 50)));
                    }
                    break;
                    case "Date_Time": {
//                        try {
//                            Thread.sleep(50);
//                            record.builder(keyName,DateUtil.dateTimeToStr());
//                        } catch (InterruptedException e) {
                        record.builder(keyName, DateUtil.dateTimeToStr(new Date((long) (1293861599 + new Random().nextDouble() * 60 * 60 * 24 * 365))));
//                        }
                    }
                    break;
                    case "STRING(100)": {
                        record.builder(keyName, UUID.randomUUID().toString());
                    }
                    break;
                    case "INT64": {
                        record.builder(keyName, System.currentTimeMillis());
                    }
                    break;
                    case "Time": {
                        record.builder(keyName, DateUtil.timeToStr());
                    }
                    break;
                    case "Year": {
                        record.builder(keyName, DateUtil.yearToStr());
                    }
                    break;
                    default:
                        record.builder(keyName, null);
                        ;
                }
            }
        });
        return record;
    }

    interface Checker {
        //!field.getPrimaryKey()
        public boolean check(TapField field);
        public default boolean check(){
            return true;
        }
    }
}
