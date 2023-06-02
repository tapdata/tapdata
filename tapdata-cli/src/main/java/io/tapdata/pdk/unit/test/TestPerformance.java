package io.tapdata.pdk.unit.test;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DelayCalculation;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.*;

public class TestPerformance {
    public static void main(String[] args) throws Exception {
        TapCodecsFilterManager codecsFilterManager = TapCodecsFilterManager.create(TapCodecsRegistry.create());

        Map<String, Object> dataMap = new HashMap<>();
        Map<String, TapField> fieldMap = new HashMap<>();

        String fieldName = "F0";
        FieldType fieldType;
        FieldType[] fieldTypes = FieldType.values();
        fieldMap.put(fieldName, newTapField(fieldName, FieldType.Int32, 0));
        for (int i = 1; i < 300; i++) {
            fieldName = "F" + i;
            fieldType = fieldTypes[(int) (Math.random() * fieldTypes.length)];

            fieldMap.put(fieldName, newTapField(fieldName, fieldType, 0));
            dataMap.put(fieldName, newValue(fieldType));
        }



        int size = 10000000;
//        try (UseTimes useTimes = new UseTimes()){
//            for (int i = 0; i < size; i++) {
//                codecsFilterManager.transformToTapValueMap(dataMap, fieldMap);
//                codecsFilterManager.transformFromTapValueMap(dataMap);
//            }
//            System.out.println(useTimes.qps(size));
//        }

        DelayCalculation delayCalculation = new DelayCalculation(1000);
        delayCalculation.log(System.currentTimeMillis());
        for (int i = 0; i < size; i++) {
            codecsFilterManager.transformToTapValueMap(dataMap, fieldMap);
            codecsFilterManager.transformFromTapValueMap(dataMap);
            delayCalculation.log(System.currentTimeMillis());
        }
        delayCalculation.log(System.currentTimeMillis());
        System.out.println(delayCalculation);
    }

    enum FieldType {
        Str, Boolean, Int32, Float32, Datetime,
        ;
    }

    public static Object newValue(FieldType fieldType) {
        switch (fieldType) {
            case Str:
                return "123";
            case Boolean:
                return Math.random() * 2 > 1;
            case Int32:
                return (int) (Math.random() * 999999999);
            case Float32:
                return (float) Math.random();
            case Datetime:
                return new Date();
            default:
                throw new RuntimeException("not found field type: " + fieldType);
        }
    }

    public static TapField newTapField(String name, FieldType fieldType, int pkPos) {
        TapField field = field(name, fieldType.name());
        switch (fieldType) {
            case Str:
                field = field.tapType(tapString().bytes(50L));
                break;
            case Boolean:
                field = field.tapType(tapBoolean());
                break;
            case Int32:
                field = field.tapType(tapNumber().bit(32).maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE)));
                break;
            case Float32:
                field = field.tapType(tapNumber().bit(32).scale(3).minValue(BigDecimal.valueOf(Float.MIN_VALUE)).maxValue(BigDecimal.valueOf(Float.MAX_VALUE)));
                break;
            case Datetime:
                field = field.tapType(tapDateTime());
                break;
            default:
                throw new RuntimeException("not found field type: " + fieldType);
        }

        field.primaryKeyPos(pkPos);

        return field;
    }
}
