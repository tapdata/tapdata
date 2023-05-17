//package io.tapdata.connector.doris;
//
//import io.tapdata.entity.schema.TapField;
//import io.tapdata.entity.schema.TapTable;
//import io.tapdata.entity.schema.value.DateTime;
//
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.LinkedHashMap;
//import java.util.Map;
//
///**
// * @Author dayun
// * @Date 7/14/22
// */
//public class DorisDMLInstance {
//    private final SimpleDateFormat tapDateTimeFormat = new SimpleDateFormat();
//    private static final DorisDMLInstance DMLInstance = new DorisDMLInstance();
//
//    public static DorisDMLInstance getInstance(){
//        return DMLInstance;
//    }
//
//    private String formatTapDateTime(DateTime dateTime, String pattern) {
//        if (dateTime.getTimeZone() != null) dateTime.setTimeZone(dateTime.getTimeZone());
//        tapDateTimeFormat.applyPattern(pattern);
//        return tapDateTimeFormat.format(new Date(dateTime.getSeconds() * 1000L));
//    }
//
//    private String formatTapDateTime(Date date, String pattern) {
//        tapDateTimeFormat.applyPattern(pattern);
//        return tapDateTimeFormat.format(date);
//    }
//
//    public Object getFieldOriginValue(TapField tapField, Object tapValue) {
//        Object result = tapValue;
//        if (tapValue instanceof DateTime) {
//            result = formatTapDateTime((DateTime) tapValue, "yyyy-MM-dd HH:mm:ss");
//        } else if(tapValue instanceof Date) {
//            result = formatTapDateTime((Date) tapValue, "yyyy-MM-dd HH:mm:ss");
//        }
//        return result;
//    }
//
//    public String buildKeyAndValue(TapTable tapTable, Map<String, Object> record, String splitSymbol) {
//        StringBuilder builder = new StringBuilder();
//        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
//        for (Map.Entry<String, Object> entry : record.entrySet()) {
//            String fieldName = entry.getKey();
//            builder.append(fieldName).append("=");
//            if(!(entry.getValue() instanceof Number))
//                builder.append("'");
//
//            builder.append(getFieldOriginValue(nameFieldMap.get(fieldName), entry.getValue()));
//
//            if(!(entry.getValue() instanceof Number))
//                builder.append("'");
//
//            builder.append(splitSymbol).append(" ");
//        }
//        builder.delete(builder.length() - splitSymbol.length() - 1, builder.length());
//        return builder.toString();
//    }
//}
