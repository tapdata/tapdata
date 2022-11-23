package io.tapdata.pdk.tdd.tests.support;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

public class Record extends HashMap {
    public static Record create(){
        return new Record();
    }
    private Record(){
        super();
    }
    public Record builder(String key,Object value){
        this.put(key,value);
        return this;
    }

    public Record reset(){
        return new Record();
    }

    public static Record[] testStart(int needCount){
        if (needCount<1) return new Record[0];
        Record[] records = new Record[needCount];
        for (int i = 0; i < needCount; i++) {
            records[i] = Record.create()
                    .builder("id", System.nanoTime())
                    .builder("name", "Test-"+i)
                    .builder("text", "Test-"+System.currentTimeMillis());
        }
        return records;
    }

    public static Record[] testRecordWithTapTable(TapTable table,int needCount){
        Record[] records = new Record[needCount];
        if (needCount<1 || null==table) return records;
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        for (int i = 0; i < needCount; i++) {
            Record record = new Record();
            nameFieldMap.forEach((key,field)->{
                String type = field.getDataType();
                String keyName = field.getName();
                switch (type) {
                    case JAVA_Array:{
                        List<String> list = new ArrayList<>();
                        list.add(UUID.randomUUID().toString());
                        list.add(UUID.randomUUID().toString());
                        record.builder(keyName,list);
                    };break;
                    case JAVA_Binary:{
                        record.builder(keyName,UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                    };break;
                    case JAVA_Integer:{
                        Date date = new Date();
                        record.builder(keyName,date.getSeconds());
                    };break;
                    case JAVA_Date:{
                        record.builder(keyName,DateUtil.dateToStr());
                    };break;
                    case JAVA_Map:{
                        Map<String,Object> map = new HashMap<>();
                        map.put(UUID.randomUUID().toString(),UUID.randomUUID().toString());
                        map.put(UUID.randomUUID().toString(),UUID.randomUUID().toString());
                        record.builder(keyName,map);
                    };break;
                    case JAVA_String:{
                        record.builder(keyName,UUID.randomUUID().toString());
                    };break;
                    case JAVA_BigDecimal:{
                        BigDecimal bd = BigDecimal.valueOf(Math.random() * 10 + 50);
                        record.builder(keyName,bd.setScale(4, RoundingMode.HALF_UP));
                    }break;
                    case JAVA_Boolean:{
                        record.builder(keyName,Math.random()*10+50>55);
                    }break;
                    case JAVA_Float:{
                        record.builder(keyName, Float.parseFloat(""+(Math.random()*10+50)));
                    }break;
                    case JAVA_Long:{
                        record.builder(keyName,System.nanoTime());
                    }break;
                    case JAVA_Double:{
                        record.builder(keyName,Double.parseDouble(""+(Math.random()*10+50)));
                    }break;
                    case "Date_Time":{
                        record.builder(keyName,DateUtil.dateTimeToStr());
                    }break;
                    case "STRING(100)":{
                        record.builder(keyName,UUID.randomUUID().toString());
                    }break;
                    case "INT64":{
                        record.builder(keyName,System.currentTimeMillis());
                    }break;
                    case "Time":{
                        record.builder(keyName, DateUtil.timeToStr());
                    }break;
                    case "Year":{
                        record.builder(keyName,DateUtil.yearToStr());
                    }break;
                    default:record.builder(keyName,null);;
                }
            });
            records[i] = record;
        }
        return records;
    }

//    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.sss";
//    public static String dateTime(){
//        return date(DATE_TIME_FORMAT);
//    }
//    public static final String TIME_FORMAT = "HH:mm:ss";
//    public static String time(){
//        return date(TIME_FORMAT);
//    }
//    public static final String DATE_FORMAT = "yyyy-MM-dd";
//    public static String date(){
//        return date(DATE_FORMAT);
//    }
//    public static final String YEAR_FORMAT = "yyyy";
//    public static String year(){
//        return date(YEAR_FORMAT);
//    }
//
//    private static String date(String format){
//        if ( null == format || (
//                !DATE_FORMAT.equals(format) &&
//                !DATE_TIME_FORMAT.equals(format) &&
//                !TIME_FORMAT.equals(format) &&
//                !YEAR_FORMAT.equals(format) ) ) format = DATE_TIME_FORMAT;
//       return  (new SimpleDateFormat(format)).format(new Date());
//    }

}
